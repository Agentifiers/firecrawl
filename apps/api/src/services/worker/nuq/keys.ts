// Deployments.ai modification: DynamoDB data model of the NuQ queue, which
// replaces upstream's Postgres tables (nuq.queue_*, nuq.group_crawl), pg_cron
// maintenance, pg_notify, and the RabbitMQ prefetch/listen channels.
//
// Job item      pk `job#<queue>#<id>`  sk `job`
//   gsi1 (queue index, eventually consistent, used only to find candidates):
//     queued   gsi1pk `q#<queue>#queued`              gsi1sk readySortKey
//     active   gsi1pk `q#<queue>#active`              gsi1sk `<lockedAt>#<id>`
//     backlog  gsi1pk `q#<queue>#backlog#<groupId>`   gsi1sk readySortKey
//   gsi2 (group index) for jobs with a group:
//     gsi2pk `g#<groupId>`; gsi2sk `done#<finishedAt>#<createdAt>#<id>` for
//     completed `single_urls` jobs (the crawl listing order), else
//     `job#<queue>#<id>`.
//   Payload chunks of `data`/`returnvalue` share the job's partition key.
//
// Group item    pk `group#<id>`        sk `group`
//   Transactional counters (pending, pendingSingle, backlogSingle,
//   completedSingle, failedSingle, enqueued, slotsUsed) are updated in the
//   same TransactWriteItems call as every job enqueue and terminal transition,
//   so `pending === 0` is a strongly consistent "no queued, active, or
//   backlogged job remains" check (upstream's NOT EXISTS in pg_cron).
//   gsi3pk `groups#active` / `groups#completed`, gsi3sk `<createdAt|expiresAt>#<id>`.
//
// Every state change is a conditional write; claims, lock renewals, and
// terminal transitions are conditioned on status and lock, so concurrent
// workers and sweepers can never apply the same transition twice.
import { sortableNumber } from "../../dynamo/client";

export type NuQJobStatus =
  | "queued"
  | "active"
  | "completed"
  | "failed"
  | "backlog";

export type NuQJob<Data = any, ReturnValue = any> = {
  id: string;
  status: NuQJobStatus;
  createdAt: Date;
  priority: number;
  data: Data;
  finishedAt?: Date;
  returnvalue?: ReturnValue;
  failedReason?: string;
  lock?: string;
  ownerId?: string;
  groupId?: string;
};

export type NuQGroupStatus = "active" | "completed" | "cancelled";

export type NuQJobGroupInstance = {
  id: string;
  status: NuQGroupStatus;
  createdAt: Date;
  ownerId: string;
  ttl: number;
  expiresAt?: Date;
};

/** Raw job item as stored; payload attributes are still encoded. */
export type JobItem = {
  pk: string;
  sk: string;
  id: string;
  queue: string;
  status: NuQJobStatus;
  priority: number;
  createdAt: number;
  readySortKey: string;
  ownerId?: string;
  groupId?: string;
  /** Time-to-live of the owning group's results, copied at enqueue. */
  groupTtlMs?: number;
  /** `data.mode`; `single_urls` jobs count toward crawl statistics. */
  mode?: string;
  /** Holds (or waits for) one of its group's concurrency slots. */
  gated?: boolean;
  /** Applied its enqueue to the group's counters (the group existed). */
  counted?: boolean;
  /** Spacing between job starts of a delayed group, copied at enqueue. */
  groupDelayMs?: number;
  data?: unknown;
  returnvalue?: unknown;
  failedReason?: string;
  lock?: string;
  lockedAt?: number;
  stalls?: number;
  finishedAt?: number;
  notBefore?: number;
  timesOutAt?: number;
  ttl?: number;
};

export type GroupItem = {
  pk: string;
  sk: string;
  id: string;
  status: NuQGroupStatus;
  ownerId: string;
  createdAt: number;
  ttlMs: number;
  expiresAt?: number;
  maxConcurrency?: number;
  delaySeconds?: number;
  pending: number;
  pendingSingle: number;
  backlogSingle: number;
  completedSingle: number;
  failedSingle: number;
  enqueued: number;
  slotsUsed: number;
  nextStartAt: number;
  ttl?: number;
};

export const JOB_SORT_KEY = "job";
export const GROUP_SORT_KEY = "group";
export const DATA_CHUNK_PREFIX = "data";
export const RESULT_CHUNK_PREFIX = "result";

export const jobPk = (queue: string, id: string) => `job#${queue}#${id}`;
export const groupPk = (id: string) => `group#${id}`;
export const jobKey = (queue: string, id: string) => ({
  pk: jobPk(queue, id),
  sk: JOB_SORT_KEY,
});
export const queuedIndexPk = (queue: string) => `q#${queue}#queued`;
export const activeIndexPk = (queue: string) => `q#${queue}#active`;
export const backlogIndexPk = (queue: string, groupId: string) =>
  `q#${queue}#backlog#${groupId}`;
export const groupIndexPk = (groupId: string) => `g#${groupId}`;
export const ACTIVE_GROUPS_PK = "groups#active";
export const COMPLETED_GROUPS_PK = "groups#completed";

/** Upstream dequeues by `priority ASC, created_at ASC`; priorities may be negative. */
export function readySortKey(priority: number, createdAt: number, id: string) {
  return `${sortableNumber(priority + 1_000_000_000, 12)}#${sortableNumber(createdAt)}#${id}`;
}

export function activeSortKey(lockedAt: number, id: string) {
  return `${sortableNumber(lockedAt)}#${id}`;
}

export function pendingGroupSortKey(queue: string, id: string) {
  return `job#${queue}#${id}`;
}

export function doneGroupSortKey(
  finishedAt: number,
  createdAt: number,
  id: string,
) {
  return `done#${sortableNumber(finishedAt)}#${sortableNumber(createdAt)}#${id}`;
}

/** Concurrency limit of a crawl group, or null when it is not gated. */
export function groupLimit(group: Pick<GroupItem, "maxConcurrency" | "delaySeconds">) {
  if (typeof group.delaySeconds === "number" && group.delaySeconds > 0) {
    return 1;
  }
  return typeof group.maxConcurrency === "number" && group.maxConcurrency > 0
    ? group.maxConcurrency
    : null;
}

export function toGroupInstance(item: GroupItem): NuQJobGroupInstance {
  return {
    id: item.id,
    status: item.status,
    createdAt: new Date(item.createdAt),
    ownerId: item.ownerId,
    ttl: item.ttlMs,
    ...(item.expiresAt !== undefined
      ? { expiresAt: new Date(item.expiresAt) }
      : {}),
  };
}

/** Retention of standalone (groupless) jobs, matching upstream's cleaners. */
export const STANDALONE_COMPLETED_RETENTION_MS = 60 * 60 * 1000;
export const STANDALONE_FAILED_RETENTION_MS = 6 * 60 * 60 * 1000;
/**
 * Grouped jobs are deleted with their group once it expires; the item TTL is
 * only a backstop in case the sweeper is down.
 */
export const GROUP_CLEANUP_GRACE_MS = 7 * 24 * 60 * 60 * 1000;
/** Default group TTL, as in upstream's `nuq.group_crawl.ttl`. */
export const DEFAULT_GROUP_TTL_MS = 24 * 60 * 60 * 1000;
