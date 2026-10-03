// Deployments.ai modification: crawl groups on DynamoDB (replaces
// nuq.group_crawl, the pg_cron group finisher, and the Redis crawl
// concurrency limiter). See keys.ts for the data model.
import {
  BatchGetCommand,
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { v7 as uuidv7 } from "uuid";
import { Logger } from "winston";
import { logger as _logger } from "../../../lib/logger";
import { normalizeOwnerId } from "../../../lib/owner-id";
import {
  dynamo,
  isConditionalCheckFailed,
  isLogicallyExpired,
  retryTransactionConflicts,
  tableName,
  transactionCancellationCodes,
  ttlAt,
} from "../../dynamo/client";
import { packPayload } from "../../dynamo/payload";
import {
  ACTIVE_GROUPS_PK,
  activeSortKey,
  backlogIndexPk,
  COMPLETED_GROUPS_PK,
  DEFAULT_GROUP_TTL_MS,
  GROUP_CLEANUP_GRACE_MS,
  GROUP_SORT_KEY,
  type GroupItem,
  groupIndexPk,
  groupLimit,
  groupPk,
  JOB_SORT_KEY,
  type JobItem,
  jobKey,
  jobPk,
  NuQJobGroupInstance,
  pendingGroupSortKey,
  queuedIndexPk,
  readySortKey,
  toGroupInstance,
} from "./keys";

/** Queue that receives one job per finished crawl group. */
export const CRAWL_FINISHED_QUEUE = "crawl_finished";
/** Queue holding crawl and batch page jobs; the only gated queue. */
export const SCRAPE_QUEUE = "scrape";
/**
 * A group with no job ever enqueued is only finished after this grace, so
 * the window between addGroup and the kickoff enqueue cannot finish it.
 */
const EMPTY_GROUP_GRACE_MS = 60 * 1000;

export async function readGroup(groupId: string): Promise<GroupItem | null> {
  const result = await dynamo().send(
    new GetCommand({
      TableName: tableName(),
      Key: { pk: groupPk(groupId), sk: GROUP_SORT_KEY },
      ConsistentRead: true,
    }),
  );
  const item = result.Item as GroupItem | undefined;
  return item && !isLogicallyExpired(item) ? item : null;
}

/** Counter deltas a job applies to its group. */
export type GroupDelta = {
  pending?: number;
  pendingSingle?: number;
  backlogSingle?: number;
  completedSingle?: number;
  failedSingle?: number;
  enqueued?: number;
  slotsUsed?: number;
};

/**
 * TransactWriteItems Update entry applying counter deltas to a group.
 * `slotLimit` adds the atomic "a concurrency slot is free" condition.
 */
export function groupUpdate(
  groupId: string,
  delta: GroupDelta,
  options: { slotLimit?: number; nextStartAt?: number } = {},
) {
  const entries = Object.entries(delta).filter(([, value]) => value);
  const names: Record<string, string> = {};
  const values: Record<string, unknown> = {};
  const adds = entries.map(([name, value], index) => {
    names[`#c${index}`] = name;
    values[`:c${index}`] = value;
    return `#c${index} :c${index}`;
  });
  let condition = "attribute_exists(pk)";
  if (options.slotLimit !== undefined) {
    condition += " AND slotsUsed < :slotLimit";
    values[":slotLimit"] = options.slotLimit;
  }
  let update = adds.length > 0 ? `ADD ${adds.join(", ")}` : "";
  if (options.nextStartAt !== undefined) {
    update += ` SET nextStartAt = :nextStartAt`;
    values[":nextStartAt"] = options.nextStartAt;
  }
  if (!update) {
    update = "ADD pending :noop";
    values[":noop"] = 0;
  }
  return {
    Update: {
      TableName: tableName(),
      Key: { pk: groupPk(groupId), sk: GROUP_SORT_KEY },
      UpdateExpression: update.trim(),
      ConditionExpression: condition,
      ...(Object.keys(names).length > 0
        ? { ExpressionAttributeNames: names }
        : {}),
      ExpressionAttributeValues: values,
    },
  };
}

/** Counter deltas of a job leaving the non-terminal set. */
export function terminalDelta(
  job: Pick<JobItem, "mode" | "gated" | "status">,
  outcome: "completed" | "failed" | "removed",
): GroupDelta {
  const single = job.mode === "single_urls" ? 1 : 0;
  return {
    pending: -1,
    pendingSingle: -single,
    backlogSingle: job.status === "backlog" ? -single : 0,
    completedSingle: outcome === "completed" ? single : 0,
    failedSingle: outcome === "failed" ? single : 0,
    // Backlogged jobs never took a slot.
    slotsUsed: job.gated && job.status !== "backlog" ? -1 : 0,
  };
}

/**
 * Deletes a backlogged job (and its payload chunks) while releasing its
 * group accounting. Returns false when another worker already moved it.
 */
async function removeBackloggedJob(job: JobItem, groupId: string) {
  try {
    await retryTransactionConflicts(() =>
      dynamo().send(
        new TransactWriteCommand({
          TransactItems: [
            {
              Delete: {
                TableName: tableName(),
                Key: { pk: job.pk, sk: JOB_SORT_KEY },
                ConditionExpression: "#status = :backlog",
                ExpressionAttributeNames: { "#status": "status" },
                ExpressionAttributeValues: { ":backlog": "backlog" },
              },
            },
            groupUpdate(groupId, terminalDelta(job, "removed")),
          ],
        }),
      ),
    );
  } catch (error) {
    if (transactionCancellationCodes(error)) return false;
    throw error;
  }
  await deletePartition(job.pk);
  return true;
}

/**
 * Moves backlogged jobs of a gated group into the queue while slots are
 * free. Each promotion atomically takes a slot and flips one job from
 * `backlog` to `queued`; a job that waited past its backlog deadline is
 * dropped instead (upstream's backlog reaper). A delayed group's promoted job
 * becomes claimable at the group's `nextStartAt`.
 */
export async function promoteBacklog(
  queue: string,
  groupId: string,
  logger: Logger = _logger,
): Promise<number> {
  let promoted = 0;
  for (let round = 0; round < 1000; round++) {
    const group = await readGroup(groupId);
    const limit = group ? groupLimit(group) : null;
    if (!group || limit === null || group.slotsUsed >= limit) return promoted;
    const candidates = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :pk",
        ExpressionAttributeValues: { ":pk": backlogIndexPk(queue, groupId) },
        Limit: 5,
      }),
    );
    const ids = (candidates.Items ?? []).map(item => item.id as string);
    if (ids.length === 0) return promoted;
    let progressed = false;
    for (const id of ids) {
      const job = await readJobItem(queue, id);
      if (!job || job.status !== "backlog") continue;
      const now = Date.now();
      try {
        if (job.timesOutAt !== undefined && job.timesOutAt < now) {
          if (await removeBackloggedJob(job, groupId)) {
            logger.info("Dropped backlogged job past its deadline", {
              module: "nuq",
              jobId: id,
              crawlId: groupId,
            });
          }
          progressed = true;
          continue;
        }
        const notBefore = Math.max(now, group.nextStartAt ?? 0);
        await retryTransactionConflicts(() =>
          dynamo().send(
            new TransactWriteCommand({
              TransactItems: [
                {
                  Update: {
                    TableName: tableName(),
                    Key: { pk: job.pk, sk: JOB_SORT_KEY },
                    UpdateExpression:
                      "SET #status = :queued, gsi1pk = :gsi1pk, gsi1sk = :gsi1sk, notBefore = :notBefore",
                    ConditionExpression: "#status = :backlog",
                    ExpressionAttributeNames: { "#status": "status" },
                    ExpressionAttributeValues: {
                      ":queued": "queued",
                      ":backlog": "backlog",
                      ":gsi1pk": queuedIndexPk(queue),
                      ":gsi1sk": job.readySortKey,
                      ":notBefore": notBefore,
                    },
                  },
                },
                groupUpdate(
                  groupId,
                  {
                    slotsUsed: 1,
                    backlogSingle: job.mode === "single_urls" ? -1 : 0,
                  },
                  { slotLimit: limit },
                ),
              ],
            }),
          ),
        );
        promoted++;
        progressed = true;
        break;
      } catch (error) {
        const codes = transactionCancellationCodes(error);
        if (!codes) throw error;
        // Another worker promoted or removed this job, or took the last slot;
        // re-read the group and try again.
        progressed = true;
        break;
      }
    }
    if (!progressed) return promoted;
  }
  return promoted;
}

/** Strongly consistent read of a job item; logically expired items are absent. */
export async function readJobItem(
  queue: string,
  id: string,
): Promise<JobItem | null> {
  const result = await dynamo().send(
    new GetCommand({
      TableName: tableName(),
      Key: jobKey(queue, id),
      ConsistentRead: true,
    }),
  );
  const item = result.Item as JobItem | undefined;
  return item && !isLogicallyExpired(item) ? item : null;
}

/**
 * Completes a group once no queued, active, or backlogged job remains and
 * enqueues its crawl-finished job in the same transaction (upstream's
 * `nuq_group_crawl_finished` cron). Safe to call from any number of workers.
 */
export async function tryCompleteGroup(
  groupId: string,
  logger: Logger = _logger,
): Promise<boolean> {
  const group = await readGroup(groupId);
  const now = Date.now();
  if (
    !group ||
    group.status !== "active" ||
    group.pending !== 0 ||
    (group.enqueued === 0 && group.createdAt > now - EMPTY_GROUP_GRACE_MS)
  ) {
    return false;
  }
  const expiresAt = now + group.ttlMs;
  const finishedJobId = uuidv7();
  const data = packPayload({}, "initial");
  const finishedJob: JobItem = {
    pk: jobPk(CRAWL_FINISHED_QUEUE, finishedJobId),
    sk: JOB_SORT_KEY,
    id: finishedJobId,
    queue: CRAWL_FINISHED_QUEUE,
    status: "queued",
    priority: 0,
    createdAt: now,
    readySortKey: readySortKey(0, now, finishedJobId),
    ownerId: group.ownerId,
    groupId,
    groupTtlMs: group.ttlMs,
    data: data.attribute,
    stalls: 0,
  };
  try {
    await dynamo().send(
      new TransactWriteCommand({
        TransactItems: [
          {
            Update: {
              TableName: tableName(),
              Key: { pk: groupPk(groupId), sk: GROUP_SORT_KEY },
              UpdateExpression:
                "SET #status = :completed, expiresAt = :expiresAt, gsi3pk = :gsi3pk, gsi3sk = :gsi3sk, #ttl = :ttl",
              ConditionExpression: "#status = :active AND pending = :zero",
              ExpressionAttributeNames: { "#status": "status", "#ttl": "ttl" },
              ExpressionAttributeValues: {
                ":completed": "completed",
                ":active": "active",
                ":zero": 0,
                ":expiresAt": expiresAt,
                ":gsi3pk": COMPLETED_GROUPS_PK,
                ":gsi3sk": activeSortKey(expiresAt, groupId),
                ":ttl": ttlAt(expiresAt + GROUP_CLEANUP_GRACE_MS),
              },
            },
          },
          {
            Put: {
              TableName: tableName(),
              Item: {
                ...finishedJob,
                gsi1pk: queuedIndexPk(CRAWL_FINISHED_QUEUE),
                gsi1sk: finishedJob.readySortKey,
                gsi2pk: groupIndexPk(groupId),
                gsi2sk: pendingGroupSortKey(CRAWL_FINISHED_QUEUE, finishedJobId),
              },
              ConditionExpression: "attribute_not_exists(pk)",
            },
          },
        ],
      }),
    );
  } catch (error) {
    if (transactionCancellationCodes(error)) return false;
    throw error;
  }
  logger.info("Crawl group completed", { module: "nuq", crawlId: groupId });
  return true;
}

class NuQJobGroup {
  public async addGroup(
    id: string,
    ownerId: string,
    ttl?: number,
    opts?: { maxConcurrency?: number; delaySeconds?: number },
    _logger_: Logger = _logger,
  ): Promise<NuQJobGroupInstance> {
    const now = Date.now();
    const item: GroupItem & Record<string, unknown> = {
      pk: groupPk(id),
      sk: GROUP_SORT_KEY,
      id,
      status: "active",
      ownerId: normalizeOwnerId(ownerId) ?? ownerId,
      createdAt: now,
      ttlMs: ttl ?? DEFAULT_GROUP_TTL_MS,
      ...(typeof opts?.maxConcurrency === "number"
        ? { maxConcurrency: opts.maxConcurrency }
        : {}),
      ...(typeof opts?.delaySeconds === "number"
        ? { delaySeconds: opts.delaySeconds }
        : {}),
      pending: 0,
      pendingSingle: 0,
      backlogSingle: 0,
      completedSingle: 0,
      failedSingle: 0,
      enqueued: 0,
      slotsUsed: 0,
      nextStartAt: 0,
      gsi3pk: ACTIVE_GROUPS_PK,
      gsi3sk: activeSortKey(now, id),
    };
    await dynamo().send(
      new PutCommand({
        TableName: tableName(),
        Item: item,
        ConditionExpression: "attribute_not_exists(pk)",
      }),
    );
    return toGroupInstance(item);
  }

  public async getGroup(
    id: string,
    _logger_: Logger = _logger,
  ): Promise<NuQJobGroupInstance | null> {
    const item = await readGroup(id);
    return item ? toGroupInstance(item) : null;
  }

  /** Active groups of one owner (bounded by the number of active crawls). */
  public async getOngoingByOwner(
    ownerId: string,
    _logger_: Logger = _logger,
  ): Promise<NuQJobGroupInstance[]> {
    const owner = normalizeOwnerId(ownerId);
    const ids: string[] = [];
    let cursor: Record<string, unknown> | undefined;
    do {
      const page = await dynamo().send(
        new QueryCommand({
          TableName: tableName(),
          IndexName: "gsi3",
          KeyConditionExpression: "gsi3pk = :pk",
          ExpressionAttributeValues: { ":pk": ACTIVE_GROUPS_PK },
          ExclusiveStartKey: cursor,
        }),
      );
      for (const key of page.Items ?? []) {
        ids.push(String(key.pk).slice("group#".length));
      }
      cursor = page.LastEvaluatedKey;
    } while (cursor);
    const groups: NuQJobGroupInstance[] = [];
    for (let offset = 0; offset < ids.length; offset += 100) {
      const batch = await dynamo().send(
        new BatchGetCommand({
          RequestItems: {
            [tableName()]: {
              Keys: ids
                .slice(offset, offset + 100)
                .map(id => ({ pk: groupPk(id), sk: GROUP_SORT_KEY })),
              ConsistentRead: true,
            },
          },
        }),
      );
      for (const item of (batch.Responses?.[tableName()] ?? []) as GroupItem[]) {
        if (item.status === "active" && item.ownerId === owner) {
          groups.push(toGroupInstance(item));
        }
      }
    }
    return groups;
  }

  /**
   * Drops the group's backlogged jobs so a cancelled crawl drains quickly;
   * queued and active jobs observe the crawl's cancelled flag and fail fast.
   */
  public async cancelGroup(
    id: string,
    logger: Logger = _logger,
  ): Promise<boolean> {
    const group = await readGroup(id);
    if (!group) return false;
    for (let page = 0; page < 1000; page++) {
      const result = await dynamo().send(
        new QueryCommand({
          TableName: tableName(),
          IndexName: "gsi1",
          KeyConditionExpression: "gsi1pk = :pk",
          ExpressionAttributeValues: { ":pk": backlogIndexPk(SCRAPE_QUEUE, id) },
          Limit: 100,
        }),
      );
      const items = result.Items ?? [];
      let removed = 0;
      for (const key of items) {
        const job = await readJobItem(SCRAPE_QUEUE, String(key.id));
        if (job?.status === "backlog" && (await removeBackloggedJob(job, id))) {
          removed++;
        }
      }
      // Index entries of already removed jobs can linger briefly; stop when a
      // page yields nothing removable.
      if (removed === 0) break;
    }
    logger.info("Cancelled crawl group backlog", { module: "nuq", crawlId: id });
    await tryCompleteGroup(id, logger);
    return true;
  }
}

export const crawlGroup = new NuQJobGroup();

/** Deletes an expired, completed group and (in bounded batches) its jobs. */
export async function cleanupExpiredGroup(
  groupId: string,
  budget: number,
): Promise<{ deleted: number; finished: boolean }> {
  let deleted = 0;
  const members = await dynamo().send(
    new QueryCommand({
      TableName: tableName(),
      IndexName: "gsi2",
      KeyConditionExpression: "gsi2pk = :pk",
      ExpressionAttributeValues: { ":pk": groupIndexPk(groupId) },
      Limit: Math.max(1, budget),
    }),
  );
  for (const member of members.Items ?? []) {
    deleted += await deletePartition(String(member.pk));
  }
  if ((members.Items ?? []).length > 0) return { deleted, finished: false };
  try {
    await dynamo().send(
      new DeleteCommand({
        TableName: tableName(),
        Key: { pk: groupPk(groupId), sk: GROUP_SORT_KEY },
        ConditionExpression: "#status = :completed AND expiresAt < :now",
        ExpressionAttributeNames: { "#status": "status" },
        ExpressionAttributeValues: { ":completed": "completed", ":now": Date.now() },
      }),
    );
  } catch (error) {
    if (!isConditionalCheckFailed(error)) throw error;
  }
  return { deleted, finished: true };
}

/** Deletes a job item and its payload chunk items (one partition). */
export async function deletePartition(pk: string): Promise<number> {
  let deleted = 0;
  let cursor: Record<string, unknown> | undefined;
  do {
    const page = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        KeyConditionExpression: "pk = :pk",
        ExpressionAttributeValues: { ":pk": pk },
        ProjectionExpression: "pk, sk",
        ExclusiveStartKey: cursor,
      }),
    );
    for (const key of page.Items ?? []) {
      await dynamo().send(
        new DeleteCommand({ TableName: tableName(), Key: { pk: key.pk, sk: key.sk } }),
      );
      deleted++;
    }
    cursor = page.LastEvaluatedKey;
  } while (cursor);
  return deleted;
}
