// Deployments.ai modification: NuQ job queue on DynamoDB. Replaces the
// Postgres `FOR UPDATE SKIP LOCKED` claim, pg_notify / RabbitMQ completion
// channels, and the RabbitMQ prefetch queue. See keys.ts for the data model;
// the producer lives in enqueue.ts and terminal transitions in settle.ts.
import {
  BatchGetCommand,
  QueryCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { Logger } from "winston";
import { logger as _logger } from "../../../lib/logger";
import { normalizeOwnerId } from "../../../lib/owner-id";
import {
  dynamo,
  isConditionalCheckFailed,
  isLogicallyExpired,
  tableName,
} from "../../dynamo/client";
import { readPayload } from "../../dynamo/payload";
import { enqueueJobs, type NuQJobOptions } from "./enqueue";
import { readGroup, readJobItem } from "./groups";
import {
  activeIndexPk,
  activeSortKey,
  DATA_CHUNK_PREFIX,
  groupIndexPk,
  type JobItem,
  jobKey,
  type NuQJob,
  type NuQJobStatus,
  queuedIndexPk,
  RESULT_CHUNK_PREFIX,
} from "./keys";
import {
  reapStalledJob,
  removeJobItem,
  settleJob,
  type StalledCandidate,
} from "./settle";

export type { NuQJobOptions } from "./enqueue";

export type NuQOptions = {
  /** Lock reaps allowed before a stalled job fails (upstream: 9). */
  maxStalls: number;
};

/** A lock not renewed for this long is stalled (upstream's 1 minute reaper). */
export const STALL_TIMEOUT_MS = 60 * 1000;
/** Queue-index page size when looking for a claimable job. */
const CLAIM_PAGE_SIZE = 25;
/** Pages of not-yet-due (delayed) candidates skipped before giving up. */
const CLAIM_MAX_PAGES = 4;

export class NuQ<JobData = any, JobReturnValue = any> {
  constructor(
    public readonly queueName: string,
    public readonly options: NuQOptions,
  ) {}

  private async toJob(item: JobItem): Promise<NuQJob<JobData, JobReturnValue>> {
    const [data, returnvalue] = await Promise.all([
      readPayload<JobData>({
        pk: item.pk,
        prefix: DATA_CHUNK_PREFIX,
        stored: item.data,
      }),
      readPayload<JobReturnValue>({
        pk: item.pk,
        prefix: RESULT_CHUNK_PREFIX,
        stored: item.returnvalue,
      }),
    ]);
    return {
      id: item.id,
      status: item.status,
      createdAt: new Date(item.createdAt),
      priority: item.priority,
      data: data as JobData,
      ...(item.finishedAt !== undefined
        ? { finishedAt: new Date(item.finishedAt) }
        : {}),
      ...(returnvalue !== undefined ? { returnvalue } : {}),
      ...(item.failedReason !== undefined
        ? { failedReason: item.failedReason }
        : {}),
      ...(item.lock !== undefined ? { lock: item.lock } : {}),
      ...(item.ownerId !== undefined ? { ownerId: item.ownerId } : {}),
      ...(item.groupId !== undefined ? { groupId: item.groupId } : {}),
    };
  }

  // === Reads

  public async getJob(
    id: string,
    _logger_: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue> | null> {
    const item = await readJobItem(this.queueName, id);
    return item ? this.toJob(item) : null;
  }

  public async getJobs(
    ids: string[],
    _logger_: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue>[]> {
    const items = new Map<string, JobItem>();
    const unique = [...new Set(ids)];
    for (let offset = 0; offset < unique.length; offset += 100) {
      let keys: Record<string, unknown>[] = unique
        .slice(offset, offset + 100)
        .map(id => jobKey(this.queueName, id));
      for (let attempt = 0; keys.length > 0; attempt++) {
        if (attempt >= 10) throw new Error("DynamoDB left job reads unprocessed.");
        const result = await dynamo().send(
          new BatchGetCommand({
            RequestItems: {
              [tableName()]: { Keys: keys, ConsistentRead: true },
            },
          }),
        );
        for (const item of (result.Responses?.[tableName()] ?? []) as JobItem[]) {
          if (!isLogicallyExpired(item)) items.set(item.id, item);
        }
        keys = (result.UnprocessedKeys?.[tableName()]?.Keys ?? []) as Record<
          string,
          unknown
        >[];
        if (keys.length > 0) await sleep(20 * 2 ** attempt);
      }
    }
    const ordered = ids
      .map(id => items.get(id))
      .filter((item): item is JobItem => item !== undefined);
    return Promise.all(ordered.map(item => this.toJob(item)));
  }

  public async getJobsWithStatus(
    ids: string[],
    status: NuQJobStatus,
    logger: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue>[]> {
    return (await this.getJobs(ids, logger)).filter(job => job.status === status);
  }

  public async getJobsWithStatuses(
    ids: string[],
    statuses: NuQJobStatus[],
    logger: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue>[]> {
    const wanted = new Set(statuses);
    return (await this.getJobs(ids, logger)).filter(job => wanted.has(job.status));
  }

  /** Any `single_urls` job of a group owned by `ownerId` (ownership probe). */
  public async getGroupAnyJob(
    groupId: string,
    ownerId: string,
    logger: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue> | null> {
    const owner = normalizeOwnerId(ownerId);
    let cursor: Record<string, unknown> | undefined;
    do {
      const page = await dynamo().send(
        new QueryCommand({
          TableName: tableName(),
          IndexName: "gsi2",
          KeyConditionExpression: "gsi2pk = :pk",
          FilterExpression: "#mode = :mode AND ownerId = :owner",
          ExpressionAttributeNames: { "#mode": "mode" },
          ExpressionAttributeValues: {
            ":pk": groupIndexPk(groupId),
            ":mode": "single_urls",
            ":owner": owner,
          },
          Limit: 100,
          ExclusiveStartKey: cursor,
        }),
      );
      for (const member of page.Items ?? []) {
        if (String(member.pk).startsWith(`job#${this.queueName}#`)) {
          const job = await this.getJob(String(member.id), logger);
          if (job) return job;
        }
      }
      cursor = page.LastEvaluatedKey;
    } while (cursor);
    return null;
  }

  /**
   * Per-status counts of a group's `single_urls` jobs, from its counters.
   * Queued and active jobs are not counted separately; both report as queued.
   */
  public async getGroupNumericStats(
    groupId: string,
    _logger_: Logger = _logger,
  ): Promise<Record<NuQJobStatus, number>> {
    const group = await readGroup(groupId);
    const backlog = group?.backlogSingle ?? 0;
    return {
      queued: Math.max(0, (group?.pendingSingle ?? 0) - backlog),
      active: 0,
      backlog,
      completed: group?.completedSingle ?? 0,
      failed: group?.failedSingle ?? 0,
    };
  }

  /**
   * Completed `single_urls` jobs of a group ordered by finish then creation
   * time (upstream `getCrawlJobsForListing`). Offsets walk the group index;
   * groups are bounded by the crawl limit.
   */
  public async getCrawlJobsForListing(
    groupId: string,
    limit: number,
    offset: number,
    logger: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue>[]> {
    const ids: string[] = [];
    let skipped = 0;
    let cursor: Record<string, unknown> | undefined;
    do {
      const page = await dynamo().send(
        new QueryCommand({
          TableName: tableName(),
          IndexName: "gsi2",
          KeyConditionExpression: "gsi2pk = :pk AND begins_with(gsi2sk, :done)",
          ExpressionAttributeValues: {
            ":pk": groupIndexPk(groupId),
            ":done": "done#",
          },
          Limit: Math.min(1000, offset - skipped + limit - ids.length),
          ExclusiveStartKey: cursor,
        }),
      );
      for (const member of page.Items ?? []) {
        if (!String(member.pk).startsWith(`job#${this.queueName}#`)) continue;
        if (skipped < offset) {
          skipped++;
        } else if (ids.length < limit) {
          ids.push(String(member.id));
        }
      }
      cursor = page.LastEvaluatedKey;
    } while (cursor && ids.length < limit);
    const jobs = await this.getJobs(ids, logger);
    return jobs.filter(job => job.status === "completed");
  }

  // === Producer

  public async addJob(
    id: string,
    data: JobData,
    options: NuQJobOptions,
  ): Promise<NuQJob<JobData, JobReturnValue>> {
    const [item] = await enqueueJobs(this.queueName, [{ id, data, options }], false);
    return this.toJob(item);
  }

  public async addJobIfNotExists(
    id: string,
    data: JobData,
    options: NuQJobOptions,
  ): Promise<NuQJob<JobData, JobReturnValue> | null> {
    const [item] = await enqueueJobs(this.queueName, [{ id, data, options }], true);
    return item ? this.toJob(item) : null;
  }

  public async addJobs(
    jobs: { id: string; data: JobData; options: NuQJobOptions }[],
  ): Promise<NuQJob<JobData, JobReturnValue>[]> {
    if (jobs.length === 0) return [];
    const items = await enqueueJobs(this.queueName, jobs, false);
    return Promise.all(items.map(item => this.toJob(item)));
  }

  public async removeJob(id: string, logger: Logger = _logger): Promise<boolean> {
    return removeJobItem(this.queueName, id, logger);
  }

  public async removeJobs(ids: string[], logger: Logger = _logger): Promise<number> {
    let removed = 0;
    for (const id of ids) {
      if (await removeJobItem(this.queueName, id, logger)) removed++;
    }
    return removed;
  }

  /**
   * Waits for a job to reach a terminal state by polling its item with
   * strongly consistent reads (replaces pg_notify / RabbitMQ listen queues).
   */
  public async waitForJob(
    id: string,
    timeout: number | null,
    _logger_: Logger = _logger,
  ): Promise<JobReturnValue> {
    const deadline = timeout !== null ? Date.now() + timeout : null;
    let interval = 100;
    for (;;) {
      const item = await readJobItem(this.queueName, id);
      if (item?.status === "completed") {
        return (await this.toJob(item)).returnvalue as JobReturnValue;
      }
      if (item?.status === "failed") {
        throw new Error(item.failedReason ?? "Job failed");
      }
      if (deadline !== null && Date.now() >= deadline) {
        throw new Error("Timed out");
      }
      await sleep(
        deadline !== null
          ? Math.min(interval, Math.max(1, deadline - Date.now()))
          : interval,
      );
      interval = Math.min(500, Math.round(interval * 1.5));
    }
  }

  // === Consumer

  /**
   * Claims the highest-priority, oldest due job. Candidates come from the
   * eventually consistent queue index; the claim itself is a conditional
   * write on the job item, so at most one worker wins each job.
   */
  public async getJobToProcess(
    _logger_: Logger = _logger,
  ): Promise<NuQJob<JobData, JobReturnValue> | null> {
    let cursor: Record<string, unknown> | undefined;
    for (let page = 0; page < CLAIM_MAX_PAGES; page++) {
      const candidates = await dynamo().send(
        new QueryCommand({
          TableName: tableName(),
          IndexName: "gsi1",
          KeyConditionExpression: "gsi1pk = :pk",
          ExpressionAttributeValues: { ":pk": queuedIndexPk(this.queueName) },
          Limit: CLAIM_PAGE_SIZE,
          ExclusiveStartKey: cursor,
        }),
      );
      const now = Date.now();
      let due = 0;
      for (const candidate of candidates.Items ?? []) {
        if (typeof candidate.notBefore === "number" && candidate.notBefore > now) {
          continue;
        }
        due++;
        const claimed = await this.claim(String(candidate.id), now);
        if (claimed) return this.toJob(claimed);
      }
      cursor = candidates.LastEvaluatedKey;
      // Keep paging only past delayed jobs; contended pages mean the queue is
      // being drained by other workers and the caller simply polls again.
      if (!cursor || due > 0) return null;
    }
    return null;
  }

  private async claim(id: string, now: number): Promise<JobItem | null> {
    try {
      const claimed = await dynamo().send(
        new UpdateCommand({
          TableName: tableName(),
          Key: jobKey(this.queueName, id),
          UpdateExpression:
            "SET #status = :active, #lock = :lock, lockedAt = :now, gsi1pk = :gsi1pk, gsi1sk = :gsi1sk REMOVE notBefore",
          ConditionExpression:
            "#status = :queued AND (attribute_not_exists(notBefore) OR notBefore <= :now)",
          ExpressionAttributeNames: { "#status": "status", "#lock": "lock" },
          ExpressionAttributeValues: {
            ":active": "active",
            ":queued": "queued",
            ":lock": randomUUID(),
            ":now": now,
            ":gsi1pk": activeIndexPk(this.queueName),
            ":gsi1sk": activeSortKey(now, id),
          },
          ReturnValues: "ALL_NEW",
        }),
      );
      return claimed.Attributes as JobItem;
    } catch (error) {
      if (isConditionalCheckFailed(error)) return null;
      throw error;
    }
  }

  public async renewLock(
    id: string,
    lock: string,
    _logger_: Logger = _logger,
  ): Promise<boolean> {
    const now = Date.now();
    try {
      await dynamo().send(
        new UpdateCommand({
          TableName: tableName(),
          Key: jobKey(this.queueName, id),
          UpdateExpression: "SET lockedAt = :now, gsi1sk = :gsi1sk",
          ConditionExpression: "#status = :active AND #lock = :lock",
          ExpressionAttributeNames: { "#status": "status", "#lock": "lock" },
          ExpressionAttributeValues: {
            ":now": now,
            ":gsi1sk": activeSortKey(now, id),
            ":active": "active",
            ":lock": lock,
          },
        }),
      );
      return true;
    } catch (error) {
      if (isConditionalCheckFailed(error)) return false;
      throw error;
    }
  }

  public async jobFinish(
    id: string,
    lock: string,
    returnvalue: unknown,
    logger: Logger = _logger,
  ): Promise<boolean> {
    return settleJob(this.queueName, id, { lock }, { outcome: "completed", returnvalue }, logger);
  }

  public async jobFail(
    id: string,
    lock: string,
    failedReason: string,
    logger: Logger = _logger,
  ): Promise<boolean> {
    return settleJob(this.queueName, id, { lock }, { outcome: "failed", failedReason }, logger);
  }

  public async reapStalled(
    candidate: StalledCandidate,
    logger: Logger = _logger,
  ): Promise<"requeued" | "failed" | "skipped"> {
    return reapStalledJob(this.queueName, this.options.maxStalls, candidate, logger);
  }

  /**
   * Active jobs whose lock was last renewed before `olderThan`, oldest first
   * (the gsi1 active partition is sorted by lock time).
   */
  public async findStalled(
    olderThan: number,
    limit: number,
  ): Promise<StalledCandidate[]> {
    const result = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        IndexName: "gsi1",
        KeyConditionExpression: "gsi1pk = :pk AND gsi1sk < :before",
        ExpressionAttributeValues: {
          ":pk": activeIndexPk(this.queueName),
          ":before": activeSortKey(olderThan, ""),
        },
        Limit: limit,
      }),
    );
    return (result.Items ?? []).flatMap(item =>
      typeof item.lock === "string" && typeof item.lockedAt === "number"
        ? [{ id: String(item.id), lock: item.lock, lockedAt: item.lockedAt }]
        : [],
    );
  }

  // === Metrics

  public async getMetrics(): Promise<string> {
    const counts: Record<string, number> = {};
    for (const status of ["queued", "active"] as const) {
      let count = 0;
      let cursor: Record<string, unknown> | undefined;
      do {
        const page = await dynamo().send(
          new QueryCommand({
            TableName: tableName(),
            IndexName: "gsi1",
            KeyConditionExpression: "gsi1pk = :pk",
            ExpressionAttributeValues: {
              ":pk":
                status === "queued"
                  ? queuedIndexPk(this.queueName)
                  : activeIndexPk(this.queueName),
            },
            Select: "COUNT",
            ExclusiveStartKey: cursor,
          }),
        );
        count += page.Count ?? 0;
        cursor = page.LastEvaluatedKey;
      } while (cursor);
      counts[status] = count;
    }
    const name = `nuq_${this.queueName}`;
    return `# HELP ${name}_job_count Number of jobs in each status\n# TYPE ${name}_job_count gauge\n${Object.entries(
      counts,
    )
      .map(([status, count]) => `${name}_job_count{status="${status}"} ${count}`)
      .join("\n")}\n`;
  }
}
