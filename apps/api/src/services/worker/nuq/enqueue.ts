// Deployments.ai modification: NuQ job producer on DynamoDB. Replaces the
// Postgres inserts and the Redis crawl concurrency limiter. Grouped jobs
// update their group's counters in the same transaction as the job write;
// jobs of a gated group (crawl `maxConcurrency` or `delay`) take a free slot
// atomically or wait in the group's backlog. See keys.ts for the data model.
import { PutCommand, TransactWriteCommand } from "@aws-sdk/lib-dynamodb";
import { normalizeOwnerId } from "../../../lib/owner-id";
import {
  dynamo,
  isConditionalCheckFailed,
  retryTransactionConflicts,
  tableName,
  transactionCancellationCodes,
  ttlAt,
} from "../../dynamo/client";
import {
  packPayload,
  type PackedPayload,
  writePayloadChunks,
} from "../../dynamo/payload";
import { groupUpdate, promoteBacklog, readGroup } from "./groups";
import {
  backlogIndexPk,
  DATA_CHUNK_PREFIX,
  type GroupItem,
  groupIndexPk,
  groupLimit,
  type JobItem,
  jobKey,
  pendingGroupSortKey,
  queuedIndexPk,
  readySortKey,
} from "./keys";

export type NuQJobOptions = {
  priority?: number;
  ownerId?: string;
  groupId?: string;
  /**
   * Enqueue immediately even when the group's concurrency slots are taken
   * (upstream's direct `_addScrapeJobToBullMQ` path). The job still occupies
   * a slot, exactly like upstream's crawl concurrency bookkeeping.
   */
  bypassGroupLimit?: boolean;
  /** Deadline for a job that has to wait in its group's backlog. */
  backlogTimeoutMs?: number;
};

export type EnqueueRequest<JobData> = {
  id: string;
  data: JobData;
  options: NuQJobOptions;
};

/** Payload chunks outlive any job; grouped jobs are deleted with their group. */
const DATA_CHUNK_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
/** Jobs per transaction; each transaction also updates the group item. */
const ENQUEUE_TRANSACTION_JOBS = 50;

type PreparedJob = {
  item: JobItem & Record<string, unknown>;
  single: boolean;
  packed: PackedPayload;
};

function prepare(
  queue: string,
  request: EnqueueRequest<unknown>,
  group: GroupItem | null,
  status: "queued" | "backlog",
  now: number,
): PreparedJob {
  const { id, data, options } = request;
  const priority = options.priority ?? 0;
  const mode =
    data && typeof data === "object" && "mode" in data
      ? String(data.mode)
      : undefined;
  const packed = packPayload(data, "initial");
  const sortKey = readySortKey(priority, now, id);
  const groupId = options.groupId;
  const item: JobItem & Record<string, unknown> = {
    ...jobKey(queue, id),
    id,
    queue,
    status,
    priority,
    createdAt: now,
    readySortKey: sortKey,
    stalls: 0,
    data: packed.attribute,
    ...(options.ownerId !== undefined
      ? { ownerId: normalizeOwnerId(options.ownerId) ?? options.ownerId }
      : {}),
    ...(mode !== undefined ? { mode } : {}),
    gsi1pk:
      status === "backlog" && groupId
        ? backlogIndexPk(queue, groupId)
        : queuedIndexPk(queue),
    gsi1sk: sortKey,
  };
  if (groupId) {
    item.groupId = groupId;
    item.gsi2pk = groupIndexPk(groupId);
    item.gsi2sk = pendingGroupSortKey(queue, id);
    if (group) {
      item.counted = true;
      item.groupTtlMs = group.ttlMs;
      if (groupLimit(group) !== null) item.gated = true;
      if ((group.delaySeconds ?? 0) > 0) {
        item.groupDelayMs = group.delaySeconds! * 1000;
      }
    }
  }
  if (status === "backlog" && options.backlogTimeoutMs !== undefined) {
    item.timesOutAt = now + options.backlogTimeoutMs;
  }
  return { item, single: mode === "single_urls", packed };
}

function put(job: PreparedJob) {
  return {
    Put: {
      TableName: tableName(),
      Item: job.item,
      ConditionExpression: "attribute_not_exists(pk)",
    },
  };
}

async function writeChunks(job: PreparedJob, now: number) {
  await writePayloadChunks({
    pk: job.item.pk,
    prefix: DATA_CHUNK_PREFIX,
    packed: job.packed,
    ttl: ttlAt(now + DATA_CHUNK_RETENTION_MS),
  });
}

/** Writes jobs that need no slot; one transaction per batch when grouped. */
async function writeUngated(
  batch: PreparedJob[],
  group: GroupItem | null,
  skipExisting: boolean,
): Promise<PreparedJob[]> {
  if (!group) {
    const accepted: PreparedJob[] = [];
    for (const job of batch) {
      try {
        await dynamo().send(new PutCommand(put(job).Put));
        accepted.push(job);
      } catch (error) {
        if (!(skipExisting && isConditionalCheckFailed(error))) throw error;
      }
    }
    return accepted;
  }
  const delta = {
    pending: batch.length,
    pendingSingle: batch.filter(job => job.single).length,
    enqueued: batch.length,
  };
  try {
    await retryTransactionConflicts(() =>
      dynamo().send(
        new TransactWriteCommand({
          TransactItems: [groupUpdate(group.id, delta), ...batch.map(put)],
        }),
      ),
    );
    return batch;
  } catch (error) {
    const codes = transactionCancellationCodes(error);
    // codes[0] is the group update: a missing group is never retried.
    if (!codes || codes[0] === "ConditionalCheckFailed") throw error;
    if (batch.length === 1) {
      if (skipExisting) return [];
      throw error;
    }
    // A duplicate id cancelled the batch: retry one job at a time.
    const accepted: PreparedJob[] = [];
    for (const job of batch) {
      accepted.push(...(await writeUngated([job], group, skipExisting)));
    }
    return accepted;
  }
}

/**
 * Writes one job of a gated group: queued with a slot when one is free (or
 * the caller bypasses the limit), otherwise backlogged. Returns null when
 * `skipExisting` and the id already exists.
 */
async function writeGated(
  queue: string,
  request: EnqueueRequest<unknown>,
  group: GroupItem,
  limit: number,
  backlogging: boolean,
  skipExisting: boolean,
): Promise<JobItem | null> {
  const bypass = request.options.bypassGroupLimit === true;
  const now = Date.now();
  if (bypass || !backlogging) {
    const prepared = prepare(queue, request, group, "queued", now);
    await writeChunks(prepared, now);
    const slot = groupUpdate(
      group.id,
      {
        pending: 1,
        pendingSingle: prepared.single ? 1 : 0,
        enqueued: 1,
        slotsUsed: 1,
      },
      bypass ? {} : { slotLimit: limit },
    );
    if (!bypass && (group.delaySeconds ?? 0) > 0) {
      slot.Update.ConditionExpression += " AND nextStartAt <= :now";
      slot.Update.ExpressionAttributeValues[":now"] = now;
    }
    try {
      await retryTransactionConflicts(() =>
        dynamo().send(
          new TransactWriteCommand({ TransactItems: [put(prepared), slot] }),
        ),
      );
      return prepared.item;
    } catch (error) {
      const codes = transactionCancellationCodes(error);
      if (!codes) throw error;
      if (codes[0] === "ConditionalCheckFailed") {
        if (skipExisting) return null;
        throw error;
      }
      if (bypass) throw error;
      // No free slot (or the delay has not elapsed): backlog instead.
    }
  }
  const prepared = prepare(queue, request, group, "backlog", now);
  await writeChunks(prepared, now);
  try {
    await retryTransactionConflicts(() =>
      dynamo().send(
        new TransactWriteCommand({
          TransactItems: [
            put(prepared),
            groupUpdate(group.id, {
              pending: 1,
              pendingSingle: prepared.single ? 1 : 0,
              backlogSingle: prepared.single ? 1 : 0,
              enqueued: 1,
            }),
          ],
        }),
      ),
    );
  } catch (error) {
    const codes = transactionCancellationCodes(error);
    if (skipExisting && codes?.[0] === "ConditionalCheckFailed") return null;
    throw error;
  }
  // A slot may have been released while this job was being written.
  await promoteBacklog(queue, group.id);
  return prepared.item;
}

/**
 * Enqueues jobs and returns the created job items in request order (per
 * group). `skipExisting` turns duplicate ids into no-ops.
 */
export async function enqueueJobs(
  queue: string,
  requests: EnqueueRequest<unknown>[],
  skipExisting: boolean,
): Promise<JobItem[]> {
  const created: JobItem[] = [];
  const byGroup = new Map<string | undefined, EnqueueRequest<unknown>[]>();
  for (const request of requests) {
    const groupId = request.options.groupId;
    const list = byGroup.get(groupId);
    if (list) list.push(request);
    else byGroup.set(groupId, [request]);
  }
  for (const [groupId, groupRequests] of byGroup) {
    const group = groupId ? await readGroup(groupId) : null;
    const limit = group ? groupLimit(group) : null;
    let backlogging = false;
    const pending: PreparedJob[] = [];
    const flush = async () => {
      if (pending.length === 0) return;
      const batch = pending.splice(0, pending.length);
      for (const job of await writeUngated(batch, group, skipExisting)) {
        created.push(job.item);
      }
    };
    for (const request of groupRequests) {
      const now = Date.now();
      if (!group || limit === null) {
        const prepared = prepare(queue, request, group, "queued", now);
        await writeChunks(prepared, now);
        pending.push(prepared);
        if (pending.length >= ENQUEUE_TRANSACTION_JOBS) await flush();
        continue;
      }
      // Once one job is backlogged, the rest of the batch follows it so
      // promotion keeps the batch's priority order.
      const item = await writeGated(
        queue,
        request,
        group,
        limit,
        backlogging,
        skipExisting,
      );
      if (item) {
        if (item.status === "backlog") backlogging = true;
        created.push(item);
      }
    }
    await flush();
  }
  return created;
}
