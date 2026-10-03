// Deployments.ai modification: NuQ terminal transitions on DynamoDB (job
// finish/fail, the stalled-lock reaper, and job removal). Every transition is
// conditioned on the job's status and lock and applies the group counter
// deltas in the same transaction, so it happens exactly once even when
// workers and sweepers race. See keys.ts for the data model.
import { TransactWriteCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { Logger } from "winston";
import {
  dynamo,
  isConditionalCheckFailed,
  retryTransactionConflicts,
  tableName,
  transactionCancellationCodes,
  ttlAt,
} from "../../dynamo/client";
import { packPayload, writePayloadChunks } from "../../dynamo/payload";
import {
  deletePartition,
  groupUpdate,
  promoteBacklog,
  readJobItem,
  terminalDelta,
  tryCompleteGroup,
} from "./groups";
import {
  doneGroupSortKey,
  GROUP_CLEANUP_GRACE_MS,
  type JobItem,
  jobKey,
  queuedIndexPk,
  RESULT_CHUNK_PREFIX,
  STANDALONE_COMPLETED_RETENTION_MS,
  STANDALONE_FAILED_RETENTION_MS,
} from "./keys";

export type SettleResult =
  | { outcome: "completed"; returnvalue: unknown }
  | { outcome: "failed"; failedReason: string; stalls?: number };

export type StalledCandidate = { id: string; lock: string; lockedAt: number };

const statusNames = {
  "#status": "status",
  "#lock": "lock",
  "#ttl": "ttl",
} as const;

function retentionTtl(item: JobItem, outcome: SettleResult["outcome"], now: number) {
  if (item.groupId !== undefined) {
    return ttlAt(now + (item.groupTtlMs ?? 0) + GROUP_CLEANUP_GRACE_MS);
  }
  return ttlAt(
    now +
      (outcome === "completed"
        ? STANDALONE_COMPLETED_RETENTION_MS
        : STANDALONE_FAILED_RETENTION_MS),
  );
}

/** Hands a released slot to the backlog and completes an emptied group. */
async function afterRelease(queue: string, item: JobItem, logger: Logger) {
  if (!item.groupId) return;
  try {
    if (item.gated) await promoteBacklog(queue, item.groupId, logger);
    await tryCompleteGroup(item.groupId, logger);
  } catch (error) {
    // The sweeper repeats both steps; a failure here only delays them.
    logger.warn("Deferred group promotion/completion to the sweeper", {
      module: "nuq",
      crawlId: item.groupId,
      error,
    });
  }
}

/**
 * Moves an active job held under `expected.lock` (and, for the reaper, the
 * exact `lockedAt` observed) to completed or failed. Returns false when the
 * job is no longer held under that lock.
 */
export async function settleJob(
  queue: string,
  id: string,
  expected: { lock: string; lockedAt?: number },
  result: SettleResult,
  logger: Logger,
): Promise<boolean> {
  const item = await readJobItem(queue, id);
  if (!item || item.status !== "active" || item.lock !== expected.lock) {
    return false;
  }
  const now = Date.now();
  const ttl = retentionTtl(item, result.outcome, now);
  const values: Record<string, unknown> = {
    ":status": result.outcome,
    ":now": now,
    ":ttl": ttl,
    ":active": "active",
    ":lock": expected.lock,
  };
  let update = "SET #status = :status, finishedAt = :now, #ttl = :ttl";
  let condition = "#status = :active AND #lock = :lock";
  if (expected.lockedAt !== undefined) {
    condition += " AND lockedAt = :lockedAt";
    values[":lockedAt"] = expected.lockedAt;
  }
  if (result.outcome === "completed") {
    // Chunk sort keys embed the lock, so a losing settle leaves only orphan
    // chunks (removed by TTL) and never corrupts the winner's result.
    const packed = packPayload(result.returnvalue ?? null, expected.lock);
    await writePayloadChunks({
      pk: item.pk,
      prefix: RESULT_CHUNK_PREFIX,
      packed,
      ttl,
    });
    update += ", returnvalue = :returnvalue";
    values[":returnvalue"] = packed.attribute;
    if (item.groupId !== undefined && item.mode === "single_urls") {
      update += ", gsi2sk = :done";
      values[":done"] = doneGroupSortKey(now, item.createdAt, id);
    }
  } else {
    update += ", failedReason = :failedReason";
    values[":failedReason"] = result.failedReason;
    if (result.stalls !== undefined) {
      update += ", stalls = :stalls";
      values[":stalls"] = result.stalls;
    }
  }
  update += " REMOVE #lock, lockedAt, gsi1pk, gsi1sk, notBefore";
  const jobUpdate = {
    Update: {
      TableName: tableName(),
      Key: jobKey(queue, id),
      UpdateExpression: update,
      ConditionExpression: condition,
      ExpressionAttributeNames: { ...statusNames },
      ExpressionAttributeValues: values,
    },
  };
  const counted = item.groupId !== undefined && item.counted === true;
  const delayMs = item.groupDelayMs ?? 0;
  const transactItems = counted
    ? [
        jobUpdate,
        groupUpdate(
          item.groupId!,
          terminalDelta(item, result.outcome),
          item.gated && delayMs > 0 ? { nextStartAt: now + delayMs } : {},
        ),
      ]
    : [jobUpdate];
  try {
    await retryTransactionConflicts(() =>
      dynamo().send(new TransactWriteCommand({ TransactItems: transactItems })),
    );
  } catch (error) {
    if (transactionCancellationCodes(error)) return false;
    throw error;
  }
  if (counted) await afterRelease(queue, item, logger);
  return true;
}

/**
 * Requeues or fails one stalled job (upstream's lock reaper). The
 * transition is conditioned on the exact lock and lock time observed, so a
 * job is reaped at most once per stall even with concurrent sweepers.
 */
export async function reapStalledJob(
  queue: string,
  maxStalls: number,
  candidate: StalledCandidate,
  logger: Logger,
): Promise<"requeued" | "failed" | "skipped"> {
  const item = await readJobItem(queue, candidate.id);
  if (
    !item ||
    item.status !== "active" ||
    item.lock !== candidate.lock ||
    item.lockedAt !== candidate.lockedAt
  ) {
    return "skipped";
  }
  const stalls = (item.stalls ?? 0) + 1;
  if (stalls >= maxStalls) {
    const failed = await settleJob(
      queue,
      candidate.id,
      { lock: candidate.lock, lockedAt: candidate.lockedAt },
      { outcome: "failed", failedReason: "Job stalled too many times", stalls },
      logger,
    );
    return failed ? "failed" : "skipped";
  }
  try {
    await dynamo().send(
      new UpdateCommand({
        TableName: tableName(),
        Key: jobKey(queue, candidate.id),
        UpdateExpression:
          "SET #status = :queued, stalls = :stalls, gsi1pk = :gsi1pk, gsi1sk = readySortKey REMOVE #lock, lockedAt",
        ConditionExpression:
          "#status = :active AND #lock = :lock AND lockedAt = :lockedAt",
        ExpressionAttributeNames: { "#status": "status", "#lock": "lock" },
        ExpressionAttributeValues: {
          ":queued": "queued",
          ":active": "active",
          ":stalls": stalls,
          ":gsi1pk": queuedIndexPk(queue),
          ":lock": candidate.lock,
          ":lockedAt": candidate.lockedAt,
        },
      }),
    );
  } catch (error) {
    if (isConditionalCheckFailed(error)) return "skipped";
    throw error;
  }
  logger.warn("Requeued stalled job", {
    module: "nuq/sweeper",
    queue,
    jobId: candidate.id,
    stalls,
  });
  return "requeued";
}

/** Removes a job; a non-terminal grouped job releases its accounting. */
export async function removeJobItem(
  queue: string,
  id: string,
  logger: Logger,
): Promise<boolean> {
  const item = await readJobItem(queue, id);
  if (!item) return false;
  const terminal = item.status === "completed" || item.status === "failed";
  const groupId = item.groupId;
  if (!terminal && groupId !== undefined && item.counted === true) {
    try {
      await retryTransactionConflicts(() =>
        dynamo().send(
          new TransactWriteCommand({
            TransactItems: [
              {
                Delete: {
                  TableName: tableName(),
                  Key: jobKey(queue, id),
                  ConditionExpression: "#status = :status",
                  ExpressionAttributeNames: { "#status": "status" },
                  ExpressionAttributeValues: { ":status": item.status },
                },
              },
              groupUpdate(groupId, terminalDelta(item, "removed")),
            ],
          }),
        ),
      );
    } catch (error) {
      if (transactionCancellationCodes(error)) return false;
      throw error;
    }
    await afterRelease(queue, item, logger);
  }
  await deletePartition(item.pk);
  return true;
}
