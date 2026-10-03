// Deployments.ai modification: periodic NuQ maintenance on DynamoDB. Replaces
// upstream's pg_cron jobs (lock reaper, group finisher, cleaners) and the
// Redis concurrency-queue reconciler. Every pass is bounded: it reads a
// limited number of index entries per partition and never scans the table.
// Each step is a conditional write, so any number of sweepers may run.
import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import { Logger } from "winston";
import { logger as _logger } from "../../../lib/logger";
import { dynamo, tableName } from "../../dynamo/client";
import {
  cleanupExpiredGroup,
  promoteBacklog,
  readGroup,
  SCRAPE_QUEUE,
  tryCompleteGroup,
} from "./groups";
import {
  ACTIVE_GROUPS_PK,
  activeSortKey,
  COMPLETED_GROUPS_PK,
  groupLimit,
} from "./keys";
import { type NuQ, STALL_TIMEOUT_MS } from "./queue";

/** Runs after the sweeper fails a job that stalled too often. */
export type StallFailureHook = (jobId: string, logger: Logger) => Promise<void>;

export type SweepOptions = {
  queues: readonly NuQ[];
  /** Keyed by queue name. */
  onStallFailure?: Readonly<Partial<Record<string, StallFailureHook>>>;
  logger?: Logger;
  now?: number;
};

export type SweepStats = {
  requeued: number;
  failed: number;
  groupsCompleted: number;
  promoted: number;
  deleted: number;
};

/** Stalled locks reaped per queue per pass. */
const STALLED_PER_QUEUE = 50;
/** Active groups inspected per pass. */
const ACTIVE_GROUPS_PER_PASS = 200;
/** Expired groups considered and items deleted per pass. */
const EXPIRED_GROUPS_PER_PASS = 20;
const DELETE_BUDGET = 200;

async function groupIds(
  partition: string,
  limit: number,
  before?: string,
): Promise<string[]> {
  const ids: string[] = [];
  let cursor: Record<string, unknown> | undefined;
  do {
    const page = await dynamo().send(
      new QueryCommand({
        TableName: tableName(),
        IndexName: "gsi3",
        KeyConditionExpression:
          before !== undefined
            ? "gsi3pk = :pk AND gsi3sk < :before"
            : "gsi3pk = :pk",
        ExpressionAttributeValues: {
          ":pk": partition,
          ...(before !== undefined ? { ":before": before } : {}),
        },
        Limit: limit - ids.length,
        ExclusiveStartKey: cursor,
      }),
    );
    for (const key of page.Items ?? []) {
      ids.push(String(key.pk).slice("group#".length));
    }
    cursor = page.LastEvaluatedKey;
  } while (cursor && ids.length < limit);
  return ids;
}

async function reapStalledJobs(options: SweepOptions, now: number, stats: SweepStats) {
  const logger = options.logger ?? _logger;
  for (const queue of options.queues) {
    const stalled = await queue.findStalled(now - STALL_TIMEOUT_MS, STALLED_PER_QUEUE);
    for (const candidate of stalled) {
      const outcome = await queue.reapStalled(candidate, logger);
      if (outcome === "requeued") stats.requeued++;
      if (outcome !== "failed") continue;
      stats.failed++;
      const hook = options.onStallFailure?.[queue.queueName];
      if (!hook) continue;
      try {
        await hook(candidate.id, logger);
      } catch (error) {
        logger.error("Stall failure hook failed", {
          module: "nuq/sweeper",
          queue: queue.queueName,
          jobId: candidate.id,
          error,
        });
      }
    }
  }
}

/**
 * Finishes active groups whose last job ended and hands free slots of gated
 * groups to their backlog. Both normally happen when a job settles; this
 * covers a worker that crashed between settling and the follow-up.
 */
async function maintainActiveGroups(logger: Logger, stats: SweepStats) {
  for (const id of await groupIds(ACTIVE_GROUPS_PK, ACTIVE_GROUPS_PER_PASS)) {
    const group = await readGroup(id);
    if (!group || group.status !== "active") continue;
    if (group.pending === 0) {
      if (await tryCompleteGroup(id, logger)) stats.groupsCompleted++;
      continue;
    }
    const limit = groupLimit(group);
    if (limit !== null && group.slotsUsed < limit && group.pending > group.slotsUsed) {
      stats.promoted += await promoteBacklog(SCRAPE_QUEUE, id, logger);
    }
  }
}

/** Deletes completed groups past their retention, with their jobs. */
async function cleanupExpiredGroups(now: number, stats: SweepStats) {
  let budget = DELETE_BUDGET;
  const expired = await groupIds(
    COMPLETED_GROUPS_PK,
    EXPIRED_GROUPS_PER_PASS,
    activeSortKey(now, ""),
  );
  for (const id of expired) {
    if (budget <= 0) return;
    const result = await cleanupExpiredGroup(id, budget);
    stats.deleted += result.deleted;
    budget -= Math.max(1, result.deleted);
  }
}

export async function sweepOnce(options: SweepOptions): Promise<SweepStats> {
  const logger = options.logger ?? _logger;
  const now = options.now ?? Date.now();
  const stats: SweepStats = {
    requeued: 0,
    failed: 0,
    groupsCompleted: 0,
    promoted: 0,
    deleted: 0,
  };
  await reapStalledJobs(options, now, stats);
  await maintainActiveGroups(logger, stats);
  await cleanupExpiredGroups(now, stats);
  return stats;
}
