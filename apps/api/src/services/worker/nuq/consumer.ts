// Deployments.ai modification: the claim/renew/settle loop shared by every
// NuQ consumer (scrape, crawl-finished, extract, deep research, llms.txt).
// Upstream had one loop per backend (Postgres/RabbitMQ prefetch, BullMQ
// workers, the RabbitMQ extract consumer); they now all poll DynamoDB.
import { setTimeout as sleep } from "node:timers/promises";
import { Logger } from "winston";
import type { NuQJob } from "./keys";
import type { NuQ } from "./queue";

/** Lock renewal period; a lock unrenewed for STALL_TIMEOUT_MS is reaped. */
export const LOCK_RENEW_INTERVAL_MS = 15_000;
/** Idle polling backs off from MIN to MAX while the queue is empty. */
const IDLE_POLL_MIN_MS = 250;
const IDLE_POLL_MAX_MS = 1_000;

export type ConsumeOptions<Data, Result> = {
  queue: NuQ<Data, Result>;
  logger: Logger;
  isShuttingDown: () => boolean;
  /** Extra log fields for one job. */
  jobLogFields?: (job: NuQJob<Data, Result>) => Record<string, unknown>;
  process: (job: NuQJob<Data, Result>, logger: Logger) => Promise<Result>;
  /** Observes the processing outcome (metrics) before the job settles. */
  onOutcome?: (job: NuQJob<Data, Result>, ok: boolean) => void;
};

function failureReason(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return JSON.stringify(error);
}

async function processOne<Data, Result>(
  options: ConsumeOptions<Data, Result>,
  job: NuQJob<Data, Result>,
) {
  const { queue } = options;
  const lock = job.lock!;
  const logger = options.logger.child({
    jobId: job.id,
    ...(options.jobLogFields?.(job) ?? {}),
  });
  logger.info("Acquired job");
  const renewal = setInterval(async () => {
    try {
      if (!(await queue.renewLock(job.id, lock, logger))) {
        logger.warn("Lost job lock; another worker may now own the job");
        clearInterval(renewal);
      }
    } catch (error) {
      // A transient failure is retried on the next tick; the stall timeout
      // is four renewal periods.
      logger.warn("Failed to renew lock", { error });
    }
  }, LOCK_RENEW_INTERVAL_MS);

  let outcome: { ok: true; data: Result } | { ok: false; error: unknown };
  try {
    outcome = { ok: true, data: await options.process(job, logger) };
  } catch (error) {
    outcome = { ok: false, error };
  } finally {
    clearInterval(renewal);
  }
  options.onOutcome?.(job, outcome.ok);
  const settled = outcome.ok
    ? await queue.jobFinish(job.id, lock, outcome.data, logger)
    : await queue.jobFail(job.id, lock, failureReason(outcome.error), logger);
  if (!settled) logger.warn("Could not update job status");
}

/** Claims and processes jobs one at a time until shutdown is requested. */
export async function consumeJobs<Data, Result>(
  options: ConsumeOptions<Data, Result>,
): Promise<void> {
  let idle = IDLE_POLL_MIN_MS;
  while (!options.isShuttingDown()) {
    let job: NuQJob<Data, Result> | null;
    try {
      job = await options.queue.getJobToProcess(options.logger);
    } catch (error) {
      options.logger.error("Failed to claim a job", { error });
      await sleep(IDLE_POLL_MAX_MS);
      continue;
    }
    if (job === null) {
      await sleep(idle);
      idle = Math.min(idle * 2, IDLE_POLL_MAX_MS);
      continue;
    }
    idle = IDLE_POLL_MIN_MS;
    try {
      await processOne(options, job);
    } catch (error) {
      options.logger.error("Failed to settle a job", { jobId: job.id, error });
    }
  }
}
