// Deployments.ai modification: scrape jobs go to the DynamoDB NuQ queue.
// Upstream's Redis concurrency limiter, team concurrency queue, FoundationDB
// router, and job-priority bookkeeping were removed: crawl `maxConcurrency`
// and `delay` are enforced by the queue's crawl-group slots (see
// worker/nuq/enqueue.ts), and the per-team concurrency limit of hosted
// database authentication is not enforced. See MODIFICATIONS.md.
import { v7 as uuidv7 } from "uuid";
import { ScrapeJobData } from "../types";
import { logger as _logger } from "../lib/logger";
import { getJobFromGCS, removeJobFromGCS } from "../lib/gcs-jobs";
import { Document } from "../controllers/v1/types";
import { Logger } from "winston";
import { ScrapeJobTimeoutError, TransportableError } from "../lib/error";
import { deserializeTransportableError } from "../lib/error-serde";
import { abTestJob } from "./ab-test";
import { NuQJob, scrapeQueue } from "./worker/nuq";
import { serializeTraceContext } from "../lib/otel-tracer";

/** Upstream's MAX_BACKLOG_TIMEOUT_MS: crawl jobs may wait two days for a slot. */
const CRAWL_BACKLOG_TIMEOUT_MS = 48 * 60 * 60 * 1000;

// Queue-wait deadline for a backlogged job (how long its owner still cares about the result)
function backlogTimeoutMs(data: ScrapeJobData): number {
  if (data.crawl_id) return CRAWL_BACKLOG_TIMEOUT_MS;
  if (data.mode === "single_urls")
    return data.scrapeOptions.timeout ?? 60 * 1000;
  return 60 * 1000;
}

function jobOptions(
  data: ScrapeJobData,
  priority: number,
  bypassGroupLimit: boolean,
) {
  return {
    priority,
    ownerId: data.team_id ?? undefined,
    groupId: data.crawl_id ?? undefined,
    bypassGroupLimit,
    backlogTimeoutMs: backlogTimeoutMs(data),
  };
}

/**
 * Enqueues a job without waiting for a crawl concurrency slot (kickoff and
 * sitemap jobs). The job still occupies a slot of its crawl.
 */
export async function _addScrapeJobToBullMQ(
  webScraperOptions: ScrapeJobData,
  jobId: string,
  priority: number = 0,
): Promise<NuQJob<ScrapeJobData>> {
  if (webScraperOptions.mode === "single_urls") {
    abTestJob(webScraperOptions);
  }
  return await scrapeQueue.addJob(
    jobId,
    webScraperOptions,
    jobOptions(webScraperOptions, priority, true),
  );
}

/**
 * Enqueues a scrape job. A job of a crawl with `maxConcurrency` or `delay`
 * waits in the crawl's backlog when no slot is free; null is returned then
 * (upstream: the job went to the concurrency queue). `directToBullMQ`
 * bypasses the crawl limit.
 */
export async function addScrapeJob(
  webScraperOptions: ScrapeJobData,
  jobId: string = uuidv7(),
  priority: number = 0,
  directToBullMQ: boolean = false,
): Promise<NuQJob<ScrapeJobData> | null> {
  // Capture trace context to propagate to worker
  const data: ScrapeJobData = {
    ...webScraperOptions,
    traceContext: serializeTraceContext(),
  };
  if (data.mode === "single_urls") {
    abTestJob(data);
  }
  const job = await scrapeQueue.addJob(
    jobId,
    data,
    jobOptions(data, priority, directToBullMQ),
  );
  return job.status === "backlog" ? null : job;
}

export async function addScrapeJobs(
  jobs: {
    jobId: string;
    data: ScrapeJobData;
    priority: number;
  }[],
) {
  if (jobs.length === 0) return true;

  // Capture trace context for all jobs
  const traceContext = serializeTraceContext();
  for (const job of jobs) {
    if (job.data.mode === "single_urls") {
      abTestJob(job.data);
    }
  }
  await scrapeQueue.addJobs(
    jobs.map(job => ({
      id: job.jobId,
      data: { ...job.data, traceContext },
      options: jobOptions(job.data, job.priority, false),
    })),
  );
  return true;
}

export async function waitForJob(
  job: NuQJob<ScrapeJobData> | string,
  timeout: number | null,
  zeroDataRetention: boolean,
  logger: Logger = _logger,
): Promise<Document> {
  const jobId = typeof job == "string" ? job : job.id;
  const isConcurrencyLimited = !!(typeof job === "string");

  let timeoutHandle: NodeJS.Timeout | undefined;
  let doc: Document | null = null;
  try {
    doc = await Promise.race(
      [
        scrapeQueue.waitForJob(
          jobId,
          timeout !== null ? timeout + 100 : null,
          logger,
        ) as Promise<Document>,
        timeout !== null
          ? new Promise<Document>((_resolve, reject) => {
              timeoutHandle = setTimeout(() => {
                reject(
                  new ScrapeJobTimeoutError(
                    "Scrape timed out" +
                      (isConcurrencyLimited
                        ? " after waiting in the concurrency limit queue"
                        : ""),
                  ),
                );
              }, timeout);
            })
          : null,
      ].filter(x => x !== null),
    );
  } catch (e) {
    if (e instanceof TransportableError) {
      throw e;
    } else if (e instanceof Error) {
      const x = deserializeTransportableError(e.message);
      if (x) {
        throw x;
      } else {
        throw e;
      }
    } else {
      throw e;
    }
  } finally {
    clearTimeout(timeoutHandle);
  }

  logger.debug("Got job");

  if (!doc) {
    const docs = await getJobFromGCS(jobId);
    logger.debug("Got job from GCS");
    if (!docs || docs.length === 0) {
      throw new Error("Job not found in GCS");
    }
    doc = docs[0]!;

    if (zeroDataRetention) {
      await removeJobFromGCS(jobId);
    }
  }

  return doc;
}
