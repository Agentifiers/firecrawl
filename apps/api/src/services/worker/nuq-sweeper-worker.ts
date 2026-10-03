// Deployments.ai modification: runs the NuQ sweeper (stalled-lock reaper,
// group finisher/promoter, expired-group cleanup) on an interval. Replaces
// upstream's pg_cron jobs, nuq-prefetch-worker, and nuq-reconciler-worker.
import "dotenv/config";
import "../sentry";
import Express from "express";
import { setTimeout as sleep } from "node:timers/promises";
import { Logger } from "winston";
import { config } from "../../config";
import { updateDeepResearch } from "../../lib/deep-research/deep-research-redis";
import { updateExtract } from "../../lib/extract/extract-redis";
import { updateGeneratedLlmsTxt } from "../../lib/generate-llmstxt/generate-llmstxt-redis";
import { logger as _logger } from "../../lib/logger";
import { setSentryServiceTag } from "../sentry";
import {
  allQueues,
  deepResearchQueue,
  extractQueue,
  llmsTxtQueue,
  nuqHealthCheck,
} from "./nuq";
import { type StallFailureHook, sweepOnce } from "./nuq/sweeper";

const STALLED_ERROR =
  "The job crashed unexpectedly (its worker stopped renewing the lock too many times). Please try again.";

/**
 * Status documents of jobs whose worker died never get a terminal status
 * from that worker; the sweeper records the failure instead (upstream: the
 * BullMQ failed handler and the extract dead-letter consumer).
 */
const onStallFailure: Record<string, StallFailureHook> = {
  [extractQueue.queueName]: async id => {
    await updateExtract(id, { status: "failed", error: STALLED_ERROR });
  },
  [deepResearchQueue.queueName]: async id => {
    await updateDeepResearch(id, { status: "failed", error: STALLED_ERROR });
  },
  [llmsTxtQueue.queueName]: async id => {
    await updateGeneratedLlmsTxt(id, { status: "failed", error: STALLED_ERROR });
  },
};

async function runSweeper(logger: Logger, isShuttingDown: () => boolean) {
  while (!isShuttingDown()) {
    const start = Date.now();
    try {
      const stats = await sweepOnce({ queues: allQueues, onStallFailure, logger });
      if (Object.values(stats).some(value => value > 0)) {
        logger.info("NuQ sweep", { ...stats, durationMs: Date.now() - start });
      }
    } catch (error) {
      logger.error("NuQ sweep failed", { error });
    }
    await sleep(Math.max(0, config.NUQ_SWEEPER_INTERVAL_MS - (Date.now() - start)));
  }
}

(async () => {
  setSentryServiceTag("nuq-sweeper-worker");
  const logger = _logger.child({ module: "nuq-sweeper-worker" });
  let shuttingDown = false;

  const app = Express();
  app.get("/health", async (_, res) => {
    res.status((await nuqHealthCheck()) ? 200 : 500).send();
  });
  const server = app.listen(config.NUQ_SWEEPER_WORKER_PORT, () => {
    logger.info("NuQ sweeper health endpoint started", {
      port: config.NUQ_SWEEPER_WORKER_PORT,
    });
  });

  const shutdown = () => {
    shuttingDown = true;
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  await runSweeper(logger, () => shuttingDown);
  server.close(() => process.exit(0));
})();
