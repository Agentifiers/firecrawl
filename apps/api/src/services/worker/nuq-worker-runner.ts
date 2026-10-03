import { config } from "../../config";
import { logger as _logger } from "../../lib/logger";
import { jobDurationSeconds } from "../../lib/job-metrics";
import { processJobInternal } from "./scrape-worker";
import type { NuQ } from "./nuq";
import { consumeJobs } from "./nuq/consumer";
import type { ScrapeJobData } from "../../types";
import { register } from "prom-client";
import Express from "express";
import { initializeBlocklist } from "../../scraper/WebScraper/utils/blocklist";
import { initializeEngineForcing } from "../../scraper/WebScraper/utils/engine-forcing";

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function runNuqWorker(options: {
  serviceName: string;
  queue: NuQ<ScrapeJobData>;
  healthCheck: () => Promise<boolean>;
  metrics?: () => string | Promise<string>;
  beforeStart?: () => void | Promise<void>;
  beforeShutdown?: () => void | Promise<void>;
  shutdown?: () => void | Promise<void>;
}) {
  try {
    await initializeBlocklist();
    initializeEngineForcing();
    await options.beforeStart?.();
  } catch (error) {
    _logger.error("Failed to initialize NuQ worker", {
      module: options.serviceName,
      error,
    });
    process.exit(1);
  }

  let isShuttingDown = false;

  const app = Express();

  app.get("/metrics", async (_, res) => {
    const localMetrics = options.metrics ? await options.metrics() : "";
    res
      .contentType("text/plain")
      .send(localMetrics + "\n" + (await register.metrics()));
  });
  app.get("/health", async (_, res) => {
    try {
      if (await withTimeout(options.healthCheck(), 1000, "NuQ health check")) {
        res.status(200).send("OK");
      } else {
        res.status(500).send("Not OK");
      }
    } catch (error) {
      _logger.warn("NuQ worker health check failed", {
        module: options.serviceName,
        error,
      });
      res.status(500).send("Not OK");
    }
  });

  const server = app.listen(config.NUQ_WORKER_PORT, (error?: Error) => {
    if (error) {
      _logger.error("Failed to start NuQ worker metrics server", {
        module: options.serviceName,
        error,
        port: config.NUQ_WORKER_PORT,
      });
      throw error;
    }

    _logger.info("NuQ worker metrics server started", {
      module: options.serviceName,
    });
  });

  function shutdown() {
    isShuttingDown = true;
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  // Deployments.ai: the claim/renew/settle loop is shared with the other
  // NuQ consumers (nuq/consumer.ts).
  // Jobs are processed one at a time, so one pending timer suffices.
  let endJobTimer: ((labels: { status: string }) => number) | undefined;
  await consumeJobs({
    queue: options.queue,
    logger: _logger.child({ module: options.serviceName }),
    isShuttingDown: () => isShuttingDown,
    jobLogFields: job => ({
      scrapeId: job.id,
      zeroDataRetention: job.data?.zeroDataRetention ?? false,
    }),
    process: async job => {
      endJobTimer = jobDurationSeconds.startTimer({ type: job.data.mode });
      return processJobInternal(job);
    },
    onOutcome: (_job, ok) => {
      endJobTimer?.({ status: ok ? "success" : "failed" });
      endJobTimer = undefined;
    },
  });

  _logger.info("NuQ worker shutting down", { module: options.serviceName });

  server.close(async () => {
    await options.beforeShutdown?.();
    await options.shutdown?.();
    _logger.info("NuQ worker shut down", { module: options.serviceName });
    process.exit(0);
  });
}
