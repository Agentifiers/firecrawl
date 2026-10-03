// Deployments.ai modification: consumes the DynamoDB `extract` NuQ queue
// instead of RabbitMQ. A crashed job is failed by the NuQ sweeper (the
// former dead-letter consumer) after its single allowed stall.
import "dotenv/config";
import { config } from "../config";
import "./sentry";
import { setSentryServiceTag } from "./sentry";
import * as Sentry from "@sentry/node";
import { logger as _logger } from "../lib/logger";
import { configDotenv } from "dotenv";
import { ExtractResult } from "../lib/extract/types";
import { updateExtract } from "../lib/extract/extract-redis";
import { performExtraction_F0 } from "../lib/extract/fire-0/extraction-service-f0";
import { createWebhookSender, WebhookEvent } from "./webhook";
import Express from "express";
import { getErrorContactMessage } from "../lib/deployment";
import { TransportableError } from "../lib/error";
import { initializeBlocklist } from "../scraper/WebScraper/utils/blocklist";
import { initializeEngineForcing } from "../scraper/WebScraper/utils/engine-forcing";
import { consumeJobs } from "./worker/nuq/consumer";
import { extractQueue, ExtractJobData, nuqShutdown } from "./worker/nuq";
import { jobDurationSeconds } from "../lib/job-metrics";
import { register } from "prom-client";

configDotenv();

const processExtractJob = async (data: ExtractJobData) => {
  const logger = _logger.child({
    module: "extract-worker",
    method: "processExtractJob",
    extractId: data.extractId,
    teamId: data.teamId,
  });

  const sender = await createWebhookSender({
    teamId: data.teamId,
    jobId: data.extractId,
    webhook: data.request.webhook,
    v0: false,
  });

  const endJobTimer = jobDurationSeconds.startTimer({ type: "extract" });

  try {
    if (sender) {
      sender.send(WebhookEvent.EXTRACT_STARTED, {
        success: true,
      });
    }

    let result: ExtractResult | null = null;

    result = await performExtraction_F0(data.extractId, {
      request: data.request,
      teamId: data.teamId,
      apiKeyId: data.apiKeyId ?? null,
    });

    if (result && result.success) {
      await updateExtract(data.extractId, {
        status: "completed",
        llmUsage: result.llmUsage,
        sources: result.sources,
        tokensBilled: result.tokensBilled,
        creditsBilled: result.creditsBilled,
      });

      if (sender) {
        sender.send(WebhookEvent.EXTRACT_COMPLETED, {
          success: true,
          data: [result],
        });
      }

      endJobTimer({ status: "success" });
      return;
    } else {
      await updateExtract(data.extractId, {
        status: "failed",
        error: result?.error ?? getErrorContactMessage(data.extractId),
      });

      if (sender) {
        sender.send(WebhookEvent.EXTRACT_FAILED, {
          success: false,
          error: result?.error ?? getErrorContactMessage(data.extractId),
        });
      }

      endJobTimer({ status: "failed" });
      return;
    }
  } catch (error) {
    endJobTimer({ status: "failed" });
    logger.error(`🚫 Extract job errored ${data.extractId} - ${error}`, {
      error,
    });

    // Filter out TransportableErrors (flow control)
    if (!(error instanceof TransportableError)) {
      Sentry.captureException(error, {
        data: {
          extractId: data.extractId,
        },
      });
    }

    await updateExtract(data.extractId, {
      status: "failed",
      error:
        (error as any)?.error ??
        (error as any)?.message ??
        getErrorContactMessage(data.extractId),
    });

    if (sender) {
      sender.send(WebhookEvent.EXTRACT_FAILED, {
        success: false,
        error:
          (error as any)?.message ?? getErrorContactMessage(data.extractId),
      });
    }
  }
};

let shuttingDown = false;

// Start the worker
const app = Express();

app.get("/health", (req, res) => {
  res.status(200).json({ ok: true });
});
app.get("/liveness", (req, res) => {
  res.status(200).json({ ok: true });
});
app.get("/metrics", async (_, res) => {
  res.contentType("text/plain").send(await register.metrics());
});

const workerPort = config.EXTRACT_WORKER_PORT || config.PORT;
app.listen(workerPort, (error?: Error) => {
  if (error) {
    _logger.error("Failed to start extract worker health endpoint", {
      error,
      port: workerPort,
    });
    throw error;
  }

  _logger.info(
    `Extract worker health endpoint is running on port ${workerPort}`,
  );
});

async function shutdown() {
  _logger.info("Shutting down extract worker...");
  shuttingDown = true;
}

if (require.main === module) {
  process.on("SIGINT", () => {
    _logger.debug("Received SIGINT. Shutting down gracefully...");
    shutdown();
  });

  process.on("SIGTERM", () => {
    _logger.debug("Received SIGTERM. Shutting down gracefully...");
    shutdown();
  });
}

(async () => {
  setSentryServiceTag("extract-worker");

  await initializeBlocklist().catch(e => {
    _logger.error("Failed to initialize blocklist", { error: e });
    process.exit(1);
  });

  initializeEngineForcing();

  _logger.info("Starting extract worker on the NuQ extract queue...");

  await consumeJobs({
    queue: extractQueue,
    logger: _logger.child({ module: "extract-worker" }),
    isShuttingDown: () => shuttingDown,
    jobLogFields: job => ({ extractId: job.id }),
    process: async job => {
      await processExtractJob(job.data);
      return null;
    },
  });

  await nuqShutdown();
  _logger.info("Extract worker shut down");
  process.exit(0);
})();
