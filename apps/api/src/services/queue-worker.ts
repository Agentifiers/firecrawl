// Deployments.ai modification: the deep research and llms.txt queues moved
// from BullMQ (Redis) to the DynamoDB NuQ, and all three queues here run on
// the shared NuQ consumer loop (claim, renew, settle) with
// NUM_WORKERS_PER_QUEUE loops per queue. Monitor scheduling (hosted-only,
// Redis/RabbitMQ) is removed.
import "dotenv/config";
import { config } from "../config";
import "./sentry";
import { setSentryServiceTag } from "./sentry";
import * as Sentry from "@sentry/node";
import { logger as _logger } from "../lib/logger";
import { configDotenv } from "dotenv";
import { updateDeepResearch } from "../lib/deep-research/deep-research-redis";
import { performDeepResearch } from "../lib/deep-research/deep-research-service";
import { performGenerateLlmsTxt } from "../lib/generate-llmstxt/generate-llmstxt-service";
import { updateGeneratedLlmsTxt } from "../lib/generate-llmstxt/generate-llmstxt-redis";
import Express from "express";
import { robustFetch } from "../scraper/scrapeURL/lib/fetch";
import { initializeBlocklist } from "../scraper/WebScraper/utils/blocklist";
import { initializeEngineForcing } from "../scraper/WebScraper/utils/engine-forcing";
import {
  crawlFinishedQueue,
  deepResearchQueue,
  DeepResearchJobData,
  llmsTxtQueue,
  LlmsTxtJobData,
  NuQJob,
  nuqShutdown,
  scrapeQueue,
} from "./worker/nuq";
import { consumeJobs } from "./worker/nuq/consumer";
import { finishCrawlSuper } from "./worker/crawl-logic";
import { getCrawl } from "../lib/crawl-redis";
import { TransportableError } from "../lib/error";

configDotenv();

function captureUnexpected(error: unknown, jobId: string) {
  // Skip TransportableErrors: they're flow control, not failures.
  if (!(error instanceof TransportableError)) {
    Sentry.captureException(error, { data: { job: jobId } });
  }
}

const processDeepResearchJob = async (job: NuQJob<DeepResearchJobData>) => {
  const logger = _logger.child({
    module: "deep-research-worker",
    method: "processJobInternal",
    jobId: job.id,
    researchId: job.data.researchId,
    teamId: job.data.teamId,
  });

  try {
    logger.info("Starting deep research");
    const result = await performDeepResearch({
      researchId: job.data.researchId,
      teamId: job.data.teamId,
      // The request schema requires query or topic.
      query: job.data.request.query ?? "",
      maxDepth: job.data.request.maxDepth,
      timeLimit: job.data.request.timeLimit,
      maxUrls: job.data.request.maxUrls,
      analysisPrompt: job.data.request.analysisPrompt,
      systemPrompt: job.data.request.systemPrompt,
      formats: job.data.request.formats,
      jsonOptions: job.data.request.jsonOptions,
      apiKeyId: job.data.apiKeyId,
      renderer: job.data.request.renderer,
    });

    if (result.success) {
      return result;
    }
    const message = "Deep research failed without specific error";
    await updateDeepResearch(job.data.researchId, {
      status: "failed",
      error: message,
    });
    throw new Error(message);
  } catch (error) {
    logger.error(`🚫 Job errored ${job.id} - ${error}`, { error });
    captureUnexpected(error, job.id);
    await updateDeepResearch(job.data.researchId, {
      status: "failed",
      error: (error instanceof Error && error.message) || "Unknown error occurred",
    });
    throw error;
  }
};

const processGenerateLlmsTxtJob = async (job: NuQJob<LlmsTxtJobData>) => {
  const logger = _logger.child({
    module: "generate-llmstxt-worker",
    method: "processJobInternal",
    jobId: job.id,
    generationId: job.data.generationId,
    teamId: job.data.teamId,
  });

  try {
    const result = await performGenerateLlmsTxt({
      generationId: job.data.generationId,
      teamId: job.data.teamId,
      url: job.data.request.url,
      maxUrls: job.data.request.maxUrls,
      showFullText: job.data.request.showFullText,
      cache: job.data.request.cache,
      apiKeyId: job.data.apiKeyId,
      renderer: job.data.request.renderer,
    });

    if (result.success) {
      // Upstream addressed the status document by `generateId`, a field the
      // job never carried; the generation id is the document key.
      await updateGeneratedLlmsTxt(job.data.generationId, {
        status: "completed",
        generatedText: result.data.generatedText,
        fullText: result.data.fullText,
      });
      return result;
    }
    throw new Error("LLMs text generation failed without specific error");
  } catch (error) {
    logger.error(`🚫 Job errored ${job.id} - ${error}`, { error });
    captureUnexpected(error, job.id);
    await updateGeneratedLlmsTxt(job.data.generationId, {
      status: "failed",
      error: (error instanceof Error && error.message) || "Unknown error occurred",
    });
    throw error;
  }
};

async function processFinishCrawlJob(_job: NuQJob) {
  const job = await crawlFinishedQueue.getJob(_job.id);

  if (!job) {
    throw new Error("crawlFinish job disappeared");
  }

  if (!job.groupId) {
    throw new Error("crawlFinish job with no groupId");
  }

  if (!job.ownerId) {
    throw new Error("crawlFinish job with no ownerId");
  }

  const sc = await getCrawl(job.groupId);

  if (!sc) {
    throw new Error("crawlFinish job with sc expired");
  }

  const anyJob = await scrapeQueue.getGroupAnyJob(job.groupId, job.ownerId);

  if (!anyJob) {
    throw new Error("crawlFinish couldn't find anyJob");
  }

  await finishCrawlSuper(anyJob);
  return null;
}

let isShuttingDown = false;

if (require.main === module) {
  process.on("SIGINT", () => {
    _logger.debug("Received SIGINT. Shutting down gracefully...");
    isShuttingDown = true;
  });

  process.on("SIGTERM", () => {
    _logger.debug("Received SIGTERM. Shutting down gracefully...");
    isShuttingDown = true;
  });
}

const app = Express();

app.get("/liveness", (req, res) => {
  _logger.info("Liveness endpoint hit");
  if (config.USE_DB_AUTHENTICATION) {
    // networking check for Kubernetes environments
    const host = config.FIRECRAWL_APP_HOST;
    const port = config.FIRECRAWL_APP_PORT;
    const scheme = config.FIRECRAWL_APP_SCHEME;

    robustFetch({
      url: `${scheme}://${host}:${port}`,
      method: "GET",
      mock: null,
      logger: _logger,
      abort: AbortSignal.timeout(5000),
      ignoreResponse: true,
      useCacheableLookup: false,
    })
      .then(() => {
        res.status(200).json({ ok: true });
      })
      .catch(e => {
        _logger.error("WORKER NETWORKING CHECK FAILED", { error: e });
        res.status(500).json({ ok: false });
      });
  } else {
    res.status(200).json({ ok: true });
  }
});

const workerPort = config.WORKER_PORT || config.PORT;
app.listen(workerPort, (error?: Error) => {
  if (error) {
    _logger.error("Failed to start liveness endpoint", {
      error,
      port: workerPort,
    });
    throw error;
  }

  _logger.info(`Liveness endpoint is running on port ${workerPort}`);
});

(async () => {
  setSentryServiceTag("queue-worker");

  await initializeBlocklist().catch(e => {
    _logger.error("Failed to initialize blocklist", { error: e });
    process.exit(1);
  });

  initializeEngineForcing();

  const isShuttingDownFn = () => isShuttingDown;
  const concurrency = config.NUM_WORKERS_PER_QUEUE;
  await Promise.all([
    consumeJobs({
      queue: deepResearchQueue,
      logger: _logger.child({ module: "deep-research-worker" }),
      isShuttingDown: isShuttingDownFn,
      concurrency,
      process: processDeepResearchJob,
    }),
    consumeJobs({
      queue: llmsTxtQueue,
      logger: _logger.child({ module: "generate-llmstxt-worker" }),
      isShuttingDown: isShuttingDownFn,
      concurrency,
      process: processGenerateLlmsTxtJob,
    }),
    consumeJobs({
      queue: crawlFinishedQueue,
      logger: _logger.child({ module: "crawl-finish-worker" }),
      isShuttingDown: isShuttingDownFn,
      concurrency,
      jobLogFields: job => ({ crawlId: job.groupId }),
      process: processFinishCrawlJob,
    }),
  ]);

  // consumeJobs returns only after its in-flight job settled.
  await nuqShutdown();
  _logger.info("All jobs finished. Shutting down...");
  process.exit(0);
})();
