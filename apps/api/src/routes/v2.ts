import express from "express";
import multer from "multer";
import { config } from "../config";
import { RateLimiterMode } from "../types";
import { SEARCH_CREDITS_FEATURE_ID } from "../services/autumn/autumn.service";
import { searchController } from "../controllers/v2/search";
import { feedbackController } from "../controllers/v2/feedback/controller";
import { searchFeedbackController } from "../controllers/v2/search-feedback";
import { scrapeController } from "../controllers/v2/scrape";
import {
  parseController,
  parseMultipartPayloadMiddleware,
} from "../controllers/v2/parse";
import { batchScrapeController } from "../controllers/v2/batch-scrape";
import { crawlController } from "../controllers/v2/crawl";
import { crawlParamsPreviewController } from "../controllers/v2/crawl-params-preview";
import { crawlStatusController } from "../controllers/v2/crawl-status";
import { mapController } from "../controllers/v2/map";
import { crawlErrorsController } from "../controllers/v2/crawl-errors";
import { ongoingCrawlsController } from "../controllers/v2/crawl-ongoing";
import { scrapeStatusController } from "../controllers/v2/scrape-status";
import { creditUsageController } from "../controllers/v2/credit-usage";
import { tokenUsageController } from "../controllers/v2/token-usage";
import { crawlCancelController } from "../controllers/v2/crawl-cancel";
import { extractController } from "../controllers/v2/extract";
import { extractStatusController } from "../controllers/v2/extract-status";
import {
  authMiddleware,
  checkCreditsMiddleware,
  blocklistMiddleware,
  scrapeBlocklistMiddleware,
  countryCheck,
  idempotencyMiddleware,
  requestTimingMiddleware,
  wrap,
  validateJobIdParam,
} from "./shared";
import { creditUsageHistoricalController } from "../controllers/v2/credit-usage-historical";
import { tokenUsageHistoricalController } from "../controllers/v2/token-usage-historical";
import { deprecationMiddleware } from "../lib/deprecations";
import { agentController } from "../controllers/v2/agent";
import { agentStatusController } from "../controllers/v2/agent-status";
import { agentCancelController } from "../controllers/v2/agent-cancel";
import { activityController } from "../controllers/v1/activity";
import { supportProxyController } from "../controllers/v2/support-proxy";

export const v2Router = express.Router();

const parseUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024, // 50 MB
  },
});

const parseUploadMiddleware: express.RequestHandler = (req, res, next) => {
  const upload = parseUpload.single("file");

  upload(req, res, err => {
    if (!err) {
      return next();
    }

    if (err instanceof multer.MulterError && err.code === "LIMIT_FILE_SIZE") {
      return res.status(400).json({
        success: false,
        code: "BAD_REQUEST",
        error: "Uploaded file exceeds maximum size of 50MB.",
      });
    }

    return res.status(400).json({
      success: false,
      code: "BAD_REQUEST",
      error: err.message || "Invalid multipart form-data request.",
    });
  });
};

const parsePayloadMiddleware: express.RequestHandler = (req, res, next) => {
  const contentType = req.headers["content-type"] || "";
  if (
    typeof contentType === "string" &&
    contentType.includes("multipart/form-data")
  ) {
    return parseUploadMiddleware(req, res, err => {
      if (err) return next(err);
      return parseMultipartPayloadMiddleware(req, res, next);
    });
  }

  // Deployments.ai: the hosted uploadRef flow (parse-upload.ts, Redis-backed
  // unparsed-ref quota) is removed; only multipart/form-data is accepted.
  return res.status(400).json({
    success: false,
    code: "BAD_REQUEST",
    error:
      "Missing file upload. Send multipart/form-data with a 'file' field.",
  });
};
// Add timing middleware to all v2 routes
v2Router.use(requestTimingMiddleware("v2"));

// Deployments.ai: /keyless/eligibility is removed (keyless access is
// hosted-only and its credit ledger lived in Redis).

v2Router.post(
  "/search",
  authMiddleware(RateLimiterMode.Search),
  countryCheck,
  checkCreditsMiddleware(undefined, SEARCH_CREDITS_FEATURE_ID),
  blocklistMiddleware,
  wrap(searchController),
);

v2Router.post(
  "/search/:jobId/feedback",
  authMiddleware(RateLimiterMode.Account),
  validateJobIdParam,
  wrap(searchFeedbackController),
);

v2Router.post(
  "/feedback",
  authMiddleware(RateLimiterMode.Account),
  wrap(feedbackController),
);

v2Router.post(
  "/parse",
  authMiddleware(RateLimiterMode.Scrape),
  countryCheck,
  checkCreditsMiddleware(1),
  parsePayloadMiddleware,
  wrap(parseController),
);

v2Router.post(
  "/scrape",
  authMiddleware(RateLimiterMode.Scrape),
  countryCheck,
  checkCreditsMiddleware(1),
  scrapeBlocklistMiddleware,
  wrap(scrapeController),
);

v2Router.get(
  "/scrape/:jobId",
  authMiddleware(RateLimiterMode.CrawlStatus),
  validateJobIdParam,
  wrap(scrapeStatusController),
);

v2Router.post(
  "/batch/scrape",
  authMiddleware(RateLimiterMode.Scrape),
  countryCheck,
  checkCreditsMiddleware(),
  blocklistMiddleware,
  wrap(batchScrapeController),
);

v2Router.post(
  "/map",
  authMiddleware(RateLimiterMode.Map),
  checkCreditsMiddleware(1),
  blocklistMiddleware,
  wrap(mapController),
);

v2Router.post(
  "/crawl",
  authMiddleware(RateLimiterMode.Crawl),
  countryCheck,
  checkCreditsMiddleware(),
  scrapeBlocklistMiddleware,
  idempotencyMiddleware,
  wrap(crawlController),
);

v2Router.post(
  "/crawl/params-preview",
  authMiddleware(RateLimiterMode.Crawl),
  checkCreditsMiddleware(),
  wrap(crawlParamsPreviewController),
);

v2Router.get(
  "/crawl/ongoing",
  authMiddleware(RateLimiterMode.CrawlStatus),
  wrap(ongoingCrawlsController),
);

v2Router.get(
  "/crawl/active",
  authMiddleware(RateLimiterMode.CrawlStatus),
  wrap(ongoingCrawlsController),
);

v2Router.get(
  "/crawl/:jobId",
  authMiddleware(RateLimiterMode.CrawlStatus),
  validateJobIdParam,
  wrap(crawlStatusController),
);

v2Router.delete(
  "/crawl/:jobId",
  authMiddleware(RateLimiterMode.CrawlStatus),
  validateJobIdParam,
  wrap(crawlCancelController),
);

v2Router.get(
  "/batch/scrape/:jobId",
  authMiddleware(RateLimiterMode.CrawlStatus),
  validateJobIdParam,
  wrap((req: any, res: any) => crawlStatusController(req, res, true)),
);

v2Router.delete(
  "/batch/scrape/:jobId",
  authMiddleware(RateLimiterMode.CrawlStatus),
  validateJobIdParam,
  wrap(crawlCancelController),
);

v2Router.get(
  "/batch/scrape/:jobId/errors",
  authMiddleware(RateLimiterMode.CrawlStatus),
  wrap(crawlErrorsController),
);

v2Router.get(
  "/crawl/:jobId/errors",
  authMiddleware(RateLimiterMode.CrawlStatus),
  validateJobIdParam,
  wrap(crawlErrorsController),
);

v2Router.post(
  "/extract",
  authMiddleware(RateLimiterMode.Extract),
  deprecationMiddleware("v2_extract"),
  countryCheck,
  checkCreditsMiddleware(20),
  blocklistMiddleware,
  wrap(extractController),
);

v2Router.get(
  "/extract/:jobId",
  authMiddleware(RateLimiterMode.ExtractStatus),
  deprecationMiddleware("v2_extract_status"),
  validateJobIdParam,
  wrap(extractStatusController),
);

v2Router.post(
  "/agent",
  authMiddleware(RateLimiterMode.Extract),
  countryCheck,
  checkCreditsMiddleware(20),
  blocklistMiddleware,
  wrap(agentController),
);

v2Router.get(
  "/agent/:jobId",
  authMiddleware(RateLimiterMode.ExtractStatus),
  validateJobIdParam,
  wrap(agentStatusController),
);

v2Router.delete(
  "/agent/:jobId",
  authMiddleware(RateLimiterMode.ExtractStatus),
  validateJobIdParam,
  wrap(agentCancelController),
);

v2Router.get(
  "/team/credit-usage",
  authMiddleware(RateLimiterMode.Account),
  wrap(creditUsageController),
);

v2Router.get(
  "/team/credit-usage/historical",
  authMiddleware(RateLimiterMode.Account),
  wrap(creditUsageHistoricalController),
);

v2Router.get(
  "/team/token-usage",
  authMiddleware(RateLimiterMode.Account),
  wrap(tokenUsageController),
);

v2Router.get(
  "/team/token-usage/historical",
  authMiddleware(RateLimiterMode.Account),
  wrap(tokenUsageHistoricalController),
);

// Deployments.ai: /concurrency-check and /team/queue-status are removed
// (Redis-backed team listings, hosted-only).

v2Router.get(
  "/team/activity",
  authMiddleware(RateLimiterMode.Account),
  wrap(activityController),
);

// Deployments.ai: /team/threat-protection is removed (hosted-only team
// settings API; threat protection is configured per deployment).

// Deployments.ai: monitor, Slack and browser/interact routes are removed
// (hosted-only; their Redis/RabbitMQ state is gone).

// Support agent proxy — forwards to the support-agent service.
v2Router.post(
  "/support/ask",
  authMiddleware(RateLimiterMode.SupportAsk),
  wrap(supportProxyController),
);
v2Router.post(
  "/support/docs-search",
  authMiddleware(RateLimiterMode.SupportDocsSearch),
  wrap(supportProxyController),
);

// Deployments.ai: /search/research and /research (hosted research proxy) are
// removed.
