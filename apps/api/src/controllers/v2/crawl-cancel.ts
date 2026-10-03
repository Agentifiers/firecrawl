// Deployments.ai modification: cancellation drops the crawl's NuQ backlog
// (DynamoDB) instead of the Redis concurrency-limited job lists.
import { Response } from "express";
import { logger } from "../../lib/logger";
import { getCrawl, saveCrawl, StoredCrawl } from "../../lib/crawl-redis";
import * as Sentry from "@sentry/node";
import { configDotenv } from "dotenv";
import { RequestWithAuth, scrapeOptions } from "./types";
import { crawlGroup } from "../../services/worker/nuq";
import { normalizeOwnerId } from "../../lib/owner-id";
configDotenv();

export async function crawlCancelController(
  req: RequestWithAuth<{ jobId: string }>,
  res: Response,
) {
  try {
    const group = await crawlGroup.getGroup(req.params.jobId);
    if (!group) {
      return res.status(404).json({ error: "Job not found" });
    }

    // group.ownerId is normalized to a UUID in NuQ, so the raw team_id
    // (e.g. "bypass" when self-hosted) must be normalized before comparing
    if (group.ownerId !== normalizeOwnerId(req.auth.team_id)) {
      return res.status(404).json({ error: "Job not found" });
    }

    if (group.status === "completed") {
      return res.status(409).json({ error: "Crawl is already completed" });
    }

    const sc: StoredCrawl = (await getCrawl(req.params.jobId)) ?? {
      team_id: req.auth.team_id,
      createdAt: Date.now(),
      crawlerOptions: null,
      scrapeOptions: scrapeOptions.parse({}),
      internalOptions: {
        teamId: req.auth.team_id,
        orgId: req.acuc?.org_id ?? null,
      },
    };

    try {
      sc.cancelled = true;
      await saveCrawl(req.params.jobId, sc);
    } catch (error) {
      logger.error(error);
    }

    await crawlGroup.cancelGroup(req.params.jobId);

    res.json({
      status: "cancelled",
    });
  } catch (error) {
    Sentry.captureException(error);
    logger.error(error);
    return res.status(500).json({ error: error.message });
  }
}
