// Deployments.ai modification: upstream's Postgres/RabbitMQ NuQ (and the
// FoundationDB backend and router) is replaced by the DynamoDB queue in
// ./nuq/. This module keeps upstream's exported names and adds the queues
// that replace BullMQ (deep research, llms.txt) and RabbitMQ (extract).
import { GetCommand } from "@aws-sdk/lib-dynamodb";
import type { DeepResearchRequest } from "../../controllers/v1/deep-research";
import type { GenerateLLMsTextRequest } from "../../controllers/v1/types";
import type { ExtractRequest } from "../../controllers/v2/types";
import { logger } from "../../lib/logger";
import { type ScrapeJobData } from "../../types";
import { dynamo, tableName } from "../dynamo/client";
import { CRAWL_FINISHED_QUEUE, SCRAPE_QUEUE } from "./nuq/groups";
import { NuQ } from "./nuq/queue";

export type {
  NuQGroupStatus,
  NuQJob,
  NuQJobGroupInstance,
  NuQJobStatus,
} from "./nuq/keys";
export { NuQ, STALL_TIMEOUT_MS } from "./nuq/queue";
export type { NuQJobOptions } from "./nuq/queue";
export { crawlGroup } from "./nuq/groups";

/** Both API versions enqueue the v2 request shape the extractor consumes. */
export type ExtractJobData = {
  extractId: string;
  request: ExtractRequest;
  teamId: string;
  apiKeyId?: number | null;
  agent?: ExtractRequest["agent"];
  createdAt: number;
};

export type DeepResearchJobData = {
  request: DeepResearchRequest;
  teamId: string;
  apiKeyId: number | null;
  researchId: string;
};

export type LlmsTxtJobData = {
  request: GenerateLLMsTextRequest;
  teamId: string;
  apiKeyId: number | null;
  generationId: string;
};

export const scrapeQueue = new NuQ<ScrapeJobData>(SCRAPE_QUEUE, {
  maxStalls: 9,
});
export const crawlFinishedQueue = new NuQ(CRAWL_FINISHED_QUEUE, {
  maxStalls: 9,
});
/** Upstream's RabbitMQ extract queue had `x-delivery-limit: 1`: no retry. */
export const extractQueue = new NuQ<ExtractJobData>("extract", {
  maxStalls: 1,
});
/** Upstream's BullMQ queues allowed 10 stalls. */
export const deepResearchQueue = new NuQ<DeepResearchJobData>("deep_research", {
  maxStalls: 10,
});
export const llmsTxtQueue = new NuQ<LlmsTxtJobData>("llmstxt", {
  maxStalls: 10,
});

export const allQueues: readonly NuQ[] = [
  scrapeQueue,
  crawlFinishedQueue,
  extractQueue,
  deepResearchQueue,
  llmsTxtQueue,
];

export function nuqGetLocalMetrics(): string {
  return "";
}

/** One strongly consistent GetItem: the table is reachable and readable. */
export async function nuqHealthCheck(): Promise<boolean> {
  const start = Date.now();
  try {
    await dynamo().send(
      new GetCommand({
        TableName: tableName(),
        Key: { pk: "health", sk: "health" },
        ConsistentRead: true,
      }),
    );
    return true;
  } catch (error) {
    logger.warn("NuQ health check failed", { module: "nuq", error });
    return false;
  } finally {
    logger.debug("NuQ health check", {
      module: "nuq",
      durationMs: Date.now() - start,
    });
  }
}

/** The DynamoDB client holds no connections that need draining. */
export async function nuqShutdown() {
  dynamo().destroy();
}
