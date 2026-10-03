// Deployments.ai modification: extract status and results are stored in
// DynamoDB (`extract#<id>`, sk `status` for 6h and `result` for 24h) instead
// of Redis; see ../status-document.ts. Module name and API are upstream's.
import { logger as _logger } from "../logger";
import { CostTracking } from "../cost-tracking";
import { statusDocumentStore } from "../status-document";

export enum ExtractStep {
  INITIAL = "initial",
  MAP = "map",
  MAP_RERANK = "map-rerank",
  MULTI_ENTITY = "multi-entity",
  MULTI_ENTITY_SCRAPE = "multi-entity-scrape",
  MULTI_ENTITY_EXTRACT = "multi-entity-extract",
  SCRAPE = "scrape",
  EXTRACT = "extract",
}

type ExtractedStep = {
  step: ExtractStep;
  startedAt: number;
  finishedAt: number | null;
  error?: any;
  discoveredLinks?: string[];
};

type StoredExtract = {
  id: string;
  team_id: string;
  createdAt: number;
  status: "processing" | "completed" | "failed" | "cancelled";
  error?: any;
  showSteps?: boolean;
  steps?: ExtractedStep[];
  showLLMUsage?: boolean;
  showSources?: boolean;
  llmUsage?: number;
  showCostTracking?: boolean;
  costTracking?: CostTracking;
  sources?: {
    [key: string]: string[];
  };
  sessionIds?: string[];
  tokensBilled?: number;
  creditsBilled?: number;
  zeroDataRetention?: boolean;
};

// Reduce TTL to 6 hours instead of 24
const EXTRACT_TTL = 6 * 60 * 60;
// Result data storage (fallback when GCS is not configured)
const EXTRACT_RESULT_TTL = 24 * 60 * 60; // 24 hours

const STEPS_MAX_DISCOVERED_LINKS = 100;

const extracts = statusDocumentStore<StoredExtract>("extract", "status", EXTRACT_TTL);
const results = statusDocumentStore<unknown>("extract", "result", EXTRACT_RESULT_TTL);

function minimalSteps(steps: ExtractedStep[] | undefined) {
  return steps?.map(step => ({
    step: step.step,
    startedAt: step.startedAt,
    finishedAt: step.finishedAt,
    error: step.error,
    discoveredLinks: step.discoveredLinks?.slice(0, STEPS_MAX_DISCOVERED_LINKS),
  }));
}

export async function saveExtract(id: string, extract: StoredExtract) {
  _logger.debug("Saving extract " + id + "...");
  // Only store essential data
  await extracts.save(id, { ...extract, steps: minimalSteps(extract.steps) });
}

export async function getExtract(id: string): Promise<StoredExtract | null> {
  return extracts.get(id);
}

export async function updateExtract(
  id: string,
  extract: Partial<StoredExtract>,
) {
  await extracts.update(id, current => {
    // Keep only the last 5 steps to prevent unbounded growth. The caller's
    // patch is not mutated, so a conflicting update can be re-merged.
    let steps = extract.steps;
    if (steps && current.steps) {
      const allSteps = [...current.steps, ...steps];
      steps = allSteps.slice(Math.max(0, allSteps.length - 5));
    }
    return { ...current, ...extract, steps: minimalSteps(steps) };
  });
}

export async function getExtractExpiry(id: string): Promise<Date> {
  return extracts.expiry(id);
}

export async function saveExtractResult(
  id: string,
  result: any,
): Promise<void> {
  _logger.debug("Saving extract result " + id + "...");
  await results.save(id, result);
}

export async function getExtractResult(id: string): Promise<any | null> {
  return results.get(id);
}
