// Deployments.ai modification: deep research status is stored in DynamoDB
// (`deep-research#<id>`, sk `status`, 6h) instead of Redis; see
// ../status-document.ts. Module name and API are upstream's.
import { logger as _logger } from "../logger";
import { statusDocumentStore } from "../status-document";

export type DeepResearchActivity = {
  type:
    | "search"
    | "extract"
    | "analyze"
    | "reasoning"
    | "synthesis"
    | "thought";
  status: "processing" | "complete" | "error";
  message: string;
  timestamp: string;
  depth: number;
};

export type DeepResearchSource = {
  url: string;
  title: string;
  description: string;
};

export type DeepResearchFinding = {
  text: string;
  source: string;
};

export type StoredDeepResearch = {
  id: string;
  team_id: string;
  createdAt: number;
  status: "processing" | "completed" | "failed" | "cancelled";
  error?: any;
  currentDepth: number;
  maxDepth: number;
  completedSteps: number;
  totalExpectedSteps: number;
  findings: DeepResearchFinding[];
  sources: DeepResearchSource[];
  activities: DeepResearchActivity[];
  summaries: string[];
  finalAnalysis?: string;
  json?: any;
};

// TTL of 6 hours
const DEEP_RESEARCH_TTL = 6 * 60 * 60;

const research = statusDocumentStore<StoredDeepResearch>(
  "deep-research",
  "status",
  DEEP_RESEARCH_TTL,
);

export async function saveDeepResearch(
  id: string,
  doc: StoredDeepResearch,
) {
  _logger.debug("Saving deep research " + id + "...");
  await research.save(id, doc);
}

export async function getDeepResearch(
  id: string,
): Promise<StoredDeepResearch | null> {
  return research.get(id);
}

export async function updateDeepResearch(
  id: string,
  patch: Partial<StoredDeepResearch>,
) {
  await research.update(id, current => ({
    ...current,
    ...patch,
    // Append new activities, sources, and summaries if provided
    activities: patch.activities
      ? [...(current.activities || []), ...patch.activities]
      : current.activities,
    sources: patch.sources
      ? [...(current.sources || []), ...patch.sources]
      : current.sources,
    summaries: patch.summaries
      ? [...(current.summaries || []), ...patch.summaries]
      : current.summaries,
  }));
}

export async function getDeepResearchExpiry(id: string): Promise<Date> {
  return research.expiry(id);
}
