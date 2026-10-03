// Deployments.ai modification: llms.txt generation status is stored in
// DynamoDB (`llmstxt#<id>`, sk `status`, 24h) instead of Redis; see
// ../status-document.ts. Module name and API are upstream's (the unused
// upstream `updateGeneratedLlmsTxtStatus` helper was dropped).
import { logger as _logger } from "../logger";
import { statusDocumentStore } from "../status-document";

interface GenerationData {
  id: string;
  team_id: string;
  createdAt: number;
  status: "processing" | "completed" | "failed";
  url: string;
  maxUrls: number;
  showFullText: boolean;
  cache?: boolean;
  generatedText: string;
  fullText: string;
  error?: string;
}

// TTL of 24 hours
const GENERATION_TTL = 24 * 60 * 60;

const generations = statusDocumentStore<GenerationData>(
  "llmstxt",
  "status",
  GENERATION_TTL,
);

export async function saveGeneratedLlmsTxt(
  id: string,
  data: GenerationData,
): Promise<void> {
  _logger.debug("Saving llmstxt generation " + id + "...");
  await generations.save(id, data);
}

export async function getGeneratedLlmsTxt(
  id: string,
): Promise<GenerationData | null> {
  return generations.get(id);
}

export async function updateGeneratedLlmsTxt(
  id: string,
  data: Partial<GenerationData>,
): Promise<void> {
  await generations.update(id, current => ({ ...current, ...data }));
}

export async function getGeneratedLlmsTxtExpiry(id: string): Promise<Date> {
  return generations.expiry(id);
}
