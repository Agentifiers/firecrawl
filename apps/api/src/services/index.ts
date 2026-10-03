import { logger as _logger } from "../lib/logger";
import {
  queryIndexAtSplitLevel as rpcQueryIndexAtSplitLevel,
  queryIndexAtDomainSplitLevel as rpcQueryIndexAtDomainSplitLevel,
  queryOmceSignatures as rpcQueryOmceSignatures,
  queryEngpickerVerdict as rpcQueryEngpickerVerdict,
  queryIndexAtSplitLevelWithMeta as rpcQueryIndexAtSplitLevelWithMeta,
  queryIndexAtDomainSplitLevelWithMeta as rpcQueryIndexAtDomainSplitLevelWithMeta,
  queryDomainPriority as rpcQueryDomainPriority,
} from "../db/rpc";
import { configDotenv } from "dotenv";
import { ApiError } from "@google-cloud/storage";
import crypto from "crypto";
import type { Logger } from "winston";
import psl from "psl";
import { MapDocument } from "../controllers/v2/types";
import type { PdfMetadata } from "../scraper/scrapeURL/engines/pdf/types";
import { storage } from "../lib/gcs-jobs";
import { withSpan, setSpanAttributes } from "../lib/otel-tracer";
import { config } from "../config";
import { getGcsScreenshotUrlResignReason } from "./index-screenshot-url";
configDotenv();

export async function getIndexFromGCS(
  url: string,
  logger?: Logger,
  opts: { indexCreatedAt?: string | null } = {},
): Promise<any | null> {
  try {
    return await withSpan("firecrawl-index-get-from-gcs", async span => {
      setSpanAttributes(span, {
        "index.operation": "get_from_gcs",
        "index.url": url,
      });

      if (!config.GCS_INDEX_BUCKET_NAME) {
        setSpanAttributes(span, { "gcs.index_bucket_configured": false });
        return null;
      }

      const bucket = storage.bucket(config.GCS_INDEX_BUCKET_NAME);
      const blob = bucket.file(`${url}`);
      const [blobContent] = await blob.download();
      const parsed = JSON.parse(blobContent.toString());

      if (typeof parsed.screenshot === "string") {
        try {
          const screenshotUrl = new URL(parsed.screenshot);
          const resignReason = getGcsScreenshotUrlResignReason(screenshotUrl, {
            indexCreatedAt: opts.indexCreatedAt,
          });
          if (resignReason !== null) {
            logger?.info("Re-signing screenshot URL", { reason: resignReason });
            const filePath = decodeURIComponent(
              screenshotUrl.pathname.split("/")[2],
            );
            const [newUrl] = await storage
              .bucket(config.GCS_MEDIA_BUCKET_NAME!)
              .file(filePath)
              .getSignedUrl({
                action: "read",
                expires: Date.now() + 1000 * 60 * 60 * 24 * 7,
              });
            parsed.screenshot = newUrl;

            // Persist the re-signed URL back to GCS in the background
            blob
              .save(JSON.stringify(parsed), {
                contentType: "application/json",
              })
              .catch(error => {
                logger?.warn("Error persisting re-signed screenshot URL", {
                  error,
                  url,
                });
              });
          }
        } catch (error) {
          logger?.warn("Error parsing screenshot URL for re-signing", {
            error,
            url,
          });
        }
      }

      setSpanAttributes(span, { "index.document_found": true });
      return parsed;
    });
  } catch (error) {
    if (
      error instanceof ApiError &&
      error.code === 404 &&
      error.message.includes("No such object:")
    ) {
      return null;
    }

    (logger ?? _logger).error(`Error getting Index document from GCS`, {
      error,
      url,
    });
    return null;
  }
}

export async function saveIndexToGCS(
  id: string,
  doc: {
    url: string;
    html: string;
    json?: unknown;
    statusCode: number;
    error?: string;
    screenshot?: string;
    pdfMetadata?: PdfMetadata;
    contentType?: string;
    postprocessorsUsed?: string[];
    proxyUsed?: "basic" | "stealth";
  },
): Promise<void> {
  return await withSpan("firecrawl-index-save-to-gcs", async span => {
    setSpanAttributes(span, {
      "index.operation": "save_to_gcs",
      "index.id": id,
      "index.url": doc.url,
      "index.status_code": doc.statusCode,
      "index.has_error": !!doc.error,
    });

    if (!config.GCS_INDEX_BUCKET_NAME) {
      setSpanAttributes(span, { "gcs.index_bucket_configured": false });
      return;
    }

    const bucket = storage.bucket(config.GCS_INDEX_BUCKET_NAME);
    const blob = bucket.file(`${id}.json`);

    for (let i = 0; i < 3; i++) {
      try {
        await blob.save(JSON.stringify(doc), {
          contentType: "application/json",
        });
        setSpanAttributes(span, { "index.save_successful": true });
        break;
      } catch (error) {
        if (i === 2) {
          throw error;
        } else {
          _logger.error(`Error saving index document to GCS, retrying`, {
            error,
            indexId: id,
            i,
          });
        }
      }
    }
  });
}

export const useIndex =
  config.INDEX_DATABASE_URL !== "" && config.INDEX_DATABASE_URL !== undefined;

export const useSearchIndex =
  config.SEARCH_SERVICE_URL !== "" && config.SEARCH_SERVICE_URL !== undefined;

export function normalizeURLForIndex(url: string): string {
  const urlObj = new URL(url);

  if (
    !urlObj.hash ||
    urlObj.hash.length <= 2 ||
    (!urlObj.hash.startsWith("#/") && !urlObj.hash.startsWith("#!/"))
  ) {
    urlObj.hash = "";
  }

  urlObj.protocol = "https";

  if (urlObj.port === "80" || urlObj.port === "443") {
    urlObj.port = "";
  }

  if (urlObj.hostname.startsWith("www.")) {
    urlObj.hostname = urlObj.hostname.slice(4);
  }

  if (urlObj.pathname.endsWith("/index.html")) {
    urlObj.pathname = urlObj.pathname.slice(0, -10);
  } else if (urlObj.pathname.endsWith("/index.php")) {
    urlObj.pathname = urlObj.pathname.slice(0, -9);
  } else if (urlObj.pathname.endsWith("/index.htm")) {
    urlObj.pathname = urlObj.pathname.slice(0, -9);
  } else if (urlObj.pathname.endsWith("/index.shtml")) {
    urlObj.pathname = urlObj.pathname.slice(0, -11);
  } else if (urlObj.pathname.endsWith("/index.xml")) {
    urlObj.pathname = urlObj.pathname.slice(0, -9);
  }

  if (urlObj.pathname.endsWith("/")) {
    urlObj.pathname = urlObj.pathname.slice(0, -1);
  }

  return urlObj.toString();
}

export function hashURL(url: string): Buffer {
  return crypto.createHash("sha256").update(url).digest();
}

export function generateURLSplits(url: string): string[] {
  const urls: string[] = [];
  const urlObj = new URL(url);
  urlObj.hash = "";
  urlObj.search = "";
  const pathnameParts = urlObj.pathname.split("/");

  for (let i = 0; i <= pathnameParts.length; i++) {
    urlObj.pathname = pathnameParts.slice(0, i).join("/");
    urls.push(urlObj.href);
  }

  urls.push(url);

  return [...new Set(urls.map(x => normalizeURLForIndex(x)))];
}

export function generateDomainSplits(
  hostname: string,
  fakeDomain?: string,
): string[] {
  if (fakeDomain) {
    const parsed = psl.parse(hostname);
    if (parsed === null) return [fakeDomain];

    const fakeParsed = psl.parse(fakeDomain);
    if (fakeParsed === null || fakeParsed.domain === null) return [fakeDomain];

    const subdomains: string[] = (fakeParsed.subdomain ?? "")
      .split(".")
      .filter(x => x !== "");
    if (subdomains.length === 1 && subdomains[0] === "www") {
      return [fakeParsed.domain];
    }

    const domains: string[] = [];
    for (let i = subdomains.length; i >= 0; i--) {
      domains.push(subdomains.slice(i).concat([fakeParsed.domain]).join("."));
    }

    return domains;
  }

  const parsed = psl.parse(hostname);
  if (parsed === null) {
    return [];
  }

  const subdomains: string[] = (parsed.subdomain ?? "")
    .split(".")
    .filter(x => x !== "");
  if (subdomains.length === 1 && subdomains[0] === "www") {
    return [parsed.domain];
  }

  const domains: string[] = [];
  for (let i = subdomains.length; i >= 0; i--) {
    domains.push(subdomains.slice(i).concat([parsed.domain]).join("."));
  }

  return domains;
}

// Deployments.ai: the Redis index-insert/OMCE buffers drained by the hosted
// index worker are removed; sendDocumentToIndex writes to the index DB directly.
// The Dragonfly index cache (services/index-cache.ts) is removed as well.

export async function queryIndexAtSplitLevel(
  url: string,
  limit: number,
  maxAge = 2 * 24 * 60 * 60 * 1000,
): Promise<string[]> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return [];
  }

  const urlObj = new URL(url);
  urlObj.search = "";

  const urlSplitsHash = generateURLSplits(urlObj.href).map(x => hashURL(x));

  const level = urlSplitsHash.length - 1;

  // Raw SQL returns the full result set (no PostgREST 1000-row cap), so the
  // previous .range() pagination loop is no longer needed.
  try {
    const data = await rpcQueryIndexAtSplitLevel(
      level,
      urlSplitsHash[level],
      new Date(Date.now() - maxAge).toISOString(),
    );
    const links = new Set<string>();
    data.forEach(x => links.add(x.resolved_url));
    return [...links].slice(0, limit);
  } catch (error) {
    _logger.warn("Error querying index", { error, url, limit });
    return [];
  }
}

export async function queryIndexAtDomainSplitLevel(
  hostname: string,
  limit: number,
  maxAge = 2 * 24 * 60 * 60 * 1000,
): Promise<string[]> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return [];
  }

  const domainSplitsHash = generateDomainSplits(hostname).map(x => hashURL(x));

  const level = domainSplitsHash.length - 1;
  if (domainSplitsHash.length === 0) {
    return [];
  }

  // Raw SQL returns the full result set (no PostgREST 1000-row cap), so the
  // previous .range() pagination loop is no longer needed.
  try {
    const data = await rpcQueryIndexAtDomainSplitLevel(
      level,
      domainSplitsHash[level],
      new Date(Date.now() - maxAge).toISOString(),
    );
    const links = new Set<string>();
    data.forEach(x => links.add(x.resolved_url));
    return [...links].slice(0, limit);
  } catch (error) {
    _logger.warn("Error querying index", { error, hostname, limit });
    return [];
  }
}

export async function queryOMCESignatures(
  hostname: string,
  maxAge = 2 * 24 * 60 * 60 * 1000,
): Promise<string[]> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return [];
  }

  const domainSplitsHash = generateDomainSplits(hostname).map(x => hashURL(x));

  const level = domainSplitsHash.length - 1;
  if (domainSplitsHash.length === 0) {
    return [];
  }

  try {
    const data = await rpcQueryOmceSignatures(
      domainSplitsHash[level],
      new Date(Date.now() - maxAge).toISOString(),
    );
    return data?.[0]?.signatures ?? [];
  } catch (error) {
    _logger.warn("Error querying index (omce)", { error, hostname });
    return [];
  }
}

export async function queryEngpickerVerdict(
  hostname: string,
): Promise<"TlsClientOk" | "ChromeCdpRequired" | "Uncertain" | "Unknown"> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return "Unknown";
  }

  const domainSplitsHash = generateDomainSplits(hostname).map(x => hashURL(x));

  const level = domainSplitsHash.length - 1;
  if (domainSplitsHash.length === 0) {
    return "Unknown";
  }

  // 250ms max time taken
  try {
    const data = await Promise.race([
      rpcQueryEngpickerVerdict(domainSplitsHash[level]),
      new Promise<{ verdict: string }[]>(resolve =>
        setTimeout(() => resolve([{ verdict: "Unknown" }]), 250),
      ),
    ]);

    return (data?.[0]?.verdict ?? "Unknown") as
      | "TlsClientOk"
      | "ChromeCdpRequired"
      | "Uncertain"
      | "Unknown";
  } catch (error) {
    _logger.warn("Error querying index (engpicker)", {
      error,
      hostname,
    });
    return "Unknown";
  }
}

export async function queryIndexAtSplitLevelWithMeta(
  url: string,
  limit: number,
): Promise<MapDocument[]> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return [];
  }

  const urlObj = new URL(url);
  urlObj.search = "";

  const urlSplitsHash = generateURLSplits(urlObj.href).map(x => hashURL(x));

  const level = urlSplitsHash.length - 1;

  // Raw SQL returns the full result set (no PostgREST 1000-row cap), so the
  // previous .range() pagination loop is no longer needed.
  try {
    const data = await rpcQueryIndexAtSplitLevelWithMeta(
      level,
      urlSplitsHash[level],
      new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
    );
    const links: MapDocument[] = data.map(x => ({
      url: x.resolved_url,
      title: x.title ?? undefined,
      description: x.description ?? undefined,
    }));
    return links.slice(0, limit);
  } catch (error) {
    _logger.warn("Error querying index", { error, url, limit });
    return [];
  }
}

export async function queryIndexAtDomainSplitLevelWithMeta(
  hostname: string,
  limit: number,
): Promise<MapDocument[]> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return [];
  }

  const domainSplitsHash = generateDomainSplits(hostname).map(x => hashURL(x));

  const level = domainSplitsHash.length - 1;
  if (domainSplitsHash.length === 0) {
    return [];
  }

  // Raw SQL returns the full result set (no PostgREST 1000-row cap), so the
  // previous .range() pagination loop is no longer needed.
  try {
    const data = await rpcQueryIndexAtDomainSplitLevelWithMeta(
      level,
      domainSplitsHash[level],
      new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString(),
    );
    const links: MapDocument[] = data.map(x => ({
      url: x.resolved_url,
      title: x.title ?? undefined,
      description: x.description ?? undefined,
    }));
    return links.slice(0, limit);
  } catch (error) {
    _logger.warn("Error querying index", { error, hostname, limit });
    return [];
  }
}

type DomainPriority = {
  domain_hash: Buffer;
  priority: number;
};

export async function queryDomainsForPrecrawl(
  date: Date,
  minEvents = 20,
  minPriority = 0.5,
  maxDomains = 50,
  logger: Logger = _logger,
): Promise<DomainPriority[]> {
  if (!useIndex || config.FIRECRAWL_INDEX_WRITE_ONLY) {
    return [];
  }

  // Raw SQL returns the full result set (no PostgREST 1000-row cap), so the
  // previous .range() pagination loop is no longer needed.
  try {
    const data = await rpcQueryDomainPriority(
      minEvents,
      minPriority,
      maxDomains,
      date.toISOString(),
    );
    return data.slice(0, maxDomains);
  } catch (error) {
    logger.error("Error getting domain priorities", { error });
    return [];
  }
}
