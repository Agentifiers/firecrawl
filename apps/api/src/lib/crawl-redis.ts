// Deployments.ai modification: the crawl store lives in DynamoDB instead of
// Redis. The module keeps upstream's name and exported API so callers are
// unchanged. Item layout, all under partition `crawl#<id>`:
//
//   meta                       crawl document (`doc`, gzip, chunked under
//                              `doc#<ref>#<n>` when large), flags
//                              (kickoffFinished, finished, error), and the
//                              counters standing in for SCARD: jobs,
//                              jobsQualified, jobsDone, doneOrdered,
//                              visitedUnique, sitemaps, sitemapJobs,
//                              sitemapJobsDone
//   job#/jq#/jd#<sha256>       crawl:<id>:jobs / jobs_qualified / jobs_done
//   donez#<doneAt>#<jobId>     crawl:<id>:jobs_donez_ordered (ZSET by time);
//                              the jd# item points at its current entry
//   visited#/vu#<sha256>       crawl:<id>:visited / visited_unique
//   rb#/tb#<sha256>            robots_blocked set / threat_blocked hash
//   sm#/sj#/sjd#<sha256>       sitemaps / sitemap_jobs / sitemap_jobs_done
//
// Every item carries a 24h `ttl` refreshed when it is written (Redis also
// refreshed some keys on read; reads here never write). Counters change in
// the same transaction as the member item they count, and the URL limit and
// the 20-sitemap cap are enforced atomically (upstream checked, then added).
import { randomUUID } from "node:crypto";
import { InternalOptions } from "../scraper/scrapeURL";
import { ScrapeOptions, TeamFlags } from "../controllers/v2/types";
import { WebhookConfig } from "../services/webhook/types";
import { WebCrawler } from "../scraper/WebScraper/crawler";
import { logger as _logger } from "./logger";
import { getAdjustedMaxDepth } from "../scraper/WebScraper/utils/maxDepthUtils";
import type { Logger } from "winston";
import { withSpan, setSpanAttributes } from "./otel-tracer";
import { getScrapeZDR, getIgnoreRobots } from "./zdr-helpers";
import { generateURLPermutations, normalizeURL } from "./crawl-urls";
import {
  isLogicallyExpired,
  sortableNumber,
  tableName,
  transactionCancellationCodes,
} from "../services/dynamo/client";
import {
  packPayload,
  payloadChunkSortKeys,
  readPayload,
  writePayloadChunks,
} from "../services/dynamo/payload";
import {
  absentOrExpired,
  addMembers,
  CONDITIONAL_CHECK_FAILED,
  counterDelta,
  deleteKeys,
  deletePrefix,
  getItem,
  listMembers,
  memberSortKey,
  nowSeconds,
  prefixEnd,
  queryLive,
  removeMember,
  transactWrite,
  updateItem,
} from "./crawl-dynamo";

export type StoredCrawl = {
  originUrl?: string;
  crawlerOptions: any;
  scrapeOptions: Omit<ScrapeOptions, "timeout">;
  internalOptions: InternalOptions;
  team_id: string;
  robots?: string;
  cancelled?: boolean;
  createdAt: number;
  maxConcurrency?: number;
  zeroDataRetention?: boolean;
  // Crawl-scoped context the finish path needs (completion webhook, api version,
  // request id). Persisted here so it survives even when a member job's input
  // data has been shed.
  v1?: boolean;
  webhook?: WebhookConfig;
  requestId?: string;
};

const CRAWL_TTL_SECONDS = 24 * 60 * 60;
const META = "meta";
const DOC_CHUNKS = "doc";
const MAX_SITEMAPS = 20;

const crawlPk = (id: string) => "crawl#" + id;
const metaKey = (id: string) => ({ pk: crawlPk(id), sk: META });
const crawlTtl = () => nowSeconds() + CRAWL_TTL_SECONDS;
const counter = (attribute: string, ttl: number) => ({ sk: META, attribute, ttl });
const donezKey = (doneAt: number, jobId: string) =>
  `donez#${sortableNumber(doneAt)}#${jobId}`;

async function readMeta(id: string) {
  const item = await getItem(metaKey(id));
  return item && !isLogicallyExpired(item) ? item : undefined;
}

async function addCrawlSetMembers(
  id: string,
  prefix: string,
  members: readonly string[],
  counterAttribute?: string,
  attributes?: Record<string, unknown>,
) {
  const ttl = crawlTtl();
  return addMembers({
    pk: crawlPk(id),
    prefix,
    members,
    ttl,
    ...(counterAttribute ? { counter: counter(counterAttribute, ttl) } : {}),
    ...(attributes ? { attributes } : {}),
  });
}

export async function saveCrawl(id: string, crawl: StoredCrawl) {
  return await withSpan("firecrawl-redis-save-crawl", async span => {
    setSpanAttributes(span, {
      "crawl.id": id,
      "crawl.team_id": crawl.team_id,
      "crawl.zero_data_retention": crawl.zeroDataRetention || false,
      operation: "save_crawl",
    });

    _logger.debug("Saving crawl " + id + "...", {
      crawl,
      module: "crawl-redis",
      method: "saveCrawl",
      crawlId: id,
      teamId: crawl.team_id,
      zeroDataRetention: crawl.zeroDataRetention,
    });

    // An update (not a put) so the counters survive re-saves such as cancel.
    const ttl = crawlTtl();
    const packed = packPayload(crawl, randomUUID());
    await writePayloadChunks({ pk: crawlPk(id), prefix: DOC_CHUNKS, packed, ttl });
    const old = await updateItem(
      metaKey(id),
      { set: { doc: packed.attribute, ttl } },
      true,
    );
    const stale = payloadChunkSortKeys(DOC_CHUNKS, old?.doc);
    if (stale.length > 0) await deleteKeys(crawlPk(id), stale);
  });
}

export async function recordRobotsBlocked(crawlId: string, url: string) {
  await addCrawlSetMembers(crawlId, "rb#", [url]);
}

/** URLs the crawl skipped because robots.txt disallowed them. */
export async function getRobotsBlocked(crawlId: string): Promise<string[]> {
  return listMembers(crawlPk(crawlId), "rb#");
}

/**
 * Records a URL that was silently skipped during crawl link discovery because
 * it was blocked by the team's threat protection policy. Returns true only
 * for the first record of a URL within this crawl (upstream HSETNX), which
 * doubles as the crawl-scoped billing dedup for blocked discoveries. The
 * records are deleted eagerly in {@link finishCrawl}.
 */
export async function recordThreatBlocked(
  crawlId: string,
  url: string,
  decision: unknown,
): Promise<boolean> {
  const [isNew] = await addCrawlSetMembers(crawlId, "tb#", [url], undefined, {
    decision: JSON.stringify(decision),
  });
  return isNew;
}

export async function getCrawl(id: string): Promise<StoredCrawl | null> {
  return await withSpan("firecrawl-redis-get-crawl", async span => {
    setSpanAttributes(span, { "crawl.id": id, operation: "get_crawl" });
    const meta = await readMeta(id);
    const crawl =
      meta?.doc !== undefined
        ? await readPayload<StoredCrawl>({
            pk: crawlPk(id),
            prefix: DOC_CHUNKS,
            stored: meta.doc,
          })
        : undefined;
    if (!crawl) {
      setSpanAttributes(span, { "crawl.found": false });
      return null;
    }
    setSpanAttributes(span, {
      "crawl.found": true,
      "crawl.team_id": crawl.team_id,
    });
    return crawl;
  });
}

export async function getCrawlExpiry(id: string): Promise<Date> {
  const meta = await readMeta(id);
  const d = new Date(typeof meta?.ttl === "number" ? meta.ttl * 1000 : Date.now());
  d.setMilliseconds(0);
  return d;
}

export async function addCrawlJob(
  id: string,
  job_id: string,
  __logger: Logger = _logger,
) {
  return addCrawlJobs(id, [job_id], __logger);
}

export async function addCrawlJobs(
  id: string,
  job_ids: string[],
  __logger: Logger = _logger,
) {
  if (job_ids.length === 0) return true;
  __logger.debug("Adding crawl jobs...", {
    jobIds: job_ids,
    module: "crawl-redis",
    method: "addCrawlJobs",
    crawlId: id,
  });
  await addCrawlSetMembers(id, "job#", job_ids, "jobs");
  await addCrawlSetMembers(id, "jq#", job_ids, "jobsQualified");
  return true;
}

/**
 * Marks a job done (SADD jobs_done) and, for a success, (re)places it in the
 * done-ordered list at the current time; a failure removes it from that list
 * (upstream ZADD / ZREM). The jd# item points at the job's donez# entry and
 * is updated under an optimistic check, so concurrent calls never leave two
 * entries for one job.
 */
export async function addCrawlJobDone(
  id: string,
  job_id: string,
  success: boolean,
  __logger: Logger = _logger,
) {
  __logger.debug("Adding done crawl job...", {
    jobId: job_id,
    module: "crawl-redis",
    method: "addCrawlJobDone",
    crawlId: id,
  });
  const pk = crawlPk(id);
  await addCrawlSetMembers(id, "jd#", [job_id], "jobsDone");
  const doneSk = memberSortKey("jd#", job_id);
  for (let attempt = 0; attempt < 25; attempt++) {
    const ttl = crawlTtl();
    const done = await getItem({ pk, sk: doneSk });
    const previous = typeof done?.donez === "string" ? done.donez : undefined;
    if (!success && previous === undefined) return;
    const next = success ? donezKey(Date.now(), job_id) : undefined;
    const pointer = {
      Update: {
        TableName: tableName(),
        Key: { pk, sk: doneSk },
        UpdateExpression: next
          ? "SET donez = :next, #ttl = :ttl"
          : "SET #ttl = :ttl REMOVE donez",
        ConditionExpression: previous
          ? "donez = :previous"
          : "attribute_not_exists(donez)",
        ExpressionAttributeNames: { "#ttl": "ttl" },
        ExpressionAttributeValues: {
          ":ttl": ttl,
          ...(next ? { ":next": next } : {}),
          ...(previous ? { ":previous": previous } : {}),
        },
      },
    };
    const items: Record<string, unknown>[] = [pointer];
    if (next) {
      items.push({
        Put: {
          TableName: tableName(),
          Item: { pk, sk: next, member: job_id, doneAt: Date.now(), ttl },
        },
      });
    }
    if (previous) {
      items.push({ Delete: { TableName: tableName(), Key: { pk, sk: previous } } });
    }
    const delta = (next ? 1 : 0) - (previous ? 1 : 0);
    if (delta !== 0) items.push(counterDelta(pk, counter("doneOrdered", ttl), delta));
    try {
      await transactWrite(items);
      return;
    } catch (error) {
      if (transactionCancellationCodes(error)?.[0] !== CONDITIONAL_CHECK_FAILED) {
        throw error;
      }
    }
  }
  throw new Error(`Could not record crawl job ${job_id} as done.`);
}

const donezUntil = (until: number) =>
  until === Infinity ? prefixEnd("donez#") : prefixEnd(`donez#${sortableNumber(until)}#`);

export async function getDoneJobsOrderedLength(
  id: string,
  until: number = Infinity,
): Promise<number> {
  if (until === Infinity) {
    const meta = await readMeta(id);
    return typeof meta?.doneOrdered === "number" ? meta.doneOrdered : 0;
  }
  return queryLive({
    pk: crawlPk(id),
    from: "donez#",
    to: donezUntil(until),
    count: true,
  });
}

export async function getLastDoneJobTimestamp(
  id: string,
): Promise<number | null> {
  const [last] = await queryLive({
    pk: crawlPk(id),
    from: "donez#",
    to: prefixEnd("donez#"),
    forward: false,
    stopAfter: 1,
  });
  return typeof last?.doneAt === "number" ? last.doneAt : null;
}

/** ZRANGEBYSCORE -inf..until LIMIT start count (count -1: all). */
export async function getDoneJobsOrderedUntil(
  id: string,
  until: number = Infinity,
  start = 0,
  count = -1,
): Promise<string[]> {
  const items = await queryLive({
    pk: crawlPk(id),
    from: "donez#",
    to: donezUntil(until),
    ...(count >= 0 ? { stopAfter: start + count } : {}),
  });
  return items
    .slice(start, count >= 0 ? start + count : undefined)
    .map(item => String(item.member));
}

export async function isCrawlKickoffFinished(id: string) {
  const meta = await readMeta(id);
  return (
    meta?.kickoffFinished === true &&
    (meta.sitemapJobsDone ?? 0) === (meta.sitemapJobs ?? 0)
  );
}

export async function finishCrawlKickoff(id: string) {
  await updateItem(metaKey(id), { set: { kickoffFinished: true, ttl: crawlTtl() } });
}

export async function setCrawlError(id: string, error: string) {
  await updateItem(metaKey(id), { set: { error, ttl: crawlTtl() } });
}

export async function getCrawlError(id: string): Promise<string | null> {
  const meta = await readMeta(id);
  return typeof meta?.error === "string" ? meta.error : null;
}

export async function finishCrawl(id: string, __logger: Logger = _logger) {
  __logger.debug("Marking crawl as finished.", {
    module: "crawl-redis",
    method: "finishCrawl",
    crawlId: id,
  });
  await updateItem(metaKey(id), {
    set: { finished: true, visitedUnique: 0, ttl: crawlTtl() },
  });
  // Drop the visited sets and the threat-protection records (URL -> decision
  // of silently skipped discoveries) eagerly instead of waiting for their
  // TTL; nothing reads them after the crawl finishes, and deleting them
  // unconditionally keeps the ZDR guarantee.
  const pk = crawlPk(id);
  await deletePrefix(pk, "visited#");
  await deletePrefix(pk, "vu#");
  await deletePrefix(pk, "tb#");
}

export async function getCrawlJobs(id: string): Promise<string[]> {
  return listMembers(crawlPk(id), "job#");
}

export async function getCrawlQualifiedJobCount(id: string): Promise<number> {
  const meta = await readMeta(id);
  return typeof meta?.jobsQualified === "number" ? meta.jobsQualified : 0;
}

/** Marks redirect permutations visited; returns how many were new (SADD). */
export async function addVisitedURLs(id: string, urls: string[]): Promise<number> {
  const added = await addCrawlSetMembers(id, "visited#", urls);
  return added.filter(Boolean).length;
}

/** Releases a failed job's URL and qualification (upstream SREMs). */
export async function releaseFailedCrawlJob(
  id: string,
  normalizedUrl: string,
  jobId: string,
) {
  const ttl = crawlTtl();
  const pk = crawlPk(id);
  await removeMember({
    pk,
    prefix: "vu#",
    member: normalizedUrl,
    counter: counter("visitedUnique", ttl),
  });
  await removeMember({
    pk,
    prefix: "jq#",
    member: jobId,
    counter: counter("jobsQualified", ttl),
  });
}

/** Claims a sitemap for a crawl, at most MAX_SITEMAPS per crawl. */
export async function lockCrawlSitemap(
  id: string,
  sitemapUrl: string,
): Promise<"locked" | "duplicate" | "limit"> {
  const pk = crawlPk(id);
  const ttl = crawlTtl();
  try {
    await transactWrite([
      {
        Put: {
          TableName: tableName(),
          Item: { pk, sk: memberSortKey("sm#", sitemapUrl), member: sitemapUrl, ttl },
          ...absentOrExpired(),
        },
      },
      {
        Update: {
          TableName: tableName(),
          Key: metaKey(id),
          UpdateExpression: "SET #ttl = :ttl ADD sitemaps :one",
          ConditionExpression: "attribute_not_exists(sitemaps) OR sitemaps < :max",
          ExpressionAttributeNames: { "#ttl": "ttl" },
          ExpressionAttributeValues: { ":ttl": ttl, ":one": 1, ":max": MAX_SITEMAPS },
        },
      },
    ]);
    return "locked";
  } catch (error) {
    const codes = transactionCancellationCodes(error);
    if (codes?.[1] === CONDITIONAL_CHECK_FAILED) return "limit";
    if (codes?.[0] === CONDITIONAL_CHECK_FAILED) return "duplicate";
    throw error;
  }
}

export async function addCrawlSitemapJob(id: string, jobId: string) {
  await addCrawlSetMembers(id, "sj#", [jobId], "sitemapJobs");
}

export async function addCrawlSitemapJobDone(id: string, jobId: string) {
  await addCrawlSetMembers(id, "sjd#", [jobId], "sitemapJobsDone");
}

const visitedMember = (sc: StoredCrawl, normalizedUrl: string) =>
  sc.crawlerOptions?.deduplicateSimilarURLs
    ? generateURLPermutations(normalizedUrl)[0].href
    : normalizedUrl;

/**
 * Locks a URL for the crawl: true when neither it (or, with
 * `deduplicateSimilarURLs`, its canonical permutation) was visited nor the
 * crawl's URL limit is reached. One transaction makes the limit exact under
 * concurrent workers.
 */
export async function lockURL(
  id: string,
  sc: StoredCrawl,
  url: string,
): Promise<boolean> {
  return await withSpan("firecrawl-redis-lock-url", async span => {
    const normalizedUrl = normalizeURL(url, sc);
    setSpanAttributes(span, {
      "crawl.id": id,
      "crawl.url": normalizedUrl,
      "crawl.team_id": sc.team_id,
      operation: "lock_url",
    });
    const limit =
      typeof sc.crawlerOptions?.limit === "number" ? sc.crawlerOptions.limit : null;
    if (limit !== null && limit <= 0) {
      setSpanAttributes(span, { "crawl.limit_reached": true });
      return false;
    }
    const pk = crawlPk(id);
    const ttl = crawlTtl();
    const memberPut = (prefix: string, member: string) => ({
      Put: {
        TableName: tableName(),
        Item: { pk, sk: memberSortKey(prefix, member), member, ttl },
        ...absentOrExpired(),
      },
    });
    const limitValues = limit !== null ? { ":limit": limit } : {};
    const limitCondition =
      limit !== null ? "attribute_not_exists(visitedUnique) OR visitedUnique < :limit" : undefined;
    const visited = memberPut("visited#", visitedMember(sc, normalizedUrl));
    const unique = [
      memberPut("vu#", normalizedUrl),
      {
        Update: {
          TableName: tableName(),
          Key: metaKey(id),
          UpdateExpression: "SET #ttl = :ttl ADD visitedUnique :one",
          ExpressionAttributeNames: { "#ttl": "ttl" },
          ExpressionAttributeValues: { ":ttl": ttl, ":one": 1, ...limitValues },
          ...(limitCondition ? { ConditionExpression: limitCondition } : {}),
        },
      },
    ];
    let locked = false;
    try {
      await transactWrite([visited, ...unique]);
      locked = true;
    } catch (error) {
      const codes = transactionCancellationCodes(error);
      if (!codes) throw error;
      // Visited already, or the limit is reached: not locked. Only the
      // unique-URL member existing (lockURLs added it) still locks.
      if (codes[0] !== CONDITIONAL_CHECK_FAILED && codes[2] !== CONDITIONAL_CHECK_FAILED) {
        const check = limitCondition
          ? [
              {
                ConditionCheck: {
                  TableName: tableName(),
                  Key: metaKey(id),
                  ConditionExpression: limitCondition,
                  ExpressionAttributeValues: limitValues,
                },
              },
            ]
          : [];
        try {
          await transactWrite([visited, ...check]);
          locked = true;
        } catch (retryError) {
          if (!transactionCancellationCodes(retryError)) throw retryError;
        }
      }
    }
    setSpanAttributes(span, { "crawl.url_locked": locked });
    return locked;
  });
}

/// NOTE: does not check limit. only use if limit is checked beforehand e.g. with sitemap
export async function lockURLs(
  id: string,
  sc: StoredCrawl,
  urls: string[],
  __logger: Logger = _logger,
): Promise<boolean> {
  if (urls.length === 0) return true;

  urls = urls.map(url => normalizeURL(url, sc));
  const logger = __logger.child({
    crawlId: id,
    module: "crawl-redis",
    method: "lockURLs",
    teamId: sc.team_id,
  });
  logger.debug("Locking " + urls.length + " URLs...");
  await addCrawlSetMembers(id, "vu#", urls, "visitedUnique");
  const added = await addCrawlSetMembers(
    id,
    "visited#",
    urls.map(url => visitedMember(sc, url)),
  );
  const res = added.filter(Boolean).length === urls.length;
  logger.debug("lockURLs final result: " + res, { res });
  return res;
}

export async function lockURLsIndividually(
  id: string,
  sc: StoredCrawl,
  jobs: { id: string; url: string }[],
) {
  const out: typeof jobs = [];

  for (const job of jobs) {
    if (await lockURL(id, sc, job.url)) {
      out.push(job);
    }
  }

  return out;
}

export function crawlToCrawler(
  id: string,
  sc: StoredCrawl,
  teamFlags: TeamFlags,
  newBase?: string,
  crawlerOptions?: any,
): WebCrawler {
  const crawler = new WebCrawler({
    jobId: id,
    initialUrl: sc.originUrl!,
    baseUrl: newBase ? new URL(newBase).origin : undefined,
    includes: (sc.crawlerOptions?.includes ?? []).filter(
      x => x.trim().length > 0,
    ),
    excludes: (sc.crawlerOptions?.excludes ?? []).filter(
      x => x.trim().length > 0,
    ),
    maxCrawledLinks: sc.crawlerOptions?.maxCrawledLinks ?? 1000,
    maxCrawledDepth: getAdjustedMaxDepth(
      sc.originUrl!,
      sc.crawlerOptions?.maxDepth ?? 10,
    ),
    limit: sc.crawlerOptions?.limit ?? 10000,
    generateImgAltText: sc.crawlerOptions?.generateImgAltText ?? false,
    allowBackwardCrawling: sc.crawlerOptions?.allowBackwardCrawling ?? false,
    allowExternalContentLinks:
      sc.crawlerOptions?.allowExternalContentLinks ?? false,
    allowSubdomains: sc.crawlerOptions?.allowSubdomains ?? false,
    ignoreRobotsTxt:
      getIgnoreRobots(teamFlags) === "forced" ||
      (getIgnoreRobots(teamFlags) === "allowed" &&
        (sc.crawlerOptions?.ignoreRobotsTxt ?? false)),
    regexOnFullURL: sc.crawlerOptions?.regexOnFullURL ?? false,
    maxDiscoveryDepth: sc.crawlerOptions?.maxDiscoveryDepth,
    currentDiscoveryDepth: crawlerOptions?.currentDiscoveryDepth ?? 0,
    zeroDataRetention:
      (getScrapeZDR(teamFlags) === "forced" || sc.zeroDataRetention) ?? false,
    location: sc.scrapeOptions?.location,
    headers: sc.scrapeOptions?.headers,
    robotsUserAgent: sc.crawlerOptions?.robotsUserAgent,
  });

  if (sc.robots !== undefined) {
    try {
      crawler.importRobotsTxt(sc.robots);
    } catch {}
  }

  return crawler;
}
