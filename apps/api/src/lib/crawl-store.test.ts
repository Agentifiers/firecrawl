// Deployments.ai modification: invariants of the DynamoDB crawl store
// (crawl-redis.ts) and the status-document store against DynamoDB Local
// (see src/services/dynamo/test-table.ts).
import { randomUUID } from "node:crypto";
import { useTestTable } from "../services/dynamo/test-table";
import { scrapeOptions } from "../controllers/v2/types";
import {
  addCrawlJobDone,
  addCrawlJobs,
  getCrawlQualifiedJobCount,
  getDoneJobsOrderedLength,
  getDoneJobsOrderedUntil,
  lockCrawlSitemap,
  lockURL,
  releaseFailedCrawlJob,
  saveCrawl,
  StoredCrawl,
} from "./crawl-redis";
import { statusDocumentStore } from "./status-document";

beforeAll(async () => {
  await useTestTable();
});

const storedCrawl = (crawlerOptions: Record<string, unknown>): StoredCrawl => ({
  originUrl: "https://example.com",
  crawlerOptions,
  scrapeOptions: scrapeOptions.parse({}),
  internalOptions: { teamId: "team", orgId: null },
  team_id: "team",
  createdAt: Date.now(),
});

describe("lockURL", () => {
  it("locks at most `limit` unique URLs under concurrent workers", async () => {
    const id = randomUUID();
    const sc = storedCrawl({ limit: 5 });
    await saveCrawl(id, sc);
    const results = await Promise.all(
      Array.from({ length: 40 }, (_, n) =>
        lockURL(id, sc, `https://example.com/page-${n}`),
      ),
    );
    expect(results.filter(Boolean)).toHaveLength(5);
  });

  it("locks one URL exactly once when raced", async () => {
    const id = randomUUID();
    const sc = storedCrawl({});
    await saveCrawl(id, sc);
    const results = await Promise.all(
      Array.from({ length: 10 }, () => lockURL(id, sc, "https://example.com/a")),
    );
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it("frees a limit slot when a failed job is released", async () => {
    const id = randomUUID();
    const sc = storedCrawl({ limit: 1 });
    await saveCrawl(id, sc);
    expect(await lockURL(id, sc, "https://example.com/a")).toBe(true);
    expect(await lockURL(id, sc, "https://example.com/b")).toBe(false);
    await addCrawlJobs(id, ["job-a"]);
    expect(await getCrawlQualifiedJobCount(id)).toBe(1);
    await releaseFailedCrawlJob(id, "https://example.com/a", "job-a");
    expect(await getCrawlQualifiedJobCount(id)).toBe(0);
    // The visited marker stays, so the failed URL is not re-crawled.
    expect(await lockURL(id, sc, "https://example.com/a")).toBe(false);
    expect(await lockURL(id, sc, "https://example.com/b")).toBe(true);
  });
});

describe("addCrawlJobDone", () => {
  it("keeps one ordered entry per job across success and failure", async () => {
    const id = randomUUID();
    await saveCrawl(id, storedCrawl({}));
    await Promise.all(
      Array.from({ length: 8 }, () => addCrawlJobDone(id, "job-1", true)),
    );
    expect(await getDoneJobsOrderedLength(id)).toBe(1);
    expect(await getDoneJobsOrderedUntil(id)).toEqual(["job-1"]);

    await addCrawlJobDone(id, "job-2", true);
    expect(await getDoneJobsOrderedUntil(id)).toEqual(["job-1", "job-2"]);

    // ZREM on failure
    await addCrawlJobDone(id, "job-1", false);
    expect(await getDoneJobsOrderedLength(id)).toBe(1);
    expect(await getDoneJobsOrderedUntil(id)).toEqual(["job-2"]);

    // ZADD again moves the job to the end
    await addCrawlJobDone(id, "job-1", true);
    expect(await getDoneJobsOrderedUntil(id)).toEqual(["job-2", "job-1"]);
    expect(await getDoneJobsOrderedUntil(id, Infinity, 1, 1)).toEqual([
      "job-1",
    ]);
  });
});

describe("lockCrawlSitemap", () => {
  it("caps a crawl at 20 sitemaps and rejects duplicates", async () => {
    const id = randomUUID();
    await saveCrawl(id, storedCrawl({}));
    const results = await Promise.all(
      Array.from({ length: 30 }, (_, n) =>
        lockCrawlSitemap(id, `https://example.com/sitemap-${n}.xml`),
      ),
    );
    expect(results.filter(r => r === "locked")).toHaveLength(20);
    expect(results.filter(r => r === "limit")).toHaveLength(10);

    const other = randomUUID();
    await saveCrawl(other, storedCrawl({}));
    expect(await lockCrawlSitemap(other, "https://example.com/s.xml")).toBe(
      "locked",
    );
    expect(await lockCrawlSitemap(other, "https://example.com/s.xml")).toBe(
      "duplicate",
    );
  });
});

describe("statusDocumentStore", () => {
  it("applies every concurrent update exactly once", async () => {
    const store = statusDocumentStore<{ steps: number[] }>(
      "test",
      "doc",
      3600,
    );
    const id = randomUUID();
    await store.save(id, { steps: [] });
    await Promise.all(
      Array.from({ length: 15 }, (_, n) =>
        store.update(id, current => ({ steps: [...current.steps, n] })),
      ),
    );
    const doc = await store.get(id);
    expect(doc?.steps.slice().sort((a, b) => a - b)).toEqual(
      Array.from({ length: 15 }, (_, n) => n),
    );
  });

  it("does not create a document on update", async () => {
    const store = statusDocumentStore<{ n: number }>("test", "doc", 3600);
    const id = randomUUID();
    await store.update(id, current => ({ n: current.n + 1 }));
    expect(await store.get(id)).toBeNull();
  });
});
