# Modifications to Firecrawl

This file records every change Deployments.ai made to the upstream Firecrawl
source in this repository (AGPL-3.0 section 5(a)). Each entry names the date
and the files or behavior it changes. Upstream history is preserved, and
`git log ece430987522c929ca45aa2e49495c237552988b..main` holds the exact
diffs.

## Upstream baseline

- Repository: https://github.com/firecrawl/firecrawl
- Commit: `ece430987522c929ca45aa2e49495c237552988b` (2026-07-21, "Merge pull
  request #4091 from firecrawl/fix/api-surface-first-used-company-group")
- Matches the previously deployed image
  `ghcr.io/firecrawl/firecrawl@sha256:998ff6d4d3d1be5ac6872aa4a2561089e3782b6d112aa4586f90f4feb842cb76`,
  whose image environment records `FIRECRAWL_BUILD_SHA` as that commit.

## Kept scope (2026-10-02)

The fork keeps upstream's `apps/api` and the root `LICENSE`, `.gitignore`,
and `.gitattributes`:

- `src/` (API server, workers, scraper, libraries, co-located unit tests)
- `native/` (the `@mendable/firecrawl-rs` Rust N-API workspace package)
- `sharedLibs/go-html-to-md/` (Go HTML-to-Markdown shared library)
- `patches/`, `package.json`, `pnpm-lock.yaml`, `pnpm-workspace.yaml`,
  `tsconfig.json`, `vitest.config.ts`, `Dockerfile`, `.dockerignore`,
  `.gitignore`, `.gitattributes`, `.prettierrc`, `.env.example`, `clickhouse/`

Removed: every other upstream app (SDKs, UI, Playwright service, Go Markdown
service, test suite, NuQ Postgres and Redis images), CI, examples, skills,
CLI, hosted-product documentation, upstream `apps/api` developer assets
(`.husky/`, `requests*`, `samples/`, `utils/`, `audit-ci.jsonc`,
`knip.config.ts`, `.env.local`, OpenAPI documents), and the
end-to-end/snapshot suites under `src/__tests__/` that require
Firecrawl-hosted services.

## Changes

### 2026-10-02 — build integration

- `apps/api/Dockerfile`: the build context is the repository root instead of
  `apps/api`, so `COPY` sources gain an `apps/api/` prefix; the runtime image
  also receives `LICENSE`, `NOTICE`, and this file in `/app`. The upstream
  `apps/api/.dockerignore` moved to the root `.dockerignore` with patterns that
  apply from the new context.
- `apps/api/package.json`: removed the `prepare` script (it ran
  `husky ./apps/api/.husky` from the parent directory and rewrote the
  enclosing repository's git hook configuration) and the `knip` script (its
  configuration is not kept). Dependencies and the lockfile are unchanged.
- Removed the hosted-only ClickHouse concurrency-log worker
  (`src/services/cclog-worker.ts`, `src/lib/cclog.ts`, `src/lib/cclog.test.ts`,
  and the `cclog-worker*` package scripts). The self-host harness never starts
  it, and it issued Redis `SCAN`.

### 2026-10-02 — NuQ job queue on DynamoDB

Replaced subsystem: upstream's NuQ queue on Postgres (`nuq.sql`, pg_cron,
LISTEN/NOTIFY, RabbitMQ prefetch), its FoundationDB backend and the backend
router.

- Added `src/services/dynamo/` (DocumentClient, gzip+chunked payload codec,
  table schema, DynamoDB Local test helper) and `src/services/worker/nuq/`
  (`keys`, `queue`, `enqueue`, `settle`, `groups`, `sweeper`, `consumer`, and
  `queue.test.ts`/`groups.test.ts`). One table, conditional writes for every
  state transition, claims through the `gsi1` ready index, crawl groups with
  `maxConcurrency`/delay gating and backlog promotion, completion detected by
  polling, and a bounded sweeper (stalled-lock reaper, group finisher, expired
  group cleanup). No table scans.
- Added `src/services/worker/nuq-sweeper-worker.ts`; it replaces pg_cron,
  `nuq-prefetch-worker.ts` and `nuq-reconciler-worker.ts`. A job that stalls
  past its queue's limit is failed once; extract, deep research and llms.txt
  status documents are then marked failed (the former BullMQ failed handler
  and RabbitMQ dead-letter consumer).
- `src/services/worker/nuq.ts` keeps upstream's exported names and now holds
  the DynamoDB queues `scrape`, `crawl_finished`, `extract`, `deep_research`
  and `llmstxt`. `nuq-worker.ts`/`nuq-worker-runner.ts` use the shared consumer
  loop.
- Deleted `src/services/worker/{nuq-router,nuq-fdb-worker,nuq-prefetch-worker,
  nuq-reconciler-worker,redis,team-semaphore}.ts` and `nuq-fdb/`.
- `src/config.ts`: removed `POSTGRES_*`, `NUQ_DATABASE_URL*`,
  `NUQ_RABBITMQ_URL`, `NUQ_BACKEND`, `NUQ_FDB_*`, `FDB_*`; added
  `FIRECRAWL_DYNAMODB_TABLE`, `FIRECRAWL_DYNAMODB_ENDPOINT`,
  `NUQ_SWEEPER_WORKER_PORT`, `NUQ_SWEEPER_INTERVAL_MS`.
- `package.json`/`pnpm-lock.yaml`: removed `ioredis`, `bullmq`,
  `@bull-board/api`, `@bull-board/express`, `redlock`, `rate-limiter-flexible`,
  `amqplib`, `@types/amqplib`, `foundationdb`; added `@aws-sdk/client-dynamodb`
  and `@aws-sdk/lib-dynamodb` 3.1090.0; replaced the `nuq-fdb-worker`,
  `nuq-prefetch-worker`, `nuq-reconciler-worker` and `index-worker` scripts
  with `nuq-sweeper-worker`.

### 2026-10-02 — crawl, extract, deep research and llms.txt state on DynamoDB; Redis, BullMQ and RabbitMQ removed

Replaced subsystems:

- Crawl store: `src/lib/crawl-redis.ts` keeps its name and API but stores the
  crawl document, job/done/visited/robots/threat/sitemap sets and the
  done-ordered list in DynamoDB (`src/lib/crawl-dynamo.ts` set and counter
  primitives). The URL limit and the 20-sitemap cap are enforced atomically
  (upstream checked, then added). `normalizeURL`/`generateURLPermutations`
  moved to `src/lib/crawl-urls.ts` (`crawl-redis.test.ts` renamed
  `crawl-urls.test.ts`). `markCrawlActive` and the `queueBackend` field are
  gone. New exports replace raw Redis calls in the scrape worker:
  `getRobotsBlocked`, `addVisitedURLs`, `releaseFailedCrawlJob`,
  `lockCrawlSitemap`, `addCrawlSitemapJob`, `addCrawlSitemapJobDone`.
  `src/scraper/WebScraper/crawler.ts`'s sitemap de-duplication uses the same
  store.
- Extract, deep research and llms.txt status documents
  (`extract-redis.ts`, `deep-research-redis.ts`, `generate-llmstxt-redis.ts`)
  use `src/lib/status-document.ts`: versioned documents updated with
  conditional puts.
- Deep research and llms.txt jobs moved from BullMQ to NuQ; extract jobs
  moved from RabbitMQ (`src/services/extract-queue.ts`, deleted) to NuQ.
  `queue-worker.ts` and `extract-worker.ts` run the shared consumer loop; the
  llms.txt worker now addresses its status document by `generationId`
  (upstream read an undefined `generateId`).
- Billing: the scrape worker hands billing operations to the in-process batch
  queue in `services/billing/batch_billing.ts` instead of a BullMQ job.
- Webhooks are delivered by direct HTTP only; webhook log rows are a log line.
- `src/harness.ts` no longer starts Postgres, RabbitMQ or FoundationDB
  containers or the prefetch/reconciler/index workers; it starts the NuQ
  sweeper. `Dockerfile`: FoundationDB client packages removed; the build
  check looks for the sweeper instead of the deleted v0 crawl-status module.
- Added `src/lib/crawl-store.test.ts` (URL-limit race, raced lock, failed-job
  release, done-ordered list across success/failure, sitemap cap,
  concurrent status-document updates) against DynamoDB Local.
- `src/config.ts`: removed `REDIS_*`, `INDEX_CACHE_*`, `BULL_AUTH_KEY`,
  keyless/Spur/Web Risk keys, `INDEXER_RABBITMQ_URL`,
  `INDEXER_TRAFFIC_SHARE`, `WEBHOOK_USE_RABBITMQ`, `SLACK_CLIENT_*`/OAuth keys,
  `PRECRAWL_TEAM_ID`, `RESEARCH_PROXY_URL`.

Dropped features (hosted-only or dependent on removed Redis/RabbitMQ state):

- All `/v0` endpoints except `/v0/health/liveness` and `/v0/health/readiness`.
- All `/admin/*` endpoints, including Bull Board.
- `/v1` and `/v2` `/concurrency-check`, `/team/queue-status` and the crawl
  status WebSocket.
- `/v2/parse/upload-url`, `PUT /v2/parse/upload/:id` and `uploadRef` bodies on
  `/v2/parse` (multipart `/v2/parse` remains).
- `/v2/monitor*`, `/v2/slack/*`, `/v2/browser*`, `/v2/interact*`,
  `/v2/scrape/:id/interact`, and the monitor scheduler, runner and
  notifications (email and Slack).
- `/v2/keyless/eligibility` and keyless (unauthenticated free-tier) access with
  its per-IP credit ledger and Spur lookups.
- `/v2/team/threat-protection` (team settings API). Threat protection still
  applies; the Google Web Risk provider and its threat-list sync are removed,
  so provider verdicts fall to the configured failure policy.
- `/v2/search/research` and `/v2/research` (hosted research proxy) and the
  research-endpoint request logger.
- Per-team rate limiting (Redis rate limiter) and the ACUC/OAuth/IP/key
  restriction caches; lookups go to the database each time.
- The per-team concurrency semaphore for `/scrape` and `/parse`, the Redis
  concurrency-limited job lists and their reconciler; crawl concurrency is
  enforced by NuQ group gating. Responses still carry
  `concurrencyLimited: false`.
- Per-team job-priority decay; jobs use upstream's base priorities (scrape 10,
  crawl pages 20, kickoff 15, sitemap and large batch jobs 21).
- PostHog first-use events, the `most-recent-success` marker, email
  notification de-duplication locks, the index Dragonfly cache, index
  link forwarding to the hosted indexer, the index worker (index rows are
  written synchronously when `INDEX_DATABASE_URL` is set), the fire-engine
  map cache, and the shared Wikipedia token cache (now per process).
- Webhook RabbitMQ queueing and `webhook_logs` rows.
- Crashed extract jobs no longer write an extract log row or send the
  `extract.failed` webhook; the sweeper marks the extract failed.

### 2026-10-02 — NuQ consumer concurrency

- `src/services/worker/nuq/consumer.ts`: `consumeJobs` takes a
  `concurrency` option and runs that many independent claim loops (default
  1; the scrape `nuq-worker` keeps one loop per process and scales with
  `NUQ_WORKER_COUNT`).
- `src/config.ts`: added `NUM_WORKERS_PER_QUEUE` (positive integer, default
  8, the value in upstream's `.env.example`). `queue-worker.ts` runs that
  many loops for each of the deep research, llms.txt and crawl-finished
  queues, and `extract-worker.ts` for the extract queue. Upstream's BullMQ
  workers fetched deep research and llms.txt jobs without a fixed limit
  (gated by `MAX_CPU`/`MAX_RAM`), its crawl-finished loop and RabbitMQ
  extract consumer (`prefetch(1)`) ran one job at a time.
- `src/services/worker/nuq/queue.test.ts`: two consumers of eight loops each
  process 60 jobs exactly once and leave every job completed.

### 2026-10-02 — local table bootstrap

- Added `src/services/dynamo/create-table.ts`, a command that creates the
  table described by `table-schema.ts` at `FIRECRAWL_DYNAMODB_ENDPOINT` under
  `FIRECRAWL_DYNAMODB_TABLE`. A local Compose stack runs it against DynamoDB
  Local before starting the API; it refuses to run without an endpoint
  override because AWS tables are created by infrastructure code.

### 2026-10-03 — Lightpanda engine and `renderer` option

- Added the `lightpanda` scrape engine
  (`src/scraper/scrapeURL/engines/lightpanda/index.ts`). It connects to a
  Lightpanda browser over CDP with `playwright-core` once per scrape and
  follows upstream `apps/playwright-service-ts`: 1280×800 viewport, blocked
  service workers, `ignoreHTTPSErrors` from `skipTlsVerification`, a `Cookie`
  request header seeded into the context cookie jar on the last two host
  labels, the other headers sent as extra HTTP headers, ad-serving domains and
  private/internal hosts aborted, `load` navigation, `waitFor`, and the
  response body for JSON and `text/plain`. Connection, navigation and timeout
  failures raise `EngineError`; HTTP 401, 403 and 429 also raise
  `EngineError` unless the request forced Lightpanda, so the waterfall tries
  Chromium. Closing the CDP connection aborts the navigation when the scrape
  is aborted.
- `src/scraper/scrapeURL/engines/index.ts`: registered `lightpanda` (enabled
  when `LIGHTPANDA_CDP_URL` is set; Playwright's feature flags; quality 30,
  above `playwright` at 20; maximum reasonable time `waitFor` + 15 s).
  `buildFallbackList` drops `lightpanda` for `renderer: "chromium"` and
  `playwright` for `renderer: "lightpanda"`; a forced engine list is used
  unchanged. `src/scraper/WebScraper/utils/ENGINE_FORCING.md` lists the new
  engine.
- `renderer` (`"auto" | "lightpanda" | "chromium"`, default `"auto"`) is a
  scrape option in the v1 and v2 `baseScrapeOptions`
  (`src/controllers/v1/types.ts` exports `rendererSchema`), so scrape, batch
  scrape, crawl, search and extract carry it to every page scrape.
- `/v1/deep-research` and `/v1/llmstxt` take a top-level `renderer`. It is
  stored with the queued job and `src/services/queue-worker.ts` passes it to
  `performDeepResearch` (search-result scrapes) and `performGenerateLlmsTxt`
  (page scrapes).
- `src/config.ts`: added `LIGHTPANDA_CDP_URL`.
- `package.json`/`pnpm-lock.yaml`: added `playwright-core` 1.58.1.
- Added `src/scraper/scrapeURL/engines/__tests__/buildFallbackList.test.ts`
  (engine order per renderer) and
  `src/scraper/scrapeURL/engines/lightpanda/__tests__/lightpanda.test.ts`
  (401/403/429 handling, CDP connection failure) with a mocked
  `playwright-core`.

### 2026-10-12 — standalone repository

- The fork moved from a vendored directory into this repository, with
  upstream history preserved.
- Added `README.md`, updated `NOTICE`, and updated the build-context comment
  in `apps/api/Dockerfile`.
- `apps/api/.env.example`: removed `REDIS_URL`, `REDIS_RATE_LIMIT_URL`, and
  `BULL_AUTH_KEY`; documents `FIRECRAWL_DYNAMODB_TABLE`,
  `FIRECRAWL_DYNAMODB_ENDPOINT`, `LIGHTPANDA_CDP_URL`,
  `NUQ_SWEEPER_INTERVAL_MS`, and `NUQ_SWEEPER_WORKER_PORT`.
- Added `.github/workflows/image.yml`, which builds the `linux/arm64` image
  from each commit on `main` and publishes it to
  `ghcr.io/agentifiers/firecrawl` tagged with the commit SHA.

### 2026-10-14 — table definition contract and versioned releases

- Added `apps/api/src/services/dynamo/dynamodb-table.json`, the single
  definition of the DynamoDB table (format `version` 1: billing mode, key
  schema, attribute types, TTL attribute, global secondary indexes with their
  projections). `table-schema.ts` builds the `CreateTable` and
  `UpdateTimeToLive` calls of `ensureTable` from it and no longer holds the
  table shape; `table-schema.test.ts` proves those calls equal the file.
  `tsconfig.json` enables `resolveJsonModule` for the import.
- `apps/api/Dockerfile`: the runtime image ships the definition at
  `/app/dynamodb-table.json` and the build check requires it, so
  infrastructure can create the AWS table from the image it deploys.
- `.github/workflows/image.yml`: commits on `main` publish only the
  `:<commit-sha>` tag (the mutable `:main` tag is gone), with SLSA provenance
  (`mode=max`) and an SBOM; actions are pinned by commit SHA and each job
  holds only the permissions it uses.
- Added `.github/workflows/release.yml`: a dispatch with a semantic `version`
  builds the dispatched commit, publishes `:<version>` and `:<commit-sha>`,
  creates the `v<version>` tag (refusing an existing one) and a GitHub
  Release whose notes give the image digest and which attaches
  `dynamodb-table.json`.
