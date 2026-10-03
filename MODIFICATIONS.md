# Modifications to Firecrawl

This file records every change Deployments.ai made to the upstream Firecrawl
source in this directory (AGPL-3.0 section 5(a)). Each entry names the date
and the files or behavior it changes. `git log -- third_party/firecrawl` holds
the exact diffs; the first commit touching this directory is the unmodified
import.

## Upstream baseline

- Repository: https://github.com/firecrawl/firecrawl
- Commit: `ece430987522c929ca45aa2e49495c237552988b` (2026-07-21, "Merge pull
  request #4091 from firecrawl/fix/api-surface-first-used-company-group")
- Matches the previously deployed image
  `ghcr.io/firecrawl/firecrawl@sha256:998ff6d4d3d1be5ac6872aa4a2561089e3782b6d112aa4586f90f4feb842cb76`,
  whose image environment records `FIRECRAWL_BUILD_SHA` as that commit.

## Imported scope (2026-10-02)

Imported verbatim from upstream `apps/api` into `apps/api`, plus the upstream
root `LICENSE`:

- `src/` (API server, workers, scraper, libraries, co-located unit tests)
- `native/` (the `@mendable/firecrawl-rs` Rust N-API workspace package)
- `sharedLibs/go-html-to-md/` (Go HTML-to-Markdown shared library)
- `patches/`, `package.json`, `pnpm-lock.yaml`, `pnpm-workspace.yaml`,
  `tsconfig.json`, `vitest.config.ts`, `Dockerfile`, `.dockerignore`,
  `.gitignore`, `.gitattributes`, `.prettierrc`, `.env.example`, `clickhouse/`

Not imported: every other upstream app (SDKs, UI, Playwright service, Go
Markdown service, test suite, NuQ Postgres and Redis images), CI, examples,
skills, upstream `apps/api` developer assets (`.husky/`, `requests*`,
`samples/`, `utils/`, `audit-ci.jsonc`, `knip.config.ts`, `.env.local`,
OpenAPI documents), and the end-to-end/snapshot suites under
`src/__tests__/` that require Firecrawl-hosted services.

## Changes

### 2026-10-02 — build integration

- `apps/api/Dockerfile`: the build context is `third_party/firecrawl` instead
  of `apps/api`, so `COPY` sources gain an `apps/api/` prefix; the runtime
  image also receives `LICENSE`, `NOTICE`, and this file in `/app`. The
  upstream `apps/api/.dockerignore` moved to `third_party/firecrawl/.dockerignore`
  with patterns that apply from the new context.
- `apps/api/package.json`: removed the `prepare` script (it ran
  `husky ./apps/api/.husky` from the parent directory and would have rewritten
  this repository's git hook configuration) and the `knip` script (its
  configuration was not imported). Dependencies and the lockfile are unchanged.
- Removed the hosted-only ClickHouse concurrency-log worker
  (`src/services/cclog-worker.ts`, `src/lib/cclog.ts`, `src/lib/cclog.test.ts`,
  and the `cclog-worker*` package scripts). The self-host harness never starts
  it, and it issued Redis `SCAN`, which this repository's DynamoDB scan
  prohibition check rejects.

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
  `PRECRAWL_TEAM_ID`, `RESEARCH_PROXY_URL`. `.env.example` documents the
  DynamoDB settings.

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
