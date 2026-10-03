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
