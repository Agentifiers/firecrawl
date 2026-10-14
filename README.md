# Firecrawl API (Deployments.ai fork)

A modified fork of [Firecrawl](https://github.com/firecrawl/firecrawl)'s API
server, maintained for the [Deployments.ai](https://deployments.ai) agent
platform. It runs fully self-hosted on AWS with no Redis, RabbitMQ, Postgres,
or FoundationDB: the job queue and all crawl, extract, deep research, and
llms.txt state live in a single DynamoDB table.

Upstream's history is preserved. This fork starts from upstream commit
[`ece4309`](https://github.com/firecrawl/firecrawl/commit/ece430987522c929ca45aa2e49495c237552988b),
and every change since is in [`MODIFICATIONS.md`](MODIFICATIONS.md) and the
commits after it. Upstream's SDKs, UI, and hosted-only services are not part of
this fork; use upstream for those.

## What differs from upstream

- **DynamoDB instead of Redis, RabbitMQ, Postgres, and FoundationDB.** The NuQ
  job queue uses conditional writes for every state transition, claims jobs
  through a ready index, gates crawl groups by concurrency and delay, and runs
  a bounded sweeper in place of pg_cron and the prefetch and reconciler
  workers. No table scans.
- **Lightpanda rendering.** With `LIGHTPANDA_CDP_URL` set, pages render in a
  Lightpanda browser first and fall back to Chromium (the Playwright service).
  A `renderer` option (`auto`, `lightpanda`, `chromium`) on scrape, batch
  scrape, crawl, search, extract, deep research, and llms.txt picks one.
- **Hosted-only features removed**, including all `/v0` routes except health,
  admin and Bull Board, keyless access, monitors, browser and interact
  sessions, the research proxy, and per-team rate limiting.
  [`MODIFICATIONS.md`](MODIFICATIONS.md) has the full list.

## Image

Releases are versioned. Dispatching the `Release` workflow with a semantic
version builds a `linux/arm64` image from the dispatched commit, publishes it as
`ghcr.io/agentifiers/firecrawl:<version>` and `:<commit-sha>`, tags the commit
`v<version>`, and creates a GitHub Release whose notes give the image digest
and which attaches the image's `dynamodb-table.json`:

```sh
docker pull ghcr.io/agentifiers/firecrawl:<version>@sha256:<digest>
```

Each commit on `main` also publishes `ghcr.io/agentifiers/firecrawl:<commit-sha>`.
There is no moving tag; pin images by digest. Every image carries SLSA
provenance and an SPDX SBOM attestation. An image's
`org.opencontainers.image.revision` label and `FIRECRAWL_BUILD_SHA` environment
variable name the commit it was built from.

To build it yourself, use the repository root as the build context:

```sh
docker build -f apps/api/Dockerfile --build-arg GIT_SHA="$(git rev-parse HEAD)" -t firecrawl .
```

## Configuration

[`apps/api/.env.example`](apps/api/.env.example) lists the settings. The ones
this fork adds:

| Variable | Purpose |
|---|---|
| `FIRECRAWL_DYNAMODB_TABLE` | Table that holds the queue and job state |
| `FIRECRAWL_DYNAMODB_ENDPOINT` | DynamoDB Local endpoint; leave unset on AWS to use the regional endpoint and the task role |
| `LIGHTPANDA_CDP_URL` | CDP WebSocket of a Lightpanda browser; unset renders with Chromium only |
| `NUM_WORKERS_PER_QUEUE` | NuQ claim loops per queue in the queue and extract workers (default 8) |
| `NUQ_SWEEPER_INTERVAL_MS` | Sweeper polling interval (default 5000) |

The table's key schema, attribute types, billing mode, TTL attribute, and
indexes are defined once, in
[`apps/api/src/services/dynamo/dynamodb-table.json`](apps/api/src/services/dynamo/dynamodb-table.json)
(field names follow the OpenTofu/Terraform `aws_dynamodb_table` arguments; its
`version` names the format). The image ships the file at
`/app/dynamodb-table.json`, so infrastructure code can read the definition of
the exact image it deploys and create the table on AWS from it. Against
DynamoDB Local, the image creates it from the same file:

```sh
docker run --rm -e FIRECRAWL_DYNAMODB_TABLE=firecrawl \
  -e FIRECRAWL_DYNAMODB_ENDPOINT=http://dynamodb-local:8000 \
  ghcr.io/agentifiers/firecrawl:<version> node dist/src/services/dynamo/create-table.js
```

The default command, `node dist/src/harness.js --start-docker`, starts the API
and its workers in one container.

## Development

The API server is a pnpm workspace in `apps/api`. Installing builds the Rust
native package, so a Rust toolchain is required:

```sh
cd apps/api
pnpm install
pnpm run build
docker run -d --rm -p 18000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory
pnpm exec vitest run src/services/worker/nuq src/lib/crawl-store.test.ts
```

The queue and crawl-store tests use DynamoDB Local at
`FIRECRAWL_TEST_DYNAMODB_ENDPOINT` (default `http://127.0.0.1:18000`).

## License

[GNU Affero General Public License v3.0](LICENSE), like upstream. See
[`NOTICE`](NOTICE) for provenance. If you run a modified version as a network
service, AGPL-3.0 requires you to offer its users the corresponding source.
