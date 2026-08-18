# SlateDB in Cloudflare Containers

This example runs the published SlateDB 0.15.0 crate unchanged in a native
`linux/amd64` Cloudflare Container. A small Worker authenticates requests and
maps each database name to one Container-backed Durable Object; the Durable
Object starts and proxies to the corresponding container.

[Back to the comparison](../README.md)

## Architecture

R2 is SlateDB's durable object store through a native Worker binding. A
Cloudflare Container cannot call that binding in-process, so the container
sends object-store requests to the virtual `slatedb.r2` hostname. The
supervising Worker intercepts them with `outboundByHost` and performs the R2
binding calls. No R2 credentials enter the container. Each logical database
receives its own R2 prefix. The container's filesystem is ephemeral and
contains only SlateDB's built-in decoupled object-store cache.

This uses Cloudflare's documented
[Worker binding access from Containers](https://developers.cloudflare.com/containers/platform-details/workers-connections/).

The configured `basic` instance provides 1 GiB of memory and 4 GB of ephemeral
disk. SlateDB uses a 128 MiB Foyer decoded-block cache, a 512 MiB filesystem
object cache, 64 MiB L0 SSTs, and a 256 MiB unflushed-data limit. These settings
leave headroom for compaction, Tokio, the HTTP client, and the process itself;
they are starting points rather than universal tuning recommendations.

The container sleeps after ten idle minutes. Cloudflare may also replace it at
any time, so a new process must reconstruct the database from R2 and refill its
cache. The service handles `SIGTERM` and asks SlateDB to close before the
platform's shutdown deadline.

Unlike the Durable Object implementation, this path has:

- no vendored SlateDB crates or local patches;
- no custom persistent cache implementation;
- normal Tokio multithreading, clocks, multipart uploads, and filesystem APIs.

The native service includes a narrow `object_store` transport for the binding
bridge. It preserves R2 conditional writes, ranges, paginated listings,
deletes, and multipart uploads; using a generic HTTP store or an R2 FUSE mount
would not preserve SlateDB's manifest fencing contract.

Container egress bodies reach the Worker as unknown-length streams, while R2
binding writes require a known length. The bridge therefore buffers each
ordinary PUT or multipart part before calling R2; reads stream back to the
container. SlateDB switches to multipart at 10 MiB, keeping this proof's normal
bridge buffers well below the Worker's 128 MiB memory limit.

## Setup

Requirements are Bun, Rust, Docker or a compatible engine, a paid Cloudflare
Workers plan. Create the bucket and configure the API token from this directory:

```sh
bun install
bunx wrangler login
bunx wrangler r2 bucket create slatedb-cloudflare-container
bunx wrangler secret put PROBE_TOKEN
bun run deploy
```

For local development, copy `.dev.vars.example` to `.dev.vars`, set the API
token, and run:

```sh
bun run dev
```

Container local development still needs Docker. Wrangler supplies the configured
R2 binding; use a separate bucket configuration for development data when
possible.

## API and verification

The API matches the Durable Object example:

- `POST /v1/db/:db/put`
- `GET /v1/db/:db/get?key=...`
- `POST /v1/db/:db/delete`
- `GET /v1/db/:db/scan?prefix=...&limit=...`
- `POST /v1/db/:db/admin/flush`
- `POST /v1/db/:db/admin/reopen`
- `POST /v1/db/:db/admin/cache/clear`
- `POST /v1/db/:db/admin/benchmark/batch`
- `GET /v1/db/:db/stats`

Run the shared functional smoke test against a local or deployed endpoint:

```sh
BASE_URL=https://slatedb-cloudflare-container.<subdomain>.workers.dev \
  PROBE_TOKEN=<token> bun run smoke
```

Run formatting, native checks, TypeScript checks, and Wrangler's Docker-backed
deployment dry run with:

```sh
bun run format:check
bun run check
```

The shared `slatedb-balanced` driver invokes the embedded batch endpoint, so its
reported operation percentiles exclude the public Worker network hop. Native
`Instant` remains available in a container, unlike a deployed Worker isolate:

```sh
BASE_URL=https://slatedb-cloudflare-container.<subdomain>.workers.dev \
  PROBE_TOKEN=<token> \
  BENCH_PROFILE=slatedb-balanced \
  BENCH_OUTPUT=json \
  bun run benchmark
```

The benchmark shape follows SlateDB's documented balanced release workload,
but its default dataset is intentionally much smaller. Do not compare results
until record count, value size, concurrency, warmup, duration, and read/write
mix match. See SlateDB's [benchmark methodology](https://slatedb.io/docs/operations/benchmarks/).

## Platform bounds

Containers remove the Worker's 128 MiB memory ceiling, but they do not make one
SlateDB writer scale without limit. A database remains bounded by one container's
CPU, memory, ephemeral disk cache, compaction throughput, and R2 request rate.
Scale independent database names across container instances; shard a logical
database only with a deliberate SlateDB partitioning design.

Cold starts are commonly one to three seconds, the filesystem is always
ephemeral, and automatic instance routing does not replace application-level
database ownership. Review Cloudflare's current
[lifecycle](https://developers.cloudflare.com/containers/platform-details/architecture/)
and [scaling model](https://developers.cloudflare.com/containers/platform-details/scaling-and-routing/)
before treating this example as an operating design.
