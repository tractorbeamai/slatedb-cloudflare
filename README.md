# SlateDB on Cloudflare Durable Objects

A working example of running SlateDB 0.15.0 inside a Cloudflare Durable Object,
with R2 as its durable storage backend and Durable Object storage as a local SST
cache.

The example validates CRUD, concurrent writes, prefix scans, and recovery from
R2 after reopening the database. Production use requires workload-specific
validation of throughput, tail latency, compaction capacity, memory use, CPU
time, and R2 cost.

## Architecture

Each logical database name maps deterministically to one `SlateDbObject`. That
Durable Object opens and retains one SlateDB instance while active, providing
the database's single-writer boundary. The outer Worker handles authentication
and routing; the Durable Object owns the database state.

R2 is SlateDB's durable system of record. Every acknowledged write uses
SlateDB's default durable write options, and each logical database gets its own
R2 prefix.

The class uses a SQLite-backed Durable Object namespace. SlateDB persists
acknowledged data in R2, so Durable Object eviction only discards the active
in-memory database handle.

SlateDB's decoupled object-store cache stores immutable compacted SST parts in
the Durable Object Storage API. Cloudflare implements its key-value interface
with the SQLite-backed object's hidden `__cf_kv` table. Parts are 1 MiB, below
the platform's 2 MB combined key-and-value limit. Manifest, WAL, listing, and
coordination operations bypass the cache and continue to use R2 directly.

The Worker build disables SlateDB's separate decoded block cache because Foyer
requires a filesystem and Moka reaches unsupported `std::time` behavior under
workerd.

## Compatibility patches

SlateDB 0.15.0's native filesystem, signal, and multithreaded Tokio dependencies
do not compile for `wasm32-unknown-unknown`. The small, reviewable diffs in
`patches/`:

- select `tokio_with_wasm` on WebAssembly;
- preserve native Tokio behavior on non-WASM targets;
- disable filesystem-only cache and admin paths on WASM;
- adapt runtime handles, task cancellation, clocks, and `Send`-bounded timers;
- expose SlateDB's existing cache-storage boundary for caller-provided storage;
- select Worker-compatible scheduling for `object_store`'s buffered multipart
  writer.

`scripts/prepare-vendor.sh` downloads the published crate archives, verifies
their checksums, applies the diffs, and creates the ignored `vendor/` directory.
See [patches/README.md](patches/README.md) for provenance.

Relevant upstream work: [SlateDB issue #179](https://github.com/slatedb/slatedb/issues/179)
and [RFC PR #2031](https://github.com/slatedb/slatedb/pull/2031).

## API

Health checks are public. All `/v1` routes require
`Authorization: Bearer $PROBE_TOKEN`.

| Method | Route | Input |
| --- | --- | --- |
| `GET` | `/health` | — |
| `POST` | `/v1/db/:db/put` | `{"key":"k","value":"v"}` |
| `GET` | `/v1/db/:db/get?key=k` | — |
| `POST` | `/v1/db/:db/delete` | `{"key":"k"}` |
| `GET` | `/v1/db/:db/scan?prefix=p&limit=100` | — |
| `POST` | `/v1/db/:db/admin/reopen` | — |
| `POST` | `/v1/db/:db/admin/flush` | — |
| `POST` | `/v1/db/:db/admin/cache/clear` | — |
| `POST` | `/v1/db/:db/admin/benchmark/put` | `{"key":"k","value":"v"}` |
| `GET` | `/v1/db/:db/stats` | — |

The example API accepts UTF-8 keys and values. SlateDB itself stores bytes.
The admin endpoints and cache-populated status exist only to exercise R2
recovery, persistent cache behavior, and release-style benchmarks. The
benchmark write endpoint is the only route that acknowledges before remote
durability; normal writes remain durability-acknowledged.

## Run locally

Install the JavaScript dependencies and the pinned Rust Worker builder:

```sh
bun install --frozen-lockfile
cargo install worker-build --version 0.8.5
cp .dev.vars.example .dev.vars
```

Start the Worker:

```sh
bun run dev
```

`wrangler.jsonc` is the source of truth for local development and deployment.
Its custom build watches `src/`; restart Wrangler after changing Cargo
dependencies or compatibility patches.

In another terminal, run the end-to-end smoke test using the token from
`.dev.vars`:

```sh
PROBE_TOKEN=replace-with-a-local-token bun run smoke
```

Local R2 and Durable Object state is stored under the ignored `.wrangler/`
directory.

## Benchmark

Start the local Worker with per-request logging suppressed, then execute the
bounded benchmark suite in another terminal:

```sh
bun run dev -- --log-level error

PROBE_TOKEN=replace-with-a-local-token bun run benchmark
```

The default run seeds 1,000 1 KiB values per logical database with eight
closed-loop clients. It then reports cache-fill reads, warm-cache reads, durable
writes, and a 50/50 read/write workload. Timed phases use a three-second warmup
followed by 15 seconds of measured activity. Results include operations per
second, errors, and p1/p50/p95/p99/p99.9/max request latency.

Environment variables configure the workload:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BASE_URL` | `http://localhost:8787` | Local or deployed Worker URL |
| `BENCH_PROFILE` | `default` | `default` or `slatedb-balanced` |
| `BENCH_DATABASES` | `1` | Logical databases and Durable Objects |
| `BENCH_RECORDS` | `1000` | Seed records per database |
| `BENCH_VALUE_BYTES` | `1024` | UTF-8 value size |
| `BENCH_CONCURRENCY` | `8` | Total closed-loop clients |
| `BENCH_WARMUP_SECONDS` | `3` | Unmeasured warmup per timed phase |
| `BENCH_DURATION_SECONDS` | `15` | Measured time per timed phase |
| `BENCH_OUTPUT` | `table` | `table` or machine-readable `json` |

Use one database to measure the ceiling of a single serialized Durable Object.
Increase `BENCH_DATABASES` to measure horizontal scaling across independent
objects. Cloudflare documents each object as inherently single-threaded with a
[soft limit of 1,000 requests per second](https://developers.cloudflare.com/durable-objects/platform/limits/).
Each run uses new database names and
leaves its R2 and Durable Object data in place; choose record counts accordingly
for a live deployment.

These HTTP results include the client network path, outer Worker, Durable
Object, SlateDB, cache, and R2. They are not directly comparable to SlateDB's
[native release suite](https://slatedb.io/docs/operations/benchmarks/), which
uses a 120 GiB shared database, 64 clients, a
five-minute warmup, and 15-minute workloads.

The `slatedb-balanced` profile reproduces the release suite's 64 closed-loop
clients, 400-byte values, scrambled-Zipfian 0.99 key selection, equal point-read
and update mix, five-minute warmup, 15-minute measurement, non-blocking
application writes, and final durability drain:

```sh
BASE_URL=https://slatedb-cloudflare-feasibility.<subdomain>.workers.dev \
  PROBE_TOKEN=<token> \
  BENCH_PROFILE=slatedb-balanced \
  BENCH_OUTPUT=json \
  bun run benchmark
```

The bounded profile seeds 10,000 records rather than SlateDB's 300 million
record, roughly 120 GiB release dataset. Increase `BENCH_RECORDS` only after
accounting for R2, Durable Object storage, and request costs.

### Live balanced result

The committed [August 17, 2026 result](benchmarks/live-slatedb-balanced-2026-08-17.json)
ran Worker version `34aab9c5-d72c-42ec-9336-84f592a30c44` against the deployed
Worker, Durable Object, and R2 bucket. It completed 548,648 measured operations
with no errors:

| API | avg/s | p1 | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `get` | 304.55 | 52.46 ms | 90.20 ms | 308.26 ms | 493.87 ms |
| `put` | 305.03 | 54.00 ms | 90.04 ms | 300.14 ms | 442.58 ms |

SlateDB 0.15.0's official balanced result reports 6,215.61 gets/s and 6,210.21
puts/s. Its direct in-process API has 0.048 ms median get latency and 0.013 ms
median put latency. The deployed HTTP result therefore has about 20.4 times
lower aggregate throughput. It includes public HTTPS, authentication, Worker
routing, and Durable Object dispatch, which the direct benchmark does not.
The much smaller working set also favors this proof, so this result demonstrates
feasibility rather than performance parity.

## Checks

```sh
bun run format:check
bun run check
bun run build
```

These commands verify formatting, the patched WASM build, native unit tests,
Clippy, and a Wrangler deployment dry run. The unit tests cover conditional
write translation, byte ranges, pagination, multipart payload assembly, and
cache-key separation. The smoke test flushes an SST, populates the cache,
reopens the database, clears the cache, and recovers the same data from R2.

## Deploy

Choose globally appropriate Worker and account-local R2 bucket names in
`wrangler.jsonc`, then authenticate and create the bucket:

```sh
bunx wrangler login
bunx wrangler r2 bucket create slatedb-cloudflare-feasibility
bunx wrangler secret put PROBE_TOKEN
bun run deploy
```

Run the same smoke test against the deployment:

```sh
BASE_URL=https://slatedb-cloudflare-feasibility.<subdomain>.workers.dev \
  PROBE_TOKEN=<token> bun run smoke
```

The configuration uses a 30-second CPU allowance and therefore targets a paid
Workers plan.

## Known limits

- Background work shares a single JavaScript event loop.
- Reads have no decoded block cache. Compacted SST reads use the persistent
  Durable Object cache.
- The proof has no automatic cache eviction. Cache entries remain until SlateDB
  deletes the corresponding SST or the test endpoint clears the object storage.
- The R2 adapter currently materializes each requested object range before
  returning it to SlateDB.
- Multipart recovery above `object_store`'s 10 MiB writer threshold is not
  covered by the live test suite.
- The HTTP API is a test interface for the listed operations.
- Authentication and error handling are suitable only for controlled
  feasibility testing.

See Cloudflare's [R2 consistency documentation](https://developers.cloudflare.com/r2/reference/consistency/),
[SQLite-backed Durable Object Storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/),
[Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/),
and [Workers WebAssembly constraints](https://developers.cloudflare.com/workers/runtime-apis/webassembly/).

## License

Licensed under the [Apache License 2.0](LICENSE).
