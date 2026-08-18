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
with the SQLite-backed object's hidden `__cf_kv` table. Parts are 64 KiB, below
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
| `POST` | `/v1/db/:db/admin/benchmark/batch` | `{"value":"v","clients":[[{"operation":"get","key":"k"}]]}` |
| `GET` | `/v1/db/:db/stats` | — |

The example API accepts UTF-8 keys and values. SlateDB itself stores bytes.
The admin endpoints and cache-populated status exist only to exercise R2
recovery, persistent cache behavior, and release-style benchmarks. Benchmark
writes use SlateDB's non-blocking write option to match its release suite;
normal writes remain durability-acknowledged.

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

The default profile seeds 1,000 1 KiB values per logical database with eight
closed-loop clients. It then reports cache-fill reads, warm-cache reads, durable
writes, and a 50/50 read/write workload. Timed phases use a three-second warmup
followed by 15 seconds of measured activity. Results include operations per
second, errors, and p1/p50/p95/p99/p99.9/max end-to-end HTTP latency.

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
| `BENCH_BATCH_SIZE` | `1` or `32` | Operations per request; balanced profile uses `32` |
| `BENCH_READ_PERCENT` | `50` | Point-read percentage in the balanced profile |
| `BENCH_OUTPUT` | `table` | `table` or machine-readable `json` |

Use one database to measure the ceiling of a single serialized Durable Object.
Increase `BENCH_DATABASES` to measure horizontal scaling across independent
objects. Cloudflare documents each object as inherently single-threaded with a
[soft limit of 1,000 requests per second](https://developers.cloudflare.com/durable-objects/platform/limits/).
Each run uses new database names and
leaves its R2 and Durable Object data in place; choose record counts accordingly
for a live deployment.

The `slatedb-balanced` profile targets SlateDB's embedded-library measurement
boundary. Each control request carries 64 independent client streams, which the
Durable Object runs concurrently against one `Db`. Timing inside the object
brackets only `Db::get` or `Db::put_with_options`; authentication, outer Worker
routing, Durable Object dispatch, request decoding, and response encoding are
outside that interval. It uses 64 closed-loop clients, 400-byte
values, scrambled-Zipfian 0.99 key selection, an equal point-read and update
mix, a five-minute warmup, a 15-minute measurement, non-blocking application
writes, and a final durability drain to match SlateDB's
[native release suite](https://slatedb.io/docs/operations/benchmarks/):

```sh
BASE_URL=https://slatedb-cloudflare-feasibility.<subdomain>.workers.dev \
  PROBE_TOKEN=<token> \
  BENCH_PROFILE=slatedb-balanced \
  BENCH_OUTPUT=json \
  bun run benchmark
```

The profile seeds 10,000 records rather than SlateDB's 300 million-record,
roughly 120 GiB release dataset. Increase `BENCH_RECORDS` only after
accounting for R2, Durable Object storage, and request costs.

Cloudflare deliberately freezes `performance.now()` and `Date.now()` between
I/O events in deployed Workers. This prevents code inside the Durable Object
from observing the elapsed time of an individual embedded database call.
Consequently, deployed embedded-operation p1/p50/p99/p99.9 values are reported
as `null`, with `unmeasurableOperations` recording the affected samples. A zero
would be a platform clock artifact, not a sub-nanosecond SlateDB result. See
[Workers performance timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)
and the [Workers security model](https://developers.cloudflare.com/workers/reference/security-model/#step-1-disallow-timers-and-multi-threading).

Aggregate throughput is measured by the external client over the complete
measurement interval. Batching amortizes the network and routing path but does
not eliminate request scheduling and serialization overhead, so throughput is
an approximate embedded comparison. The working set is also much smaller than
the release suite's 120 GiB dataset.

The JSON report captures `/stats` before warmup, after warmup, after the measured
phase, and after the durability drain. Each snapshot includes SlateDB's native
metrics plus aggregate counters at the two platform adapters:

- cache part/head hits, misses, reads, and writes;
- cache bytes requested by SlateDB, loaded from Durable Object storage, and
  returned to SlateDB;
- R2 GET, HEAD, PUT, LIST, DELETE, multipart, error, and byte counts.

Subtract adjacent snapshots to attribute a phase without adding a clock read to
each database operation. In particular,
`cacheLoadedBytes / cacheRequestedBytes` measures storage-read amplification,
while the cache hit rate and R2 operation deltas show whether a workload is
actually exercising the persistent cache. SlateDB metrics expose L0 growth,
backpressure, flushes, compactions, object-store calls, and cache behavior at the
engine boundary.

Production tracing is enabled at a 10% sample rate in `wrangler.jsonc`. Cloudflare's automatic trace
spans provide request CPU and wall time plus Durable Object, R2, and storage
binding operations. Use a unique `BENCH_DATABASE_PREFIX` to correlate a run in
Workers Observability. The benchmark report and Cloudflare traces are the two
halves of the performance loop: the report attributes engine and adapter work;
the traces locate CPU and platform I/O cost.

For a short attribution matrix, keep the official 400-byte value and Zipfian
selection, then vary only one dimension per run:

```sh
BENCH_PROFILE=slatedb-balanced BENCH_OUTPUT=json \
  BENCH_RECORDS=10000 BENCH_VALUE_BYTES=400 \
  BENCH_WARMUP_SECONDS=15 BENCH_DURATION_SECONDS=60 \
  BENCH_READ_PERCENT=100 BENCH_CONCURRENCY=64 bun run benchmark

BENCH_PROFILE=slatedb-balanced BENCH_OUTPUT=json \
  BENCH_RECORDS=10000 BENCH_VALUE_BYTES=400 \
  BENCH_WARMUP_SECONDS=15 BENCH_DURATION_SECONDS=60 \
  BENCH_READ_PERCENT=0 BENCH_CONCURRENCY=64 bun run benchmark
```

Repeat the 50/50 workload at client counts 1, 8, 32, and 64. This separates
read-path/cache cost, write/flush/compaction cost, and single-object scheduling
before changing cache part size or SlateDB settings.

### Live embedded result

The committed [August 18, 2026 result](benchmarks/live-embedded-balanced-2026-08-18.json)
ran Worker version `37d9a72d-d804-44fe-8994-5b917275381c` for the full
five-minute warmup and 15-minute measurement. It completed 2,134,016 measured
operations with no errors:

| Operation | Operations | avg/s | p1 | p50 | p99 | p99.9 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 1,066,341 | 1,182.80 | unavailable | unavailable | unavailable | unavailable |
| `put` | 1,067,675 | 1,184.27 | unavailable | unavailable | unavailable | unavailable |

The aggregate rate was 2,367.07 operations/s. SlateDB 0.15.0's
[official balanced result](https://benchmark.slatedb.io/0.15.0/run/github-30489389676/workload/balanced/)
reports 12,425.82 operations/s: 6,215.61 gets/s and 6,210.21 puts/s. The
Cloudflare proof reached 19.05% of that aggregate throughput. The different
CPU, object-store path, cache, and 10,000-record working set prevent a
hardware-normalized comparison.

The official suite reports get p1/p50/p99/p99.9 of
0.010/0.048/51.199/91.839 ms and put values of
0.005/0.013/0.039/0.075 ms. Those latency values are the valid reference until
Cloudflare exposes a production timer that can measure computation between I/O
events.

### Performance attribution result

The August 18 instrumentation runs found that persistent-cache part size, not
R2, limited SST point reads. With 1 MiB parts, the measured read-only phase
loaded 5.61 GB from Durable Object storage to satisfy 265 MB requested by
SlateDB: 21.18× byte amplification. Changing only the part size to 64 KiB
loaded 8.63 GB for 2.12 GB requested, or 4.07×, while throughput increased from
58.55 to 539.47 gets/s (9.21×).

| Workload | Clients × batch | Measured rate | Result |
| --- | ---: | ---: | --- |
| Read-only, 1 MiB parts | 64 × 32 | 58.55 gets/s | [JSON](benchmarks/live-perf-read-only-2026-08-18.json) |
| Read-only, 64 KiB parts | 64 × 32 | 539.47 gets/s | [JSON](benchmarks/live-perf-read-only-64k-2026-08-18.json) |
| 50/50, 64 KiB parts | 64 × 32 | 14,583.48 ops/s | [JSON](benchmarks/live-perf-balanced-64k-2026-08-18.json) |
| Write-only, 64 KiB parts | 8 × 32 | 11,688.83 puts/s | [JSON](benchmarks/live-perf-write-only-c8-64k-2026-08-18.json) |
| Write-only, 64 KiB parts | 64 × 4 | 8,910.31 puts/s | [JSON](benchmarks/live-perf-write-only-c64-b4-64k-2026-08-18.json) |

These are 30- or 60-second attribution runs, not replacements for the full
SlateDB release benchmark. The short 50/50 rate benefits from updates and hot
reads remaining in memory; its 10,000-record working set is also far smaller
than SlateDB's roughly 120 GiB release dataset. A 64-client write-only run with
32 operations per client reset the Durable Object with an incomplete promise;
reducing the request to four operations per client completed. This identifies
per-request work size and the single-threaded Worker runtime as the write-side
limit to investigate next.

The current read ceiling still performs about four Durable Object part reads
per point lookup and materializes roughly four bytes for every byte SlateDB
requests. The next useful experiment is a 16 or 32 KiB part size; an in-memory
decoded-block cache would be a larger architectural change and is intentionally
not hidden inside this adapter.

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
