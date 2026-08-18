# SlateDB on Cloudflare Durable Objects

A working example of running SlateDB 0.15.0 inside a Cloudflare Durable Object,
with R2 as its durable storage backend.

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

Reads use SlateDB's in-memory structures and R2. The Worker build disables
decoded block caching because Foyer requires a filesystem and Moka reaches
unsupported `std::time` behavior under workerd.

## Compatibility patches

SlateDB 0.15.0's native filesystem, signal, and multithreaded Tokio dependencies
do not compile for `wasm32-unknown-unknown`. The small, reviewable diffs in
`patches/`:

- select `tokio_with_wasm` on WebAssembly;
- preserve native Tokio behavior on non-WASM targets;
- disable filesystem-only cache and admin paths on WASM;
- adapt runtime handles, task cancellation, clocks, and `Send`-bounded timers;
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

The example API accepts UTF-8 keys and values. SlateDB itself stores bytes.
The reopen endpoint exists only to exercise R2 recovery.

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

## Checks

```sh
bun run format:check
bun run check
bun run build
```

These commands verify formatting, the patched WASM build, native unit tests,
Clippy, and a Wrangler deployment dry run. The unit tests cover conditional
write translation, byte ranges, pagination, and multipart payload assembly.

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
- Reads have no decoded block cache or persistent local cache.
- The R2 adapter currently materializes each requested object range before
  returning it to SlateDB.
- Multipart recovery above `object_store`'s 10 MiB writer threshold is not
  covered by the live test suite.
- The HTTP API is a test interface for the listed operations.
- Authentication and error handling are suitable only for controlled
  feasibility testing.

See Cloudflare's [R2 consistency documentation](https://developers.cloudflare.com/r2/reference/consistency/),
[Durable Object guidance](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/),
and [Workers WebAssembly constraints](https://developers.cloudflare.com/workers/runtime-apis/webassembly/).

## License

Licensed under the [Apache License 2.0](LICENSE).
