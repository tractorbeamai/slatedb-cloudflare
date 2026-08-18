# SlateDB on Cloudflare Durable Objects

A working example of running SlateDB 0.15.0 inside a Cloudflare Durable Object,
with R2 as its durable storage backend.

This is a feasibility example, not a production-readiness claim. It demonstrates
CRUD, concurrent writes, prefix scans, and recovery from R2 after reopening the
database. It does not establish production throughput, latency, compaction
capacity, memory use, or cost.

## Architecture

Each logical database name maps deterministically to one `SlateDbObject`. That
Durable Object opens and retains one SlateDB instance while active, providing
the database's single-writer boundary. The outer Worker authenticates and routes
requests; it does not hold database state.

R2 is SlateDB's durable system of record. Every acknowledged write uses
SlateDB's default durable write options, and each logical database gets its own
R2 prefix.

The Durable Object class uses Cloudflare's required SQLite-backed namespace,
but this example does not store SlateDB data or cache entries in Durable Object
storage. Losing or evicting the Durable Object's in-memory state does not lose
acknowledged data.

SlateDB's decoded block cache is disabled. Its filesystem-backed cache is not
available in Workers, and its Moka cache currently reaches unsupported
`std::time` behavior under workerd. Reads therefore use SlateDB's in-memory
structures and R2.

## Compatibility patches

Published SlateDB 0.15.0 enables native filesystem, signal, and multithreaded
Tokio behavior that does not compile for `wasm32-unknown-unknown`. The small,
reviewable diffs in `patches/`:

- select `tokio_with_wasm` on WebAssembly;
- preserve native Tokio behavior on non-WASM targets;
- disable filesystem-only cache and admin paths on WASM;
- adapt runtime handles, task cancellation, clocks, and `Send`-bounded timers;
- keep `object_store`'s existing buffered multipart writer on the same
  Worker-compatible runtime.

`scripts/prepare-vendor.sh` downloads the published crate archives, verifies
their checksums, applies the diffs, and creates the ignored `vendor/` directory.
See [patches/README.md](patches/README.md) for provenance.

The upstream context is [SlateDB issue #179](https://github.com/slatedb/slatedb/issues/179)
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

Wrangler uses the primary `wrangler.jsonc` for both local development and
deployment. Its custom build watches `src/` rather than generated Rust and
vendor directories, so a second development config is unnecessary. Restart
Wrangler after changing Cargo dependencies or compatibility patches.

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
- There is no decoded block cache or persistent local cache.
- The R2 adapter currently materializes each requested object range before
  returning it to SlateDB rather than exposing the binding's body stream.
- The Worker-compatible multipart path compiles, but this repository does not
  yet include a live recovery test for an SST above the writer's 10 MiB
  multipart threshold.
- The HTTP API is intentionally small and is not a general SlateDB service
  interface.
- Bearer-token comparison and error responses are test controls, not a
  production authentication or public error-handling design.

See Cloudflare's [R2 consistency documentation](https://developers.cloudflare.com/r2/reference/consistency/),
[Durable Object guidance](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/),
and [Workers WebAssembly constraints](https://developers.cloudflare.com/workers/runtime-apis/webassembly/).

## License

Licensed under the [Apache License 2.0](LICENSE), matching SlateDB.
