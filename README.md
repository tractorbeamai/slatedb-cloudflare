# SlateDB on Cloudflare

Two runnable proofs of concept for using [SlateDB](https://slatedb.io/) with
Cloudflare R2 as the durable object store. Both map one logical database to one
stateful coordinator and expose the same small HTTP API, but they make different
runtime and cache tradeoffs.

| Example | SlateDB runtime | Local object cache | Main constraint |
| --- | --- | --- | --- |
| [Durable Object](durable-object/) | Patched `wasm32-unknown-unknown` inside a Worker isolate | Persistent Durable Object storage, backed by SQLite | 128 MiB, single-threaded Worker runtime |
| [Container](container/) | Unmodified SlateDB 0.15.0 on native Linux | SlateDB's bounded filesystem cache on ephemeral SSD | Container cold starts and active-resource billing |

R2 is authoritative in both examples. Losing a Worker isolate, Durable Object
cache, container, or container filesystem must not lose an acknowledged write.
One Durable Object or container instance owns each database name, preserving
SlateDB's single-writer boundary.

## Which example to use

Use the Durable Object proof to evaluate the smallest Cloudflare-native
deployment. It can sleep cheaply and its SST cache survives isolate eviction,
but SlateDB currently needs a focused WASM compatibility patch and conservative
memory settings.

Use the Container proof when normal SlateDB behavior matters more. It uses the
published crate unchanged, has real Tokio threads and timers, supports the
normal multipart upload path, and has substantially higher memory and disk
ceilings. Its local cache disappears whenever Cloudflare replaces or sleeps the
container, and a cold start commonly adds seconds before the database is ready.

Cloudflare Containers are controlled through a Durable Object, so the
Container example still has a small Worker and Durable Object routing layer.
SlateDB itself runs only in the native container. Its R2 traffic uses a virtual
hostname intercepted by that Worker, keeping the native R2 binding and all R2
credentials outside the container. See Cloudflare's
[architecture](https://developers.cloudflare.com/containers/platform-details/architecture/),
[instance types](https://developers.cloudflare.com/containers/platform-details/limits/),
and [pricing](https://developers.cloudflare.com/containers/pricing/) for the
current platform bounds.

## Repository layout

- [`durable-object/`](durable-object/) contains the Worker Rust source, minimal
  upstream patches, Durable Object cache adapter, deployment config, benchmark
  evidence, and its own setup guide.
- [`container/`](container/) contains a native Rust service, Docker image,
  routing Worker, deployment config, and its own setup guide.
- [`scripts/`](scripts/) contains the API-compatible smoke and benchmark drivers.
- The root Bun workspace coordinates checks and formatting across both examples.

Install dependencies and check both examples:

```sh
bun install
bun run format:check
bun run check
```

The full check builds a Docker image for the Container example. Docker or a
compatible engine must be running.

This repository is a feasibility demonstration, not a production-readiness
claim. Throughput, tail latency, compaction capacity, recovery time, and cost
still need validation against the intended dataset and workload.

## License

Apache-2.0, matching SlateDB. See [LICENSE](LICENSE).
