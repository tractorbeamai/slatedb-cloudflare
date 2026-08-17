# slatedb-cloudflare

A Rust Cloudflare Worker with a Rust Durable Object.

## Development

```sh
bun install
bun run dev
```

Then try the Worker and its named Durable Object:

```sh
curl http://localhost:8787/
curl http://localhost:8787/objects/example
```

Each request to the same object name increments a counter persisted in Durable
Object storage. Different names address different objects.

## Checks and deployment

```sh
bun run format:check
bun run check
bun run build
bunx wrangler login
bun run deploy
```
