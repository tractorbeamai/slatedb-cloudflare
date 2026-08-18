FROM rust:1.97.1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --locked --release \
    && cp /src/target/release/slatedb-cloudflare-container /slatedb-cloudflare-container

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/cache/slatedb \
    && chown -R 1000:1000 /var/cache/slatedb
COPY --from=build /slatedb-cloudflare-container /usr/local/bin/slatedb-cloudflare-container
USER 1000:1000
EXPOSE 8080
ENTRYPOINT ["slatedb-cloudflare-container"]
