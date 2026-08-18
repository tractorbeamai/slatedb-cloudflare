# Patched crate provenance

`scripts/prepare-vendor.sh` downloads the published crate archives, verifies
their SHA-256 checksums, and applies the unified diffs in this directory. The
generated `vendor/` directory is ignored by Git.

| Crate | Version | Upstream release commit |
| --- | --- | --- |
| `object_store` | 0.14.1 | Apache Arrow Rust release `object_store_0.14.1` |
| `slatedb` | 0.15.0 | `7db4911082c8af96beb4be3ec2e4f8cbf0b142c8` |
| `slatedb-common` | 0.15.0 | `7db4911082c8af96beb4be3ec2e4f8cbf0b142c8` |
| `slatedb-txn-obj` | 0.15.0 | `7db4911082c8af96beb4be3ec2e4f8cbf0b142c8` |

The SlateDB patches only add Worker runtime compatibility and disable native
filesystem-only paths on `wasm32`. The `object_store` patch selects
`tokio_with_wasm` for its buffered multipart writer on WebAssembly. None of the
patches change SlateDB's cache API.
