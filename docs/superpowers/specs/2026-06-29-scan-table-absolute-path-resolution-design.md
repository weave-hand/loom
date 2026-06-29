# `scan_table` must resolve absolute input paths

- **Date:** 2026-06-29
- **Area:** iceberg
- **Register item:** [[iss-iceberg-transform-chain-path]]
- **Status:** spec (ready for a work agent to plan + build)

## Problem

`datafusion_io::scan_table` (`src/services/datafusion-io/src/scan.rs`) registers a
table's data files as a named DataFusion table so a transform's SQL can read them.
For each file it builds the object key by prepending the warehouse root:

```rust
let key = format!("{LOOM_STORE_URL}/{}/{}/{}", table.schema, table.name, f.path);
ListingTableUrl::parse(key)
```

and registers an object store **only** under `LOOM_STORE_URL` (`loom://data`). This is
correct for **landed** tables, whose `FileRef.path` is table-directory-relative
(e.g. `<run_id>/part-0.parquet`).

It is wrong for **transform/compact outputs**. Those are written by `write_dataset`
(relative paths) and then promoted by `datafusion_io::absolute_data_files` into
**absolute** mirror paths — `{root_url}/{schema}/{table}/{rel}` where `root_url` is a
real object-store URL (`file://<data_path>` or `s3://<bucket>`), with
`path_is_relative = false`. That promotion was the fix for
[[iss-iceberg-transform-relpath]] (so a relative path can never leak into the mirror
and break the serving read path).

So when a transform reads **another transform's output**, `scan_table`:

1. prepends `loom://data/<schema>/<table>/` to an already-absolute path, producing a
   mangled key like `loom://data/schema/table/file://…`; and
2. registers no object store under the file's real scheme/authority (`file://` or
   `s3://bucket`).

DataFusion then fails at scan time. The defect is invisible on the landing path
(relative paths) and only bites transform-reads-transform chains.

`FileRef` — what `scan_table` receives from `catalog.files()` — carries only
`path: String` (`src/control-plane/core/src/catalog.rs:35`); the authoritative
`path_is_relative` flag that `DataFile` carries on the write side is dropped on
read-back. The read side must therefore infer absolute-vs-relative **from the path
string itself**, which is exactly what the serving engine already does.

## Reference: the serving engine already solves this

`engine-serving/src/serving.rs` reads the same absolute mirror paths through
`IcebergMirrorTableProvider`. It has a private resolver:

```rust
fn object_store_url_for(path: &str) -> datafusion::error::Result<ObjectStoreUrl> {
    if let Some(rest) = path.strip_prefix("s3://") {
        let bucket = rest.split('/').next().unwrap_or("");
        ObjectStoreUrl::parse(format!("s3://{bucket}"))
    } else {
        Ok(ObjectStoreUrl::local_filesystem())
    }
}
```

and `register_iceberg_table` registers a `LocalFileSystem` under
`local_filesystem()` plus (when an S3 warehouse is configured) the S3 store under
`s3://{bucket}`. `scan_table` should resolve absolute paths the same way.

## Fix

### Part 1 — share the resolver (relocate, do not duplicate)

Relocate `object_store_url_for` out of `engine-serving/src/serving.rs` and into
**`datafusion-io`** as a `pub` helper (the natural home — `scan_table`/`write_dataset`
own the warehouse path layout it resolves). `engine-serving` gains a dependency on
`datafusion-io` (a clean downward dependency — `datafusion-io` has no reverse edge to
`engine-serving`, so no cycle) and replaces its private copy with the shared one.

This unifies the two resolvers in one move rather than deferring the duplication to a
follow-up. Mechanics: add `datafusion-io` to `engine-serving`'s `Cargo.toml` + `BUCK`
deps and run `./tools/buckify.sh`.

### Part 2 — make `scan_table` scheme-aware (no signature change)

For each `FileRef`, detect a URI scheme (presence of `"://"`):

- **Absolute path** → use the path **verbatim** as the `ListingTableUrl`, and register
  the object store under its derived `ObjectStoreUrl` (`object_store_url_for`):
  - `file://` / local → a `LocalFileSystem` store (a fresh `LocalFileSystem`, matching
    `register_iceberg_table`);
  - `s3://bucket` → the passed warehouse `store`, registered under `s3://{bucket}`.
- **Relative path** → today's behavior unchanged: build
  `{LOOM_STORE_URL}/{schema}/{table}/{path}` and register `store` under
  `LOOM_STORE_URL`.

Register each distinct `ObjectStoreUrl` once (idempotent across files in the call).
In practice a single `scan_table` call reads one table at one snapshot, so its files
are uniformly relative (landed) or uniformly absolute (transform/compact) — but the
per-file branch is robust to a mix and costs nothing.

The signature stays `scan_table(ctx, store, name, table, files)`. The two callers —
`transform/src/run.rs:152` and `transform/src/compact.rs:75` — pass `store.clone()`
and need no change; the bucket is derivable from the path.

## Scope

In scope:

- `datafusion-io`: relocate `object_store_url_for` (now `pub`); make `scan_table`
  scheme-aware.
- `engine-serving`: depend on `datafusion-io`, drop the private resolver, use the
  shared one; `./tools/buckify.sh` for the dependency change.
- `transform`: the new e2e test (below).

Out of scope:

- Changing `FileRef` to carry `path_is_relative`. Path-sniffing is sufficient and
  matches the serving engine's proven approach; threading the flag through
  `catalog.files()` and every `FileRef` construction site is a larger, unrelated
  change.
- The `loom://data` virtual-store convention for relative (landed) paths — unchanged.

## Testing

Per the planning decision, a **transform-chain e2e** — the one path that fails today.
Extend the worker/transform e2e suite (`src/services/transform/tests/transform_e2e.rs`
with helpers in `transform_e2e_support.rs`):

1. Run transform **A** that lands an output table; its files commit to the mirror with
   **absolute** `file://` paths (via `absolute_data_files`).
2. Run transform **B** that declares A's output table as its input.
3. Assert B's `scan_table` resolves A's absolute-path files and B commits the expected
   rows (i.e. the chain reads back correctly).

This is a `loom_fixture_test` (boots the hermetic Postgres + a local warehouse), per
loom's testing rules — a `rust_test` integration target, never an inline
`#[cfg(test)]` module. The local-filesystem (`file://`) absolute path is the
deterministic case; the `s3://` registration branch is covered structurally by reusing
the serving engine's working pattern and is not separately driven here (no MinIO in
this test).

## Risk

- Landed (relative-path) tables are unaffected — that branch is byte-identical.
- The Part 1 relocate is a pure move: `engine-serving` behavior is identical, now
  sharing one resolver instead of two copies.
- The main new surface is the absolute-`s3://` registration, mitigated by reusing the
  serving engine's proven `object_store_url_for` + dual-registration shape and by the
  new `file://` e2e covering the absolute path end-to-end.
