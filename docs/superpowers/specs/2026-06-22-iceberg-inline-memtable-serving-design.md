# Iceberg inline reads: serve from a MemTable, drop the Parquet round-trip — Design

> Closes `iss-iceberg-inline-reparse`. For an Iceberg table with un-flushed inline
> rows, every governed read pays an avoidable cost in `register_iceberg_table`
> (`src/services/query-api/src/serving_datafusion.rs:116-139`):
> `IcebergCatalog::inline_parquet` reconstructs the rows from Postgres, encodes them
> to in-memory Parquet via `ArrowWriter`, drops the bytes into a fresh `InMemory`
> object store, and a `ListingTable` re-parses the Parquet footer to *infer* a
> schema that the reconstruction already produced exactly. The Parquet hop is pure
> overhead — inline rows are already a separate provider that is `UNION`ed with the
> file provider, so they never needed to be Parquet. This slice serves inline rows
> from an in-memory batch-backed provider instead, with no change to served results.

## Where the cost sits today

`register_iceberg_table` builds the inline provider like this
(`serving_datafusion.rs:116-139`):

1. `catalog.inline_parquet(table, snap.id)` → `Option<Vec<u8>>`. Internally
   (`src/control-plane/postgres/src/iceberg_inline.rs:398-415`) this calls
   `inline_live_batch` (PG read + Arrow array build → `RecordBatch`) and then
   `ArrowWriter`-encodes that batch to Parquet bytes.
2. The bytes go into a freshly-allocated `InMemory` object store registered under a
   `memory://` URL.
3. `listing_table(ctx, vec![url])` builds a `ListingTable`, which calls
   `infer_schema` — re-parsing the Parquet footer to recover a schema that
   `inline_live_batch` already returned as a precise Arrow schema.

All three steps run on **every read** of a table that has live inline rows, until a
flush moves those rows to object storage. The only reason inline was ever encoded
to Parquet is the comment at `:114-115`: "A ListingTable can't span two object
stores, so inline is a SEPARATE provider, unioned with the file provider below."
But being a separate provider does not require being a `ListingTable` — any
`TableProvider` unions identically.

The **flush** path does not use `inline_parquet`: `iceberg_flush.rs:73` already
reconstructs via `inline_live_batch` directly. So `inline_parquet` is used only by
the serving engine and a few tests.

## Change

Serve inline rows from a DataFusion `MemTable` over the reconstructed
`RecordBatch`, eliminating the encode + footer-reparse + `InMemory`-store dance.

1. **Serving (`serving_datafusion.rs`).** Replace the `inline_parquet` →
   `InMemory` → `ListingTable` block (lines 116-139) with:
   - call `IcebergCatalog::inline_live_batch(table, snap.id)` (returns
     `Option<(i64, Vec<i64>, RecordBatch)>`); on `Some`, take the `RecordBatch`
     and ignore `tid`/`row_ids` (they exist for the flush path);
   - build `MemTable::try_new(batch.schema(), vec![vec![batch]])` and set
     `inline_provider = Some(Arc::new(memtable))`; on `None`, `inline_provider =
     None` exactly as today.
   - The `memory://` object-store registration and the inline use of the
     `listing_table` helper go away. (`listing_table` itself: see step 4.)
2. **Union logic unchanged.** The downstream `match (file_provider,
   inline_provider)` (`:147-161`) is untouched — `MemTable` is a `TableProvider`,
   so `ctx.read_table(...).union(...)` and the file-only / inline-only / both arms
   all work as-is.
3. **Remove the encoder.** Delete `IcebergCatalog::inline_parquet`
   (`iceberg_inline.rs:398-415`) and the now-unused `parquet57::arrow::ArrowWriter`
   import. `inline_live_batch` is the surviving reconstruction primitive (serving +
   flush both use it) and is **not** changed.
4. **`listing_table` helper.** After the change it is used only for the file path —
   actually the file path uses `IcebergMirrorTableProvider`, not `listing_table`,
   so `listing_table` may become dead. If so, remove it too (and its lone use was
   the inline branch). Confirm during implementation and delete if unreferenced.

## Correctness

The served union is identical to today's, row for row:

- **Datatypes match.** The `MemTable` carries the precise Arrow schema from
  `inline_live_batch`'s `arrow_field` — canonical `Utf8`/`Int32`/`Int64`/
  `Float64`/`Boolean`/`Date32`/`Timestamp(µs)`. The file provider infers with
  `ParquetFormat::default().with_force_view_types(false)`, i.e. canonical `Utf8`
  (not `Utf8View`), so the two sides agree on datatypes — the same agreement the
  current Parquet-encoded inline path relies on.
- **Column order matches.** Both the inline batch and the file schema derive from
  `self.schema(table, at)` / the same table schema, so positional order agrees.
- **Nullability.** The `MemTable` schema reflects the real `nullable` flags;
  Parquet inference on the file side often marks columns nullable regardless.
  `DataFrame::union` widens nullability (as the current code already notes at
  `:142-146`), so any difference is absorbed — exactly as today.

No DataFusion types leak into `control-plane-postgres`: the catalog returns a
`RecordBatch` (as it already does), and the `MemTable` is constructed in the
query-api serving crate.

## Testing

- **Migrate the `inline_parquet` callers** in tests to the surviving primitive or
  to behavioral assertions:
  - `src/control-plane/postgres/tests/iceberg_flush.rs` (lines ~117, 230, 260,
    319, 338),
  - `src/services/worker/tests/e2e.rs` (lines ~186, 272),
  - `src/services/engine/tests/wire.rs` (line ~320).
  Replace `inline_parquet(...)` assertions with `inline_live_batch(...)` assertions
  on the reconstructed batch (row count / schema / values), since `inline_parquet`
  is being deleted. Where a test only used the bytes as a "are there inline rows"
  probe, assert on `inline_live_batch` being `Some`/`None`.
- **Add one focused regression test** (query-api inline serving e2e): a table with
  **both** inline rows and at least one Parquet data file serves the correct
  `UNION ALL` (all file rows + all live inline rows), and a second identical read
  returns the same rows — proving the `MemTable` path preserves the union and is
  stable across reads.
- **Success criterion.** Existing inline serving tests stay green (behavior
  preserved). The perf win — no `ArrowWriter` encode and no Parquet footer
  re-inference on the read path — is proven *structurally* by the deletion of
  `inline_parquet` and its encoder, verifiable on review. No runtime
  metric/counter or observability seam is added.

## Out of scope

- **No snapshot-keyed caching.** Each read still does the PG read + Arrow build via
  `inline_live_batch` (correctness-required to observe freshly-committed inline
  rows). Memoizing the reconstruction by `(table_id, snapshot_id)` on the
  long-lived engine is a separable follow-up, only worth it if repeated
  identical-snapshot read latency is ever shown to matter; not done here.
- **Stays an ISSUES defect.** `iss-iceberg-inline-reparse` remains in
  `docs/ISSUES.md` (`open` → `fixed` when this lands); it is not promoted to
  ROADMAP. See `[[iss-iceberg-inline-visibility]]` (the related, accepted
  bounded-staleness item) and `[[road-iceberg-inline-flush]]` (the flush vertical
  that bounds how long inline rows live before becoming Parquet).
