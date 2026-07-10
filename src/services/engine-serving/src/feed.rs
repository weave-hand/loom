//! The changelog feed scan (road-stream-subscribe): the ONE invented substrate
//! primitive. For a CDC base table, the ordered union of its durable changelog
//! files and its live inline tail, from per-bucket resume positions — a plain
//! disjoint UNION ALL (slice-2b flush appends to the changelog and end-caps the
//! same inline rows in ONE tx, so an event is inline XOR files; no dedup, no
//! flush watermark). Governance is enforced by wrapping the union in
//! `GovernedTableProvider` BEFORE the ordered read — a subject never sees an
//! event, or a column, it may not read.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use control_plane_core::{Catalog, ChangeEvent, ChangeFeedPage, ControlPlaneError, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::changelog_table_ref;
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::{Expr, SessionContext, col, lit};
use object_store::local::LocalFileSystem;
use serde_json::Value;
use store_config::ServingStore;

use crate::governed::{GovernedTableProvider, TablePolicy};
use crate::serving::{
    EngineServingError, IcebergMirrorTableProvider, arrow_schema_from_mirror, to_serving,
};

/// The three reserved framing fields a feed tier presents AFTER the user
/// columns: `loom_change_kind`, `loom_bucket`, `loom_offset`. Sibling of
/// `with_cdc_framing_fields` (`serving.rs`), plus the bucket column the feed
/// orders/resumes on — the exact order + types of `framing_column_specs`
/// (`iceberg_landing.rs`) that the physical changelog/inline schema carries.
fn with_feed_framing_fields(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("loom_change_kind", DataType::Utf8, false));
    fields.push(Field::new("loom_bucket", DataType::Int32, true));
    fields.push(Field::new("loom_offset", DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

/// One Arrow cell -> `serde_json::Value`. Mirrors the value coercions in
/// query-api's `serving_datafusion::batches_to_rows` (Utf8, the integer/float/bool
/// scalars, Date32 -> ISO date, microsecond Timestamp -> ISO string) but is a
/// private reimplementation — engine-serving must not depend on query-api. Never
/// panics: a null cell is `Value::Null`, a failed downcast or an unmapped type
/// falls back to the row's formatted string (bounded to the single cell).
fn cell_to_json(array: &dyn Array, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map_or(Value::Null, |a| Value::String(a.value(row).to_string())),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map_or(Value::Null, |a| Value::from(a.value(row))),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map_or(Value::Null, |a| Value::from(a.value(row))),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map_or(Value::Null, |a| Value::from(a.value(row))),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map_or(Value::Null, |a| Value::Bool(a.value(row))),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map_or(Value::Null, |a| {
                let days = a.value(row);
                let d = time::macros::date!(1970 - 01 - 01) + time::Duration::days(i64::from(days));
                Value::String(d.to_string())
            }),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map_or(Value::Null, |a| {
                let micros = a.value(row);
                let odt =
                    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
                        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                let pdt = time::PrimitiveDateTime::new(odt.date(), odt.time());
                Value::String(pdt.to_string())
            }),
        // Defensive: a type loom doesn't serve as a first-class scalar. Render the
        // single cell (not the whole array) so the fallback is bounded and
        // row-correct; a formatter failure degrades to null, never a panic.
        _ => match ArrayFormatter::try_new(array, &FormatOptions::default()) {
            Ok(fmt) => Value::String(fmt.value(row).to_string()),
            Err(_) => Value::Null,
        },
    }
}

/// Tier 1 of the feed union: the changelog Iceberg files (absent until the
/// first flush). Resolves the changelog table ref for `base`, reads its
/// current snapshot/schema/files, and builds the framed
/// `IcebergMirrorTableProvider`. Returns `None` when nothing has flushed yet
/// (no changelog mirror row, or a mirror row with zero files).
async fn build_file_tier(
    catalog: &IcebergCatalog,
    base: &TableRef,
) -> Result<Option<IcebergMirrorTableProvider>, EngineServingError> {
    let clog = changelog_table_ref(base);
    match catalog.current_snapshot(&clog).await {
        Ok(snap) => {
            let cols = catalog
                .schema(&clog, snap.id)
                .await
                .map_err(to_serving)?
                .columns;
            let framed = with_feed_framing_fields(&arrow_schema_from_mirror(&cols)?);
            let files = catalog
                .files_with_stats(&clog, snap.id)
                .await
                .map_err(to_serving)?;
            if files.is_empty() {
                Ok(None)
            } else {
                Ok(Some(IcebergMirrorTableProvider::try_new_with_schema(
                    files, framed,
                )))
            }
        }
        // No changelog mirror row yet (nothing flushed): file tier absent.
        Err(ControlPlaneError::NotFound(_)) => Ok(None),
        Err(e) => Err(to_serving(e)),
    }
}

/// Tier 2 of the feed union: the base table's LIVE inline tail (full — keeps
/// `-U`), whose physical columns already carry the framing (`iceberg_inline`).
/// `snapshot_id` is `base`'s current snapshot id, already resolved by the
/// caller (who also needs it for the union's column projection).
async fn build_inline_tier(
    catalog: &IcebergCatalog,
    base: &TableRef,
    snapshot_id: control_plane_core::SnapshotId,
) -> Result<Option<MemTable>, EngineServingError> {
    match catalog
        .inline_live_batch_full(base, snapshot_id)
        .await
        .map_err(to_serving)?
    {
        Some((_tid, _row_ids, batch)) => {
            let schema = batch.schema();
            Ok(Some(
                MemTable::try_new(schema, vec![vec![batch]]).map_err(to_serving)?,
            ))
        }
        None => Ok(None),
    }
}

/// Decode: framing -> envelope; every other (governed) column -> fields.
/// Walks `batches` in order, extracting the reserved `loom_bucket` /
/// `loom_offset` / `loom_change_kind` framing columns per row (never masked
/// by governance) and every remaining column into the event's JSON `fields`
/// via `cell_to_json`. `positions` seeds the returned `next` map, which is
/// folded forward per row (`bucket -> offset + 1`).
fn decode_page(
    batches: &[arrow::record_batch::RecordBatch],
    positions: &BTreeMap<i32, i64>,
) -> Result<ChangeFeedPage, EngineServingError> {
    let mut events = Vec::new();
    let mut next = positions.clone();
    for batch in batches {
        let schema = batch.schema();
        let bidx = schema.index_of("loom_bucket").map_err(to_serving)?;
        let oidx = schema.index_of("loom_offset").map_err(to_serving)?;
        let kidx = schema.index_of("loom_change_kind").map_err(to_serving)?;
        let buckets = batch
            .column(bidx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| EngineServingError::Engine("loom_bucket is not Int32".into()))?;
        let offsets = batch
            .column(oidx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| EngineServingError::Engine("loom_offset is not Int64".into()))?;
        let kinds = batch
            .column(kidx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| EngineServingError::Engine("loom_change_kind is not Utf8".into()))?;
        for row in 0..batch.num_rows() {
            if buckets.is_null(row) || offsets.is_null(row) || kinds.is_null(row) {
                return Err(EngineServingError::Engine(
                    "changelog feed row has a null framing column".into(),
                ));
            }
            let bucket = buckets.value(row);
            let offset = offsets.value(row);
            let change_kind = kinds.value(row).to_string();
            let mut fields = serde_json::Map::new();
            for (ci, field) in schema.fields().iter().enumerate() {
                if field.name().starts_with("loom_") {
                    continue;
                }
                fields.insert(
                    field.name().clone(),
                    cell_to_json(batch.column(ci).as_ref(), row),
                );
            }
            next.insert(bucket, offset + 1);
            events.push(ChangeEvent {
                bucket,
                offset,
                change_kind,
                fields,
            });
        }
    }
    Ok(ChangeFeedPage { events, next })
}

/// Registers the object stores a feed scan's `SessionContext` needs
/// (idempotent, per `build_serving_provider`): the local filesystem for
/// absolute `file://` warehouse paths always; the S3 store under
/// `s3://{bucket}` when a serving store is configured.
fn register_object_stores(
    ctx: &SessionContext,
    serving_store: Option<&ServingStore>,
) -> Result<(), EngineServingError> {
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );
    if let Some(ServingStore { bucket, store }) = serving_store {
        let url = ObjectStoreUrl::parse(format!("s3://{bucket}")).map_err(to_serving)?;
        ctx.register_object_store(url.as_ref(), store.clone());
    }
    Ok(())
}

/// The union's column projection, common to both tiers: `[user_cols...,
/// loom_change_kind, loom_bucket, loom_offset]`.
fn union_select_columns(base_cols: &[control_plane_core::ColumnDef]) -> Vec<Expr> {
    let mut names: Vec<String> = base_cols.iter().map(|c| c.name.clone()).collect();
    names.extend([
        "loom_change_kind".to_string(),
        "loom_bucket".to_string(),
        "loom_offset".to_string(),
    ]);
    names
        .iter()
        .map(|n| Expr::Column(Column::new_unqualified(n)))
        .collect()
}

/// Projects each present tier to `select` and folds them into one disjoint
/// `UNION ALL`, propagating `DataFrame::union` errors via `?`. `None` when
/// neither tier is present (both flush-absent and inline-absent).
fn union_tiers(
    ctx: &SessionContext,
    file_provider: Option<IcebergMirrorTableProvider>,
    inline_provider: Option<MemTable>,
    select: &[Expr],
) -> Result<Option<datafusion::prelude::DataFrame>, EngineServingError> {
    let mut tiers = Vec::new();
    if let Some(f) = file_provider {
        tiers.push(
            ctx.read_table(Arc::new(f))
                .map_err(to_serving)?
                .select(select.to_vec())
                .map_err(to_serving)?,
        );
    }
    if let Some(i) = inline_provider {
        tiers.push(
            ctx.read_table(Arc::new(i))
                .map_err(to_serving)?
                .select(select.to_vec())
                .map_err(to_serving)?,
        );
    }
    let mut acc: Option<datafusion::prelude::DataFrame> = None;
    for tier in tiers {
        acc = Some(match acc {
            None => tier,
            Some(prev) => prev.union(tier).map_err(to_serving)?,
        });
    }
    Ok(acc)
}

/// The resume predicate: OR over buckets of `(bucket = b AND offset >=
/// next_b)`. `None` when `positions` is empty (caller already short-circuits
/// on that, but this stays total).
fn build_resume_predicate(positions: &BTreeMap<i32, i64>) -> Option<Expr> {
    let mut pred: Option<Expr> = None;
    for (b, off) in positions {
        let leaf = col("loom_bucket")
            .eq(lit(*b))
            .and(col("loom_offset").gt_eq(lit(*off)));
        pred = Some(match pred {
            None => leaf,
            Some(p) => p.or(leaf),
        });
    }
    pred
}

/// One bounded, governed, ordered page of `base`'s changelog feed from
/// per-bucket `positions` (bucket -> first offset not yet consumed; the caller
/// supplies EVERY bucket). Returns the events ordered by `(bucket, offset)`
/// (cross-bucket interleaving is positional, not chronological) plus the
/// advanced positions. `-U` before-images are included — the changelog contract
/// is full events.
///
/// The union is disjoint by construction: the slice-2b flush appends every change
/// row to the changelog table AND end-caps the same inline rows in one tx, so an
/// event is inline XOR a changelog file — no dedup, no watermark. Governance wraps
/// the union view BEFORE the ordered read; the reserved framing columns are never
/// denied/masked, so ordering/resume survive while user columns are filtered/masked.
pub async fn changelog_feed_scan(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
) -> Result<ChangeFeedPage, EngineServingError> {
    let empty = || ChangeFeedPage {
        events: vec![],
        next: positions.clone(),
    };
    if positions.is_empty() || limit == 0 {
        return Ok(empty());
    }

    let ctx = SessionContext::new();
    register_object_stores(&ctx, serving_store)?;

    // --- Tier 1: the changelog Iceberg files (absent until the first flush). ---
    let file_provider = build_file_tier(catalog, base).await?;

    // --- Tier 2: the base table's LIVE inline tail (full — keeps -U), whose
    // physical columns already carry the framing (iceberg_inline). ---
    let base_snap = catalog.current_snapshot(base).await.map_err(to_serving)?;
    let inline_provider = build_inline_tier(catalog, base, base_snap.id).await?;

    // --- Disjoint UNION ALL, both tiers projected to the SAME column order:
    // [user_cols..., loom_change_kind, loom_bucket, loom_offset]. ---
    let base_cols = catalog
        .schema(base, base_snap.id)
        .await
        .map_err(to_serving)?
        .columns;
    let select = union_select_columns(&base_cols);
    let Some(unioned) = union_tiers(&ctx, file_provider, inline_provider, &select)? else {
        return Ok(empty());
    };

    // --- Governance BEFORE the ordered read: wrap the union view. Framing
    // columns are never denied/masked (reserved names), so ordering survives;
    // row filters and column masks apply to the user columns. ---
    let governed = GovernedTableProvider::new(unioned.into_view(), policy.clone())?;
    let df = ctx.read_table(Arc::new(governed)).map_err(to_serving)?;

    // --- Resume predicate: OR over buckets of (bucket = b AND offset >= next_b). ---
    let Some(pred) = build_resume_predicate(positions) else {
        return Ok(empty());
    };

    // --- Order + bound. ---
    let batches = df
        .filter(pred)
        .map_err(to_serving)?
        .sort(vec![
            col("loom_bucket").sort(true, false),
            col("loom_offset").sort(true, false),
        ])
        .map_err(to_serving)?
        .limit(0, Some(limit))
        .map_err(to_serving)?
        .collect()
        .await
        .map_err(to_serving)?;

    // --- Decode: framing -> envelope; every other (governed) column -> fields. ---
    decode_page(&batches, positions)
}
