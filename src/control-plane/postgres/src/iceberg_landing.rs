//! The Iceberg landing entrypoint: decode an Arrow IPC body (arrow-57), route by
//! in-memory size between an inline (mirror-only) write and a real Parquet write,
//! and return the loom mirror snapshot id. Both branches emit lineage atomically.
//!
//! This lives in the postgres crate (not ingest) because the iceberg writer chain
//! is arrow-57 and the ingest crate is arrow-58 — the ingest `IcebergMaterializer`
//! forwards the raw IPC body so the cross-major boundary stays inside this crate.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc57::reader::StreamReader;
use arrow_schema::Schema;
use arrow_select57::concat::concat_batches;
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, LineageEvent, Result, SnapshotId, TableRef,
};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{Catalog as IceCatalog, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_inline::inline_append;
use crate::iceberg_sql_catalog::{InlineEndCap, SqlCatalog};
use crate::iceberg_type::iceberg_physical_type;
use crate::iceberg_writer::append_batches_with_extras;

/// Boxing helper: wrap any boxable error as a control-plane `Backend` fault.
fn be<E: std::error::Error + Send + Sync + 'static>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}

/// Decode an Arrow IPC stream body into its (arrow-57) schema + batches.
fn decode_ipc_57(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(body), None).map_err(be)?;
    let schema = reader.schema();
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(be)?;
    Ok((schema, batches))
}

/// Land an Iceberg request. `inline_byte_limit` is the in-memory (uncompressed)
/// Arrow size at/below which the request inlines (mirror-only typed rows) instead
/// of writing real Parquet. `flush_byte_threshold` is the live-inline-byte total
/// at/above which a `flush_table` job is enqueued after an inline write. Returns
/// the loom mirror snapshot id either way.
#[allow(clippy::too_many_arguments)]
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    ipc_body: &[u8],
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let (schema, batches) = decode_ipc_57(ipc_body)?;
    // Project the decoded columns to `columns` order, by name. Both downstream
    // branches align columns POSITIONALLY (inline indexes `columns[c]` against
    // batch column `c`; the Parquet branch re-wraps under the table's schema in
    // `columns` order), but on the model-gate path `columns` is the model's
    // declared order, which need not match the wire order — so without this
    // realignment, same-typed reordered columns would silently swap values.
    let (schema, batches) = align_to_columns(&schema, batches, columns)?;
    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= inline_byte_limit {
        let batch = concat_batches(&schema, &batches).map_err(be)?;
        inline_append(
            pool,
            table,
            columns,
            &batch,
            lineage,
            Some(flush_byte_threshold),
        )
        .await
    } else {
        land_parquet(pool, catalog, table, columns, batches, lineage).await
    }
}

/// Project `batches` to `columns` order, matching by name, preserving the wire
/// arrow types. Extra wire columns not named in `columns` are dropped (the model
/// defines the landed schema). Errors if a declared column is absent from the data.
/// Fast-paths the common inferred case (`columns` already in wire order) to a
/// no-op clone, so that path is unchanged.
fn align_to_columns(
    schema: &Schema,
    batches: Vec<RecordBatch>,
    columns: &[ColumnSpec],
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let already_aligned = columns.len() == schema.fields().len()
        && columns
            .iter()
            .zip(schema.fields())
            .all(|(c, f)| c.name == *f.name());
    if already_aligned {
        return Ok((Arc::new(schema.clone()), batches));
    }

    let indices = columns
        .iter()
        .map(|c| {
            schema.index_of(&c.name).map_err(|_| {
                ControlPlaneError::Backend(
                    format!(
                        "landing: column {:?} declared but absent from the data",
                        c.name
                    )
                    .into(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
    let projected = Arc::new(Schema::new(fields));
    let batches = batches
        .into_iter()
        .map(|b| {
            let cols: Vec<_> = indices.iter().map(|&i| b.column(i).clone()).collect();
            RecordBatch::try_new(projected.clone(), cols).map_err(be)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((projected, batches))
}

/// Ensure the iceberg table exists (create-if-absent from `columns`), append
/// `batches` (bare arrow-57 — re-wrapped under the table's field-id schema) as a
/// real Parquet snapshot running `extras` in the commit tx, and return the mirror
/// snapshot id. Shared by the landing Parquet path and the flush path.
pub(crate) async fn append_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
) -> Result<SnapshotId> {
    let ns = NamespaceIdent::new(table.schema.clone());
    if !catalog.namespace_exists(&ns).await.map_err(be)? {
        catalog
            .create_namespace(&ns, Default::default())
            .await
            .map_err(be)?;
    }
    let ident = TableIdent::new(ns.clone(), table.name.clone());
    if !catalog.table_exists(&ident).await.map_err(be)? {
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(ice_schema(columns)?)
            .build();
        catalog.create_table(&ns, creation).await.map_err(be)?;
    }
    let ice_table = catalog.load_table(&ident).await.map_err(be)?;

    // A decoded IPC body carries a bare arrow schema; the iceberg writer chain needs
    // the table's arrow schema (which carries the iceberg field-id metadata) or it
    // can't map columns to field ids. Re-wrap each batch's columns under that
    // field-id-bearing schema. Positional alignment is safe here because `batches`
    // were already projected to `columns` order (= the table schema order) by
    // `align_to_columns` upstream.
    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ice_table.metadata().current_schema())
            .map_err(be)?,
    );
    let batches = batches
        .into_iter()
        .map(|b| RecordBatch::try_new(ice_arrow.clone(), b.columns().to_vec()).map_err(be))
        .collect::<Result<Vec<_>>>()?;

    append_batches_with_extras(catalog, &ice_table, batches, lineage, end_cap)
        .await
        .map_err(be)?;

    Ok(IcebergCatalog::new(pool.clone())
        .current_snapshot(table)
        .await?
        .id)
}

/// The Parquet branch: ensure the namespace + table exist, then append real
/// Parquet with an atomic lineage emit, and read the resulting mirror snapshot id
/// back. Idempotent on namespace/table (create-if-absent). Delegates to
/// [`append_parquet_snapshot`].
async fn land_parquet(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    append_parquet_snapshot(pool, catalog, table, columns, batches, Some(&lineage), None).await
}

/// Build an iceberg `Schema` from loom `ColumnSpec`s, assigning 1-based field ids.
fn ice_schema(columns: &[ColumnSpec]) -> Result<IceSchema> {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let ty = Type::Primitive(primitive_from(&c.ty)?);
            let id = (i + 1) as i32;
            Ok(Arc::new(if c.nullable {
                NestedField::optional(id, &c.name, ty)
            } else {
                NestedField::required(id, &c.name, ty)
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    IceSchema::builder().with_fields(fields).build().map_err(be)
}

/// loom logical type name -> iceberg `PrimitiveType` (the physical mapping reuses
/// `iceberg_physical_type` so the closed vocabulary stays in one place).
fn primitive_from(logical: &str) -> Result<PrimitiveType> {
    let phys = iceberg_physical_type(logical).ok_or_else(|| {
        ControlPlaneError::Backend(format!("landing: no iceberg type for {logical:?}").into())
    })?;
    Ok(match phys {
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "double" => PrimitiveType::Double,
        "boolean" => PrimitiveType::Boolean,
        "string" => PrimitiveType::String,
        "date" => PrimitiveType::Date,
        "timestamp" => PrimitiveType::Timestamp,
        other => {
            return Err(ControlPlaneError::Backend(
                format!("landing: unsupported iceberg primitive {other:?}").into(),
            ));
        }
    })
}
