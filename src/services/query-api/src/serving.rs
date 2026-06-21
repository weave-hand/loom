//! The serving-engine seam: run read-only SQL against loom's DuckLake catalog.
//! `EmbeddedDuckDb` runs in-process; `QuackServingEngine` forwards to a remote
//! `quack_serve`'d DuckDB via the `quack_query` table function.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use control_plane_core::{BaseType, ColumnSpec, resolve_logical};

use crate::sql::{DuckDbDialect, SqlDialect};

/// A backend-neutral scalar cell. Scalar-only by design (lists are expanded into
/// placeholders before binding — see sql::compile_select_with).
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Double(f64),
    Date(time::Date),
    Timestamp(time::PrimitiveDateTime),
    Null,
}

/// ISO-8601 date `YYYY-MM-DD`. Shared by the JSON renderer and the Quack literal path.
pub(crate) fn iso_date(d: &time::Date) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    d.format(&fmt).unwrap_or_else(|_| d.to_string())
}

/// ISO-8601 datetime `YYYY-MM-DDThh:mm:ss` (no subseconds, no offset — bare timestamp).
pub(crate) fn iso_timestamp(ts: &time::PrimitiveDateTime) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    ts.format(&fmt).unwrap_or_else(|_| ts.to_string())
}

/// A result set: column names plus rows of cells (row-major, aligned to `columns`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServingError {
    #[error("serving engine: {0}")]
    Engine(String),
}

/// Build a one-row Arrow `RecordBatch` + `Schema` + loom `ColumnSpec` list from an
/// aligned `(columns, values, logical_types)` triple. Each logical type resolves
/// (via `resolve_logical`) to a `BaseType` that fixes BOTH the Arrow `DataType` and
/// the loom logical `ColumnSpec.ty` (its canonical name). Every field is nullable —
/// an action write passes `SqlValue::Null` for any property it does not set. A length
/// mismatch, an empty input, an unknown logical type, or a value whose variant does
/// not match its column's base type is a `ServingError::Engine`.
pub fn build_object_batch(
    columns: &[String],
    values: &[SqlValue],
    logical_types: &[String],
) -> Result<(Arc<Schema>, RecordBatch, Vec<ColumnSpec>), ServingError> {
    if columns.is_empty() || columns.len() != values.len() || columns.len() != logical_types.len() {
        return Err(ServingError::Engine(format!(
            "build_object_batch: {} columns / {} values / {} types (need >= 1, equal counts)",
            columns.len(),
            values.len(),
            logical_types.len()
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut specs: Vec<ColumnSpec> = Vec::with_capacity(columns.len());
    for ((name, value), logical) in columns.iter().zip(values).zip(logical_types) {
        let base = resolve_logical(logical)
            .ok_or_else(|| ServingError::Engine(format!("unknown logical type `{logical}`")))?;
        let (dt, array) = one_cell(base, value, name)?;
        fields.push(Field::new(name, dt, true));
        arrays.push(array);
        specs.push(ColumnSpec {
            name: name.clone(),
            ty: base.canonical_name().to_string(),
            nullable: true,
        });
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    Ok((schema, batch, specs))
}

/// One single-row Arrow array for a cell of base type `base`. `SqlValue::Null`
/// yields a typed null; any non-null variant must match `base` (the same
/// scalar-to-Arrow mapping `to_duck` uses) or it is a `ServingError`.
fn one_cell(base: BaseType, v: &SqlValue, col: &str) -> Result<(DataType, ArrayRef), ServingError> {
    let mismatch = || {
        ServingError::Engine(format!(
            "value for column `{col}` does not match its {base:?} type"
        ))
    };
    Ok(match base {
        BaseType::Integer => {
            let cell: Option<i32> = match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some((*i).try_into().map_err(|_| {
                    ServingError::Engine(format!("integer overflow for column `{col}`"))
                })?),
                _ => return Err(mismatch()),
            };
            (DataType::Int32, Arc::new(Int32Array::from(vec![cell])))
        }
        BaseType::Long => {
            let cell: Option<i64> = match v {
                SqlValue::Null => None,
                SqlValue::Int(i) => Some(*i),
                _ => return Err(mismatch()),
            };
            (DataType::Int64, Arc::new(Int64Array::from(vec![cell])))
        }
        BaseType::Double => {
            let cell: Option<f64> = match v {
                SqlValue::Null => None,
                SqlValue::Double(f) => Some(*f),
                _ => return Err(mismatch()),
            };
            (DataType::Float64, Arc::new(Float64Array::from(vec![cell])))
        }
        BaseType::Boolean => {
            let cell: Option<bool> = match v {
                SqlValue::Null => None,
                SqlValue::Bool(b) => Some(*b),
                _ => return Err(mismatch()),
            };
            (DataType::Boolean, Arc::new(BooleanArray::from(vec![cell])))
        }
        BaseType::String => {
            let cell: Option<String> = match v {
                SqlValue::Null => None,
                SqlValue::Text(s) => Some(s.clone()),
                _ => return Err(mismatch()),
            };
            (DataType::Utf8, Arc::new(StringArray::from(vec![cell])))
        }
        BaseType::Date => {
            let cell: Option<i32> = match v {
                SqlValue::Null => None,
                SqlValue::Date(d) => {
                    Some((*d - time::macros::date!(1970 - 01 - 01)).whole_days() as i32)
                }
                _ => return Err(mismatch()),
            };
            (DataType::Date32, Arc::new(Date32Array::from(vec![cell])))
        }
        BaseType::Timestamp => {
            let cell: Option<i64> = match v {
                SqlValue::Null => None,
                SqlValue::Timestamp(ts) => Some(
                    (ts.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                        .whole_microseconds()
                        .try_into()
                        .unwrap_or(i64::MAX),
                ),
                _ => return Err(mismatch()),
            };
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![cell])),
            )
        }
    })
}

#[async_trait]
pub trait ServingEngine: Send + Sync {
    /// Execute read-only `sql`, binding `params` positionally (`?` placeholders).
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>;

    /// The SQL dialect this engine speaks. Defaults to DuckDB — every serving engine
    /// loom ships today (`EmbeddedDuckDb`, `QuackServingEngine`) is DuckDB-compatible.
    /// A future non-DuckDB engine overrides this.
    fn dialect(&self) -> &'static dyn SqlDialect {
        &DuckDbDialect
    }
}

/// A write-capable serving engine — the atomic action write-back seam. An impl
/// writes ONE row AND commits its lineage event in the same transaction, so the
/// snapshot and its lineage land or roll back together (no dangling slice). The
/// seam is Arrow-free (takes `logical_types`, not a `RecordBatch`); each impl
/// builds Arrow internally. Mirrors the Iceberg `inline_append(.., lineage) ->
/// SnapshotId` contract.
#[async_trait]
pub trait ActionEngine: Send + Sync {
    async fn write_object(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError>;
}

use control_plane_core::{ControlPlane, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

/// Action writer that routes a single-row write through loom's OWN atomic
/// snapshot-commit primitive (`ingest::materialize::land_ducklake`): build a
/// one-row Parquet file, then create_table (idempotent — the target table already
/// exists) + append_files + emit(lineage) + commit, all in one Postgres
/// transaction, returning the new `SnapshotId`. The row and its lineage land or
/// roll back together. Replaces the inline `EmbeddedDuckDbWriter`; the part-1
/// "inline row, no Parquet" low-latency property is retired in favor of atomicity
/// (actions are interactive, low-frequency; small files are handled by compaction).
pub struct DuckLakeActionWriter {
    cp: Arc<dyn ControlPlane>,
    store: Arc<dyn ObjectStore>,
}

impl DuckLakeActionWriter {
    pub fn new(cp: Arc<dyn ControlPlane>, store: Arc<dyn ObjectStore>) -> Self {
        Self { cp, store }
    }
}

#[async_trait]
impl ActionEngine for DuckLakeActionWriter {
    async fn write_object(
        &self,
        table: &TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        let (schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        // Unique per action: the run id keeps each write's files in their own dir.
        let file_prefix = format!("action-{}", event.run_id.0);
        ingest::materialize::land_ducklake(
            self.cp.as_ref(),
            self.store.clone(),
            table,
            schema,
            &specs,
            std::slice::from_ref(&batch),
            &file_prefix,
            event,
        )
        .await
        .map_err(|e| ServingError::Engine(e.to_string()))
    }
}

/// Embedded DuckDB that has ATTACHed loom's DuckLake catalog read-only.
/// duckdb-rs is synchronous; calls run on a blocking thread. A fresh connection
/// per query keeps the slice simple (pool later).
pub struct EmbeddedDuckDb {
    attach_sql: String,
}

impl EmbeddedDuckDb {
    /// `pg_conn` is a libpq connection string (e.g. "dbname=loom host=/sock user=postgres"
    /// or "dbname=loom host=db.internal port=5432 user=loom password=secret").
    /// `data_path` must match the dir the writer used (relative file paths resolve under it).
    pub async fn attach(pg_conn: &str, data_path: &std::path::Path) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        let attach_sql = format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:{}' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\nUSE lake;",
            ext_dir,
            pg_conn,
            data_path.display(),
        );
        Ok(Self { attach_sql })
    }
}

#[async_trait]
impl ServingEngine for EmbeddedDuckDb {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        let attach = self.attach_sql.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        tokio::task::spawn_blocking(move || run_sync(&attach, &sql, &params))
            .await
            .map_err(|e| ServingError::Engine(format!("join: {e}")))?
    }
}

/// A Quack-protocol client `ServingEngine`: forwards SQL to a separate
/// `quack_serve`'d DuckDB (which owns the DuckLake `ATTACH`) via the `quack_query`
/// table function. The local duckdb-rs connection only `LOAD quack`s — no catalog
/// is attached client-side; execution happens on the serving tier.
pub struct QuackServingEngine {
    /// e.g. "quack:127.0.0.1:9494" — the server's listen URI (client form).
    uri: String,
    /// Auth token agreed with the server.
    token: String,
    /// Offline extension dir to `LOAD quack` from (DUCKDB_EXTENSION_DIR).
    ext_dir: String,
}

impl QuackServingEngine {
    pub fn new(uri: impl Into<String>, token: impl Into<String>) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        Ok(Self {
            uri: uri.into(),
            token: token.into(),
            ext_dir,
        })
    }
}

#[async_trait]
impl ServingEngine for QuackServingEngine {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        // 1. Inline params (quack_query has no bind slot). 2. Prefix `USE lake;` —
        // a quack-forwarded session does NOT inherit the server's USE lake.
        let forwarded = format!("USE lake; {}", inline_params(sql, params));
        // All three values are wrapped in sql_escape: `forwarded` carries
        // user-derived param values, and uri/token are escaped defensively so a
        // stray quote can never break out of the quack_query call. disable_ssl is
        // hardcoded for now — this engine is test/local-scope; making TLS
        // configurable is part of the later HTTP-wiring slice.
        let wrapper = format!(
            "SELECT * FROM quack_query('{}', '{}', token := '{}', disable_ssl := true)",
            sql_escape(&self.uri),
            sql_escape(&forwarded),
            sql_escape(&self.token),
        );
        let preamble = format!("SET extension_directory='{}';\nLOAD quack;", self.ext_dir);
        tokio::task::spawn_blocking(move || run_sync(&preamble, &wrapper, &[]))
            .await
            .map_err(|e| ServingError::Engine(format!("join: {e}")))?
    }
}

fn run_sync(attach: &str, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
    use duckdb::types::Value;
    let conn =
        duckdb::Connection::open_in_memory().map_err(|e| ServingError::Engine(e.to_string()))?;
    conn.execute_batch(attach)
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    let bound: Vec<Value> = params.iter().map(to_duck).collect();
    let pref: Vec<&dyn duckdb::ToSql> = bound.iter().map(|v| v as &dyn duckdb::ToSql).collect();
    let mut q = stmt
        .query(pref.as_slice())
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    // Capture the result schema from the executed query BEFORE stepping, so a
    // zero-row result still reports its columns (an empty `Rows.columns` would be
    // wrong for a governed read that legitimately matched no rows). Column metadata
    // is only populated after `query()` executes the statement — reading it off the
    // freshly-prepared statement panics.
    let columns: Vec<String> = q.as_ref().map(|s| s.column_names()).unwrap_or_default();

    let mut rows: Vec<Vec<SqlValue>> = Vec::new();
    while let Some(row) = q.next().map_err(|e| ServingError::Engine(e.to_string()))? {
        let mut cells = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let v: Value = row
                .get(i)
                .map_err(|e| ServingError::Engine(e.to_string()))?;
            cells.push(from_duck(v));
        }
        rows.push(cells);
    }
    Ok(Rows { columns, rows })
}

/// Escape a string for embedding in a DuckDB single-quoted literal: double every
/// `'`. This is the complete escape for DuckDB standard string literals (no
/// backslash escapes by default).
fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Render a typed scalar as a DuckDB SQL literal.
fn render_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Double(f) => f.to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Text(s) => format!("'{}'", sql_escape(s)),
        SqlValue::Date(d) => format!("DATE '{}'", iso_date(d)),
        SqlValue::Timestamp(ts) => format!("TIMESTAMP '{}'", iso_timestamp(ts)),
    }
}

/// Substitute each `?` placeholder in `sql` with the next rendered param, copying
/// every other character verbatim. The Quack path uses this because `quack_query`
/// takes SQL as a string with no bind slot. Relies on the `compile_select`
/// contract that `?` appears ONLY as a bind placeholder (never a literal `?`
/// inside a string), so a single left-to-right pass over the ORIGINAL `sql` is
/// correct — it never re-scans substituted text (a rendered value may contain `?`).
pub fn inline_params(sql: &str, params: &[SqlValue]) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut it = params.iter();
    for ch in sql.chars() {
        if ch == '?' {
            match it.next() {
                Some(p) => out.push_str(&render_literal(p)),
                None => out.push('?'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn to_duck(v: &SqlValue) -> duckdb::types::Value {
    use duckdb::types::{TimeUnit, Value};
    match v {
        SqlValue::Text(s) => Value::Text(s.clone()),
        SqlValue::Int(i) => Value::BigInt(*i),
        SqlValue::Bool(b) => Value::Boolean(*b),
        SqlValue::Double(f) => Value::Double(*f),
        SqlValue::Date(d) => {
            Value::Date32((*d - time::macros::date!(1970 - 01 - 01)).whole_days() as i32)
        }
        SqlValue::Timestamp(ts) => {
            // whole_microseconds() is i128; saturate rather than silently wrap on the
            // (implausible, far-from-epoch) overflow case.
            let micros = (ts.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                .whole_microseconds()
                .try_into()
                .unwrap_or(i64::MAX);
            Value::Timestamp(TimeUnit::Microsecond, micros)
        }
        SqlValue::Null => Value::Null,
    }
}

fn from_duck(v: duckdb::types::Value) -> SqlValue {
    use duckdb::types::Value;
    match v {
        Value::Null => SqlValue::Null,
        Value::Boolean(b) => SqlValue::Bool(b),
        Value::TinyInt(i) => SqlValue::Int(i as i64),
        Value::SmallInt(i) => SqlValue::Int(i as i64),
        Value::Int(i) => SqlValue::Int(i as i64),
        Value::BigInt(i) => SqlValue::Int(i),
        Value::Float(f) => SqlValue::Double(f as f64),
        Value::Double(f) => SqlValue::Double(f),
        Value::Date32(days) => SqlValue::Date(date_from_epoch_days(days)),
        Value::Timestamp(unit, n) => SqlValue::Timestamp(timestamp_from_unit(unit, n)),
        Value::Text(s) => SqlValue::Text(s),
        // Decimal, Time64, HugeInt, lists/structs, etc. are not yet first-class; keep
        // the defensive debug fallback so an unmapped variant never panics a read.
        other => SqlValue::Text(format!("{other:?}")),
    }
}

/// Days since the Unix epoch -> a calendar date.
fn date_from_epoch_days(days: i32) -> time::Date {
    time::macros::date!(1970 - 01 - 01) + time::Duration::days(days as i64)
}

/// A DuckDB timestamp (unit + count since epoch) -> a wall-clock datetime.
fn timestamp_from_unit(unit: duckdb::types::TimeUnit, n: i64) -> time::PrimitiveDateTime {
    use duckdb::types::TimeUnit;
    let nanos: i128 = match unit {
        TimeUnit::Second => n as i128 * 1_000_000_000,
        TimeUnit::Millisecond => n as i128 * 1_000_000,
        TimeUnit::Microsecond => n as i128 * 1_000,
        TimeUnit::Nanosecond => n as i128,
    };
    let odt = time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    time::PrimitiveDateTime::new(odt.date(), odt.time())
}
