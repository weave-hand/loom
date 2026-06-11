//! The serving-engine seam: run read-only SQL against loom's DuckLake catalog.
//! `EmbeddedDuckDb` runs in-process; `QuackServingEngine` forwards to a remote
//! `quack_serve`'d DuckDB via the `quack_query` table function.

use async_trait::async_trait;

/// A backend-neutral scalar cell. Scalar-only by design (lists are expanded into
/// placeholders before binding — see sql::compile_select).
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Null,
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

#[async_trait]
pub trait ServingEngine: Send + Sync {
    /// Execute read-only `sql`, binding `params` positionally (`?` placeholders).
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>;
}

/// Embedded DuckDB that has ATTACHed loom's DuckLake catalog read-only.
/// duckdb-rs is synchronous; calls run on a blocking thread. A fresh connection
/// per query keeps the slice simple (pool later).
pub struct EmbeddedDuckDb {
    attach_sql: String,
}

impl EmbeddedDuckDb {
    /// `data_path` must match the dir the writer used (relative file paths resolve
    /// under it). For data-free reads (e.g. SELECT 42) any existing dir works.
    pub async fn attach(
        socket: &std::path::Path,
        db: &str,
        data_path: &std::path::Path,
    ) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        // `USE lake;` makes the single attached DuckLake catalog the default, so the
        // unqualified table names compile_select emits (e.g. "main"."orders") resolve
        // against it and not DuckDB's default in-memory `memory` catalog (where the data
        // does not live). `lake` is the fixed ATTACH alias on the line above.
        let attach_sql = format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\nUSE lake;",
            ext_dir,
            db,
            socket.display(),
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
        // 3. Wrap in quack_query, embedding `forwarded` as a string literal (escaped
        // once more for THIS literal — table-function args must be constant, so no
        // bind). uri/token are loom-internal constants. Escaping composes.
        let wrapper = format!(
            "SELECT * FROM quack_query('{}', '{}', token := '{}', disable_ssl := true)",
            self.uri,
            sql_escape(&forwarded),
            self.token,
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
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Text(s) => format!("'{}'", sql_escape(s)),
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
    use duckdb::types::Value;
    match v {
        SqlValue::Text(s) => Value::Text(s.clone()),
        SqlValue::Int(i) => Value::BigInt(*i),
        SqlValue::Bool(b) => Value::Boolean(*b),
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
        Value::Text(s) => SqlValue::Text(s),
        other => SqlValue::Text(format!("{other:?}")),
    }
}
