//! The serving-engine seam: run read-only SQL against loom's DuckLake catalog.
//! One impl now (EmbeddedDuckDb); a Quack-client impl drops in later unchanged.

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
        let attach_sql = format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);",
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

    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<SqlValue>> = Vec::new();
    while let Some(row) = q.next().map_err(|e| ServingError::Engine(e.to_string()))? {
        if columns.is_empty() {
            // `column_names` lives on the statement; `AsRef<Statement>` for Row hands
            // it back, and the query has been stepped, so the schema is available.
            let stmt: &duckdb::Statement = row.as_ref();
            columns = stmt.column_names();
        }
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
