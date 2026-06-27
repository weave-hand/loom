//! The `iceberg_mirror.vector_index` binding (row type + insert/lookup) and the
//! build primitive (Task 6). loom records `(table, column, covered_snapshot) ->
//! puffin_path` as a mirror row in lieu of a REST catalog.

use control_plane_core::{ControlPlaneError, Result};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Row};

fn backend<E: std::fmt::Display>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(e.to_string().into())
}

/// A bound vector index: the metadata loom needs to find and decode the sidecar.
#[derive(Clone, Debug)]
pub struct VectorIndexRow {
    pub table_id: i64,
    pub column: String,
    pub covered_snapshot: i64,
    pub metric: String,
    pub index_kind: String,
    pub dim: i32,
    pub row_count: i64,
    pub puffin_path: String,
}

// SQL-STYLE (env-forced runtime, see plan "Decisions"): this cloud session cannot
// regenerate the .sqlx cache (Postgres won't boot as root; libxml2 egress is
// policy-blocked), so the new vector_index queries use RUNTIME
// `sqlx::query(AssertSqlSafe(...))` + bind params instead of compile-time
// `query!`/`query_scalar!`. The SQL is a fixed literal (no interpolation — every
// value is a bound `$n` param), so AssertSqlSafe carries no injection risk. This
// mirrors the runtime pattern already used in `iceberg_inline.rs`/`fixture.rs`.
// (Promotable to compile-time `query!` in a follow-up when a Postgres-capable env
// is available — tracked as a FUTURE item.)

/// Insert a `vector_index` binding row in the caller's transaction.
pub async fn insert_vector_index(tx: &mut PgConnection, row: &VectorIndexRow) -> Result<()> {
    sqlx::query(AssertSqlSafe(
        "insert into iceberg_mirror.vector_index \
         (table_id, column_name, covered_snapshot, metric, index_kind, dim, row_count, puffin_path) \
         values ($1, $2, $3, $4, $5, $6, $7, $8)",
    ))
    .bind(row.table_id)
    .bind(&row.column)
    .bind(row.covered_snapshot)
    .bind(&row.metric)
    .bind(&row.index_kind)
    .bind(row.dim)
    .bind(row.row_count)
    .bind(&row.puffin_path)
    .execute(&mut *tx)
    .await
    .map_err(backend)?;
    Ok(())
}

/// The newest bound index for `(table_id, column)` with `covered_snapshot <= at`,
/// or `None` if none is bound.
pub async fn lookup_vector_index(
    pool: &PgPool,
    table_id: i64,
    column: &str,
    at: i64,
) -> Result<Option<VectorIndexRow>> {
    let row = sqlx::query(AssertSqlSafe(
        "select table_id, column_name, covered_snapshot, metric, index_kind, dim, \
                row_count, puffin_path \
         from iceberg_mirror.vector_index \
         where table_id = $1 and column_name = $2 and covered_snapshot <= $3 \
         order by covered_snapshot desc limit 1",
    ))
    .bind(table_id)
    .bind(column)
    .bind(at)
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    row.map(|r| {
        Ok(VectorIndexRow {
            table_id: r.try_get("table_id").map_err(backend)?,
            column: r.try_get("column_name").map_err(backend)?,
            covered_snapshot: r.try_get("covered_snapshot").map_err(backend)?,
            metric: r.try_get("metric").map_err(backend)?,
            index_kind: r.try_get("index_kind").map_err(backend)?,
            dim: r.try_get("dim").map_err(backend)?,
            row_count: r.try_get("row_count").map_err(backend)?,
            puffin_path: r.try_get("puffin_path").map_err(backend)?,
        })
    })
    .transpose()
}
