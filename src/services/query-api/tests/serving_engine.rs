//! ServingEngine mechanics: a plain SELECT returns typed rows, and caller values
//! are BOUND (a value containing a quote cannot break or inject into the SQL).

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{EmbeddedDuckDb, ServingEngine, SqlValue};

async fn engine(fx: &PgFixture) -> (EmbeddedDuckDb, DuckLakeWriter) {
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    // No catalog data needed for these mechanics tests, but DuckLake records the
    // writer's DATA_PATH in the catalog on bootstrap and rejects a mismatched one
    // on re-ATTACH, so reuse it. Keep the writer alive (it owns the temp dir).
    let eng = EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path())
        .await
        .expect("attach");
    (eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_rows_returns_typed_cells() {
    let fx = PgFixture::start();
    let (eng, _writer) = engine(&fx).await;
    let rows = eng
        .fetch_rows("SELECT 42 AS n, 'hi' AS s", &[])
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["n".to_string(), "s".to_string()]);
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::Int(42));
    assert_eq!(rows.rows[0][1], SqlValue::Text("hi".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_rows_still_reports_columns() {
    // A governed read can legitimately match no rows; the result must still carry
    // the schema. Columns are captured from the prepared statement, not lazily.
    let fx = PgFixture::start();
    let (eng, _writer) = engine(&fx).await;
    let rows = eng
        .fetch_rows("SELECT 42 AS n, 'hi' AS s WHERE 1 = 0", &[])
        .await
        .unwrap();
    assert!(rows.rows.is_empty(), "no rows matched");
    assert_eq!(
        rows.columns,
        vec!["n".to_string(), "s".to_string()],
        "schema reported even with zero rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn params_are_bound_not_interpolated() {
    let fx = PgFixture::start();
    let (eng, _writer) = engine(&fx).await;
    let evil = SqlValue::Text("x'; DROP TABLE lake.t; --".into());
    let rows = eng
        .fetch_rows("SELECT ? AS v", std::slice::from_ref(&evil))
        .await
        .unwrap();
    assert_eq!(
        rows.rows[0][0], evil,
        "value round-trips as a literal, not SQL"
    );
}
