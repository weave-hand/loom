//! Serving-layer type fidelity: real DuckDB DATE/TIMESTAMP/DOUBLE values come back
//! as faithful typed SqlValues, not Text(debug). The JSON rendering of these values
//! is covered by the pure render-matrix unit test; this proves the DuckDB -> SqlValue
//! decode against the real engine.

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{EmbeddedDuckDb, ServingEngine, SqlValue};

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_temporal_and_float_decode_faithfully() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    let eng = EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path())
        .await
        .unwrap();
    let rows = eng
        .fetch_rows(
            "SELECT DATE '2026-06-12' AS d, \
             TIMESTAMP '2026-06-12 14:09:42' AS ts, \
             CAST(3.5 AS DOUBLE) AS amt",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(rows.columns, vec!["d", "ts", "amt"]);
    let r = &rows.rows[0];
    assert_eq!(
        r[0],
        SqlValue::Date(time::macros::date!(2026 - 06 - 12)),
        "DATE decodes to SqlValue::Date"
    );
    assert_eq!(
        r[1],
        SqlValue::Timestamp(time::macros::datetime!(2026 - 06 - 12 14:09:42)),
        "TIMESTAMP decodes to SqlValue::Timestamp"
    );
    assert_eq!(
        r[2],
        SqlValue::Double(3.5),
        "DOUBLE decodes to SqlValue::Double"
    );
}
