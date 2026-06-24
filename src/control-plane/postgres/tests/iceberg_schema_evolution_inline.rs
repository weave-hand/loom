//! Inline-path parity: a divergent inline write is rejected (detect+reject), an
//! identical re-append is a no-op.
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};

#[tokio::test]
async fn inline_identical_reappend_ok_divergent_rejected() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db));
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let run = uuid::Uuid::from_u128(1);

    // First inline write creates + projects.
    writer.inline("s", "t", &cols, &[(1, "a")], run).await;
    // Identical re-append: no-op, succeeds.
    writer
        .inline("s", "t", &cols, &[(2, "b")], uuid::Uuid::from_u128(2))
        .await;

    // Divergent inline write (extra nullable column) must be rejected. `inline` writes a
    // fixed (id,name) batch, so drive divergence through inline_append directly with a
    // wider ColumnSpec list — see helper below.
    let wider = {
        let mut c = cols.clone();
        c.push(("extra".to_string(), "long".to_string(), true));
        c
    };
    let err = writer.inline_expect_err("s", "t", &wider).await;
    assert!(err.contains("schema evolution unsupported"), "got: {err}");
}
