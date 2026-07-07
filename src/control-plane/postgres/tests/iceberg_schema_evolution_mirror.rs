//! Fixture test for the mirror reconciliation helpers, driven directly against a
//! seeded mirror (no Parquet) so each policy branch is exercised in isolation.
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{
    ProjectedColumn, ensure_table, live_columns, next_snapshot, reconcile_and_project,
    stamp_schema_version,
};

fn col(order: i64, name: &str, ty: &str, nullable: bool) -> ProjectedColumn {
    ProjectedColumn {
        order,
        name: name.into(),
        iceberg_type: ty.into(),
        nullable,
    }
}

#[tokio::test]
async fn reconcile_creates_then_appends_then_stamps() {
    let fixture = PgFixture::shared();
    let (cp, _db) = fixture.fresh_db().await;
    let pool = cp.pool().clone();
    // `ensure_table`'s savepoint retry requires an explicit transaction (SAVEPOINT
    // is illegal outside one), so use `pool.begin()` rather than a bare
    // `pool.acquire()` connection.
    let mut conn = pool.begin().await.unwrap();

    // Creation: empty live → project all (incl. a required column).
    let s1 = next_snapshot(&mut conn, None).await.unwrap();
    let tid = ensure_table(&mut conn, "s", "t", s1).await.unwrap();
    let base = vec![col(1, "a", "long", false), col(2, "b", "string", true)];
    reconcile_and_project(&mut conn, tid, s1, &base)
        .await
        .unwrap();
    stamp_schema_version(&mut conn, tid, s1).await.unwrap();
    assert_eq!(live_columns(&mut conn, tid, s1).await.unwrap().len(), 2);

    // Additive: append nullable c at a new snapshot.
    let s2 = next_snapshot(&mut conn, None).await.unwrap();
    let mut wider = base.clone();
    wider.push(col(3, "c", "long", true));
    reconcile_and_project(&mut conn, tid, s2, &wider)
        .await
        .unwrap();
    stamp_schema_version(&mut conn, tid, s2).await.unwrap();

    // Three live columns at s2; c.begin_snapshot == s2.
    let live = live_columns(&mut conn, tid, s2).await.unwrap();
    assert_eq!(
        live.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    let c_begin: i64 = sqlx::query_scalar(
        "select begin_snapshot from iceberg_mirror.column where table_id = $1 and column_name = 'c'",
    )
    .bind(tid)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!(c_begin, s2.0);

    // schema_version bumped: s1 → 1, s2 → 2.
    let v1: i64 = sqlx::query_scalar(
        "select schema_version from iceberg_mirror.snapshot where snapshot_id = $1",
    )
    .bind(s1.0)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let v2: i64 = sqlx::query_scalar(
        "select schema_version from iceberg_mirror.snapshot where snapshot_id = $1",
    )
    .bind(s2.0)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!((v1, v2), (1, 2));
}

#[tokio::test]
async fn reconcile_rejects_drop() {
    let fixture = PgFixture::shared();
    let (cp, _db) = fixture.fresh_db().await;
    let pool = cp.pool().clone();
    // See the transaction note in `reconcile_creates_then_appends_then_stamps`.
    let mut conn = pool.begin().await.unwrap();
    let s1 = next_snapshot(&mut conn, None).await.unwrap();
    let tid = ensure_table(&mut conn, "s", "t", s1).await.unwrap();
    reconcile_and_project(
        &mut conn,
        tid,
        s1,
        &[col(1, "a", "long", false), col(2, "b", "string", true)],
    )
    .await
    .unwrap();
    let s2 = next_snapshot(&mut conn, None).await.unwrap();
    let err = reconcile_and_project(&mut conn, tid, s2, &[col(1, "a", "long", false)])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("schema evolution unsupported"),
        "got: {err}"
    );
}
