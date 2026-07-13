//! `current_snapshots_pair` (road-stream-subscribe-wire, Part A): the atomic
//! dual-snapshot read the changelog feed pins both of its tiers against. ONE SQL
//! statement over `iceberg_mirror.snapshot` => ONE Postgres MVCC snapshot => the
//! returned pair is mutually consistent by construction. (The slice-2b flush appends
//! the changelog files AND end-caps the base's inline rows in a single Postgres
//! transaction, so this read can never observe half of a flush — which two
//! independent `current_snapshot` calls can.)

use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.to_string(),
        name: name.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pair_read_matches_single_reads_and_maps_absence_to_none() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let cat = IcebergCatalog::new(pool.clone());

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer.seed("main", "a", &cols, &[3]).await;
    writer.seed("main", "b", &cols, &[2]).await;

    let a = tref("main", "a");
    let b = tref("main", "b");
    let ghost = tref("main", "nope");

    // Both live: each element agrees with the single-table read and carries the FULL
    // Snapshot (id + time + schema_version), not just the id.
    let (pa, pb) = cat
        .current_snapshots_pair(&a, &b)
        .await
        .expect("pair read must not error");
    let sa = cat.current_snapshot(&a).await.expect("current_snapshot a");
    let sb = cat.current_snapshot(&b).await.expect("current_snapshot b");
    assert_eq!(pa.as_ref().map(|s| s.id), Some(sa.id), "base id");
    assert_eq!(pb.as_ref().map(|s| s.id), Some(sb.id), "clog id");
    assert_eq!(
        pa.as_ref().map(|s| s.schema_version),
        Some(sa.schema_version),
        "the pair carries the full Snapshot"
    );
    assert_eq!(pa.as_ref().map(|s| s.time), Some(sa.time), "snapshot time");

    // An absent table reads as `None`, NOT an error. This is exactly the feed's
    // "nothing has flushed yet, so no changelog table exists" case: the file tier must
    // be absent, not a failure. (A NULL decoded into a non-Option column would panic
    // here with UnexpectedNullError — this asserts the `?` overrides are right.)
    let (pa2, pghost) = cat
        .current_snapshots_pair(&a, &ghost)
        .await
        .expect("an absent table is None, not an error");
    assert_eq!(pa2.map(|s| s.id), Some(sa.id));
    assert!(pghost.is_none(), "absent table must read as None");

    // Both absent is still Ok.
    let (g1, g2) = cat
        .current_snapshots_pair(&ghost, &ghost)
        .await
        .expect("both absent is still Ok");
    assert!(g1.is_none() && g2.is_none());
}
