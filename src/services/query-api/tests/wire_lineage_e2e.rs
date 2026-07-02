//! Proves `WireControlPlane::lineage()` delegates to its direct Postgres plane on
//! the real production type (the e2e HTTP tests use a `PgControlPlane` as `cp`, so
//! they never exercise the wire plane — this guards the production wiring).

use std::sync::Arc;

// `ControlPlane` in scope for `cp.lineage()`/`wire.lineage()`; `Lineage` is NOT
// imported (its methods run on the returned `&dyn Lineage`, needing no trait in
// scope — an unused import would fail clippy's `unused_imports` on this test target).
use control_plane_core::{ControlPlane, DatasetRef, EventType, LineageEvent, PageReq, RunId};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{connect_gov_client, spawn_engine};
use query_api::wire_control_plane::WireControlPlane;

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.to_string(),
        name: name.to_string(),
    }
}

fn edge(inp: DatasetRef, out: DatasetRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_lineage_reads_delegate_to_direct() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let cp = Arc::new(cp);
    let warehouse = tempfile::tempdir().expect("warehouse");

    // A spawned engine gives us a real gov client for WireControlPlane::new; the
    // warehouse/engine are unused by lineage reads (they go through `direct`).
    let (sock, _guard) = spawn_engine(&fx, &db, warehouse.path(), 0, i64::MAX).await;
    let client = connect_gov_client(&sock).await;
    let wire = WireControlPlane::new(client, cp.clone() as Arc<dyn ControlPlane>);

    // Seed A -> B on the DIRECT plane's lineage.
    let (a, b) = (ds("w", "wire.a"), ds("w", "wire.b"));
    cp.lineage()
        .emit(edge(a.clone(), b.clone()))
        .await
        .expect("emit");

    // Read it back THROUGH the wire plane — must not panic, must return the edge.
    let events = wire
        .lineage()
        .events_for(&RunId(uuid::Uuid::nil()), PageReq::unbounded())
        .await;
    assert!(events.is_ok(), "events_for delegates without panicking");

    let up = wire
        .lineage()
        .upstream(&b, 1, PageReq::unbounded())
        .await
        .expect("upstream via wire plane");
    let names: Vec<String> = up.items.iter().map(|d| d.name.clone()).collect();
    assert_eq!(
        names,
        vec!["wire.a".to_string()],
        "upstream(B) = {{A}} via delegation"
    );
}
