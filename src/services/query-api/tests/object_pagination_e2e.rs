//! Cursor pagination (`?limit=`/`?cursor=`) on `GET /objects/{type}` e2e over the real HTTP
//! router backed by an Iceberg/DataFusion serving engine. Proves the keyset walk covers every
//! row exactly once across pages (disjoint + contiguous, final page `next == null`), and the
//! five fail-closed 400s: no declared identity, a masked/denied identity column, a
//! non-round-trippable identity logical type, `_ids` + pagination together, and a malformed
//! cursor.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{ObjectType, Ontology, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, get, grant_read, grant_read_columns, ids_i64, prop, subject_with_role,
    tref,
};

/// Seed orders(id, amount) with ids 1..=5. Define `Order` (identity `id`) and `Plain`, a
/// type backed by the same table but with NO declared identity (the no-identity 400 path).
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    let ord = tref("main", "orders");
    let ord_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("amount".to_string(), "long".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &ord_cols,
            &[
                SeedCol::Long(vec![1, 2, 3, 4, 5]),
                SeedCol::Long(vec![10, 20, 30, 40, 50]),
            ],
        )
        .await;

    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("amount", "Long", false)],
        derived: vec![],
        table: ord.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    // Plain: no declared identity, backed by the same table.
    cp.define_type(ObjectType {
        name: TypeName("Plain".into()),
        properties: vec![prop("id", "Long", true), prop("amount", "Long", false)],
        derived: vec![],
        table: ord.clone(),
        identity: None,
    })
    .await
    .unwrap();
    // DoubleId: identity declared as a `Double` logical type — not one the cursor
    // round-trips (`sqlvalue_to_id_string` only handles Int/Text losslessly). The guard
    // in `read_object_page` must reject this before compiling/executing, so the
    // underlying `amount` column's physical type (long) never matters for this fixture.
    cp.define_type(ObjectType {
        name: TypeName("DoubleId".into()),
        properties: vec![prop("id", "Long", true), prop("amount", "Double", false)],
        derived: vec![],
        table: ord.clone(),
        identity: Some("amount".into()),
    })
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

#[tokio::test(flavor = "multi_thread")]
async fn paginates_with_cursor_covering_all_rows() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Order").await;

    let mut seen: Vec<i64> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let uri = match &cursor {
            Some(c) => format!("/objects/Order?limit=2&cursor={c}"),
            None => "/objects/Order?limit=2".to_string(),
        };
        let (status, body) = get(cp.clone(), eng.clone(), &uri, "alice").await;
        assert_eq!(status, StatusCode::OK);
        let page_ids = ids_i64(&body);
        pages += 1;
        assert!(pages <= 10, "runaway pagination loop");

        if pages < 3 {
            assert_eq!(page_ids.len(), 2, "full page {pages}");
        }
        // Disjoint from everything seen so far.
        for id in &page_ids {
            assert!(!seen.contains(id), "id {id} appeared in more than one page");
        }
        seen.extend(page_ids);

        let next = body["next"].as_str().map(str::to_string);
        match next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 2, 3, 4, 5], "every row covered exactly once");
    assert_eq!(pages, 3, "5 rows at limit=2 -> 3 pages (2,2,1)");
}

#[tokio::test(flavor = "multi_thread")]
async fn pagination_on_no_identity_type_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Plain").await;

    let (status, _body) = get(cp.clone(), eng.clone(), "/objects/Plain?limit=2", "alice").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "pagination on a type with no declared identity -> 400"
    );
}

/// A masked identity column must fail closed: `read_object_page`'s `identity_is_governed`
/// guard treats denied and masked identically (never emit a cursor over an
/// ungoverned-visibility identity), so masking `id` on `Order` -> 400, same as denying it.
#[tokio::test(flavor = "multi_thread")]
async fn masked_identity_pagination_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read_columns(&cp, &role, "Order", vec![], vec!["id".into()]).await;

    let (status, _body) = get(cp.clone(), eng.clone(), "/objects/Order?limit=2", "alice").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "masked identity column -> pagination fails closed with 400"
    );
}

/// A `Double` identity is not one the cursor round-trips losslessly (only Integer/Long/
/// String are — `sqlvalue_to_id_string` falls back to lossy `{:?}` Debug formatting for
/// everything else). The guard must reject it up front rather than emit a cursor that
/// page 2 can't decode.
#[tokio::test(flavor = "multi_thread")]
async fn non_round_trippable_identity_type_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "DoubleId").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/DoubleId?limit=2",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "non-round-trippable (Double) identity type -> pagination fails closed with 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_and_pagination_are_mutually_exclusive_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Order").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Order?_ids=1&limit=2",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "_ids and pagination together -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_cursor_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Order").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Order?cursor=notanumber",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "malformed cursor -> 400");
}
