//! End-to-end tests for the governance, concurrency, time-travel, conformance, and
//! flush-suppression properties of the O(change) inline-shadow copy-on-write mutate
//! path (`run_action` → `run_mutate`). The companion `cow_inline_shadow_e2e.rs` covers
//! the mechanical shadow/tombstone/latest-wins invariants; this file covers the
//! remaining cross-cutting guarantees:
//!
//!   * **time travel** — an inline shadow at a later snapshot does not disturb the
//!     pre-mutation version, which is still readable AS-OF its own snapshot;
//!   * **CAS concurrency** — two updates PATCHing different columns of the same object
//!     from the same base both complete `Ok` and converge (no lost update), while a
//!     concurrent update to a different identity never contends;
//!   * **governance over the merge view** — the fine-grained Write policy still denies
//!     (writing nothing), AND a masked/denied column NEVER leaks through the governed
//!     read that sits OVER the identity-dedup merge-on-read provider;
//!   * **identity-less conformance** — UPDATE/DELETE on a type with no declared
//!     identity is a `Misconfigured` conformance rejection (not a whole-table COW);
//!   * **flush suppression** — once a table carries a shadow, the inline byte-trigger
//!     never enqueues a flush (which would resurrect/duplicate a file row).
//!
//! The write goes over the real wire engine (`spawn_engine_writer`); the read goes
//! through the in-process Iceberg/DataFusion engine's identity-aware merge view — the
//! same split as production.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use arrow_array::Int64Array;
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, Catalog, CompareOp, ControlPlane, ObjectType, Policy,
    PolicyTarget, RowFilter, ScalarValue, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, count_live_inline_rows, data_file_paths, define_widget, grant_writer,
    grant_writer_role, read_widget,
};
use query_api::action::{ActionDeps, ActionError, WriteDenialReason, run_action};
use serde_json::json;

/// The `main.widget` table `define_widget` binds `Widget` to.
fn widget_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "widget".into(),
    }
}

// ---------------------------------------------------------------------------
// Test 1 — time travel: an inline shadow at a later snapshot leaves the
//          pre-mutation version intact AS-OF its own (earlier) snapshot.
//
// The inline-shadow COW NEVER end-caps the prior inline row (unlike the old
// whole-table COW): the pre-mutation version keeps its `begin_snapshot`, so an
// as-of read of the inline tier at that snapshot still returns the old value.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_sees_pre_mutation_value() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Inline tier (large inline_byte_limit): the seed lands as an inline row at s0, so
    // its snapshot is the as-of point the pre-mutation version is read against.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1} → inline row.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (inline tier)");

    // Capture snapshot s0 (the pre-mutation state).
    let ice = IcebergCatalog::new(pool.clone());
    let s0 = ice.current_snapshot(&widget_table()).await.unwrap();

    // updateWidget {id:1, qty:9} → an inline shadow at a LATER snapshot s1.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update runs");

    // Live merged read reflects the shadow: qty == 9.
    let live = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present after update");
    assert_eq!(live["qty"], json!("9"), "live qty updated to 9");

    // The shadow's snapshot is strictly later than s0 — the mutation is invisible at s0.
    let s1 = ice.current_snapshot(&widget_table()).await.unwrap();
    assert!(
        s1.id.0 > s0.id.0,
        "the shadow allocated a later snapshot ({} vs {})",
        s0.id.0,
        s1.id.0
    );

    // Time travel: the inline tier AS-OF s0 still holds exactly the pre-mutation row
    // (qty == 1) — the shadow (begin_snapshot = s1 > s0) is not visible there.
    let inline_at_s0 = ice.inline_live_batch(&widget_table(), s0.id).await.unwrap();
    let (_tid, row_ids, batch) =
        inline_at_s0.expect("the pre-mutation inline row must still exist at s0");
    assert_eq!(row_ids.len(), 1, "exactly one inline row visible at s0");
    assert_eq!(batch.num_rows(), 1, "exactly one inline row visible at s0");
    let qty_col = batch
        .column_by_name("qty")
        .expect("qty column present in the s0 inline batch");
    let qty_arr = qty_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("qty is Int64");
    assert_eq!(
        qty_arr.value(0),
        1_i64,
        "time-travel qty at s0 is the pre-mutation 1, not the shadow's 9"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 2 — CAS concurrency: two updates PATCHing DIFFERENT columns of the same
//          object from the same base both complete Ok and converge; a concurrent
//          update to a DIFFERENT identity never contends.
//
// The authoritative "one aborts with Conflict + retries" proof is the postgres-level
// `delta_write_and_cas_conflict` (Task 4) with an explicit stale expected_version.
// This e2e asserts the observable end-to-end invariant: convergent, no lost update.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_cas_no_lost_update() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Widget(id Long identity, name String, qty Long) with an updateWidget whose name
    // AND qty params are OPTIONAL — so the two writers can PATCH different columns.
    // (`define_widget`'s updateWidget requires qty; here we need a name-only PATCH too.)
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("Widget", ("main", "widget"))
                .prop_req("id", "Long")
                .prop("name", "String")
                .prop("qty", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("createWidget", "Widget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("name", "String")
                .param("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("updateWidget", "Widget", ActionKind::Update)
                .param_req("id", "Long")
                .param("name", "String")
                .param("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    let subj = grant_writer(&cp, &widget).await;

    // File tier: the seed rows land as Parquet, so each update is the first inline
    // shadow off a shared version-0 base — the setup that makes the CAS race real.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, name:"x", qty:1} and {id:2, name:"init", qty:2}.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "x", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=1");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "init", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=2");

    // A + B PATCH DIFFERENT columns of id=1 from the same base; C patches id=2.
    // Run all three CONCURRENTLY (sequential would never exercise the CAS retry).
    let a = json!({ "id": "1", "name": "y" });
    let b = json!({ "id": "1", "qty": "9" });
    let c = json!({ "id": "2", "name": "z" });
    let (ra, rb, rc) = tokio::join!(
        run_action("updateWidget", a.as_object().unwrap(), &subj, &deps),
        run_action("updateWidget", b.as_object().unwrap(), &subj, &deps),
        run_action("updateWidget", c.as_object().unwrap(), &subj, &deps),
    );
    // BOTH contending writers complete Ok — the loser re-read the winner and re-applied.
    ra.expect("A (name:=y) completes Ok");
    rb.expect("B (qty:=9) completes Ok");
    rc.expect("C (id=2 name:=z) completes Ok alongside — no cross-identity contention");

    // No lost update: id=1 converged to BOTH patches — {name:"y", qty:9}.
    let r1 = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row 1 present");
    assert_eq!(r1["name"], json!("y"), "A's name patch survived");
    assert_eq!(
        r1["qty"],
        json!("9"),
        "B's qty patch survived (no lost update)"
    );

    // The different identity got its independent patch, qty untouched.
    let r2 = read_widget(&cp, &pool, &subj, 2)
        .await
        .expect("row 2 present");
    assert_eq!(r2["name"], json!("z"), "C's name patch applied to id=2");
    assert_eq!(r2["qty"], json!("2"), "id=2 qty untouched by C");

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 3a — governance: a deny-column UPDATE is denied and writes NOTHING (the
//           merged read is unchanged). File tier (base row is Parquet).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governance_update_column_denied_writes_nothing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1} (file tier) — no policy yet.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert");

    // Deny writes to `qty`.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: None,
            deny_columns: vec!["qty".to_string()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // updateWidget {id:1, qty:9} → WriteDenied(Column("qty")).
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "qty"),
        "expected WriteDenied(Column(\"qty\")), got: {err:?}"
    );

    // Nothing was written: the merged read still shows qty=1, and no inline shadow row
    // was created (a denied write must not touch the inline tier).
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row still present");
    assert_eq!(row["qty"], json!("1"), "denied update wrote nothing");
    assert_eq!(
        count_live_inline_rows(&pool, "main", "widget").await,
        0,
        "a denied update added no inline shadow row"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 3b — governance: a row-filter denies an UPDATE whose EXISTING row is
//           outside the writable region, and the new-row leg denies a PATCH that
//           would move the row out. Both write nothing. File tier.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governance_row_filter_denies_update() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:9} (outside qty<5) and {id:2, qty:1} (inside) — no policy yet.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=1 (qty=9)");
    run_action(
        "createWidget",
        json!({ "id": "2", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=2 (qty=1)");

    // row_filter `qty < 5`.
    let filter = RowFilter::Compare {
        property: "qty".into(),
        op: CompareOp::Lt,
        value: ScalarValue::Int(5),
    };
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(filter),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // EXISTING-row leg: UPDATE id=1 (existing qty=9 fails qty<5) → WriteDenied(RowFilter).
    let err_existing = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            err_existing,
            ActionError::WriteDenied(WriteDenialReason::RowFilter)
        ),
        "existing row outside the filter must deny, got: {err_existing:?}"
    );

    // RESULTING-row leg: UPDATE id=2 (existing qty=1 passes, new qty=9 fails) → denied.
    let err_new = run_action(
        "updateWidget",
        json!({ "id": "2", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            err_new,
            ActionError::WriteDenied(WriteDenialReason::RowFilter)
        ),
        "resulting row outside the filter must deny, got: {err_new:?}"
    );

    // Both writes were suppressed: no inline shadow rows, values unchanged.
    assert_eq!(
        count_live_inline_rows(&pool, "main", "widget").await,
        0,
        "denied updates added no inline shadow rows"
    );
    assert_eq!(
        read_widget(&cp, &pool, &subj, 1).await.expect("id=1")["qty"],
        json!("9"),
        "id=1 unchanged"
    );
    assert_eq!(
        read_widget(&cp, &pool, &subj, 2).await.expect("id=2")["qty"],
        json!("1"),
        "id=2 unchanged"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 3c — governance: a row-filter denies a DELETE whose target row is outside
//           the writable region (no tombstone written). File tier.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governance_row_filter_denies_delete() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:9} (outside qty<5) — no policy yet.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed id=1 (qty=9)");

    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "qty".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(5),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // DELETE id=1 (existing qty=9 fails qty<5) → WriteDenied(RowFilter).
    let err = run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "delete on a row outside the filter must deny, got: {err:?}"
    );

    // No tombstone was written: the row still reads, and the inline tier is empty.
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_some(),
        "denied delete left the row in place"
    );
    assert_eq!(
        count_live_inline_rows(&pool, "main", "widget").await,
        0,
        "a denied delete wrote no tombstone"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 3d — governance READ over the merge-on-read provider: a masked column is
//           redacted and a denied column is dropped from the merged read output
//           (which reads the inline shadow OVER the file tier). The real value of
//           the governed column NEVER leaks.
//
// This is the leg that exercises the GOVERNED read path SITTING OVER the
// identity-dedup merge view — the mutation produces the shadow, and governance
// must still shape the merged result.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governance_masked_and_denied_columns_never_leak_over_merge() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    // File tier: the seed is a Parquet row, so the later shadow forces the identity
    // merge view (the read the governed layer must sit over).
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, name:"topsecret", qty:1} (file), then updateWidget qty:9 so the
    // merged row is served by the identity-dedup view (shadow over the file row).
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "topsecret", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (file tier)");
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update runs (creates the inline shadow)");

    // Sanity (no column policy yet): the merged read carries the shadow's qty AND the
    // real name — confirming both tiers merge before governance shapes them.
    let full = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("merged row present");
    assert_eq!(
        full["qty"],
        json!("9"),
        "shadow qty served by the merge view"
    );
    assert_eq!(full["name"], json!("topsecret"), "real name pre-governance");

    // MASK `name`: the merged read redacts it — the real value never leaks — while qty
    // stays visible.
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["name".to_string()],
        },
    )
    .await
    .unwrap();
    let masked = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("masked merged row present");
    assert_eq!(
        masked["name"],
        json!("***"),
        "masked name redacted over the merge view"
    );
    assert_ne!(
        masked["name"],
        json!("topsecret"),
        "the real name never leaks through the masked merged read"
    );
    assert_eq!(masked["qty"], json!("9"), "qty still visible under masking");

    // DENY `name`: the merged read drops the column entirely.
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: None,
            deny_columns: vec!["name".to_string()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let denied = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("denied merged row present");
    assert!(
        denied.get("name").is_none(),
        "denied name is absent from the merged output, got: {denied:?}"
    );
    assert_eq!(
        denied["qty"],
        json!("9"),
        "qty still served under column denial"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 4 — identity-less type: UPDATE/DELETE on a type with NO declared identity
//          is a conformance rejection (Misconfigured → 500), NOT a whole-table COW.
//          Reconciles design spec test 7 to the actual behavior.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_less_type_mutation_is_misconfigured() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Gadget(id Long, qty Long) — deliberately NO identity declared.
    let gadget = TypeName("Gadget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("Gadget", ("main", "gadget"))
                .prop_req("id", "Long")
                .prop("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    // An Update action CAN be DEFINED on it (define_action does not enforce conformance;
    // it only checks the target type exists). The rejection is deferred to run time.
    cp.ontology()
        .define_action(
            ActionDef::build("createGadget", "Gadget", ActionKind::Insert)
                .param_req("id", "Long")
                .param("qty", "Long")
                .done(),
        )
        .await
        .expect("insert action defines fine");
    cp.ontology()
        .define_action(
            ActionDef::build("updateGadget", "Gadget", ActionKind::Update)
                .param_req("id", "Long")
                .param_req("qty", "Long")
                .done(),
        )
        .await
        .expect("update action defines even on an identity-less type");
    cp.ontology()
        .define_action(
            ActionDef::build("deleteGadget", "Gadget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .expect("delete action defines even on an identity-less type");

    // Grant Write + Read so we clear the coarse Write gate and reach conformance.
    let subj = grant_writer(&cp, &gadget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(fx.pool_for(&db).await));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // UPDATE on an identity-less type: conformance rejects it (no row lookup, no COW).
    let upd = run_action(
        "updateGadget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(upd, ActionError::Misconfigured(_)),
        "UPDATE on an identity-less type must be Misconfigured (500), got: {upd:?}"
    );

    // DELETE likewise.
    let del = run_action(
        "deleteGadget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(del, ActionError::Misconfigured(_)),
        "DELETE on an identity-less type must be Misconfigured (500), got: {del:?}"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 5 — flush suppression end-to-end: once a table carries a shadow, further
//          inline appends that cross the byte-trigger must NOT enqueue a flush
//          (a flush would resurrect/duplicate the file row). The mutated value is
//          served unchanged.
//
// Two engine writers over the SAME db + warehouse: a file-tier writer seeds the
// file-only object (no inline trigger), then an inline-tier writer with a LOW flush
// threshold mutates it (setting the shadow flag) and appends more inline rows that
// cross the trigger — which the has_shadow guard must suppress.
// ---------------------------------------------------------------------------

/// Runtime sqlx: count queued `flush_table` jobs. Mirrors `inline_flush_trigger.rs`'s
/// `job_count`. A suppressed byte-trigger enqueues zero; an un-guarded one would
/// enqueue at least one — the load-bearing signal for the suppression.
async fn flush_job_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*) from queue.jobs where kind = $1")
        .bind(control_plane_core::FLUSH_JOB_KIND)
        .fetch_one(pool)
        .await
        .expect("count flush jobs")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_suppressed_after_mutation_no_corruption() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));

    // Writer A — FILE tier (inline_byte_limit=0), no trigger (flush_threshold=MAX).
    // Seeds the file-only object {id:1, qty:1} — the primary copy-on-write target.
    let (engine_file, _eg_file) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 0, i64::MAX).await;
    let deps_file = ActionDeps {
        cp: &cp,
        action_engine: &engine_file,
        serving: &serving,
    };
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps_file,
    )
    .await
    .expect("seed file row id=1");

    let files_before = data_file_paths(&pool, "main", "widget").await;
    assert!(
        !files_before.is_empty(),
        "the seed produced a Parquet file (file tier)"
    );
    assert_eq!(
        flush_job_count(&pool).await,
        0,
        "the file-tier seed enqueues no flush"
    );

    // Writer B — INLINE tier (large inline_byte_limit) with a LOW flush threshold, so
    // any inline append trips the byte trigger.
    let (engine_inline, _eg_inline) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, 1).await;
    let deps_inline = ActionDeps {
        cp: &cp,
        action_engine: &engine_inline,
        serving: &serving,
    };

    // Mutate {id:1, qty:9} → an inline shadow, flagging the table as shadowed.
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps_inline,
    )
    .await
    .expect("update runs (sets has_shadow)");

    // Append MORE inline rows that DO cross the LOW byte trigger. Because the table is
    // now shadowed, the trigger must be SUPPRESSED — no flush job is enqueued.
    for id in ["2", "3", "4"] {
        run_action(
            "createWidget",
            json!({ "id": id, "qty": "5" }).as_object().unwrap(),
            &subj,
            &deps_inline,
        )
        .await
        .unwrap_or_else(|e| panic!("inline append id={id} runs: {e:?}"));
    }

    // The load-bearing suppression signal: the byte trigger was reached (LOW threshold,
    // real inline appends) but the has_shadow guard enqueued ZERO flush jobs. Without
    // the guard, the first crossing would have enqueued one.
    assert_eq!(
        flush_job_count(&pool).await,
        0,
        "the has_shadow guard suppressed the byte-trigger flush enqueue"
    );

    // No flush ran (none was even enqueued): the file row was never rewritten — the live
    // data_file set is exactly the seed's, so no duplicate/resurrected file row exists.
    let files_after = data_file_paths(&pool, "main", "widget").await;
    assert_eq!(
        files_before, files_after,
        "no flush rewrote Parquet — the file set is unchanged"
    );

    // The mutated value is served correctly (the shadow wins; not resurrected to 1),
    // and the shadow row is still live (nothing end-capped by a flush).
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row 1 present");
    assert_eq!(
        row["qty"],
        json!("9"),
        "the mutated value is served, not resurrected to the pre-mutation 1"
    );
    assert!(
        count_live_inline_rows(&pool, "main", "widget").await >= 1,
        "the inline shadow row is still live (no flush drained it)"
    );
    // The sibling inline appends are visible too (the merge serves both tiers).
    for id in [2, 3, 4] {
        assert!(
            read_widget(&cp, &pool, &subj, id).await.is_some(),
            "sibling inline row id={id} is served"
        );
    }

    drop(warehouse);
}
