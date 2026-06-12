//! Backend-agnostic contract tests for the control-plane traits.
//! Each adapter runs the same suite against its own implementation.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Catalog, CompareOp, ControlPlane, ControlPlaneError, DatasetRef,
    Decision, Effect, EventType, Lineage, LineageEvent, LinkDef, NewJob, ObjectType, Ontology,
    Page, PageReq, Policy, PolicyTarget, PropertyDef, Queue, RetryPolicy, RoleId, RowFilter, RunId,
    ScalarValue, SnapshotId, SubjectId, TableRef, TypeName,
};
use time::OffsetDateTime;

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

/// Contract for the `Queue` ops including transactional `enqueue`.
/// `cp` must be freshly empty; `lock_timeout` must match the adapter's configured
/// value so the reclaim assertion is timed correctly.
pub async fn queue_contract<CP: ControlPlane + Queue>(cp: &CP, lock_timeout: Duration) {
    let k = vec!["t".to_string()];
    let w = "worker-1";

    // enqueue -> dequeue (attempts=1) -> complete deletes
    let id = cp.enqueue(job("t")).await.expect("enqueue");
    let j = cp.dequeue(&k, w).await.expect("dequeue").expect("a job");
    assert_eq!(j.id, id);
    assert_eq!(j.attempts, 1);
    cp.complete(id).await.expect("complete");
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "completed job is gone"
    );

    // priority: higher first
    cp.enqueue(NewJob {
        priority: 1,
        ..job("t")
    })
    .await
    .unwrap();
    let hi = cp
        .enqueue(NewJob {
            priority: 5,
            ..job("t")
        })
        .await
        .unwrap();
    assert_eq!(
        cp.dequeue(&k, w).await.unwrap().unwrap().id,
        hi,
        "higher priority first"
    );
    cp.complete(hi).await.unwrap();
    cp.complete(cp.dequeue(&k, w).await.unwrap().unwrap().id)
        .await
        .unwrap();

    // future run_at is not eligible
    let future = OffsetDateTime::now_utc() + Duration::from_secs(3600);
    cp.enqueue(NewJob {
        run_at: Some(future),
        ..job("t")
    })
    .await
    .unwrap();
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "future job not eligible"
    );

    // fail + Retry reschedules; attempts increments
    let r = cp.enqueue(job("t")).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().attempts, 1);
    cp.fail(
        r,
        "boom",
        RetryPolicy::Retry {
            delay: Duration::from_millis(150),
        },
    )
    .await
    .unwrap();
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "retry is delayed"
    );
    tokio::time::sleep(Duration::from_millis(220)).await;
    let again = cp
        .dequeue(&k, w)
        .await
        .unwrap()
        .expect("retry eligible after delay");
    assert_eq!(again.attempts, 2);
    cp.complete(again.id).await.unwrap();

    // fail + Abandon -> terminal
    let a = cp.enqueue(job("t")).await.unwrap();
    cp.dequeue(&k, w).await.unwrap().unwrap();
    cp.fail(a, "dead", RetryPolicy::Abandon).await.unwrap();
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "abandoned never returns"
    );

    // expired-lock reclaim
    let e = cp.enqueue(job("t")).await.unwrap();
    assert_eq!(cp.dequeue(&k, w).await.unwrap().unwrap().id, e);
    assert!(
        cp.dequeue(&k, w).await.unwrap().is_none(),
        "locked, not yet reclaimed"
    );
    tokio::time::sleep(lock_timeout + Duration::from_millis(50)).await;
    let reclaimed = cp
        .dequeue(&k, "worker-2")
        .await
        .unwrap()
        .expect("expired lock reclaimed");
    assert_eq!(reclaimed.id, e);
    assert_eq!(reclaimed.attempts, 2);

    // heartbeat keeps the lock fresh, then complete
    cp.heartbeat(e).await.unwrap();
    cp.complete(e).await.unwrap();

    // transactional enqueue: rolled back -> never dequeued; committed -> dequeued
    let mut tx = cp.begin().await.unwrap();
    tx.enqueue(job("tx")).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(
        cp.dequeue(&["tx".into()], w).await.unwrap().is_none(),
        "rolled-back enqueue is invisible"
    );

    let mut tx = cp.begin().await.unwrap();
    let enqueued = tx.enqueue(job("tx")).await.unwrap();
    let _ = tx.commit().await.unwrap();
    let committed = cp
        .dequeue(&["tx".into()], w)
        .await
        .unwrap()
        .expect("committed enqueue is visible");
    assert_eq!(
        committed.id, enqueued,
        "dequeued job has the id returned by Tx::enqueue"
    );
    cp.complete(committed.id).await.unwrap();
}

/// Contract for `Queue::await_jobs`. Takes `cp` by value (must be `Clone + Send +
/// Sync + 'static`) so the test can hold one handle in a spawned waiter and use
/// another to enqueue. Both adapters satisfy these bounds.
pub async fn await_jobs_contract<CP>(cp: CP)
where
    CP: ControlPlane + Queue + Clone + Send + Sync + 'static,
{
    let k = vec!["w".to_string()];

    // (a) idle: returns cleanly at/after the timeout (no job ever arrives).
    let t0 = std::time::Instant::now();
    cp.await_jobs(&k, Duration::from_millis(150))
        .await
        .expect("await_jobs returns Ok on timeout");
    let idle = t0.elapsed();
    assert!(
        idle >= Duration::from_millis(120) && idle < Duration::from_secs(2),
        "idle await_jobs should block ~the timeout, blocked {idle:?}"
    );

    // (b) wakeup: a concurrent enqueue releases a waiter well before its long timeout.
    let cp2 = cp.clone();
    let kk = k.clone();
    let waiter = tokio::spawn(async move { cp2.await_jobs(&kk, Duration::from_secs(30)).await });
    tokio::time::sleep(Duration::from_millis(100)).await; // let the waiter register / LISTEN
    let t1 = std::time::Instant::now();
    cp.enqueue(NewJob {
        kind: "w".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("enqueue");
    waiter.await.expect("waiter task").expect("await_jobs ok");
    assert!(
        t1.elapsed() < Duration::from_secs(5),
        "enqueue wakeup should beat the 30s timeout, took {:?}",
        t1.elapsed()
    );
}

/// A column to create in a seeded table.
pub struct SeedColumn {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// A request to arrange catalog state: create `table` with `columns`, then apply
/// each entry of `row_batches` as its own snapshot adding one data file of that
/// many rows.
pub struct SeedSpec {
    pub table: TableRef,
    pub columns: Vec<SeedColumn>,
    pub row_batches: Vec<usize>,
}

/// A snapshot produced by seeding one batch.
#[derive(Clone, Copy, Debug)]
pub struct SeededSnapshot {
    pub snapshot: SnapshotId,
    pub files_added: usize,
}

/// Test-only seam for arranging catalog state. Each backend implements it
/// differently (the fake builds its state directly; the pg adapter drives real
/// DuckLake). Never referenced by production code.
#[async_trait]
pub trait CatalogSeed {
    /// Create the table if absent and apply each row-batch as its own snapshot.
    /// Returns the per-batch snapshots, in order.
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot>;

    /// Drop `table`. Returns the snapshot `D` at which it was dropped: the table and
    /// its files/columns gain `D` as their `end_snapshot`, so the table is not live at
    /// `D` or later, but remains live (time-travellable) at any snapshot `< D`.
    async fn drop_table(&self, table: &TableRef) -> SnapshotId;
}

/// Contract for the `Catalog` read surface. `catalog` and `seeder` may be the
/// same backend behind two handles. Assertions key off the snapshot ids the
/// seeder reports, so this suite is backend-agnostic and fidelity-safe.
pub async fn catalog_contract<C, S>(catalog: &C, seeder: &S)
where
    C: Catalog,
    S: CatalogSeed,
{
    let t = TableRef {
        schema: "main".into(),
        name: "events".into(),
    };
    let seeded = seeder
        .seed(SeedSpec {
            table: t.clone(),
            columns: vec![
                SeedColumn {
                    name: "id".into(),
                    ty: "BIGINT".into(),
                    nullable: false,
                },
                SeedColumn {
                    name: "name".into(),
                    ty: "VARCHAR".into(),
                    nullable: true,
                },
            ],
            row_batches: vec![10, 20],
        })
        .await;
    assert_eq!(seeded.len(), 2, "two batches => two snapshots");

    // current_snapshot is the last batch's snapshot.
    let cur = catalog
        .current_snapshot(&t)
        .await
        .expect("current_snapshot");
    assert_eq!(
        cur.id, seeded[1].snapshot,
        "current is the latest seeded snapshot"
    );

    // files: one live at the first batch, two by the second (begin_snapshot range).
    assert_eq!(
        catalog
            .files(&t, seeded[0].snapshot, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        1,
        "one file live at the first batch"
    );
    assert_eq!(
        catalog
            .files(&t, seeded[1].snapshot, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        2,
        "two files live by the second batch"
    );

    // snapshots: ascending history, includes both batch snapshots, ends at current.
    let hist = catalog
        .snapshots(&t, PageReq::unbounded())
        .await
        .unwrap()
        .items;
    assert!(
        hist.windows(2).all(|w| w[0].id < w[1].id),
        "snapshots are ascending"
    );
    assert!(
        hist.iter().any(|s| s.id == seeded[0].snapshot),
        "history includes the first batch snapshot"
    );
    assert_eq!(
        hist.last().unwrap().id,
        cur.id,
        "history ends at the current snapshot"
    );

    // schema at current: the two columns, in order, with non-empty (opaque) types
    // and correct nullability. `ty` is backend-spelled (DuckLake: `varchar`/`int64`;
    // the fake: whatever was seeded) so it is treated as opaque, never compared to a literal.
    let sch = catalog.schema(&t, cur.id).await.unwrap();
    assert_eq!(
        sch.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"],
        "columns in order"
    );
    assert!(
        sch.columns.iter().all(|c| !c.ty.is_empty()),
        "every column has a (backend-spelled) type"
    );
    assert!(
        sch.columns[1].nullable && !sch.columns[0].nullable,
        "nullability preserved"
    );

    // a table that never existed -> NotFound (variant, not message).
    let missing = TableRef {
        schema: "main".into(),
        name: "nope".into(),
    };
    assert!(
        matches!(
            catalog.current_snapshot(&missing).await,
            Err(control_plane_core::ControlPlaneError::NotFound(_))
        ),
        "missing table current_snapshot is NotFound"
    );
    assert!(
        matches!(
            catalog.snapshots(&missing, PageReq::unbounded()).await,
            Err(control_plane_core::ControlPlaneError::NotFound(_))
        ),
        "missing table snapshots is NotFound"
    );
}

/// Contract for the MVCC `end`-bound and before-existence branches of the catalog
/// read surface — the half the append-only `catalog_contract` never reaches. A table
/// dropped at snapshot `D` is not live at `D` or later (`end > s` false) but remains
/// time-travellable at any snapshot `< D` (`end > s` true with a non-null `end`); and a
/// table is not live before its `begin` (`begin <= s` false). `current_snapshot`/
/// `snapshots` stay drop-aware (latest *live* snapshot), distinct from a never-existed
/// table's `NotFound`.
pub async fn catalog_delete_contract<C, S>(catalog: &C, seeder: &S)
where
    C: Catalog,
    S: CatalogSeed,
{
    use control_plane_core::ControlPlaneError::NotFound;

    // Pre-seed an unrelated table FIRST so the global snapshot counter advances; its
    // snapshot predates the target table's existence.
    let other = TableRef {
        schema: "main".into(),
        name: "other".into(),
    };
    let pre = seeder
        .seed(SeedSpec {
            table: other.clone(),
            columns: vec![SeedColumn {
                name: "id".into(),
                ty: "BIGINT".into(),
                nullable: false,
            }],
            row_batches: vec![1],
        })
        .await;
    let before = pre[0].snapshot;

    // Seed the target table with two batches.
    let t = TableRef {
        schema: "main".into(),
        name: "events".into(),
    };
    let seeded = seeder
        .seed(SeedSpec {
            table: t.clone(),
            columns: vec![
                SeedColumn {
                    name: "id".into(),
                    ty: "BIGINT".into(),
                    nullable: false,
                },
                SeedColumn {
                    name: "name".into(),
                    ty: "VARCHAR".into(),
                    nullable: true,
                },
            ],
            row_batches: vec![10, 20],
        })
        .await;
    assert_eq!(seeded.len(), 2);
    let s1 = seeded[1].snapshot;

    // Live before the drop.
    assert_eq!(catalog.current_snapshot(&t).await.unwrap().id, s1);
    assert_eq!(
        catalog
            .files(&t, s1, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        2
    );

    // before-existence: not live at a snapshot before its begin (`begin <= s` false).
    assert!(
        matches!(
            catalog.files(&t, before, PageReq::unbounded()).await,
            Err(NotFound(_))
        ),
        "not live before it existed (files)"
    );
    assert!(
        matches!(catalog.schema(&t, before).await, Err(NotFound(_))),
        "not live before it existed (schema)"
    );

    // Drop it.
    let d = seeder.drop_table(&t).await;
    assert!(d > s1, "drop creates a later snapshot");

    // `end > s` false: not live AT the drop snapshot.
    assert!(
        matches!(
            catalog.files(&t, d, PageReq::unbounded()).await,
            Err(NotFound(_))
        ),
        "not live at the drop snapshot (files)"
    );
    assert!(
        matches!(catalog.schema(&t, d).await, Err(NotFound(_))),
        "not live at the drop snapshot (schema)"
    );

    // `end > s` true (non-null end): time-travel into the live past still works.
    assert_eq!(
        catalog
            .files(&t, s1, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        2,
        "time-travel before the drop still sees files"
    );
    assert_eq!(
        catalog.schema(&t, s1).await.unwrap().columns.len(),
        2,
        "time-travel before the drop still sees the schema"
    );

    // History excludes the drop snapshot; current is still the last LIVE snapshot.
    let hist = catalog
        .snapshots(&t, PageReq::unbounded())
        .await
        .unwrap()
        .items;
    assert!(
        hist.iter().all(|sn| sn.id < d),
        "history excludes the drop snapshot"
    );
    assert_eq!(
        catalog.current_snapshot(&t).await.unwrap().id,
        s1,
        "current is the last live snapshot (drop-aware)"
    );

    // A never-existed table is still NotFound (distinct from dropped).
    let nope = TableRef {
        schema: "main".into(),
        name: "nope".into(),
    };
    assert!(
        matches!(catalog.current_snapshot(&nope).await, Err(NotFound(_))),
        "never-existed table is NotFound"
    );
}

/// Contract for the `Ontology` read+write surface. Self-seeds via `define_*`
/// (loom owns the ontology schema), so it needs only an `Ontology` handle.
pub async fn ontology_contract<O: Ontology>(o: &O) {
    let tn = |s: &str| TypeName(s.to_string());
    let tref = |s: &str, n: &str| TableRef {
        schema: s.to_string(),
        name: n.to_string(),
    };

    // define + get round-trips with ordered properties and the backing table.
    let customer = ObjectType {
        name: tn("Customer"),
        table: tref("main", "customer"),
        properties: vec![PropertyDef {
            name: "email".into(),
            ty: "EmailAddress".into(),
            required: true,
        }],
    };
    o.define_type(customer.clone())
        .await
        .expect("define Customer");
    let order = ObjectType {
        name: tn("Order"),
        table: tref("main", "orders"),
        properties: vec![
            PropertyDef {
                name: "total".into(),
                ty: "Currency".into(),
                required: true,
            },
            PropertyDef {
                name: "note".into(),
                ty: "Text".into(),
                required: false,
            },
        ],
    };
    o.define_type(order.clone()).await.expect("define Order");

    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap(),
        order,
        "round-trips"
    );
    assert_eq!(
        o.get_type(&tn("Order"))
            .await
            .unwrap()
            .properties
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>(),
        vec!["total".to_string(), "note".to_string()],
        "property order preserved"
    );

    // resolve -> backing table.
    assert_eq!(
        o.resolve(&tn("Customer")).await.unwrap(),
        tref("main", "customer")
    );

    // list_types contains both.
    let names: std::collections::HashSet<String> = o
        .list_types(PageReq::unbounded())
        .await
        .unwrap()
        .into_iter()
        .map(|t| t.name.0)
        .collect();
    assert_eq!(
        names,
        ["Customer", "Order"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );

    // re-define replaces the property list (no stale properties).
    let order_v2 = ObjectType {
        name: tn("Order"),
        table: tref("main", "orders"),
        properties: vec![PropertyDef {
            name: "total".into(),
            ty: "Currency".into(),
            required: true,
        }],
    };
    o.define_type(order_v2).await.unwrap();
    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap().properties.len(),
        1,
        "redefine replaces properties"
    );

    // link between existing types, then read it back.
    let link = LinkDef {
        name: "customer".into(),
        from: tn("Order"),
        to: tn("Customer"),
        cardinality: Cardinality::One,
    };
    o.define_link(link.clone()).await.expect("define link");
    assert_eq!(
        o.links(&tn("Order"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![link.clone()]
    );

    // re-define same (name, from) upserts (no duplicate; cardinality updated).
    o.define_link(LinkDef {
        cardinality: Cardinality::Many,
        ..link.clone()
    })
    .await
    .unwrap();
    let ls = o
        .links(&tn("Order"), PageReq::unbounded())
        .await
        .unwrap()
        .items;
    assert_eq!(ls.len(), 1, "link upsert, not duplicate");
    assert_eq!(ls[0].cardinality, Cardinality::Many, "cardinality updated");

    // link to an undefined endpoint -> NotFound.
    assert!(
        matches!(
            o.define_link(LinkDef {
                name: "ghost".into(),
                from: tn("Order"),
                to: tn("Ghost"),
                cardinality: Cardinality::One,
            })
            .await,
            Err(control_plane_core::ControlPlaneError::NotFound(_))
        ),
        "link to undefined type is NotFound"
    );

    // unknown-type reads -> NotFound.
    let nope = tn("Nope");
    assert!(matches!(
        o.get_type(&nope).await,
        Err(control_plane_core::ControlPlaneError::NotFound(_))
    ));
    assert!(matches!(
        o.resolve(&nope).await,
        Err(control_plane_core::ControlPlaneError::NotFound(_))
    ));
    assert!(matches!(
        o.links(&nope, PageReq::unbounded()).await,
        Err(control_plane_core::ControlPlaneError::NotFound(_))
    ));
}

/// Contract for the `Acl` ops. `a` must be freshly empty.
pub async fn acl_contract<A: Acl + Ontology>(a: &A) {
    let sid = |s: &str| SubjectId(s.to_string());
    let rid = |s: &str| RoleId(s.to_string());
    let ttype = |s: &str| PolicyTarget::Type(TypeName(s.to_string()));
    let ttable = |s: &str, n: &str| {
        PolicyTarget::Table(TableRef {
            schema: s.to_string(),
            name: n.to_string(),
        })
    };

    // --- subjects / roles / grants / check ---
    a.define_subject(&sid("alice")).await.unwrap();
    a.define_role(&rid("reader")).await.unwrap();
    a.assign_role(&sid("alice"), &rid("reader")).await.unwrap();
    a.grant(
        &rid("reader"),
        Action::Read,
        ttype("Customer"),
        Effect::Allow,
    )
    .await
    .unwrap();

    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .unwrap(),
        Decision::Allow
    );
    assert_eq!(
        a.check(&sid("alice"), Action::Write, &ttype("Customer"))
            .await
            .unwrap(),
        Decision::Deny,
        "ungranted action denied"
    );
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Order"))
            .await
            .unwrap(),
        Decision::Deny,
        "different target denied"
    );
    assert_eq!(
        a.check(&sid("nobody"), Action::Read, &ttype("Customer"))
            .await
            .unwrap(),
        Decision::Deny,
        "unknown subject denied, not error"
    );

    // grant idempotent; one revoke clears it
    a.grant(
        &rid("reader"),
        Action::Read,
        ttype("Customer"),
        Effect::Allow,
    )
    .await
    .unwrap();
    a.revoke(&rid("reader"), Action::Read, &ttype("Customer"))
        .await
        .unwrap();
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .unwrap(),
        Decision::Deny,
        "single revoke clears double grant"
    );

    // --- role union over two roles, with a Table target ---
    a.define_role(&rid("writer")).await.unwrap();
    a.assign_role(&sid("alice"), &rid("writer")).await.unwrap();
    a.grant(
        &rid("reader"),
        Action::Read,
        ttable("main", "raw"),
        Effect::Allow,
    )
    .await
    .unwrap();
    a.grant(
        &rid("writer"),
        Action::Write,
        ttable("main", "raw"),
        Effect::Allow,
    )
    .await
    .unwrap();
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttable("main", "raw"))
            .await
            .unwrap(),
        Decision::Allow
    );
    assert_eq!(
        a.check(&sid("alice"), Action::Write, &ttable("main", "raw"))
            .await
            .unwrap(),
        Decision::Allow
    );
    a.revoke(&rid("reader"), Action::Read, &ttable("main", "raw"))
        .await
        .unwrap();
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttable("main", "raw"))
            .await
            .unwrap(),
        Decision::Deny,
        "revoking one role's grant leaves nothing for Read"
    );
    assert_eq!(
        a.check(&sid("alice"), Action::Write, &ttable("main", "raw"))
            .await
            .unwrap(),
        Decision::Allow,
        "the other role's grant still effective"
    );

    // --- referential integrity on assign_role ---
    assert!(matches!(
        a.assign_role(&sid("ghost"), &rid("reader")).await,
        Err(ControlPlaneError::NotFound(_))
    ));
    assert!(matches!(
        a.assign_role(&sid("alice"), &rid("ghost")).await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // Define the Customer type so the strict Type-target policy validation passes.
    a.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: ["tenant", "is_public", "owner", "region", "active"]
            .iter()
            .map(|n| PropertyDef {
                name: (*n).into(),
                ty: "String".into(),
                required: false,
            })
            .collect(),
        table: TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
    })
    .await
    .expect("define Customer type");

    // --- policies: nested filter + deny columns round-trip ---
    let filter = RowFilter::And(vec![
        RowFilter::Compare {
            property: "tenant".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("acme".into()),
        },
        RowFilter::Or(vec![
            RowFilter::Compare {
                property: "is_public".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Bool(true),
            },
            RowFilter::Compare {
                property: "owner".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("alice".into()),
            },
        ]),
        RowFilter::Not(Box::new(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::In,
            value: ScalarValue::List(vec![
                ScalarValue::Text("EU".into()),
                ScalarValue::Text("UK".into()),
            ]),
        })),
    ]);
    let pol = Policy {
        target: ttype("Customer"),
        row_filter: Some(filter),
        deny_columns: vec!["ssn".into(), "dob".into()],
        mask_columns: vec![],
    };
    a.set_policy(&rid("reader"), pol.clone()).await.unwrap();

    let got = a
        .policies_for(&sid("alice"), &ttype("Customer"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(
        got.items[0], pol,
        "nested filter + deny columns round-trip intact"
    );

    // upsert replaces (no duplicate); None row_filter round-trips as None
    let pol2 = Policy {
        target: ttype("Customer"),
        row_filter: None,
        deny_columns: vec![],
        mask_columns: vec![],
    };
    a.set_policy(&rid("reader"), pol2.clone()).await.unwrap();
    let got = a
        .policies_for(&sid("alice"), &ttype("Customer"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(got.len(), 1, "upsert, not duplicate");
    assert_eq!(got.items[0], pol2);
    assert!(
        got.items[0].row_filter.is_none(),
        "None row_filter stays None"
    );

    // two roles -> two policies for the same target, no merge
    let pol_w = Policy {
        target: ttype("Customer"),
        row_filter: Some(RowFilter::Compare {
            property: "active".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Bool(true),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    a.set_policy(&rid("writer"), pol_w.clone()).await.unwrap();
    let got = a
        .policies_for(&sid("alice"), &ttype("Customer"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(got.len(), 2, "both roles' policies returned, no merge");
    assert!(got.items.contains(&pol2) && got.items.contains(&pol_w));

    // target kinds don't bleed; unknown subject -> empty
    assert!(
        a.policies_for(&sid("alice"), &ttable("main", "raw"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "a Type policy is not returned for a Table target"
    );
    assert!(
        a.policies_for(&sid("nobody"), &ttype("Customer"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty()
    );

    // clear_policy removes only the named (role, target)
    a.clear_policy(&rid("reader"), &ttype("Customer"))
        .await
        .unwrap();
    let got = a
        .policies_for(&sid("alice"), &ttype("Customer"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(got.items, vec![pol_w], "only the writer policy remains");

    // mask_columns round-trips (stored + returned, distinct from deny_columns).
    // Uses a fresh target (Invoice) no other assertion touches, via `reader`
    // (still assigned to alice), so the returned vec is exactly this one policy.
    let pol_mask = Policy {
        target: ttype("Invoice"),
        row_filter: None,
        deny_columns: vec!["ssn".into()],
        mask_columns: vec!["email".into(), "phone".into()],
    };
    a.set_policy(&rid("reader"), pol_mask.clone())
        .await
        .expect("set_policy with mask_columns");
    let got_mask = a
        .policies_for(&sid("alice"), &ttype("Invoice"), PageReq::unbounded())
        .await
        .expect("policies_for after mask set");
    assert_eq!(
        got_mask.items,
        vec![pol_mask],
        "mask_columns must round-trip alongside deny_columns",
    );

    // --- grant / set_policy on a missing role -> NotFound ---
    assert!(matches!(
        a.grant(&rid("ghost"), Action::Read, ttype("X"), Effect::Allow)
            .await,
        Err(ControlPlaneError::NotFound(_))
    ));
    assert!(matches!(
        a.set_policy(
            &rid("ghost"),
            Policy {
                target: ttype("X"),
                row_filter: None,
                deny_columns: vec![],
                mask_columns: vec![],
            },
        )
        .await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // --- inverses are idempotent no-ops on absent rows ---
    a.unassign_role(&sid("alice"), &rid("never-assigned"))
        .await
        .unwrap();
    a.revoke(&rid("reader"), Action::Read, &ttype("Nothing"))
        .await
        .unwrap();
    a.clear_policy(&rid("reader"), &ttype("Nothing"))
        .await
        .unwrap();

    // unassign actually unassigns: alice loses writer -> Write on raw denied
    a.unassign_role(&sid("alice"), &rid("writer"))
        .await
        .unwrap();
    assert_eq!(
        a.check(&sid("alice"), Action::Write, &ttable("main", "raw"))
            .await
            .unwrap(),
        Decision::Deny,
        "unassigned role's grants no longer apply"
    );

    // --- deny-override (deny wins over allow) ---
    a.define_role(&rid("blocked"))
        .await
        .expect("define blocked");
    a.assign_role(&sid("alice"), &rid("blocked"))
        .await
        .expect("assign blocked");
    a.grant(
        &rid("reader"),
        Action::Read,
        ttype("Customer"),
        Effect::Allow,
    )
    .await
    .expect("re-allow reader");
    a.grant(
        &rid("blocked"),
        Action::Read,
        ttype("Customer"),
        Effect::Deny,
    )
    .await
    .expect("deny via blocked");
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .expect("check deny-override"),
        Decision::Deny,
        "a Deny grant in any of the subject's roles overrides Allow",
    );
    // Remove the deny -> Allow is restored.
    a.revoke(&rid("blocked"), Action::Read, &ttype("Customer"))
        .await
        .expect("revoke deny");
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .expect("check after revoke"),
        Decision::Allow,
        "revoking the deny restores Allow",
    );
    // Upsert flips effect: granting Deny on the existing Allow key denies.
    a.grant(
        &rid("reader"),
        Action::Read,
        ttype("Customer"),
        Effect::Deny,
    )
    .await
    .expect("flip reader to deny");
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .expect("check after flip"),
        Decision::Deny,
        "re-granting the same key with Deny upserts the effect",
    );

    // --- role inheritance: edge invariants ---
    a.define_role(&rid("h_parent"))
        .await
        .expect("define h_parent");
    a.define_role(&rid("h_child"))
        .await
        .expect("define h_child");
    assert!(matches!(
        a.add_role_inheritance(&rid("h_parent"), &rid("h_nonexistent"))
            .await,
        Err(ControlPlaneError::NotFound(_)),
    ));
    assert!(matches!(
        a.add_role_inheritance(&rid("h_parent"), &rid("h_parent"))
            .await,
        Err(ControlPlaneError::Conflict(_)),
    ));
    a.add_role_inheritance(&rid("h_parent"), &rid("h_child"))
        .await
        .expect("add edge");
    a.add_role_inheritance(&rid("h_parent"), &rid("h_child"))
        .await
        .expect("add edge idempotent");
    assert!(matches!(
        a.add_role_inheritance(&rid("h_child"), &rid("h_parent"))
            .await,
        Err(ControlPlaneError::Conflict(_)),
    ));
    a.remove_role_inheritance(&rid("h_parent"), &rid("h_child"))
        .await
        .expect("remove edge");
    a.remove_role_inheritance(&rid("h_parent"), &rid("h_child"))
        .await
        .expect("remove idempotent");

    // --- role inheritance: resolution through check/policies_for ---
    a.define_subject(&sid("h_user"))
        .await
        .expect("define h_user");
    a.define_role(&rid("senior")).await.expect("define senior");
    a.define_role(&rid("junior")).await.expect("define junior");
    a.define_role(&rid("base")).await.expect("define base");
    a.assign_role(&sid("h_user"), &rid("senior"))
        .await
        .expect("assign senior");
    a.add_role_inheritance(&rid("senior"), &rid("junior"))
        .await
        .expect("senior inherits junior");
    a.add_role_inheritance(&rid("junior"), &rid("base"))
        .await
        .expect("junior inherits base");

    a.grant(&rid("junior"), Action::Read, ttype("Widget"), Effect::Allow)
        .await
        .expect("junior allow");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Widget"))
            .await
            .expect("check inherited allow"),
        Decision::Allow,
        "senior inherits junior's allow grant",
    );
    a.grant(&rid("base"), Action::Read, ttype("Gadget"), Effect::Allow)
        .await
        .expect("base allow");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Gadget"))
            .await
            .expect("check transitive allow"),
        Decision::Allow,
        "inheritance is transitive (senior -> junior -> base)",
    );
    a.grant(&rid("base"), Action::Read, ttype("Widget"), Effect::Deny)
        .await
        .expect("base deny");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Widget"))
            .await
            .expect("check inherited deny"),
        Decision::Deny,
        "an inherited Deny wins over an inherited Allow",
    );
    a.set_policy(
        &rid("junior"),
        Policy {
            target: ttype("Widget"),
            row_filter: None,
            deny_columns: vec!["cost".into()],
            mask_columns: vec![],
        },
    )
    .await
    .expect("junior policy");
    let inh_pols = a
        .policies_for(&sid("h_user"), &ttype("Widget"), PageReq::unbounded())
        .await
        .expect("inherited policies_for");
    assert!(
        inh_pols
            .items
            .iter()
            .any(|p| p.deny_columns == vec!["cost".to_string()]),
        "policies_for returns an inherited policy",
    );
    a.remove_role_inheritance(&rid("senior"), &rid("junior"))
        .await
        .expect("remove senior->junior");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Gadget"))
            .await
            .expect("check after remove"),
        Decision::Deny,
        "removing the edge drops transitively-inherited grants",
    );

    // --- set_policy write-time validation ---
    let malformed = Policy {
        target: ttype("Customer"),
        row_filter: Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::In,
            value: ScalarValue::Text("EU".into()), // In needs a list -> malformed
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    assert!(matches!(
        a.set_policy(&rid("reader"), malformed).await,
        Err(ControlPlaneError::Validation(_)),
    ));
    let unknown_prop = Policy {
        target: ttype("Customer"),
        row_filter: Some(RowFilter::Compare {
            property: "not_a_property".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    assert!(matches!(
        a.set_policy(&rid("reader"), unknown_prop).await,
        Err(ControlPlaneError::Validation(_)),
    ));
    let undefined_type = Policy {
        target: ttype("NoSuchType"),
        row_filter: Some(RowFilter::Compare {
            property: "x".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    assert!(matches!(
        a.set_policy(&rid("reader"), undefined_type).await,
        Err(ControlPlaneError::Validation(_)),
    ));
    let table_ok = Policy {
        target: ttable("main", "raw"),
        row_filter: Some(RowFilter::Compare {
            property: "anything".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    a.set_policy(&rid("reader"), table_ok)
        .await
        .expect("table-target structural ok");
}

/// Contract for the `Lineage` ops, including the first cross-concern atomic unit
/// (`emit` + `enqueue` in one `Tx`). `cp` must be freshly empty.
pub async fn lineage_contract<CP: ControlPlane + Lineage + Queue>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef {
        namespace: ns.to_string(),
        name: n.to_string(),
    };
    // A fixed whole-second timestamp so the pg `timestamptz` round-trip is exact.
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let set = |p: Page<DatasetRef>| p.into_iter().collect::<HashSet<_>>();

    // --- emit -> events_for round-trips the envelope + opaque payload ---
    let run = RunId(uuid::Uuid::new_v4());
    let event = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![ds("ducklake", "main.a"), ds("ducklake", "main.b")],
        outputs: vec![ds("ducklake", "main.c")],
        payload: serde_json::json!({"eventType": "COMPLETE", "run": {"runId": run.0.to_string()}}),
    };
    cp.emit(event.clone()).await.expect("emit");

    let got = cp
        .events_for(&run, PageReq::unbounded())
        .await
        .expect("events_for");
    assert_eq!(
        got.items,
        vec![event.clone()],
        "envelope + payload round-trip intact"
    );
    assert!(
        cp.events_for(&RunId(uuid::Uuid::new_v4()), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "unknown run -> empty"
    );

    // --- one-hop graph via per-event co-membership ---
    assert_eq!(
        set(cp
            .upstream(&ds("ducklake", "main.c"), PageReq::unbounded())
            .await
            .unwrap()),
        [ds("ducklake", "main.a"), ds("ducklake", "main.b")]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        set(cp
            .downstream(&ds("ducklake", "main.a"), PageReq::unbounded())
            .await
            .unwrap()),
        [ds("ducklake", "main.c")]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        set(cp
            .downstream(&ds("ducklake", "main.b"), PageReq::unbounded())
            .await
            .unwrap()),
        [ds("ducklake", "main.c")]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert!(
        cp.downstream(&ds("ducklake", "main.c"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "nothing consumes c -> no downstream"
    );
    assert!(
        cp.upstream(&ds("ducklake", "main.a"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "nothing produces a -> no upstream"
    );
    assert!(
        cp.upstream(&ds("ducklake", "main.missing"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "unknown dataset -> empty"
    );

    // multiple events for one run come back in emit order
    let run2 = RunId(uuid::Uuid::new_v4());
    let start = LineageEvent {
        run_id: run2,
        event_type: EventType::Start,
        event_time: ts,
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({"eventType": "START"}),
    };
    let complete = LineageEvent {
        run_id: run2,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![ds("ducklake", "main.c")],
        outputs: vec![ds("ontology", "Customer")],
        payload: serde_json::json!({"eventType": "COMPLETE"}),
    };
    cp.emit(start.clone()).await.unwrap();
    cp.emit(complete.clone()).await.unwrap();
    assert_eq!(
        cp.events_for(&run2, PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![start, complete],
        "events returned in emit order"
    );
    // graph spans namespaces (physical -> ontology)
    assert_eq!(
        set(cp
            .downstream(&ds("ducklake", "main.c"), PageReq::unbounded())
            .await
            .unwrap()),
        [ds("ontology", "Customer")]
            .into_iter()
            .collect::<HashSet<_>>()
    );

    // --- the headline cross-concern atomicity test: emit + enqueue in one Tx ---
    let kinds = vec!["lineage-test".to_string()];

    // rollback -> neither the event nor the job is visible
    let rolled = RunId(uuid::Uuid::new_v4());
    {
        let mut tx = cp.begin().await.expect("begin");
        tx.emit(LineageEvent {
            run_id: rolled,
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![],
            outputs: vec![ds("ducklake", "main.rolled")],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        tx.enqueue(NewJob {
            kind: "lineage-test".into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .unwrap();
        tx.rollback().await.expect("rollback");
    }
    assert!(
        cp.events_for(&rolled, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "rolled-back emit is not visible"
    );
    assert!(
        cp.dequeue(&kinds, "w").await.unwrap().is_none(),
        "rolled-back enqueue is not visible"
    );

    // commit -> both the event and the job are visible
    let committed = RunId(uuid::Uuid::new_v4());
    {
        let mut tx = cp.begin().await.expect("begin");
        tx.emit(LineageEvent {
            run_id: committed,
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![],
            outputs: vec![ds("ducklake", "main.committed")],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
        tx.enqueue(NewJob {
            kind: "lineage-test".into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .unwrap();
        let _ = tx.commit().await.expect("commit");
    }
    assert_eq!(
        cp.events_for(&committed, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        1,
        "committed emit is visible"
    );
    assert!(
        cp.dequeue(&kinds, "w").await.unwrap().is_some(),
        "committed enqueue is visible"
    );
}

/// Contract for `Tx` isolation: while a transaction is open and uncommitted, the
/// autocommit read path observes none of its writes; on commit the whole unit
/// (enqueue + emit) becomes visible; on rollback nothing ever does. Deterministic —
/// the read happens on the same thread while the `Tx` handle is still alive (for pg
/// the read uses a distinct pool connection, so READ COMMITTED hides the open tx).
pub async fn tx_isolation_contract<CP: ControlPlane + Queue + Lineage>(cp: &CP) {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let event = |run: RunId| LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "ducklake".into(),
            name: "main.out".into(),
        }],
        payload: serde_json::json!({}),
    };

    // --- commit path: invisible while open, both visible after commit ---
    let run = RunId(uuid::Uuid::new_v4());
    let kinds = vec!["tx-iso".to_string()];
    let mut tx = cp.begin().await.expect("begin");
    tx.enqueue(NewJob {
        kind: "tx-iso".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("staged enqueue");
    tx.emit(event(run)).await.expect("staged emit");

    // Open + uncommitted: the autocommit read path sees neither write.
    assert!(
        cp.dequeue(&kinds, "reader").await.unwrap().is_none(),
        "uncommitted enqueue is invisible while the tx is open"
    );
    assert!(
        cp.events_for(&run, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "uncommitted emit is invisible while the tx is open"
    );

    let _ = tx.commit().await.expect("commit");

    // After commit: the whole unit is visible.
    let job = cp
        .dequeue(&kinds, "reader")
        .await
        .unwrap()
        .expect("committed enqueue is visible");
    cp.complete(job.id).await.unwrap();
    assert_eq!(
        cp.events_for(&run, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        1,
        "committed emit is visible"
    );

    // --- rollback path: nothing ever becomes visible ---
    let run_rb = RunId(uuid::Uuid::new_v4());
    let kinds_rb = vec!["tx-iso-rb".to_string()];
    let mut tx = cp.begin().await.expect("begin");
    tx.enqueue(NewJob {
        kind: "tx-iso-rb".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .unwrap();
    tx.emit(event(run_rb)).await.unwrap();
    tx.rollback().await.expect("rollback");

    assert!(
        cp.dequeue(&kinds_rb, "reader").await.unwrap().is_none(),
        "rolled-back enqueue never becomes visible"
    );
    assert!(
        cp.events_for(&run_rb, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "rolled-back emit never becomes visible"
    );
}

/// Contract for concurrent dequeue: N workers draining M jobs must claim each job
/// **exactly once** (no double-claim, none lost) — the `SKIP LOCKED` fairness guarantee.
/// `cp` is taken by value (`Clone + Send + Sync + 'static`) so clones move into spawned
/// tasks. A plain current-thread runtime suffices: the spawned tasks interleave at each
/// `dequeue().await`, so the pg adapter issues genuinely concurrent `SKIP LOCKED` queries.
pub async fn queue_concurrency_contract<CP>(cp: CP)
where
    CP: ControlPlane + Queue + Clone + Send + Sync + 'static,
{
    use std::collections::HashSet;

    let kind = "conc";
    let m: usize = 50; // jobs
    let n: usize = 8; // concurrent workers

    for _ in 0..m {
        cp.enqueue(NewJob {
            kind: kind.into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue");
    }

    let mut handles = Vec::new();
    for w in 0..n {
        let cp = cp.clone();
        let kinds = vec![kind.to_string()];
        handles.push(tokio::spawn(async move {
            let me = format!("w{w}");
            let mut claimed = Vec::new();
            while let Some(job) = cp.dequeue(&kinds, &me).await.expect("dequeue") {
                claimed.push(job.id);
                cp.complete(job.id).await.expect("complete");
            }
            claimed
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.expect("worker task"));
    }

    assert_eq!(
        all.len(),
        m,
        "every job claimed exactly once (none lost, none double-claimed)"
    );
    assert_eq!(
        all.iter().collect::<HashSet<_>>().len(),
        m,
        "no job claimed by two workers"
    );
}

/// Contract for the object-safe `ControlPlane` facade: every concern is reachable
/// through a `&dyn ControlPlane` accessor and dispatches to the live adapter impl.
/// `cp` must be freshly empty.
pub async fn control_plane_facade_contract<CP: ControlPlane>(cp: &CP) {
    // Erase to the trait object: everything below goes through the facade, not the
    // concrete adapter — that is the whole point of the accessors.
    let cp: &dyn ControlPlane = cp;

    // queue: a job enqueued through the facade is dequeued through the facade.
    let id = cp
        .queue()
        .enqueue(job("facade"))
        .await
        .expect("enqueue via facade");
    let j = cp
        .queue()
        .dequeue(&["facade".to_string()], "facade-worker")
        .await
        .expect("dequeue via facade")
        .expect("the enqueued job");
    assert_eq!(j.id, id, "facade queue() dispatches to the live queue");

    // ontology: empty on a fresh control plane, reached through the facade.
    let types = cp
        .ontology()
        .list_types(PageReq::unbounded())
        .await
        .expect("list_types via facade");
    assert!(
        types.is_empty(),
        "fresh ontology() is empty through the facade"
    );

    // acl: deny-by-default for an unknown subject, reached through the facade.
    let decision = cp
        .acl()
        .check(
            &SubjectId("nobody".into()),
            Action::Read,
            &PolicyTarget::Type(TypeName("Whatever".into())),
        )
        .await
        .expect("check via facade");
    assert_eq!(decision, Decision::Deny, "facade acl() denies by default");

    // lineage: no events for an unknown run, reached through the facade.
    let events = cp
        .lineage()
        .events_for(&RunId(uuid::Uuid::new_v4()), PageReq::unbounded())
        .await
        .expect("events_for via facade");
    assert!(
        events.is_empty(),
        "fresh lineage() has no events through the facade"
    );

    // catalog: the accessor is object-safe and returns a live trait object. Behavioral
    // catalog reads hit ducklake_* tables that need an attached catalog (covered by
    // catalog_contract); binding the ref here keeps this contract DuckLake-free so the
    // postgres facade test runs postgres-only.
    let _catalog: &(dyn Catalog + Send + Sync) = cp.catalog();
}

/// Contract for the snapshot-commit primitive (`create_table` + `append_files` +
/// `emit` + `enqueue` all in one transaction). Proves the four-leg atomic unit.
/// `cp` must be freshly empty.
pub async fn snapshot_commit_contract<C>(cp: &C)
where
    C: control_plane_core::ControlPlane
        + control_plane_core::Catalog
        + control_plane_core::Lineage
        + control_plane_core::Queue,
{
    use control_plane_core::{ColumnSpec, DataFile, PageReq, TableRef};
    let t = TableRef {
        schema: "main".into(),
        name: "events".into(),
    };
    // A fixed whole-second timestamp so the pg `timestamptz` round-trip is exact.
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let run = RunId(uuid::Uuid::new_v4());

    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[ColumnSpec {
            name: "id".into(),
            ty: "int64".into(),
            nullable: false,
        }],
    )
    .await
    .unwrap();
    tx.append_files(
        &t,
        &[DataFile {
            path: "a.parquet".into(),
            path_is_relative: true,
            record_count: 3,
            file_size_bytes: 48,
            footer_size: 10,
            column_stats: vec![],
        }],
    )
    .await
    .unwrap();
    // Stage lineage emit in the same tx.
    tx.emit(LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "ducklake".into(),
            name: "main.events".into(),
        }],
        payload: serde_json::json!({"eventType": "COMPLETE"}),
    })
    .await
    .unwrap();
    // Stage a job enqueue in the same tx.
    let job_id = tx
        .enqueue(NewJob {
            kind: "downstream".into(),
            payload: serde_json::json!({}),
            run_at: None,
            priority: 0,
        })
        .await
        .unwrap();
    let snap = tx.commit().await.unwrap();
    assert!(snap.is_some(), "catalog op produces a snapshot id");

    // All four legs visible together after commit.
    let snaps = cp.snapshots(&t, PageReq::unbounded()).await.unwrap();
    assert!(!snaps.is_empty(), "snapshot recorded");
    let latest = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(
        cp.files(&t, latest.id, PageReq::unbounded())
            .await
            .unwrap()
            .len(),
        1,
        "one file live"
    );
    assert!(
        !cp.events_for(&run, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "lineage event committed in same tx"
    );
    let dequeued = cp.dequeue(&["downstream".to_string()], "w1").await.unwrap();
    assert!(
        dequeued.is_some(),
        "enqueued job committed in same tx (dequeue-able)"
    );
    assert_eq!(
        dequeued.unwrap().id,
        job_id,
        "dequeued job has the id returned by Tx::enqueue"
    );

    // rollback leaves nothing
    let t2 = TableRef {
        schema: "main".into(),
        name: "rolled".into(),
    };
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t2,
        &[ColumnSpec {
            name: "x".into(),
            ty: "int64".into(),
            nullable: true,
        }],
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        cp.current_snapshot(&t2).await.is_err(),
        "rolled-back table absent"
    );
}
