#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "conformance test-harness library, not a production path"
)]
//! Backend-agnostic contract tests for the control-plane traits.
//! Each adapter runs the same suite against its own implementation.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, Aggregation, Auth, Cardinality, Catalog,
    CompareOp, ConstAssignment, ControlPlane, ControlPlaneError, DatasetRef, Decision,
    DerivedPropertyDef, Effect, EventType, IndexSpec, LINEAGE_MAX_DEPTH, Lineage, LineageEvent,
    LinkBacking, LinkDef, Metric, NewJob, NewServiceAccount, NewUser, ObjectType, Ontology, Page,
    PageReq, ParamDef, Policy, PolicyTarget, PropertyDef, Queue, RetryPolicy, RoleId, RowFilter,
    RunId, ScalarValue, SnapshotId, SubjectId, TableRef, TypeName, VectorIndexDef,
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

/// Define a minimal ontology type (all-`String`, optional properties) so a
/// contract can reference it as a Type target. The table name is the lowercased
/// type name. Used where a type only needs to *exist* (e.g. to satisfy the
/// existence checks on `grant`/`set_policy`/`define_action`).
async fn define_min_type<O: Ontology>(o: &O, name: &str, props: &[&str]) {
    o.define_type(ObjectType {
        name: TypeName(name.into()),
        properties: props
            .iter()
            .map(|n| PropertyDef {
                name: (*n).into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            })
            .collect(),
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: name.to_lowercase(),
        },
        identity: None,
    })
    .await
    .expect("define type");
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
/// differently (the fake builds its state directly; the pg adapter drives its
/// Iceberg catalog). Never referenced by production code.
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
                    ty: "long".into(),
                    nullable: false,
                },
                SeedColumn {
                    name: "name".into(),
                    ty: "string".into(),
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

    // schema at current: the two columns, in order, with loom logical types and
    // correct nullability. The contract seeds loom LOGICAL types (`long`/`string`);
    // `Catalog::schema()` returns logical types for every backend (the pg adapter
    // round-trips them through its physical catalog), so we pin the exact values.
    let sch = catalog.schema(&t, cur.id).await.unwrap();
    assert_eq!(
        sch.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"],
        "columns in order"
    );
    assert_eq!(
        sch.columns[0].ty, "long",
        "seeded `long` reads back as loom logical `long`"
    );
    assert_eq!(
        sch.columns[1].ty, "string",
        "seeded `string` reads back as loom logical `string`"
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
                ty: "long".into(),
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
                    ty: "long".into(),
                    nullable: false,
                },
                SeedColumn {
                    name: "name".into(),
                    ty: "string".into(),
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
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "email".into(),
                ty: "EmailAddress".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
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
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "note".into(),
                ty: "Text".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: None,
    };
    o.define_type(order.clone()).await.expect("define Order");

    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap(),
        order,
        "round-trips"
    );

    // identity round-trips (declared PK property name persists).
    let got_customer = o.get_type(&tn("Customer")).await.unwrap();
    assert_eq!(
        got_customer.identity.as_deref(),
        Some("id"),
        "declared identity persists through define_type/get_type"
    );
    // an undeclared identity stays None.
    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap().identity,
        None,
        "an undeclared identity stays None"
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
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
        identity: None,
    };
    o.define_type(order_v2).await.unwrap();
    assert_eq!(
        o.get_type(&tn("Order")).await.unwrap().properties.len(),
        1,
        "redefine replaces properties"
    );

    // --- Model constraints: round-trip + define-time rejection. ---
    let constrained = ObjectType {
        name: tn("Account"),
        table: tref("main", "account"),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints {
                    range: Some(control_plane_core::RangeConstraint {
                        min: Some(1.0),
                        max: None,
                    }),
                    ..control_plane_core::PropertyConstraints::default()
                },
            },
            PropertyDef {
                name: "code".into(),
                ty: "String".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints {
                    length: Some(control_plane_core::LengthConstraint {
                        min: Some(2),
                        max: Some(8),
                    }),
                    pattern: Some("^[A-Z]+$".into()),
                    one_of: None,
                    range: None,
                },
            },
            PropertyDef {
                name: "note".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
    };
    o.define_type(constrained.clone())
        .await
        .expect("define constrained type");
    assert_eq!(
        o.get_type(&tn("Account")).await.unwrap(),
        constrained,
        "constraints round-trip unchanged"
    );

    // A `range` on a string property is rejected at define time.
    let bad_range = ObjectType {
        name: tn("BadRange"),
        table: tref("main", "bad_range"),
        properties: vec![PropertyDef {
            name: "name".into(),
            ty: "String".into(),
            required: false,
            constraints: control_plane_core::PropertyConstraints {
                range: Some(control_plane_core::RangeConstraint {
                    min: Some(0.0),
                    max: None,
                }),
                ..control_plane_core::PropertyConstraints::default()
            },
        }],
        derived: vec![],
        identity: None,
    };
    assert!(
        matches!(
            o.define_type(bad_range).await,
            Err(control_plane_core::ControlPlaneError::Validation(_))
        ),
        "range on a string property is a define-time Validation error"
    );

    // An invalid regex `pattern` is rejected at define time.
    let bad_regex = ObjectType {
        name: tn("BadRegex"),
        table: tref("main", "bad_regex"),
        properties: vec![PropertyDef {
            name: "code".into(),
            ty: "String".into(),
            required: false,
            constraints: control_plane_core::PropertyConstraints {
                pattern: Some("(".into()),
                ..control_plane_core::PropertyConstraints::default()
            },
        }],
        derived: vec![],
        identity: None,
    };
    assert!(
        matches!(
            o.define_type(bad_regex).await,
            Err(control_plane_core::ControlPlaneError::Validation(_))
        ),
        "invalid regex is a define-time Validation error"
    );

    // link between existing types, then read it back.
    let link = LinkDef {
        name: "customer".into(),
        from: tn("Order"),
        to: tn("Customer"),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "customer_id".into(),
            to_column: "id".into(),
        },
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

    // many-to-many link with a join-table backing round-trips intact.
    let m2m = LinkDef {
        name: "items".into(),
        from: tn("Customer"),
        to: tn("Order"),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: tref("main", "customer_order"),
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
    };
    o.define_link(m2m.clone()).await.expect("define m2m link");
    assert_eq!(
        o.links(&tn("Customer"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![m2m.clone()],
        "join-table backing round-trips"
    );

    // inbound adjacency: links_to(X) returns links whose `to` is X — the inverse of
    // `links`. `customer` (Order -> Customer, upserted to Many above) is inbound to
    // Customer; `items` (Customer -> Order, join-table) is inbound to Order.
    let customer_link = LinkDef {
        cardinality: Cardinality::Many,
        ..link.clone()
    };
    assert_eq!(
        o.links_to(&tn("Customer"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![customer_link],
        "links_to returns inbound links (FK)"
    );
    assert_eq!(
        o.links_to(&tn("Order"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![m2m.clone()],
        "links_to returns inbound links (join-table)"
    );

    // link to an undefined endpoint -> NotFound.
    assert!(
        matches!(
            o.define_link(LinkDef {
                name: "ghost".into(),
                from: tn("Order"),
                to: tn("Ghost"),
                cardinality: Cardinality::One,
                backing: LinkBacking::ForeignKey {
                    from_column: "ghost_id".into(),
                    to_column: "id".into(),
                },
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
    assert!(matches!(
        o.links_to(&nope, PageReq::unbounded()).await,
        Err(control_plane_core::ControlPlaneError::NotFound(_))
    ));

    // --- Actions ---
    // The target type must exist (FK in the pg adapter).
    o.define_type(ObjectType {
        name: tn("Widget"),
        table: tref("main", "widget"),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: None,
    })
    .await
    .expect("define Widget");

    let create_widget = ActionDef {
        name: ActionName("createWidget".into()),
        target: tn("Widget"),
        parameters: vec![
            ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                binds: None,
            },
        ],
        kind: ActionKind::Insert,
        assignments: vec![],
    };
    o.define_action(create_widget.clone())
        .await
        .expect("define action");
    assert_eq!(
        o.get_action(&ActionName("createWidget".into()))
            .await
            .unwrap(),
        create_widget,
        "action round-trips"
    );
    assert_eq!(
        o.get_action(&ActionName("createWidget".into()))
            .await
            .unwrap()
            .parameters
            .iter()
            .map(|p| p.name.clone())
            .collect::<Vec<_>>(),
        vec!["id".to_string(), "name".to_string()],
        "parameter order preserved"
    );
    // Upsert replaces the parameter list.
    o.define_action(ActionDef {
        name: ActionName("createWidget".into()),
        target: tn("Widget"),
        parameters: vec![ParamDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            binds: None,
        }],
        kind: ActionKind::Insert,
        assignments: vec![],
    })
    .await
    .expect("redefine action");
    assert_eq!(
        o.get_action(&ActionName("createWidget".into()))
            .await
            .unwrap()
            .parameters
            .len(),
        1,
        "redefine replaces parameters"
    );
    // Unknown action -> NotFound.
    assert!(matches!(
        o.get_action(&ActionName("nope".into())).await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // --- Action param→property mapping (binds + constant assignments) ---
    o.define_type(ObjectType {
        name: tn("Gadget"),
        table: tref("main", "gadget"),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: None,
    })
    .await
    .expect("define Gadget");

    let create_gadget = ActionDef {
        name: ActionName("createGadget".into()),
        target: tn("Gadget"),
        parameters: vec![
            ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
            // Renamed: the operation param `displayName` writes the `name` property.
            ParamDef {
                name: "displayName".into(),
                ty: "String".into(),
                required: false,
                binds: Some("name".into()),
            },
        ],
        kind: ActionKind::Insert,
        assignments: vec![ConstAssignment {
            property: "status".into(),
            value: serde_json::json!("active"),
        }],
    };
    o.define_action(create_gadget.clone())
        .await
        .expect("define mapping action");
    assert_eq!(
        o.get_action(&ActionName("createGadget".into()))
            .await
            .unwrap(),
        create_gadget,
        "action round-trips with binds + constant assignments",
    );

    // Redefining with an empty mapping clears binds + assignments (upsert replaces both).
    o.define_action(ActionDef {
        name: ActionName("createGadget".into()),
        target: tn("Gadget"),
        parameters: vec![ParamDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            binds: None,
        }],
        kind: ActionKind::Insert,
        assignments: vec![],
    })
    .await
    .expect("redefine mapping action");
    let redef = o
        .get_action(&ActionName("createGadget".into()))
        .await
        .unwrap();
    assert!(redef.assignments.is_empty(), "redefine clears assignments");
    assert_eq!(redef.parameters.len(), 1, "redefine replaces parameters");
    assert_eq!(redef.parameters[0].binds, None, "redefine clears binds");

    // --- Derived properties ---
    o.define_type(ObjectType {
        name: tn("Account"),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![
            DerivedPropertyDef {
                name: "txnCount".into(),
                ty: "Long".into(),
                link: "transactions".into(),
                agg: Aggregation::Count,
            },
            DerivedPropertyDef {
                name: "balance".into(),
                ty: "Double".into(),
                link: "transactions".into(),
                agg: Aggregation::Sum("amount".into()),
            },
        ],
        table: tref("main", "account"),
        identity: None,
    })
    .await
    .expect("define Account with derived");
    let got = o.get_type(&tn("Account")).await.unwrap();
    assert_eq!(
        got.derived
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<_>>(),
        vec!["txnCount".to_string(), "balance".to_string()],
        "derived order preserved"
    );
    assert_eq!(got.derived[0].agg, Aggregation::Count);
    assert_eq!(got.derived[1].agg, Aggregation::Sum("amount".into()));
    // Redefine with fewer derived -> replaced.
    o.define_type(ObjectType {
        name: tn("Account"),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
        table: tref("main", "account"),
        identity: None,
    })
    .await
    .unwrap();
    assert!(
        o.get_type(&tn("Account")).await.unwrap().derived.is_empty(),
        "redefine replaces derived"
    );

    // --- vector index declarations -------------------------------------------
    let doc = ObjectType {
        name: tn("Document"),
        table: tref("main", "document"),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "embedding".into(),
                ty: "vector(8)".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "title".into(),
                ty: "Text".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
    };
    o.define_type(doc.clone()).await.expect("define Document");

    let by_sim = VectorIndexDef {
        name: "by_sim".into(),
        type_name: tn("Document"),
        property: "embedding".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Hnsw {
            m: Some(16),
            ef_construction: Some(200),
        },
    };
    let by_cluster = VectorIndexDef {
        name: "by_cluster".into(),
        type_name: tn("Document"),
        property: "embedding".into(),
        metric: Metric::L2,
        spec: IndexSpec::IvfFlat { nlist: Some(4) },
    };
    o.define_vector_index(by_sim.clone())
        .await
        .expect("define by_sim");
    o.define_vector_index(by_cluster.clone())
        .await
        .expect("define by_cluster");

    assert_eq!(
        o.get_vector_index(&tn("Document"), "by_sim").await.unwrap(),
        Some(by_sim.clone())
    );
    assert_eq!(
        o.get_vector_index(&tn("Document"), "by_cluster")
            .await
            .unwrap(),
        Some(by_cluster.clone())
    );
    assert_eq!(
        o.get_vector_index(&tn("Document"), "nope").await.unwrap(),
        None
    );

    let mut names: Vec<String> = o
        .vector_indexes_for(&tn("Document"))
        .await
        .unwrap()
        .into_iter()
        .map(|d| d.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["by_cluster".to_string(), "by_sim".to_string()]);

    // upsert replaces (exercises as_cols → from_label round-trip on the postgres adapter)
    let by_sim_v2 = VectorIndexDef {
        metric: Metric::L2,
        ..by_sim.clone()
    };
    o.define_vector_index(by_sim_v2.clone())
        .await
        .expect("redeclare by_sim");
    assert_eq!(
        o.get_vector_index(&tn("Document"), "by_sim").await.unwrap(),
        Some(by_sim_v2)
    );

    // validation: non-vector property and missing property both error
    let bad_prop = VectorIndexDef {
        name: "bad".into(),
        type_name: tn("Document"),
        property: "title".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Flat,
    };
    assert!(
        o.define_vector_index(bad_prop).await.is_err(),
        "non-vector property rejected"
    );
    let missing = VectorIndexDef {
        name: "bad2".into(),
        type_name: tn("Document"),
        property: "ghost".into(),
        metric: Metric::Cosine,
        spec: IndexSpec::Flat,
    };
    assert!(
        o.define_vector_index(missing).await.is_err(),
        "missing property rejected"
    );
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

    // grant/set_policy now reject Type targets that don't exist, so every Type
    // this contract references as a grant/policy target must be defined first.
    // Customer carries the properties the row-filter assertions use; the rest
    // only need to exist (their policies use row_filter: None / unvalidated cols).
    define_min_type(
        a,
        "Customer",
        &["tenant", "is_public", "owner", "region", "active"],
    )
    .await;
    for t in ["Invoice", "Ticket", "Widget", "Gadget"] {
        define_min_type(a, t, &[]).await;
    }

    // --- subjects / roles / grants / check ---
    a.define_subject(&sid("alice")).await.unwrap();
    a.define_role(&rid("reader")).await.unwrap();
    a.assign_role(&sid("alice"), &rid("reader")).await.unwrap();

    // --- has_role: direct membership only ---
    assert!(
        a.has_role(&sid("alice"), &rid("reader")).await.unwrap(),
        "alice was assigned reader"
    );
    assert!(
        !a.has_role(&sid("alice"), &rid("writer")).await.unwrap(),
        "alice not assigned writer yet"
    );
    assert!(
        !a.has_role(&sid("ghost"), &rid("reader")).await.unwrap(),
        "unknown subject → false, not error"
    );
    assert!(
        !a.has_role(&sid("alice"), &rid("no-such-role"))
            .await
            .unwrap(),
        "unknown role → false, not error"
    );

    // --- list_roles: all defined roles, sorted ---
    let roles = a.list_roles().await.unwrap();
    assert!(
        roles.contains(&rid("reader")),
        "list_roles includes a defined role"
    );
    assert!(
        roles.windows(2).all(|w| w[0].0 <= w[1].0),
        "list_roles is sorted by id ascending"
    );

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

    // (Customer is defined up front, alongside the other Type targets.)

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
    a.set_policy(&rid("reader"), Action::Read, pol.clone())
        .await
        .unwrap();

    let got = a
        .policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Customer"),
            PageReq::unbounded(),
        )
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
    a.set_policy(&rid("reader"), Action::Read, pol2.clone())
        .await
        .unwrap();
    let got = a
        .policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Customer"),
            PageReq::unbounded(),
        )
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
    a.set_policy(&rid("writer"), Action::Read, pol_w.clone())
        .await
        .unwrap();
    let got = a
        .policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Customer"),
            PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert_eq!(got.len(), 2, "both roles' policies returned, no merge");
    assert!(got.items.contains(&pol2) && got.items.contains(&pol_w));

    // target kinds don't bleed; unknown subject -> empty
    assert!(
        a.policies_for(
            &sid("alice"),
            Action::Read,
            &ttable("main", "raw"),
            PageReq::unbounded()
        )
        .await
        .unwrap()
        .is_empty(),
        "a Type policy is not returned for a Table target"
    );
    assert!(
        a.policies_for(
            &sid("nobody"),
            Action::Read,
            &ttype("Customer"),
            PageReq::unbounded()
        )
        .await
        .unwrap()
        .is_empty()
    );

    // clear_policy removes only the named (role, target)
    a.clear_policy(&rid("reader"), Action::Read, &ttype("Customer"))
        .await
        .unwrap();
    let got = a
        .policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Customer"),
            PageReq::unbounded(),
        )
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
    a.set_policy(&rid("reader"), Action::Read, pol_mask.clone())
        .await
        .expect("set_policy with mask_columns");
    let got_mask = a
        .policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Invoice"),
            PageReq::unbounded(),
        )
        .await
        .expect("policies_for after mask set");
    assert_eq!(
        got_mask.items,
        vec![pol_mask],
        "mask_columns must round-trip alongside deny_columns",
    );

    // --- action scoping: read and write policies are independent ---
    // Distinct Read and Write policies on the SAME (role, target). row_filter: None so no
    // ontology validation is needed; the point is storage keyed by action.
    let ticket_read = Policy {
        target: ttype("Ticket"),
        row_filter: None,
        deny_columns: vec!["priority".into()],
        mask_columns: vec![],
    };
    let ticket_write = Policy {
        target: ttype("Ticket"),
        row_filter: None,
        deny_columns: vec!["assignee".into()],
        mask_columns: vec![],
    };
    a.set_policy(&rid("reader"), Action::Read, ticket_read.clone())
        .await
        .unwrap();
    a.set_policy(&rid("reader"), Action::Write, ticket_write.clone())
        .await
        .unwrap();
    // A Read query returns only the Read policy; a Write query only the Write policy.
    let r = a
        .policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Ticket"),
            PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert_eq!(
        r.items,
        vec![ticket_read.clone()],
        "Read query returns only the Read-scoped policy"
    );
    let w = a
        .policies_for(
            &sid("alice"),
            Action::Write,
            &ttype("Ticket"),
            PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert_eq!(
        w.items,
        vec![ticket_write.clone()],
        "Write query returns only the Write-scoped policy"
    );
    // clear_policy is action-scoped: clearing Read leaves Write intact.
    a.clear_policy(&rid("reader"), Action::Read, &ttype("Ticket"))
        .await
        .unwrap();
    assert!(
        a.policies_for(
            &sid("alice"),
            Action::Read,
            &ttype("Ticket"),
            PageReq::unbounded()
        )
        .await
        .unwrap()
        .is_empty(),
        "Read policy cleared"
    );
    assert_eq!(
        a.policies_for(
            &sid("alice"),
            Action::Write,
            &ttype("Ticket"),
            PageReq::unbounded()
        )
        .await
        .unwrap()
        .items,
        vec![ticket_write],
        "Write policy survives clearing the Read policy"
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
            Action::Read,
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
    a.clear_policy(&rid("reader"), Action::Read, &ttype("Nothing"))
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
        Action::Read,
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
        .policies_for(
            &sid("h_user"),
            Action::Read,
            &ttype("Widget"),
            PageReq::unbounded(),
        )
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
        a.set_policy(&rid("reader"), Action::Read, malformed).await,
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
        a.set_policy(&rid("reader"), Action::Read, unknown_prop)
            .await,
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
        a.set_policy(&rid("reader"), Action::Read, undefined_type)
            .await,
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
    a.set_policy(&rid("reader"), Action::Read, table_ok)
        .await
        .expect("table-target structural ok");
}

/// Contract for the `Auth` ops. `a` must be freshly empty. Bound on `Acl` too so
/// we can prove `create_user` made the subject a real ACL principal.
pub async fn auth_contract<A: Auth + Acl>(a: &A) {
    let sid = |s: &str| SubjectId(s.to_string());
    let h = |b: u8| -> [u8; 32] { [b; 32] };

    // Fresh store: no users.
    assert!(!a.has_any_user().await.unwrap());

    // --- bootstrap sealing: one-way ---
    assert!(
        !a.is_bootstrap_sealed().await.unwrap(),
        "fresh CP is not sealed"
    );
    a.seal_bootstrap().await.unwrap();
    assert!(
        a.is_bootstrap_sealed().await.unwrap(),
        "sealed after seal_bootstrap"
    );
    assert!(
        matches!(
            a.seal_bootstrap().await,
            Err(control_plane_core::ControlPlaneError::Conflict(_))
        ),
        "second seal is a Conflict, never a silent success"
    );

    // --- create_user ---
    a.create_user(&NewUser {
        subject_id: sid("u-alice"),
        username: "alice".into(),
        password_phc: "phc-alice".into(),
    })
    .await
    .unwrap();
    assert!(a.has_any_user().await.unwrap());

    // create_user ensured the ACL subject: assigning a role must succeed (it
    // returns NotFound for an unknown subject).
    a.define_role(&RoleId("r".into())).await.unwrap();
    a.assign_role(&sid("u-alice"), &RoleId("r".into()))
        .await
        .unwrap();

    // duplicate username → Conflict
    let dup = a
        .create_user(&NewUser {
            subject_id: sid("u-other"),
            username: "alice".into(),
            password_phc: "phc-other".into(),
        })
        .await;
    assert!(matches!(dup, Err(ControlPlaneError::Conflict(_))));

    // --- find_password_credential ---
    let cred = a.find_password_credential("alice").await.unwrap().unwrap();
    assert_eq!(cred.subject_id, sid("u-alice"));
    assert_eq!(cred.password_phc, "phc-alice");
    assert!(a.find_password_credential("ghost").await.unwrap().is_none());

    // --- sessions ---
    let now = OffsetDateTime::now_utc();
    let future = now + time::Duration::hours(1);
    a.create_session(&sid("u-alice"), &h(1), future)
        .await
        .unwrap();

    // resolve while unexpired
    assert_eq!(
        a.resolve_session(&h(1), now).await.unwrap(),
        Some(sid("u-alice"))
    );
    // unknown token → None
    assert!(a.resolve_session(&h(9), now).await.unwrap().is_none());
    // expiry boundary: at/after expires_at → None
    assert!(
        a.resolve_session(&h(1), now + time::Duration::hours(2))
            .await
            .unwrap()
            .is_none()
    );

    // revoke is effective and idempotent
    a.revoke_session(&h(1)).await.unwrap();
    assert!(a.resolve_session(&h(1), now).await.unwrap().is_none());
    a.revoke_session(&h(1)).await.unwrap(); // no-op, no error

    // --- user management: list ---
    // alice (created above) plus a second user; both appear, no verifier surfaced.
    a.create_user(&NewUser {
        subject_id: sid("u-bob"),
        username: "bob".into(),
        password_phc: "phc-bob".into(),
    })
    .await
    .unwrap();
    let listed = a.list_users(PageReq::unbounded()).await.unwrap();
    let names: Vec<String> = listed.items.iter().map(|u| u.username.clone()).collect();
    assert!(names.contains(&"alice".to_string()));
    assert!(names.contains(&"bob".to_string()));
    assert!(
        listed.items.iter().all(|u| !u.disabled),
        "all active initially"
    );

    // --- disable: login + session both rejected ---
    // Give bob a live session, then disable bob.
    let now2 = OffsetDateTime::now_utc();
    let future2 = now2 + time::Duration::hours(1);
    a.create_session(&sid("u-bob"), &h(7), future2)
        .await
        .unwrap();
    assert_eq!(
        a.resolve_session(&h(7), now2).await.unwrap(),
        Some(sid("u-bob")),
        "session resolves while active"
    );
    a.set_user_disabled("bob", true).await.unwrap();
    // login lookup now hides bob (uniform None)
    assert!(
        a.find_password_credential("bob").await.unwrap().is_none(),
        "disabled user not returned for login"
    );
    // the pre-existing session is rejected (revoked + disabled-honoring resolve)
    assert!(
        a.resolve_session(&h(7), now2).await.unwrap().is_none(),
        "disabled user's session rejected"
    );
    // the listing reflects disabled state
    let listed = a.list_users(PageReq::unbounded()).await.unwrap();
    assert!(
        listed
            .items
            .iter()
            .any(|u| u.username == "bob" && u.disabled),
        "bob shows disabled"
    );

    // --- enable: login works again (a fresh login, not a resurrected session) ---
    a.set_user_disabled("bob", false).await.unwrap();
    let cred = a.find_password_credential("bob").await.unwrap().unwrap();
    assert_eq!(cred.subject_id, sid("u-bob"));

    // --- set_user_disabled on unknown user → NotFound ---
    assert!(matches!(
        a.set_user_disabled("ghost", true).await,
        Err(ControlPlaneError::NotFound(_))
    ));
}

/// Contract for the service-account + service-token `Auth` ops. `a` must be freshly
/// empty. Bound on `Acl` too so we can prove `create_service_account` made the
/// subject a real ACL principal (role assignment succeeds).
pub async fn service_account_contract<A: Auth + Acl>(a: &A) {
    let sid = |s: &str| SubjectId(s.to_string());
    let h = |b: u8| -> [u8; 32] { [b; 32] };
    let now = OffsetDateTime::now_utc();
    let future = now + time::Duration::hours(1);

    // --- create_service_account ---
    a.create_service_account(&NewServiceAccount {
        subject_id: sid("svc-etl"),
        name: "nightly-etl".into(),
    })
    .await
    .unwrap();

    // create_service_account ensured the ACL subject: assigning a role succeeds
    // (it returns NotFound for an unknown subject), proving ACL parity with a user.
    a.define_role(&RoleId("r".into())).await.unwrap();
    a.assign_role(&sid("svc-etl"), &RoleId("r".into()))
        .await
        .unwrap();

    // duplicate name → Conflict
    let dup = a
        .create_service_account(&NewServiceAccount {
            subject_id: sid("svc-other"),
            name: "nightly-etl".into(),
        })
        .await;
    assert!(matches!(dup, Err(ControlPlaneError::Conflict(_))));

    // subject_id must not overlap the human-user namespace: creating a service
    // account whose subject_id already belongs to a user → Conflict, so a minted
    // token can never authenticate as an existing user's subject.
    a.create_user(&NewUser {
        subject_id: sid("human-1"),
        username: "human-1".into(),
        password_phc: "phc".into(),
    })
    .await
    .unwrap();
    let clash = a
        .create_service_account(&NewServiceAccount {
            subject_id: sid("human-1"),
            name: "human-1-svc".into(),
        })
        .await;
    assert!(
        matches!(clash, Err(ControlPlaneError::Conflict(_))),
        "a service account cannot adopt an existing user's subject_id"
    );

    // list_service_accounts returns the account metadata (name), never a token.
    let accounts = a.list_service_accounts(PageReq::unbounded()).await.unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts.items[0].name, "nightly-etl");
    assert_eq!(accounts.items[0].subject_id, sid("svc-etl"));

    // --- create_service_token / resolve ---
    a.create_service_token(&sid("svc-etl"), &h(1), "primary", future)
        .await
        .unwrap();
    assert_eq!(
        a.resolve_service_token(&h(1), now).await.unwrap(),
        Some(sid("svc-etl")),
        "a live token resolves to its account"
    );

    // minting for an unknown account → NotFound
    assert!(matches!(
        a.create_service_token(&sid("ghost"), &h(2), "x", future)
            .await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // unknown token hash → None
    assert!(a.resolve_service_token(&h(9), now).await.unwrap().is_none());

    // expired (expires_at <= now) → None
    assert!(
        a.resolve_service_token(&h(1), now + time::Duration::hours(2))
            .await
            .unwrap()
            .is_none(),
        "an expired token does not resolve"
    );

    // rotation: a second live token overlaps the first.
    a.create_service_token(&sid("svc-etl"), &h(3), "rotated", future)
        .await
        .unwrap();
    assert_eq!(
        a.resolve_service_token(&h(3), now).await.unwrap(),
        Some(sid("svc-etl"))
    );

    // list_service_tokens returns BOTH, metadata only (label/expiry), never the raw
    // token — the type has no plaintext field. Scoped to the account's subject.
    let tokens = a
        .list_service_tokens(&sid("svc-etl"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(tokens.len(), 2, "both minted tokens are listed");
    let labels: HashSet<String> = tokens.items.iter().map(|t| t.label.clone()).collect();
    assert_eq!(
        labels,
        ["primary", "rotated"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
    assert!(
        tokens.items.iter().all(|t| t.revoked_at.is_none()),
        "neither token is revoked yet"
    );

    // --- revoke (idempotent; reflected in resolve) ---
    a.revoke_service_token(&h(1)).await.unwrap();
    assert!(
        a.resolve_service_token(&h(1), now).await.unwrap().is_none(),
        "a revoked token does not resolve"
    );
    assert_eq!(
        a.resolve_service_token(&h(3), now).await.unwrap(),
        Some(sid("svc-etl")),
        "revoking one token leaves the other live"
    );
    a.revoke_service_token(&h(1)).await.unwrap(); // idempotent no-op

    // the revoked token still lists, now with revoked_at set.
    let after = a
        .list_service_tokens(&sid("svc-etl"), PageReq::unbounded())
        .await
        .unwrap();
    let revoked = after
        .items
        .iter()
        .find(|t| t.token_sha256 == h(1))
        .expect("revoked token still listed");
    assert!(revoked.revoked_at.is_some(), "revoked_at recorded");

    // tokens are scoped: an account with no tokens lists empty.
    a.create_service_account(&NewServiceAccount {
        subject_id: sid("svc-empty"),
        name: "empty".into(),
    })
    .await
    .unwrap();
    assert!(
        a.list_service_tokens(&sid("svc-empty"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty()
    );
}

/// Both-adapter contract for type-existence validation on the three loom-owned
/// write paths that store a reference to an ontology type: `Acl::grant`,
/// `Acl::set_policy`, and `Ontology::define_action`. Each must reject a
/// non-existent type with `ControlPlaneError::Validation`, identically on the
/// Postgres adapter and the in-memory fake. `cp` must be freshly empty.
pub async fn existence_validation_contract<CP: Acl + Ontology>(cp: &CP) {
    let rid = |s: &str| RoleId(s.to_string());
    let tn = |s: &str| TypeName(s.to_string());
    let ttype = |s: &str| PolicyTarget::Type(TypeName(s.to_string()));
    let policy = |target: PolicyTarget, row_filter: Option<RowFilter>| Policy {
        target,
        row_filter,
        deny_columns: vec![],
        mask_columns: vec![],
    };

    // Seed a type `T` and a role `R`.
    define_min_type(cp, "T", &["col"]).await;
    cp.define_role(&rid("R")).await.unwrap();

    // grant: an existing Type is accepted; an unknown Type is a Validation error.
    cp.grant(&rid("R"), Action::Read, ttype("T"), Effect::Allow)
        .await
        .expect("grant on existing type");
    assert!(
        matches!(
            cp.grant(&rid("R"), Action::Read, ttype("Nope"), Effect::Allow)
                .await,
            Err(ControlPlaneError::Validation(_))
        ),
        "grant on unknown type rejected"
    );

    // set_policy: an existing Type is accepted with and without a row_filter; an
    // unknown Type is rejected (even with no row_filter — the always-check).
    cp.set_policy(&rid("R"), Action::Read, policy(ttype("T"), None))
        .await
        .expect("set_policy on existing type, no row_filter");
    cp.set_policy(
        &rid("R"),
        Action::Read,
        policy(
            ttype("T"),
            Some(RowFilter::Compare {
                property: "col".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("x".into()),
            }),
        ),
    )
    .await
    .expect("set_policy on existing type, with row_filter");
    assert!(
        matches!(
            cp.set_policy(&rid("R"), Action::Read, policy(ttype("Nope"), None))
                .await,
            Err(ControlPlaneError::Validation(_))
        ),
        "set_policy on unknown type rejected even without a row_filter"
    );

    // define_action: an existing target type is accepted; an unknown one is rejected.
    cp.define_action(ActionDef {
        name: ActionName("act".into()),
        target: tn("T"),
        parameters: vec![],
        kind: ActionKind::Insert,
        assignments: vec![],
    })
    .await
    .expect("define_action on existing target");
    assert!(
        matches!(
            cp.define_action(ActionDef {
                name: ActionName("actBad".into()),
                target: tn("Nope"),
                parameters: vec![],
                kind: ActionKind::Insert,
                assignments: vec![],
            })
            .await,
            Err(ControlPlaneError::Validation(_))
        ),
        "define_action on unknown target type rejected"
    );

    // Deferred boundary: `Table` targets are NOT existence-checked, so a Table grant
    // must still be accepted.
    cp.grant(
        &rid("R"),
        Action::Read,
        PolicyTarget::Table(TableRef {
            schema: "main".into(),
            name: "raw".into(),
        }),
        Effect::Allow,
    )
    .await
    .expect("Table targets are not existence-checked");
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
        inputs: vec![ds("warehouse", "main.a"), ds("warehouse", "main.b")],
        outputs: vec![ds("warehouse", "main.c")],
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
            .upstream(&ds("warehouse", "main.c"), 1, PageReq::unbounded())
            .await
            .unwrap()),
        [ds("warehouse", "main.a"), ds("warehouse", "main.b")]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        set(cp
            .downstream(&ds("warehouse", "main.a"), 1, PageReq::unbounded())
            .await
            .unwrap()),
        [ds("warehouse", "main.c")]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert_eq!(
        set(cp
            .downstream(&ds("warehouse", "main.b"), 1, PageReq::unbounded())
            .await
            .unwrap()),
        [ds("warehouse", "main.c")]
            .into_iter()
            .collect::<HashSet<_>>()
    );
    assert!(
        cp.downstream(&ds("warehouse", "main.c"), 1, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "nothing consumes c -> no downstream"
    );
    assert!(
        cp.upstream(&ds("warehouse", "main.a"), 1, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "nothing produces a -> no upstream"
    );
    assert!(
        cp.upstream(&ds("warehouse", "main.missing"), 1, PageReq::unbounded())
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
        inputs: vec![ds("warehouse", "main.c")],
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
            .downstream(&ds("warehouse", "main.c"), 1, PageReq::unbounded())
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
            outputs: vec![ds("warehouse", "main.rolled")],
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
            outputs: vec![ds("warehouse", "main.committed")],
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

/// Contract: transitive closure with a depth cap and cycle termination. Run against
/// every `Lineage` adapter.
pub async fn lineage_closure_contract<CP: Lineage>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef {
        namespace: ns.to_string(),
        name: n.to_string(),
    };
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let set = |p: Page<DatasetRef>| p.into_iter().collect::<HashSet<_>>();
    let edge = |inp: DatasetRef, out: DatasetRef| LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    };

    // chain  A -> B -> C -> D  (each event: input -> output)
    let (a, b, c, d) = (
        ds("w", "clo.a"),
        ds("w", "clo.b"),
        ds("w", "clo.c"),
        ds("w", "clo.d"),
    );
    cp.emit(edge(a.clone(), b.clone())).await.unwrap();
    cp.emit(edge(b.clone(), c.clone())).await.unwrap();
    cp.emit(edge(c.clone(), d.clone())).await.unwrap();

    // upstream (ancestry) of D
    assert_eq!(
        set(cp.upstream(&d, 1, PageReq::unbounded()).await.unwrap()),
        [c.clone()].into_iter().collect(),
        "depth=1 is one hop"
    );
    assert_eq!(
        set(cp.upstream(&d, 2, PageReq::unbounded()).await.unwrap()),
        [b.clone(), c.clone()].into_iter().collect(),
        "depth=2 = two hops"
    );
    assert_eq!(
        set(cp.upstream(&d, 3, PageReq::unbounded()).await.unwrap()),
        [a.clone(), b.clone(), c.clone()].into_iter().collect(),
        "depth=3 = full ancestry"
    );
    // downstream (descendancy) of A
    assert_eq!(
        set(cp.downstream(&a, 3, PageReq::unbounded()).await.unwrap()),
        [b.clone(), c.clone(), d.clone()].into_iter().collect(),
        "downstream closure of A"
    );

    // depth cap + zero depth are rejected (never an unbounded walk)
    assert!(
        matches!(
            cp.upstream(&d, LINEAGE_MAX_DEPTH + 1, PageReq::unbounded())
                .await,
            Err(ControlPlaneError::Validation(_))
        ),
        "over-cap depth rejected"
    );
    assert!(
        matches!(
            cp.upstream(&d, 0, PageReq::unbounded()).await,
            Err(ControlPlaneError::Validation(_))
        ),
        "zero depth rejected"
    );

    // cycle  P -> Q -> R -> P  terminates and returns the finite reachable set
    let (p, q, r) = (ds("w", "cyc.p"), ds("w", "cyc.q"), ds("w", "cyc.r"));
    cp.emit(edge(p.clone(), q.clone())).await.unwrap();
    cp.emit(edge(q.clone(), r.clone())).await.unwrap();
    cp.emit(edge(r.clone(), p.clone())).await.unwrap();
    assert_eq!(
        set(cp
            .upstream(&p, LINEAGE_MAX_DEPTH, PageReq::unbounded())
            .await
            .unwrap()),
        [q.clone(), r.clone()].into_iter().collect(),
        "cyclic upstream terminates; seed P excluded"
    );
}

/// Contract: cursor pagination on dataset closure and `events_for`. Run against
/// every `Lineage` adapter.
pub async fn lineage_pagination_contract<CP: Lineage>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef {
        namespace: ns.to_string(),
        name: n.to_string(),
    };
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

    // fan-out: 7 inputs each feeding one output Z (one event apiece)
    let z = ds("w", "pag.z");
    let inputs: Vec<DatasetRef> = (0..7).map(|i| ds("w", &format!("pag.in{i:02}"))).collect();
    for inp in &inputs {
        cp.emit(LineageEvent {
            run_id: RunId(uuid::Uuid::new_v4()),
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![inp.clone()],
            outputs: vec![z.clone()],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
    }

    // page upstream(Z) in pages of 3; every dataset exactly once, in sorted order
    let mut seen: Vec<DatasetRef> = Vec::new();
    let mut after: Option<control_plane_core::Cursor> = None;
    loop {
        let req = PageReq {
            after: after.clone(),
            limit: Some(3),
        };
        let page = cp.upstream(&z, 1, req).await.unwrap();
        assert!(page.items.len() <= 3, "page never exceeds limit");
        seen.extend(page.items.iter().cloned());
        match page.next {
            Some(cur) => after = Some(cur),
            None => break,
        }
    }
    let mut expected = inputs.clone();
    expected.sort();
    assert_eq!(
        seen, expected,
        "paged upstream returns every dataset once, in stable order"
    );

    // events_for pagination: a run with 5 events, pages of 2
    let run = RunId(uuid::Uuid::new_v4());
    for i in 0..5 {
        cp.emit(LineageEvent {
            run_id: run,
            event_type: EventType::Running,
            event_time: ts,
            inputs: vec![],
            outputs: vec![ds("w", &format!("ev.o{i}"))],
            payload: serde_json::json!({ "i": i }),
        })
        .await
        .unwrap();
    }
    let mut count = 0usize;
    let mut after: Option<control_plane_core::Cursor> = None;
    let ended_with_null;
    loop {
        let page = cp
            .events_for(
                &run,
                PageReq {
                    after: after.clone(),
                    limit: Some(2),
                },
            )
            .await
            .unwrap();
        assert!(page.items.len() <= 2, "events page never exceeds limit");
        count += page.items.len();
        match page.next {
            Some(cur) => after = Some(cur),
            None => {
                ended_with_null = true;
                break;
            }
        }
    }
    assert_eq!(
        count, 5,
        "all events returned across pages, none duplicated/dropped"
    );
    assert!(ended_with_null, "final page signals no next");
}

/// Contract: `events_for` hydrates every event's inputs/outputs completely
/// and in ordinal order, however the adapter batches the reads (pins the
/// per-event-N+1 → `event_id = any($1)` collapse). `cp` must be freshly empty.
pub async fn events_for_hydration_contract<CP: Lineage>(cp: &CP) {
    let ds = |n: &str| DatasetRef {
        namespace: "w".to_string(),
        name: n.to_string(),
    };
    let run = RunId(uuid::Uuid::new_v4());
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    for i in 0..3 {
        cp.emit(LineageEvent {
            run_id: run,
            event_type: EventType::Complete,
            event_time: ts,
            inputs: vec![ds(&format!("in{i}.a")), ds(&format!("in{i}.b"))],
            outputs: vec![ds(&format!("out{i}.a")), ds(&format!("out{i}.b"))],
            payload: serde_json::json!({ "i": i }),
        })
        .await
        .unwrap();
    }
    let page = cp.events_for(&run, PageReq::unbounded()).await.unwrap();
    assert_eq!(page.items.len(), 3, "all three events returned in order");
    for (i, e) in page.items.iter().enumerate() {
        assert_eq!(
            e.inputs,
            vec![ds(&format!("in{i}.a")), ds(&format!("in{i}.b"))],
            "event {i}: inputs hydrated in ordinal order"
        );
        assert_eq!(
            e.outputs,
            vec![ds(&format!("out{i}.a")), ds(&format!("out{i}.b"))],
            "event {i}: outputs hydrated in ordinal order"
        );
    }
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
            namespace: "warehouse".into(),
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

/// A commit that fails mid-apply must roll back EVERYTHING — queue, lineage, AND
/// catalog together — exactly like the postgres single `sqlx::Transaction`. We
/// stage an enqueue + emit alongside a compaction that will conflict (a named
/// expire target that is not live); the failed commit must leave neither the job
/// nor the event visible. Guards against a partial commit where queue/lineage were
/// applied before a later catalog op errored. See iss-memory-tx-not-atomic.
pub async fn tx_atomic_rollback_contract<CP: ControlPlane + Queue + Lineage>(cp: &CP) {
    use control_plane_core::{DataFile, FileFormat, TableRef};
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();
    let run = RunId(uuid::Uuid::new_v4());
    let kinds = vec!["tx-atomic".to_string()];

    let mut tx = cp.begin().await.expect("begin");
    tx.enqueue(NewJob {
        kind: "tx-atomic".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("staged enqueue");
    tx.emit(LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "warehouse".into(),
            name: "main.tx_atomic".into(),
        }],
        payload: serde_json::json!({}),
    })
    .await
    .expect("staged emit");
    // A compaction whose expire target is not live -> Conflict at commit time.
    tx.compact_files(
        &TableRef {
            schema: "main".into(),
            name: "never_created".into(),
        },
        &["ghost.parquet".into()],
        &[DataFile {
            path: "d.parquet".into(),
            path_is_relative: true,
            file_format: FileFormat::Parquet,
            record_count: 1,
            file_size_bytes: 16,
            column_stats: vec![],
            parquet_footer_size: Some(10),
        }],
    )
    .await
    .expect("staged compaction");

    let res = tx.commit().await;
    assert!(
        res.is_err(),
        "commit must fail when a staged compaction conflicts"
    );

    // Nothing from the failed commit is visible — no partial commit across concerns.
    assert!(
        cp.dequeue(&kinds, "reader").await.unwrap().is_none(),
        "a failed commit must not leave the staged job visible"
    );
    assert!(
        cp.events_for(&run, PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "a failed commit must not leave the staged event visible"
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
    // catalog reads are covered by catalog_contract; binding the ref here keeps this
    // contract catalog-independent so the postgres facade test runs postgres-only.
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
    use control_plane_core::{ColumnSpec, DataFile, FileFormat, PageReq, TableRef};
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
            ty: "long".into(),
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
            file_format: FileFormat::Parquet,
            record_count: 3,
            file_size_bytes: 48,
            column_stats: vec![],
            parquet_footer_size: Some(10),
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
            namespace: "warehouse".into(),
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
            ty: "long".into(),
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

/// Contract for `Tx::replace_files` (overwrite). Append a file, then replace it: the
/// current snapshot lists ONLY the replacement (stats reflect the new file, not the
/// sum), and the prior snapshot still time-travels to the original file. `cp` must be
/// freshly empty.
pub async fn snapshot_replace_contract<C>(cp: &C)
where
    C: control_plane_core::ControlPlane + control_plane_core::Catalog,
{
    use control_plane_core::{ColumnSpec, DataFile, FileFormat, PageReq, TableRef};
    let t = TableRef {
        schema: "main".into(),
        name: "replace_me".into(),
    };
    let file = |path: &str, rows: i64| DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![],
        parquet_footer_size: Some(10),
    };

    // create + append a.parquet (3 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        }],
    )
    .await
    .unwrap();
    tx.append_files(&t, &[file("a.parquet", 3)]).await.unwrap();
    let s1 = tx
        .commit()
        .await
        .unwrap()
        .expect("append yields a snapshot");

    let at1 = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(at1.len(), 1, "one file live after append");
    assert_eq!(at1.items[0].path, "a.parquet");

    // replace with b.parquet (5 rows).
    let mut tx = cp.begin().await.unwrap();
    tx.replace_files(&t, &[file("b.parquet", 5)]).await.unwrap();
    let s2 = tx
        .commit()
        .await
        .unwrap()
        .expect("replace yields a snapshot");

    // current snapshot lists ONLY the replacement.
    let cur = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cur.id, s2, "replace advanced the current snapshot");
    let at2 = cp.files(&t, s2, PageReq::unbounded()).await.unwrap();
    assert_eq!(at2.len(), 1, "replace leaves exactly the new file live");
    assert_eq!(at2.items[0].path, "b.parquet");
    assert_eq!(
        at2.items[0].record_count, 5,
        "stats reflect the new file only"
    );

    // time travel: the prior snapshot still lists the original file.
    let back = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(
        back.len(),
        1,
        "prior snapshot retains its file (time travel)"
    );
    assert_eq!(back.items[0].path, "a.parquet");
}

/// Contract for `Tx::compact_files` (selective compaction). Append three files, then
/// compact two of them into one: the current snapshot lists the untouched file plus the
/// coalesced one (the two compacted files expired), the total record_count is preserved,
/// and the prior snapshot still time-travels to all three originals. `cp` must be freshly
/// empty.
pub async fn snapshot_compact_contract<C>(cp: &C)
where
    C: control_plane_core::ControlPlane + control_plane_core::Catalog,
{
    use control_plane_core::{ColumnSpec, DataFile, FileFormat, PageReq, TableRef};
    let t = TableRef {
        schema: "main".into(),
        name: "compact_me".into(),
    };
    let file = |path: &str, rows: i64| DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![],
        parquet_footer_size: Some(10),
    };

    // create + append a(3) + b(5) + c(2) as three files.
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(
        &t,
        &[ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        }],
    )
    .await
    .unwrap();
    tx.append_files(
        &t,
        &[
            file("a.parquet", 3),
            file("b.parquet", 5),
            file("c.parquet", 2),
        ],
    )
    .await
    .unwrap();
    let s1 = tx
        .commit()
        .await
        .unwrap()
        .expect("append yields a snapshot");

    let at1 = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    assert_eq!(at1.len(), 3, "three files live after append");
    let total1: i64 = at1.items.iter().map(|f| f.record_count).sum();
    assert_eq!(total1, 10);

    // compact a + b into d(8); c untouched.
    let mut tx = cp.begin().await.unwrap();
    tx.compact_files(
        &t,
        &["a.parquet".into(), "b.parquet".into()],
        &[file("d.parquet", 8)],
    )
    .await
    .unwrap();
    let s2 = tx
        .commit()
        .await
        .unwrap()
        .expect("compact yields a snapshot");

    // current snapshot lists c + d only; record_count preserved.
    let cur = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cur.id, s2, "compact advanced the current snapshot");
    let at2 = cp.files(&t, s2, PageReq::unbounded()).await.unwrap();
    let mut paths: Vec<&str> = at2.items.iter().map(|f| f.path.as_str()).collect();
    paths.sort();
    assert_eq!(
        paths,
        vec!["c.parquet", "d.parquet"],
        "a,b expired; d added; c untouched"
    );
    let total2: i64 = at2.items.iter().map(|f| f.record_count).sum();
    assert_eq!(total2, 10, "compaction preserves the row set");

    // time travel: the prior snapshot still lists all three originals.
    let back = cp.files(&t, s1, PageReq::unbounded()).await.unwrap();
    let mut bpaths: Vec<&str> = back.items.iter().map(|f| f.path.as_str()).collect();
    bpaths.sort();
    assert_eq!(bpaths, vec!["a.parquet", "b.parquet", "c.parquet"]);
}

/// Contract: `define_type` emits a type↔table *binding edge* so a provenance walk
/// crosses the type↔table seam. The physical table is upstream of the type. The edge
/// is an ordinary `LineageEvent`, so the closure needs no changes. Run against every
/// adapter implementing `Ontology + Lineage`.
pub async fn type_table_binding_contract<CP: Ontology + Lineage>(cp: &CP) {
    let ds = |ns: &str, n: &str| DatasetRef {
        namespace: ns.to_string(),
        name: n.to_string(),
    };
    let set = |p: Page<DatasetRef>| p.into_iter().collect::<HashSet<_>>();
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let edge = |inp: DatasetRef, out: DatasetRef| LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: ts,
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    };
    let otype = |name: &str, schema: &str, table: &str| ObjectType {
        name: TypeName(name.into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: schema.into(),
            name: table.into(),
        },
        identity: None,
    };

    // define type X bound to main.customers -> emits binding edge {customers -> type/X}
    cp.define_type(otype("Bnd_X", "main", "bnd_customers"))
        .await
        .unwrap();

    let table_ref = ds("loom", "main.bnd_customers");
    let type_x = ds("loom:type", "Bnd_X");
    let type_y = ds("loom:type", "Bnd_Y");
    let s3 = ds("s3://raw", "bnd_src");

    // ingest edge s3 -> loom/main.bnd_customers ; transform edge type/X -> type/Y
    cp.emit(edge(s3.clone(), table_ref.clone())).await.unwrap();
    cp.emit(edge(type_x.clone(), type_y.clone())).await.unwrap();

    // 1. crosses the seam: upstream(Y, depth=3) reaches X, the backing table, and s3.
    let up_y3 = set(cp.upstream(&type_y, 3, PageReq::unbounded()).await.unwrap());
    assert!(up_y3.contains(&type_x), "upstream(Y) reaches X");
    assert!(
        up_y3.contains(&table_ref),
        "upstream(Y) crosses the binding to the backing table"
    );
    assert!(
        up_y3.contains(&s3),
        "upstream(Y) reaches the table's ingest source"
    );
    // downstream(table, depth=2) reaches the type and its typed descendant.
    let down_t2 = set(cp
        .downstream(&table_ref, 2, PageReq::unbounded())
        .await
        .unwrap());
    assert!(
        down_t2.contains(&type_x),
        "downstream(table) reaches the type"
    );
    assert!(down_t2.contains(&type_y), "downstream(table) reaches Y");

    // 4. Direction: table is upstream of type; type is downstream of table (one hop).
    let up_x1 = set(cp.upstream(&type_x, 1, PageReq::unbounded()).await.unwrap());
    assert_eq!(
        up_x1,
        [table_ref.clone()].into_iter().collect(),
        "table is the sole one-hop upstream of the type"
    );
    let down_t1 = set(cp
        .downstream(&table_ref, 1, PageReq::unbounded())
        .await
        .unwrap());
    assert!(
        down_t1.contains(&type_x),
        "type is downstream of the table (one hop)"
    );

    // 5. Depth accounting: the seam costs one hop.
    let up_y1 = set(cp.upstream(&type_y, 1, PageReq::unbounded()).await.unwrap());
    assert_eq!(
        up_y1,
        [type_x.clone()].into_iter().collect(),
        "depth=1 reaches only X — the seam is not yet crossed"
    );
    let up_y2 = set(cp.upstream(&type_y, 2, PageReq::unbounded()).await.unwrap());
    assert!(
        up_y2.contains(&table_ref),
        "depth=2 crosses the seam to the backing table"
    );

    // 2. Idempotent re-define: re-defining X->same table adds no spurious upstream;
    //    the type stays single-sourced to exactly its backing table. (Row-level
    //    exactly-once is pinned by the postgres guard-count test in Task 3, since the
    //    read is set-deduplicated and cannot count duplicate identical edges.)
    cp.define_type(otype("Bnd_X", "main", "bnd_customers"))
        .await
        .unwrap();
    let up_x_redef = set(cp.upstream(&type_x, 1, PageReq::unbounded()).await.unwrap());
    assert_eq!(
        up_x_redef,
        [table_ref.clone()].into_iter().collect(),
        "redundant re-define leaves the type single-sourced to its table"
    );

    // 3. Rebind to a NEW table appends a second edge (append-only history); the old
    //    binding is retained.
    cp.define_type(otype("Bnd_X", "main", "bnd_customers_v2"))
        .await
        .unwrap();
    let table_ref_v2 = ds("loom", "main.bnd_customers_v2");
    let up_x_rebind = set(cp.upstream(&type_x, 1, PageReq::unbounded()).await.unwrap());
    assert!(
        up_x_rebind.contains(&table_ref),
        "rebind retains the original binding (append-only)"
    );
    assert!(
        up_x_rebind.contains(&table_ref_v2),
        "rebind adds the new backing table as an upstream"
    );
}
