//! **ACL enforcement survives the LiquidCache serving path.**
//!
//! This is the safety test for `engine_serving::liquid_cache`. It exists because the
//! cached session is NOT confined to ungoverned queries, contrary to what the cache
//! module's first draft claimed: `read_object` compiles the subject's ACL row filter and
//! column projection into SQL text in query-api, then ships it as a plain
//! `CommandStatementQuery`, which the engine runs through `execute_query_stream` — i.e.
//! through exactly the `SessionContext` [`engine_serving::liquid_cache::new_session`]
//! hands out. So every governed object read lands on the cache when it is mounted.
//!
//! That is sound in principle — LiquidCache caches *scanned file data*, keyed per file,
//! not query results, so a shared cache entry is no more revealing than the shared
//! Parquet file underneath it. But "in principle" is not the standard for the governance
//! boundary, and the property is cheap to assert directly, so this test asserts it:
//!
//! Two subjects with **disjoint** row filters read the **same** table, in the **same**
//! process, over the **same** mounted cache, and are then interleaved so the later reads
//! are served warm. Each must see only its own rows, every time. A cache that memoized a
//! filtered result, or keyed an entry by file while forgetting the predicate, fails at
//! step 4 or 5 — the first warm read after the other subject has populated the cache.
//!
//! Deliberately a separate `rust_test` target: [`engine_serving::liquid_cache::configure`]
//! installs a process-lifetime `OnceCell`, so a binary that mounts the cache mounts it for
//! every test in that binary. Keeping it alone makes "cache on" unambiguous.
//!
//! # Where the cache cannot mount
//!
//! LiquidCache's `t4` store enables its `io-uring` feature by default, so the mount fails
//! with `ENOSYS` wherever the syscall is blocked — which includes the BuildBuddy RE workers
//! that `buck2 test` routes fixture runs to from a root host, and the container seccomp
//! profiles common in Kubernetes. This test does **not** skip there. It asserts the same
//! ACL properties against the uncached path and records which mode it ran in, so the
//! governance assertions always execute and only the cache-specific coverage is conditional.
//! Any mount failure that is *not* the missing syscall fails the test outright.

use control_plane_core::{
    Acl, Action, CompareOp, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget,
    RoleId, RowFilter, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::handler::{ObjectQuery, QueryDeps, QueryError, Subject, read_object};
use query_api::serving::SqlValue;

/// Announce, in the test log, that the cache could not mount and the run is proceeding
/// against the uncached path. A separate fn because the lint exemption has to sit on an
/// item — an attribute on a bare macro invocation is silently ignored.
#[expect(
    clippy::print_stderr,
    reason = "which mode this test ran in must be visible in the test log; a silent \
              downgrade to the uncached path is exactly what would make a green run \
              misleading"
)]
fn note_running_uncached(e: &dyn std::fmt::Display) {
    eprintln!(
        "NOTE: liquid cache could not mount (io_uring unavailable: {e}); running the ACL \
         assertions against the UNCACHED path instead"
    );
}

/// Read every `Order` visible to `subject`, returning the ids in ascending order.
/// Panics on an ACL denial — the callers that expect one assert on the error directly.
async fn visible_ids(deps: &QueryDeps<'_>, subject: &SubjectId) -> Vec<i64> {
    let rows = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subject.clone()),
        deps,
    )
    .await
    .unwrap_or_else(|e| panic!("read_object for {subject:?} failed: {e:?}"));
    // `secret` is denied for both subjects, so the projection must never carry it —
    // asserted here rather than in each caller so every read in this test checks it.
    assert!(
        !rows.columns.iter().any(|c| c == "secret"),
        "denied column leaked into the projection for {subject:?}: {:?}",
        rows.columns
    );
    let mut ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match r[0] {
            SqlValue::Int(i) => i,
            ref other => panic!("identity was not an integer: {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Grant `role` coarse `Read` on `Order`, then narrow it to `region = <region>` and
/// deny the `secret` column.
async fn grant_region(cp: &control_plane_postgres::PgControlPlane, role: &RoleId, region: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text(region.into()),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_row_filters_hold_across_subjects_on_a_shared_liquid_cache() {
    let cache_dir = tempfile::tempdir().expect("tempdir for the liquid cache");
    // Mount the cache FIRST: `execute_query_stream` reads the `OnceCell` per query, so a
    // late mount would leave the early reads uncached and quietly weaken the test.
    let cached = match engine_serving::liquid_cache::configure(&engine_serving::LiquidCacheConfig {
        cache_dir: cache_dir.path().to_path_buf(),
        max_memory_bytes: 64 * 1024 * 1024,
    })
    .await
    {
        Ok(()) => true,
        // `t4`'s io_uring mount on a host/sandbox where the syscall is blocked. Narrow on
        // purpose: only this errno is tolerated, and the run continues uncached rather than
        // skipping, so the ACL assertions below still execute. See the module note.
        Err(e) if e.to_string().contains("os error 38") => {
            note_running_uncached(&e);
            false
        }
        Err(e) => panic!("liquid cache failed to mount for a reason other than io_uring: {e}"),
    };
    assert_eq!(
        engine_serving::liquid_cache::is_enabled(),
        cached,
        "is_enabled() must agree with whether the mount actually succeeded"
    );

    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let table = TableRef {
        schema: "main".into(),
        name: "orders".into(),
    };

    // Seed one Iceberg table both subjects read: ids 1..=4, two per region, each row
    // carrying a `secret` neither subject may project.
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("region".to_string(), "string".to_string(), true),
        ("secret".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3, 4]),
                SeedCol::Str(vec!["eu", "us", "eu", "us"]),
                SeedCol::Str(vec!["s1", "s2", "s3", "s4"]),
            ],
        )
        .await;

    cp.define_type(
        ObjectType::build("Order", (table.schema.clone(), table.name.clone()))
            .prop_req("id", "Long")
            .prop("region", "String")
            .prop("secret", "String")
            .done(),
    )
    .await
    .unwrap();

    // Two subjects, disjoint row filters over the same rows.
    let eu = SubjectId("eu-analyst".into());
    let us = SubjectId("us-analyst".into());
    let eu_role = RoleId("eu-analysts".into());
    let us_role = RoleId("us-analysts".into());
    for (subj, role) in [(&eu, &eu_role), (&us, &us_role)] {
        cp.define_subject(subj).await.unwrap();
        cp.define_role(role).await.unwrap();
        cp.assign_role(subj, role).await.unwrap();
    }
    grant_region(&cp, &eu_role, "eu").await;
    grant_region(&cp, &us_role, "us").await;

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &eng,
        default_limit: 1000,
        gc_retention: e2e_support::TEST_GC_RETENTION,
    };

    // 1-2. Cold reads: each subject populates the cache for the shared data files.
    // (Uncached, these are simply the first two reads; the assertions are unchanged.)
    assert_eq!(visible_ids(&deps, &eu).await, vec![1, 3], "cold eu read");
    assert_eq!(visible_ids(&deps, &us).await, vec![2, 4], "cold us read");

    // 3-5. Warm, interleaved. Each of these is served from cache entries that the OTHER
    // subject's scan helped populate. This is the assertion that matters: a cache that
    // leaked across subjects shows up here and nowhere earlier.
    assert_eq!(
        visible_ids(&deps, &eu).await,
        vec![1, 3],
        "warm eu read after the us scan populated the cache"
    );
    assert_eq!(
        visible_ids(&deps, &us).await,
        vec![2, 4],
        "warm us read after the eu scan re-read the same files"
    );
    assert_eq!(
        visible_ids(&deps, &eu).await,
        vec![1, 3],
        "eu must be stable across repeated warm reads"
    );

    // 6. A request filter still narrows WITHIN the ACL filter, warm — it must not be able
    // to reach a row the row filter excludes.
    let narrowed = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![("id".into(), "2".into())],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(eu.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert!(
        narrowed.rows.is_empty(),
        "eu asked for id=2 (a us row); the ACL row filter must win over the request filter"
    );

    // 7. The read gate is still deny-by-default with the cache warm: an ungranted subject
    // is refused before any scan, so a populated cache is never a way around the gate.
    let err = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(SubjectId("stranger".into())),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "ungranted subject must be denied even with the cache warm, got {err:?}"
    );
}
