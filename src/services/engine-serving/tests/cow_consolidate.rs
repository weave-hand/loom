//! Tombstone-aware copy-on-write consolidation (scalable COW, slice 2).
//!
//! A shadow-bearing non-CDC identity table accumulates an inline tier — plain
//! appends, `+U` version rows, and `-D` tombstones — on top of its Parquet base.
//! `consolidate_table` folds `files ∪ inline` under `Precedence::Snapshot`
//! (greatest `begin_snapshot` per identity wins; a tombstoned winner drops the
//! identity), materializes that merge-on-read view as a NEW base snapshot, retires
//! exactly the consumed inline rows in the same commit, and re-enables the
//! suppressed flush by clearing the quiescent `has_shadow` flag.
//!
//! The correctness invariant these tests pin: the fold IS the read, materialized —
//! a merge-on-read result BEFORE consolidation must equal the result AFTER,
//! row-set-wise, because the fold writes exactly what `build_merge_view` serves.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, LineageEvent, ObjectType, Ontology, PropertyDef, RunId, SnapshotId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::{has_shadow, write_inline_delta};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::read_files_as_batches;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use uuid::Uuid;

/// The `(id, name)` logical schema shared by every case, in mirror column order.
/// `name` is NULLABLE — a `-D` tombstone writes every non-id column NULL, so a
/// DELETE-able identity table must declare its non-id columns nullable (as the
/// real e2e types do with `required: false`).
fn cols() -> Vec<(String, String, bool)> {
    vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), true),
    ]
}

/// Full data specs `[id long, name string?]` — the `columns` arg for a `+U` version write.
fn full_specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

/// Just the id spec `[id long]` — the `columns` arg for a `-D` tombstone write.
fn id_specs() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A one-row `(id, name)` batch (the post-PATCH image for a `+U` version write).
fn version_batch(id: i64, name: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name])),
        ],
    )
    .expect("version batch")
}

/// A one-cell `(id)` batch (the tombstone target for a `-D` delete write).
fn id_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id batch")
}

fn lineage() -> LineageEvent {
    LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "cow-consolidate-test" }),
    }
}

/// Define `(schema.name)` as an ontology type with the given identity column, so
/// `identity_for_table` resolves (or not) during the consolidation dispatch.
async fn define_type(cp: &PgControlPlane, schema: &str, name: &str, identity: Option<&str>) {
    cp.define_type(ObjectType {
        name: TypeName(format!("Type_{schema}_{name}")),
        table: TableRef {
            schema: schema.into(),
            name: name.into(),
        },
        identity: identity.map(str::to_string),
        version: None,
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
    })
    .await
    .expect("define_type");
}

/// A fully-seeded COW table: file rows 1..=4 landed as Parquet, an identity type
/// on `id`, then an inline `+U` UPDATE id=2, a `-D` DELETE id=3, and a plain
/// append id=5. Returns the pieces plus the file-seed snapshot (for time travel).
struct Seeded {
    cp: PgControlPlane,
    pool: sqlx::PgPool,
    writer: IcebergWriter,
    table: TableRef,
    file_seed_snap: i64,
}

async fn seed(fx: &PgFixture, schema: &str, name: &str) -> Seeded {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let table = TableRef {
        schema: schema.into(),
        name: name.into(),
    };

    // File rows {1:"one", 2:"two", 3:"three", 4:"four"} → real Parquet.
    let file_seed_snap = writer
        .seed_arrays(
            schema,
            name,
            &cols(),
            &[
                SeedCol::Long(vec![1, 2, 3, 4]),
                SeedCol::Str(vec!["one", "two", "three", "four"]),
            ],
        )
        .await;
    define_type(&cp, schema, name, Some("id")).await;

    // Inline tier: UPDATE id=2 (+U), DELETE id=3 (-D tombstone), append id=5 (+I).
    // Each id has no prior inline row, so the CAS witness is version 0.
    write_inline_delta(
        &pool,
        &table,
        &full_specs(),
        "id",
        false,
        &version_batch(2, "two-v2"),
        None,
        lineage(),
        0,
        None,
        &[],
    )
    .await
    .expect("update id=2");
    write_inline_delta(
        &pool,
        &table,
        &id_specs(),
        "id",
        true,
        &id_batch(3),
        None,
        lineage(),
        0,
        None,
        &[],
    )
    .await
    .expect("delete id=3");
    writer
        .inline(schema, name, &cols(), &[(5, "five")], Uuid::new_v4())
        .await;

    Seeded {
        cp,
        pool,
        writer,
        table,
        file_seed_snap,
    }
}

/// Flatten result batches into sorted `(id, name)` pairs.
fn rows_sorted(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id Int64");
        let names = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name Utf8");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out.sort();
    out
}

/// Merge-on-read result for `table` at `at` (None = live), as sorted `(id, name)`.
async fn merged_at(
    catalog: &IcebergCatalog,
    table: &TableRef,
    at: Option<SnapshotId>,
) -> Vec<(i64, String)> {
    let ctx = SessionContext::new();
    let provider = engine_serving::build_serving_provider(&ctx, catalog, table, None, at)
        .await
        .expect("build_serving_provider")
        .expect("provider present");
    let df = ctx.read_table(provider).expect("read_table");
    let batches = df.collect().await.expect("collect");
    rows_sorted(&batches)
}

// ---------------------------------------------------------------------------
// Case 1 — fold correctness: the NEW base holds exactly the merged survivors,
//          user columns only, no duplicates, id=3 (tombstoned) absent.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fold_materializes_the_merge_view() {
    let fx = PgFixture::shared();
    let s = seed(fx, "wh", "items").await;
    let catalog = IcebergCatalog::new(s.pool.clone());
    let sql_catalog = s.writer.sql_catalog().await;

    let snap = engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");
    assert!(
        snap > 0,
        "consolidation returns a real new snapshot id, got {snap}"
    );

    // Read the NEW base's files DIRECTLY (bypassing merge-on-read) — the fold's
    // materialized output, not a re-merge.
    let files = catalog
        .files_with_stats(&s.table, SnapshotId(snap))
        .await
        .expect("files_with_stats");
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    assert!(
        !paths.is_empty(),
        "the fold wrote at least one base Parquet file"
    );
    let (schema, batches) = read_files_as_batches(&sql_catalog, &s.table, &paths)
        .await
        .expect("read_files_as_batches");

    // User columns only — no framing/shadow columns leaked into the base Parquet.
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "name"],
        "base Parquet carries only user columns"
    );

    // Exactly {1 original, 2 updated image, 4 original, 5 appended}; id=3 dropped.
    assert_eq!(
        rows_sorted(&batches),
        vec![
            (1, "one".to_string()),
            (2, "two-v2".to_string()),
            (4, "four".to_string()),
            (5, "five".to_string()),
        ],
        "latest-begin_snapshot-per-identity wins; the tombstoned id=3 is gone"
    );
}

// ---------------------------------------------------------------------------
// Case 2 — the consumed inline rows are end-capped at the consolidation snapshot.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_rows_are_consumed() {
    let fx = PgFixture::shared();
    let s = seed(fx, "wh", "items2").await;
    let catalog = IcebergCatalog::new(s.pool.clone());
    let sql_catalog = s.writer.sql_catalog().await;

    let snap = engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");

    // No live inline row survives at the consolidation snapshot.
    let live = catalog
        .inline_live_batch(&s.table, SnapshotId(snap))
        .await
        .expect("inline_live_batch");
    assert!(
        live.is_none(),
        "all folded inline rows are retired, got {live:?}"
    );
}

// ---------------------------------------------------------------------------
// Case 3 — the quiescent flag is cleared (flush re-enabled).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_flag_cleared() {
    let fx = PgFixture::shared();
    let s = seed(fx, "wh", "items3").await;
    let sql_catalog = s.writer.sql_catalog().await;

    let mut conn = s.pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &s.table.schema, &s.table.name)
        .await
        .expect("live_table_id")
        .expect("tid");
    assert!(
        has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "shadow set before"
    );
    drop(conn);

    engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");

    let mut conn = s.pool.acquire().await.expect("acquire");
    assert!(
        !has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "shadow flag cleared after a quiescent consolidation"
    );
}

// ---------------------------------------------------------------------------
// Case 4 — merge-view equivalence: the merged read is identical before and after.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_view_equivalent_before_and_after() {
    let fx = PgFixture::shared();
    let s = seed(fx, "wh", "items4").await;
    let catalog = IcebergCatalog::new(s.pool.clone());
    let sql_catalog = s.writer.sql_catalog().await;

    let before = merged_at(&catalog, &s.table, None).await;
    assert_eq!(
        before,
        vec![
            (1, "one".to_string()),
            (2, "two-v2".to_string()),
            (4, "four".to_string()),
            (5, "five".to_string()),
        ],
        "sanity: the pre-consolidation merge view"
    );

    engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");

    let after = merged_at(&catalog, &s.table, None).await;
    assert_eq!(before, after, "reads are byte-identical by construction");
}

// ---------------------------------------------------------------------------
// Case 5 — time travel: an as-of read before the deltas still sees the originals.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_preserves_originals() {
    let fx = PgFixture::shared();
    let s = seed(fx, "wh", "items5").await;
    let catalog = IcebergCatalog::new(s.pool.clone());
    let sql_catalog = s.writer.sql_catalog().await;

    engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");

    // As-of the file-seed snapshot (before any inline delta): the ORIGINAL rows,
    // with id=2 == "two" and id=3 still present — the overwrite preserved history.
    let historical = merged_at(&catalog, &s.table, Some(SnapshotId(s.file_seed_snap))).await;
    assert_eq!(
        historical,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
            (4, "four".to_string()),
        ],
        "time-travel to the pre-delta base returns the originals"
    );
}

// ---------------------------------------------------------------------------
// Case 6 — no-op arm: an identity-less table returns 0 (never touched).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_less_table_is_a_noop() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let table = TableRef {
        schema: "wh".into(),
        name: "noident".into(),
    };
    writer
        .seed_arrays(
            "wh",
            "noident",
            &cols(),
            &[SeedCol::Long(vec![1]), SeedCol::Str(vec!["one"])],
        )
        .await;
    // Type with NO identity → the COW dispatch arm short-circuits to 0.
    define_type(&cp, "wh", "noident", None).await;
    let sql_catalog = writer.sql_catalog().await;

    let snap = engine_serving::consolidate_table(&cp, &sql_catalog, &pool, &table)
        .await
        .expect("consolidate_table");
    assert_eq!(snap, 0, "an identity-less table is a no-op");
}

// ---------------------------------------------------------------------------
// Case 7 — a residual delta keeps the flag: a new mutation after consolidation
//          re-arms the shadow flag, and a second consolidation quiesces it.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn residual_delta_keeps_the_flag() {
    let fx = PgFixture::shared();
    let s = seed(fx, "wh", "items7").await;
    let sql_catalog = s.writer.sql_catalog().await;

    let mut conn = s.pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &s.table.schema, &s.table.name)
        .await
        .expect("live_table_id")
        .expect("tid");
    drop(conn);

    // First consolidation quiesces the flag.
    engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("first consolidate");
    let mut conn = s.pool.acquire().await.expect("acquire");
    assert!(
        !has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "flag clear after first"
    );
    drop(conn);

    // A fresh UPDATE (id=1 has no live inline row post-fold → CAS witness 0)
    // re-arms the shadow flag.
    write_inline_delta(
        &s.pool,
        &s.table,
        &full_specs(),
        "id",
        false,
        &version_batch(1, "one-v2"),
        None,
        lineage(),
        0,
        None,
        &[],
    )
    .await
    .expect("residual update id=1");
    let mut conn = s.pool.acquire().await.expect("acquire");
    assert!(
        has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "the residual delta set the shadow flag again"
    );
    drop(conn);

    // A second consolidation drains the residual and quiesces the flag again.
    engine_serving::consolidate_table(&s.cp, &sql_catalog, &s.pool, &s.table)
        .await
        .expect("second consolidate");
    let mut conn = s.pool.acquire().await.expect("acquire");
    assert!(
        !has_shadow(&mut conn, tid).await.expect("has_shadow"),
        "the second consolidation quiesces the flag"
    );
}
