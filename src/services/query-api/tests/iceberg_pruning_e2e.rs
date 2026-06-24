//! End-to-end: a governed read over a file-backed Iceberg table prunes the files a
//! predicate provably cannot match (via `IcebergMirrorTableProvider`), the served rows
//! are exactly the matching ones, and pruning never changes the governed result set.
//! loom_fixture_test (Postgres + LocalFsStorage; no DuckDB).

use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use engine_serving::{IcebergMirrorTableProvider, prune_files, register_iceberg_table};
use query_api::serving::SqlValue;
use query_api::serving_datafusion::batches_to_rows;

/// Seed `"s"."t"` spanning TWO files with disjoint `id` ranges, register it, and prove
/// the pruning path: correctness, an actual file skip, and governance invariance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pruning_skips_files_and_preserves_governed_results() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // Two appends -> two Parquet files with DISJOINT id ranges.
    //   file A: id in [1, 3]   file B: id in [100, 102]
    writer
        .seed_arrays(
            "s",
            "t",
            &cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["a", "b", "c"]),
            ],
        )
        .await;
    writer
        .seed_arrays(
            "s",
            "t",
            &cols,
            &[
                SeedCol::Long(vec![100, 101, 102]),
                SeedCol::Str(vec!["x", "y", "z"]),
            ],
        )
        .await;

    let catalog = IcebergCatalog::new(pool);
    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    // --- (1) Correctness: a predicate only file A satisfies returns exactly A's row.
    let v: i64 = 2; // lives only in file A ([1,3]); never in file B ([100,102]).
    let ctx = SessionContext::new();
    register_iceberg_table(&ctx, &catalog, &table, None)
        .await
        .expect("register");
    let df = ctx
        .sql("SELECT id FROM \"s\".\"t\" WHERE id = 2")
        .await
        .expect("sql");
    let rows = batches_to_rows(df.collect().await.expect("collect"));
    assert_eq!(rows.columns, vec!["id".to_string()]);
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(2)]],
        "exactly file A's matching row comes back"
    );

    // --- (2) Pruning happened: build the provider over the mirror stats and assert
    // prune_files drops file B (deterministic; off fragile DataFusion metric APIs).
    let snap = catalog.current_snapshot(&table).await.expect("snapshot");
    let files = catalog
        .files_with_stats(&table, snap.id)
        .await
        .expect("files_with_stats");
    assert_eq!(files.len(), 2, "two appends -> two data files");
    let provider = IcebergMirrorTableProvider::try_new(&ctx, files.clone())
        .await
        .expect("provider");
    let schema = provider.schema();
    let kept = prune_files(&schema, &[col("id").eq(lit(v))], &files);
    assert_eq!(kept.len(), 1, "exactly one file survives id = 2");
    // The survivor is file A: its id range covers 2; file B's does not.
    let kept_stats = &kept[0]
        .column_stats
        .iter()
        .find(|c| c.column_name == "id")
        .expect("id stats on survivor");
    let min = match kept_stats.min.as_ref() {
        Some(control_plane_core::snapshot::StatValue::I64(n)) => *n,
        other => panic!("expected I64 min, got {other:?}"),
    };
    let max = match kept_stats.max.as_ref() {
        Some(control_plane_core::snapshot::StatValue::I64(n)) => *n,
        other => panic!("expected I64 max, got {other:?}"),
    };
    assert!(
        min <= v && v <= max,
        "survivor is file A: id range [{min},{max}] contains {v}"
    );

    // --- (2b) Execution-layer proof: the plan `scan()` actually builds reads ONLY the
    // survivor file. This guards the headline file-skip claim at the layer that matters
    // — a regression where `scan` stopped pruning (or scanned all files) would still
    // return correct rows (filters are re-applied per row) and pass every assertion
    // above, but would render both files here. We assert against the executed
    // `DataSourceExec`'s `file_groups`, not fragile post-run metric APIs.
    let plan = provider
        .scan(&ctx.state(), None, &[col("id").eq(lit(v))], None)
        .await
        .expect("scan");
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let basename = |p: &str| {
        std::path::Path::new(p)
            .file_name()
            .and_then(|n| n.to_str())
            .expect("file basename")
            .to_string()
    };
    let survivor = basename(&kept[0].path);
    let dropped = basename(
        &files
            .iter()
            .find(|f| f.path != kept[0].path)
            .expect("a dropped file exists")
            .path,
    );
    assert!(
        rendered.contains(&survivor),
        "executed plan reads the survivor file ({survivor}):\n{rendered}"
    );
    assert!(
        !rendered.contains(&dropped),
        "executed plan must NOT read the pruned file ({dropped}):\n{rendered}"
    );

    // --- (3) Governance invariance: an extra ANDed caller filter (as the governed
    // compile would inject) yields an identical result set to the ungoverned query.
    let governed = ctx
        .sql("SELECT id FROM \"s\".\"t\" WHERE id = 2 AND id > 0")
        .await
        .expect("governed sql");
    let governed_rows = batches_to_rows(governed.collect().await.expect("governed collect"));
    assert_eq!(
        governed_rows.rows, rows.rows,
        "pruning never changes the governed result set"
    );

    // Keep the writer (its warehouse TempDir holds the files) alive until here.
    drop(writer);
}
