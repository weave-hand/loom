//! Writing via iceberg_writer populates the loom mirror atomically with the
//! Iceberg pointer, so the slice-1 IcebergCatalog read path round-trips it; and
//! concurrent appends leave the mirror consistent (one snapshot row per commit,
//! every snapshot carrying its files — no orphans from rolled-back CAS attempts).
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use control_plane_core::{Catalog, PageReq, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::iceberg_writer::append_batches;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog as _, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

async fn create_t(catalog: &SqlCatalog, warehouse: &str) {
    let ns = NamespaceIdent::new("wh".to_string());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("ns");
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .expect("schema");
    catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("t".to_string())
                .location(format!("file://{warehouse}/wh/t"))
                .schema(schema)
                .build(),
        )
        .await
        .expect("create");
}

fn batch(catalog_schema: &iceberg::spec::Schema, ids: Vec<i64>) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("row{i}")).collect();
    RecordBatch::try_new(
        Arc::new(iceberg::arrow::schema_to_arrow_schema(catalog_schema).unwrap()),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(
                names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_round_trips_through_the_mirror() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &whs).await;
    create_t(&catalog, &whs).await;
    let table = catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("wh".into()),
            "t".into(),
        ))
        .await
        .expect("load");
    let cs = table.metadata().current_schema().clone();

    append_batches(&catalog, &table, vec![batch(&cs, vec![1, 2, 3])])
        .await
        .expect("append");

    let pool: PgPool = fx.pool_for(&db).await;
    let read = IcebergCatalog::new(pool);
    let tref = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let snap = read
        .current_snapshot(&tref)
        .await
        .expect("current_snapshot");
    let files = read
        .files(&tref, snap.id, PageReq::unbounded())
        .await
        .expect("files");
    assert_eq!(files.items.len(), 1, "one data file in the mirror");
    assert_eq!(files.items[0].record_count, 3);
    let schema = read.schema(&tref, snap.id).await.expect("schema");
    assert_eq!(schema.columns.len(), 2, "id + name projected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_unappended_table_succeeds_without_orphan_snapshot() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let catalog = make_catalog(fx.pg_dsn(&db), &whs).await;
    create_t(&catalog, &whs).await; // creates the table; never appended

    let ident = TableIdent::new(NamespaceIdent::new("wh".into()), "t".into());
    catalog.drop_table(&ident).await.expect("drop");
    assert!(
        !catalog.table_exists(&ident).await.expect("exists"),
        "an un-appended table must still be droppable"
    );

    let pool: PgPool = fx.pool_for(&db).await;
    let snaps: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.snapshot")
        .fetch_one(&pool)
        .await
        .expect("count snapshots");
    assert_eq!(
        snaps, 0,
        "no snapshot allocated when there is no mirror to end"
    );
}

/// Spawn `n` writers that each append one file to the same table concurrently,
/// then assert the mirror is consistent: exactly `n` snapshots, no orphan
/// snapshot (every snapshot carries its files), and exactly `n` data files
/// (none dropped, none duplicated). With the CAS-conflict retry in place this
/// holds even when writers collide on the pointer CAS.
async fn concurrent_appends_consistent(n: i64) {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let whs = wh.path().display().to_string();
    let setup = make_catalog(fx.pg_dsn(&db), &whs).await;
    create_t(&setup, &whs).await;
    let cs = setup
        .load_table(&TableIdent::new(
            NamespaceIdent::new("wh".into()),
            "t".into(),
        ))
        .await
        .expect("load")
        .metadata()
        .current_schema()
        .clone();

    let mut handles = Vec::new();
    for k in 0..n {
        let dsn = fx.pg_dsn(&db);
        let whs = whs.clone();
        let cs = cs.clone();
        handles.push(tokio::spawn(async move {
            let catalog = make_catalog(dsn, &whs).await;
            let table = catalog
                .load_table(&TableIdent::new(
                    NamespaceIdent::new("wh".into()),
                    "t".into(),
                ))
                .await
                .expect("load");
            append_batches(&catalog, &table, vec![batch(&cs, vec![k * 10, k * 10 + 1])])
                .await
                .expect("append");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    let pool: PgPool = fx.pool_for(&db).await;
    // Exactly n snapshots, each carrying at least one data file (no orphan snapshot
    // rows from a rolled-back CAS attempt), and all n files present (none dropped).
    let snap_count: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.snapshot")
        .fetch_one(&pool)
        .await
        .expect("count snapshots");
    assert_eq!(snap_count, n, "one snapshot per successful append");
    let orphans: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.snapshot s \
         where not exists (select 1 from iceberg_mirror.data_file f where f.begin_snapshot = s.snapshot_id)",
    )
    .fetch_one(&pool)
    .await
    .expect("orphan check");
    assert_eq!(orphans, 0, "no snapshot without its files");
    let files: i64 = sqlx::query_scalar("select count(*) from iceberg_mirror.data_file")
        .fetch_one(&pool)
        .await
        .expect("count files");
    assert_eq!(files, n, "all appended files present, none duplicated");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_keep_the_mirror_consistent() {
    // Baseline contention level; kept green before the retry existed.
    concurrent_appends_consistent(4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_tolerate_contention() {
    // N=8 went red without the CAS-conflict retry (lost pointer CAS, no re-drive)
    // and the bump was reverted. Green here is the proof the retry re-commits a
    // lost CAS. No minimum-retry-count assertion: contention is nondeterministic,
    // so the invariant set (n snapshots, no orphans, n files) is the robust proof.
    concurrent_appends_consistent(8).await;
}
