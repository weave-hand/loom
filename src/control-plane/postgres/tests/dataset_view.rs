//! Postgres passes the catalog view contract + adapter-specific guards
//! (base-drop protection, lineage edge emission).

use async_trait::async_trait;
use control_plane_core::{
    Catalog, CompareOp, ControlPlane, ControlPlaneError, DatasetId, PageReq, RowFilter,
    ScalarValue, SnapshotId, TableRef, ViewDef,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{mark_dropped, next_snapshot};
use control_plane_testkit::{CatalogSeed, SeedSpec, SeededSnapshot, catalog_view_contract};

struct IcebergSeeder {
    writer: IcebergWriter,
}

#[async_trait]
impl CatalogSeed for IcebergSeeder {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        let cols: Vec<(String, String, bool)> = spec
            .columns
            .into_iter()
            .map(|c| (c.name, c.ty, c.nullable))
            .collect();
        self.writer
            .seed(
                &spec.table.schema,
                &spec.table.name,
                &cols,
                &spec.row_batches,
            )
            .await
            .into_iter()
            .map(|s| SeededSnapshot {
                snapshot: SnapshotId(s),
                // One Parquet file per appended batch: the contract's batches are far below
                // RollingFileWriterBuilder's default roll size, so each append produces exactly
                // one rolling file. A batch large enough to roll would add more.
                files_added: 1,
            })
            .collect()
    }

    async fn drop_table(&self, table: &TableRef) -> SnapshotId {
        SnapshotId(self.writer.drop_table(&table.schema, &table.name).await)
    }
}

#[tokio::test]
async fn postgres_passes_catalog_view_contract() {
    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool().clone());
    let seeder = IcebergSeeder {
        writer: IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db)),
    };
    catalog_view_contract(&catalog, &seeder).await;
}

#[tokio::test]
async fn dropping_a_base_with_dependent_views_is_refused() {
    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool().clone());
    let writer = IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db));

    let base = TableRef {
        schema: "gov".into(),
        name: "customers".into(),
    };
    writer
        .seed(
            &base.schema,
            &base.name,
            &[("id".into(), "long".into(), false)],
            &[3],
        )
        .await;

    let view = TableRef {
        schema: "gov".into(),
        name: "customers_eu".into(),
    };
    catalog
        .define_view(ViewDef {
            view: view.clone(),
            base: base.clone(),
            predicate: None,
            columns: None,
        })
        .await
        .expect("define_view");

    // Drive `mark_dropped` directly — the guarded seam the production drop path
    // (`SqlCatalog::drop_table`) calls — rather than through `IcebergWriter::drop_table`
    // (which `.expect()`s and would panic on the refusal instead of letting us assert it).
    let mut tx = cp.pool().begin().await.expect("begin tx");
    let at = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let err = mark_dropped(&mut tx, &base.schema, &base.name, at)
        .await
        .expect_err("drop refused while a dependent view exists");
    assert!(
        matches!(err, ControlPlaneError::Conflict(_)),
        "expected Conflict, got {err:?}"
    );
    assert!(
        err.to_string().contains("gov.customers_eu"),
        "conflict message should name the dependent view: {err}"
    );
    tx.rollback().await.expect("rollback");

    // Drop the view; now the base can be dropped.
    catalog.drop_view(&view).await.expect("drop_view");

    // Drive the production drop path (IcebergWriter::drop_table → SqlCatalog::drop_table
    // → mark_dropped), not the low-level mark_dropped directly.
    writer.drop_table(&base.schema, &base.name).await;

    // Dropped: no longer a live physical table (though — same MVCC semantics as
    // `catalog_delete_contract` — `current_snapshot` still resolves to its last
    // LIVE snapshot, not `NotFound`; only `list_tables` reflects the drop).
    let tables = catalog
        .list_tables(PageReq::unbounded())
        .await
        .expect("list_tables");
    assert!(
        !tables.items.contains(&base),
        "dropped base no longer listed as a live table"
    );
}

#[tokio::test]
async fn define_view_emits_base_to_view_lineage_edge() {
    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool().clone());
    let writer = IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db));

    let base = TableRef {
        schema: "gov".into(),
        name: "orders".into(),
    };
    writer
        .seed(
            &base.schema,
            &base.name,
            &[("id".into(), "long".into(), false)],
            &[2],
        )
        .await;

    let view = TableRef {
        schema: "gov".into(),
        name: "orders_eu".into(),
    };
    catalog
        .define_view(ViewDef {
            view: view.clone(),
            base: base.clone(),
            predicate: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Int(1),
            }),
            columns: None,
        })
        .await
        .expect("define_view");

    let base_ref = DatasetId::from(&base).dataset_ref();
    let view_ref = DatasetId::from(&view).dataset_ref();

    let downstream = cp
        .lineage()
        .downstream(&base_ref, 1, PageReq::unbounded())
        .await
        .expect("downstream");
    assert!(
        downstream.items.contains(&view_ref),
        "base->view edge visible downstream of base"
    );

    let upstream = cp
        .lineage()
        .upstream(&view_ref, 1, PageReq::unbounded())
        .await
        .expect("upstream");
    assert!(
        upstream.items.contains(&base_ref),
        "base->view edge visible upstream of view"
    );
}
