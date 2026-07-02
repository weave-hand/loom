use async_trait::async_trait;
use control_plane_core::{SnapshotId, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_testkit::{
    CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract, catalog_delete_contract,
};

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
async fn iceberg_passes_catalog_contract() {
    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool().clone());
    let seeder = IcebergSeeder {
        writer: IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db)),
    };
    catalog_contract(&catalog, &seeder).await;
}

#[tokio::test]
async fn iceberg_passes_catalog_delete_contract() {
    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool().clone());
    let seeder = IcebergSeeder {
        writer: IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db)),
    };
    catalog_delete_contract(&catalog, &seeder).await;
}
