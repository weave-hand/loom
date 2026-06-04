use async_trait::async_trait;
use control_plane_core::SnapshotId;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_testkit::{CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract};

struct PgSeeder {
    writer: DuckLakeWriter,
}

#[async_trait]
impl CatalogSeed for PgSeeder {
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
                files_added: 1,
            })
            .collect()
    }
}

#[tokio::test]
async fn postgres_passes_catalog_contract() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let seeder = PgSeeder {
        writer: DuckLakeWriter::new(fixture.socket_path(), &db),
    };
    catalog_contract(&cp, &seeder).await;
}
