use async_trait::async_trait;
use control_plane_core::SnapshotId;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_testkit::{
    CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract, catalog_delete_contract,
};

struct PgSeeder {
    writer: DuckLakeWriter,
}

/// The contract seeds loom LOGICAL types; the pg seeder drives real DuckLake, so it
/// maps each logical type to the DuckDB physical type used in `CREATE TABLE`. The
/// adapter's `schema()` then maps DuckLake's stored type back to logical — the
/// round-trip the contract asserts.
fn logical_to_duckdb(ty: &str) -> &'static str {
    match ty {
        "long" => "BIGINT",
        "integer" => "INTEGER",
        "double" => "DOUBLE",
        "boolean" => "BOOLEAN",
        "string" => "VARCHAR",
        "date" => "DATE",
        "timestamp" => "TIMESTAMP",
        other => panic!("seed: unmapped logical type {other:?}"),
    }
}

#[async_trait]
impl CatalogSeed for PgSeeder {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        let cols: Vec<(String, String, bool)> = spec
            .columns
            .into_iter()
            .map(|c| (c.name, logical_to_duckdb(&c.ty).to_string(), c.nullable))
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

    async fn drop_table(&self, table: &control_plane_core::TableRef) -> SnapshotId {
        SnapshotId(self.writer.drop_table(&table.schema, &table.name).await)
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

#[tokio::test]
async fn postgres_passes_catalog_delete_contract() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let seeder = PgSeeder {
        writer: DuckLakeWriter::new(fixture.socket_path(), &db),
    };
    catalog_delete_contract(&cp, &seeder).await;
}
