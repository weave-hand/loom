use async_trait::async_trait;
use control_plane_memory::MemoryControlPlane;
use control_plane_testkit::{
    CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract, catalog_delete_contract,
};

/// Adapts the testkit `CatalogSeed` seam to the fake's inherent seeding method.
struct MemSeeder<'a>(&'a MemoryControlPlane);

#[async_trait]
impl CatalogSeed for MemSeeder<'_> {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        let cols: Vec<(String, String, bool)> = spec
            .columns
            .into_iter()
            .map(|c| (c.name, c.ty, c.nullable))
            .collect();
        self.0
            .seed_catalog(&spec.table, &cols, &spec.row_batches)
            .into_iter()
            .map(|snapshot| SeededSnapshot {
                snapshot,
                files_added: 1,
            })
            .collect()
    }

    async fn drop_table(
        &self,
        table: &control_plane_core::TableRef,
    ) -> control_plane_core::SnapshotId {
        self.0.drop_table_catalog(table)
    }
}

#[tokio::test]
async fn memory_passes_catalog_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    catalog_contract(&cp, &MemSeeder(&cp)).await;
}

#[tokio::test]
async fn memory_passes_catalog_delete_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    catalog_delete_contract(&cp, &MemSeeder(&cp)).await;
}
