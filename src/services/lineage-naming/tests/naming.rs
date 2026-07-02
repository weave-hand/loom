//! Forward + reverse naming-bridge tests for `lineage_naming`. Pure logic; no fixture.

use control_plane_core::{DatasetRef, TableRef, TypeName};
use lineage_naming::{LineageNaming, ResolvedDataset};
use store_config::{ObjectStoreBackend, ObjectStoreConfig};

fn s3_naming() -> LineageNaming {
    // Warehouse URI carries a key prefix; the derived namespace must drop it and
    // keep only the `s3://<bucket>` datasource authority.
    let cfg = ObjectStoreConfig::for_s3_test(
        "s3://bucket/warehouse/prefix".to_string(),
        "bucket".to_string(),
        "http://localhost:9000".to_string(),
        "ak".to_string(),
        "sk".to_string(),
    );
    LineageNaming::from_object_store(&cfg)
}

fn file_naming() -> LineageNaming {
    let cfg = ObjectStoreConfig {
        warehouse_uri: "file:///var/lib/loom/warehouse".to_string(),
        backend: ObjectStoreBackend::Local,
    };
    LineageNaming::from_object_store(&cfg)
}

fn table(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.to_string(),
        name: name.to_string(),
    }
}

#[test]
fn forward_s3_table_uses_storage_namespace_and_dotted_name() {
    let n = s3_naming();
    assert_eq!(
        n.dataset_ref(&table("main", "orders")),
        DatasetRef {
            namespace: "s3://bucket".to_string(),
            name: "main.orders".to_string()
        }
    );
}

#[test]
fn forward_s3_type_stays_on_logical_type_namespace() {
    let n = s3_naming();
    assert_eq!(
        n.type_ref(&TypeName("Customer".to_string())),
        DatasetRef {
            namespace: "loom:type".to_string(),
            name: "Customer".to_string()
        }
    );
}

#[test]
fn forward_file_table_uses_warehouse_root_namespace() {
    let n = file_naming();
    assert_eq!(
        n.dataset_ref(&table("main", "orders")),
        DatasetRef {
            namespace: "file:///var/lib/loom/warehouse".to_string(),
            name: "main.orders".to_string(),
        }
    );
}

#[test]
fn forward_namespace_derivation_drops_key_prefix() {
    // s3://bucket/warehouse/prefix -> namespace s3://bucket (authority only).
    let n = s3_naming();
    assert_eq!(
        n.dataset_ref(&table("main", "orders")).namespace,
        "s3://bucket"
    );
}

#[test]
fn forward_non_default_schema_round_trips_name() {
    let n = s3_naming();
    assert_eq!(
        n.dataset_ref(&table("analytics", "report")).name,
        "analytics.report"
    );
}
