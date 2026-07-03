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

fn dr(namespace: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: namespace.to_string(),
        name: name.to_string(),
    }
}

#[test]
fn reverse_logical_table_resolves_regardless_of_warehouse() {
    // Logical "loom" refs are emitted by control-plane producers with no storage
    // context, so they must resolve under ANY deployment's warehouse.
    for n in [s3_naming(), file_naming()] {
        assert_eq!(
            n.resolve(&dr("loom", "main.orders")),
            ResolvedDataset::Table(table("main", "orders"))
        );
    }
}

#[test]
fn reverse_logical_type_resolves_regardless_of_warehouse() {
    for n in [s3_naming(), file_naming()] {
        assert_eq!(
            n.resolve(&dr("loom:type", "Customer")),
            ResolvedDataset::Type(TypeName("Customer".to_string()))
        );
    }
}

#[test]
fn reverse_storage_derived_table_resolves_for_this_warehouse() {
    let n = s3_naming();
    assert_eq!(
        n.resolve(&dr("s3://bucket", "main.orders")),
        ResolvedDataset::Table(table("main", "orders"))
    );
}

#[test]
fn reverse_external_datasources_are_not_rejected() {
    let n = s3_naming();
    for ext in [
        dr("s3://other-bucket", "main.orders"),
        dr("postgres://h", "public.t"),
        dr("kafka://broker", "topic"),
    ] {
        assert_eq!(n.resolve(&ext), ResolvedDataset::External(ext.clone()));
    }
}

#[test]
fn reverse_malformed_under_owned_namespace_is_unresolvable() {
    // A ref whose namespace is loom-owned (logical "loom"/"loom:type" OR this
    // deployment's site_namespace) but whose name fails the parse must fail closed —
    // Unresolvable, NOT External (which would default-allow and widen disclosure).
    let n = s3_naming(); // site_namespace = "s3://bucket"
    for bad in [
        dr("loom", "nodot"),           // logical table ns, no schema separator
        dr("loom", ".x"),              // empty schema
        dr("loom", "x."),              // empty table
        dr("loom:type", ""),           // logical type ns, empty name
        dr("s3://bucket", "a.b.c"),    // site ns, ambiguous multi-dot
        dr("s3://bucket", "Customer"), // site ns, no separator (type-shaped, tables only)
    ] {
        assert_eq!(
            n.resolve(&bad),
            ResolvedDataset::Unresolvable(bad.clone()),
            "owned-namespace malformed ref must fail closed"
        );
    }
}

#[test]
fn round_trip_table_and_type() {
    let n = s3_naming();
    let t = table("main", "orders");
    assert_eq!(
        n.resolve(&n.dataset_ref(&t)),
        ResolvedDataset::Table(t.clone())
    );

    let ty = TypeName("Customer".to_string());
    assert_eq!(
        n.resolve(&n.type_ref(&ty)),
        ResolvedDataset::Type(ty.clone())
    );
}
