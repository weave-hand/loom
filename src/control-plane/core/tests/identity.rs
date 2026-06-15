use control_plane_core::{
    DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TableRef, TypeId, TypeName,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

#[test]
fn table_maps_to_loom_namespaced_dotted_dataset_ref() {
    let dr = DatasetId::from(&tref("main", "customer")).dataset_ref();
    assert_eq!(
        dr,
        DatasetRef {
            namespace: "loom".into(),
            name: "main.customer".into(),
        }
    );
    assert_eq!(LOOM_DATASET_NAMESPACE, "loom");
}

#[test]
fn dataset_ref_round_trips_for_loom_datasets() {
    let t = tref("analytics", "orders");
    let dr: DatasetRef = (&t).into();
    assert_eq!(DatasetId::from_dataset_ref(&dr), Some(DatasetId::from(&t)));
    assert_eq!(DatasetId::from_dataset_ref(&dr).unwrap().table(), &t);
}

#[test]
fn external_namespaces_are_not_loom_datasets() {
    for ns in ["s3://bucket", "postgres://h:5432", "loom-ingest", ""] {
        let dr = DatasetRef {
            namespace: ns.into(),
            name: "main.customer".into(),
        };
        assert_eq!(DatasetId::from_dataset_ref(&dr), None, "namespace={ns}");
    }
}

#[test]
fn malformed_loom_names_do_not_parse() {
    for nm in ["nodot", ".x", "x.", "", "a.b.c"] {
        let dr = DatasetRef {
            namespace: "loom".into(),
            name: nm.into(),
        };
        assert_eq!(DatasetId::from_dataset_ref(&dr), None, "name={nm}");
    }
}

#[test]
fn type_maps_to_loom_type_namespaced_ref() {
    let dr: DatasetRef = (&TypeName("Customer".into())).into();
    assert_eq!(
        dr,
        DatasetRef {
            namespace: "loom:type".into(),
            name: "Customer".into(),
        }
    );
    assert_eq!(LOOM_TYPE_NAMESPACE, "loom:type");
}

#[test]
fn type_dataset_ref_round_trips() {
    let ty = TypeName("Order".into());
    let dr: DatasetRef = (&ty).into();
    assert_eq!(TypeId::from_dataset_ref(&dr), Some(TypeId::from(&ty)));
}

#[test]
fn type_and_table_refs_do_not_collide() {
    let table_dr: DatasetRef = (&tref("main", "Customer")).into();
    let type_dr: DatasetRef = (&TypeName("Customer".into())).into();
    assert_ne!(table_dr.namespace, type_dr.namespace);
    assert_eq!(
        DatasetId::from_dataset_ref(&type_dr),
        None,
        "a type ref is not a table dataset"
    );
    assert_eq!(
        TypeId::from_dataset_ref(&table_dr),
        None,
        "a table ref is not a type"
    );
}

#[test]
fn external_and_empty_are_not_type_refs() {
    for (ns, nm) in [
        ("s3://b", "Customer"),
        ("loom", "Customer"),
        ("loom:type", ""),
    ] {
        let dr = DatasetRef {
            namespace: ns.into(),
            name: nm.into(),
        };
        assert_eq!(TypeId::from_dataset_ref(&dr), None, "ns={ns} nm={nm}");
    }
}
