use control_plane_core::{DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, TableRef};

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
