//! Unit tests for `infer_object_type`: Arrow schema -> inferred ObjectType (the
//! reverse of `model_shape_from_type`). Pure logic, no fixture.

use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{TableRef, TypeName};
use ingest::{InferTypeError, infer_object_type};

fn schema(fields: Vec<Field>) -> Schema {
    Schema::new(fields)
}

#[test]
fn maps_each_field_to_a_property_in_order() {
    let s = schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]);
    let ty = infer_object_type(&TypeName("gadget".into()), &s, None).expect("infer");

    assert_eq!(ty.name, TypeName("gadget".into()));
    assert_eq!(
        ty.table,
        TableRef {
            schema: "main".into(),
            name: "gadget".into()
        }
    );
    assert_eq!(ty.identity, None);
    assert!(ty.derived.is_empty());

    let props: Vec<(&str, &str, bool)> = ty
        .properties
        .iter()
        .map(|p| (p.name.as_str(), p.ty.as_str(), p.required))
        .collect();
    // required = !nullable: id is non-null -> required; name/score nullable -> not.
    assert_eq!(
        props,
        vec![
            ("id", "long", true),
            ("name", "string", false),
            ("score", "double", false)
        ]
    );
}

#[test]
fn declared_identity_is_recorded_and_forced_required() {
    let s = schema(vec![
        Field::new("sku", DataType::Utf8, true), // nullable in the batch...
        Field::new("qty", DataType::Int64, true),
    ]);
    let ty = infer_object_type(&TypeName("widget".into()), &s, Some("sku")).expect("infer");

    assert_eq!(ty.identity, Some("sku".into()));
    let sku = ty
        .properties
        .iter()
        .find(|p| p.name == "sku")
        .expect("sku prop");
    assert!(
        sku.required,
        "the declared identity is forced required even if the field is nullable"
    );
}

#[test]
fn unmappable_column_is_unsupported_naming_the_column() {
    let s = schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("when", DataType::Date32, true), // no loom logical mapping
    ]);
    let err = infer_object_type(&TypeName("ev".into()), &s, None).expect_err("unmappable");
    match err {
        InferTypeError::UnsupportedColumns(vs) => {
            assert_eq!(vs.len(), 1);
            assert_eq!(vs[0].column, "when");
        }
        other => panic!("expected UnsupportedColumns, got {other:?}"),
    }
}

#[test]
fn identity_naming_absent_column_is_identity_not_found() {
    let s = schema(vec![Field::new("id", DataType::Int64, false)]);
    let err = infer_object_type(&TypeName("ev".into()), &s, Some("nope")).expect_err("bad id");
    match err {
        InferTypeError::IdentityNotFound(col) => assert_eq!(col, "nope"),
        other => panic!("expected IdentityNotFound, got {other:?}"),
    }
}
