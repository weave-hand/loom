use control_plane_postgres::iceberg_mirror::ProjectedColumn;
use control_plane_postgres::iceberg_schema_evolution::{
    SchemaEvolutionError, SchemaPlan, classify_schema_change,
};

fn col(order: i64, name: &str, ty: &str, nullable: bool) -> ProjectedColumn {
    ProjectedColumn {
        order,
        name: name.into(),
        iceberg_type: ty.into(),
        nullable,
    }
}

fn base() -> Vec<ProjectedColumn> {
    vec![col(1, "a", "long", false), col(2, "b", "string", true)]
}

#[test]
fn identical_is_identical() {
    assert_eq!(
        classify_schema_change(&base(), &base()),
        Ok(SchemaPlan::Identical)
    );
}

#[test]
fn append_nullable_is_additive() {
    let mut incoming = base();
    incoming.push(col(3, "c", "long", true));
    let plan = classify_schema_change(&base(), &incoming).unwrap();
    match plan {
        SchemaPlan::Additive { new_columns } => {
            assert_eq!(new_columns.len(), 1);
            assert_eq!(new_columns[0].name, "c");
        }
        other => panic!("expected Additive, got {other:?}"),
    }
}

#[test]
fn append_required_is_rejected() {
    let mut incoming = base();
    incoming.push(col(3, "c", "long", false));
    assert_eq!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::NonNullableColumnAdded { name: "c".into() })
    );
}

#[test]
fn drop_is_rejected() {
    let incoming = vec![col(1, "a", "long", false)];
    assert_eq!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnDropped { name: "b".into() })
    );
}

#[test]
fn rename_is_rejected() {
    let incoming = vec![col(1, "a", "long", false), col(2, "bb", "string", true)];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnChangedAtPosition { .. })
    ));
}

#[test]
fn retype_is_rejected() {
    let incoming = vec![col(1, "a", "string", false), col(2, "b", "string", true)];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnTypeChanged { .. })
    ));
}

#[test]
fn nullability_change_is_rejected() {
    let incoming = vec![col(1, "a", "long", true), col(2, "b", "string", true)];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnNullabilityChanged { .. })
    ));
}

#[test]
fn middle_insert_is_rejected() {
    // inserting nullable "m" between a and b shows up as a prefix mismatch at position 1
    let incoming = vec![
        col(1, "a", "long", false),
        col(2, "m", "long", true),
        col(3, "b", "string", true),
    ];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnChangedAtPosition { .. })
    ));
}

#[test]
fn from_empty_live_appends_all() {
    // creation case is the caller's concern, but classify against empty live treats
    // every incoming column as "new"; a required column from empty live is rejected,
    // so callers must special-case creation (project all) before calling classify.
    let plan = classify_schema_change(&[], &[col(1, "a", "long", true)]).unwrap();
    assert!(matches!(plan, SchemaPlan::Additive { .. }));
}
