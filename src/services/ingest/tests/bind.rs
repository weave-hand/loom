//! bind validation against the real catalog: a conforming type binds and persists;
//! a non-conforming type is rejected with ALL violations and nothing is persisted.

use control_plane_core::{
    ControlPlaneError, ObjectType, Ontology, PropertyDef, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{BindError, BindViolationReason, bind};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn customer() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "customer".into(),
    }
}

// Seed main.customer: id BIGINT (int64) NOT NULL, email VARCHAR (varchar) NULL,
// amount INTEGER (int32) NULL.
async fn seed_customer(writer: &DuckLakeWriter) {
    writer
        .seed(
            "main",
            "customer",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("email".into(), "VARCHAR".into(), true),
                ("amount".into(), "INTEGER".into(), true),
                ("score".into(), "INTEGER".into(), true),
            ],
            &[2],
        )
        .await;
}

#[tokio::test]
async fn bind_accepts_conforming_type_and_persists_it() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            prop("id", "Long", true),             // int64, non-null -> ok
            prop("email", "EmailAddress", false), // varchar, optional -> ok
        ],
        derived: vec![],
        table: customer(),
        identity: None,
    };
    bind(&cp, &cp, type_def.clone()).await.unwrap();

    let got = cp.get_type(&TypeName("Customer".into())).await.unwrap();
    assert_eq!(got, type_def);
}

#[tokio::test]
async fn bind_collects_all_violations_and_persists_nothing() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    // phone: not a column (MissingColumn)
    // id: Integer over int64 column (TypeMismatch)
    // amount: Money is unknown (UnknownLogicalType)
    // email: String required over a nullable column (NullabilityViolation)
    // score: required Long (int64) over a nullable int32 column (TypeMismatch + NullabilityViolation)
    let type_def = ObjectType {
        name: TypeName("Bad".into()),
        properties: vec![
            prop("phone", "String", false),
            prop("id", "Integer", false),
            prop("amount", "Money", false),
            prop("email", "String", true),
            prop("score", "Long", true),
        ],
        derived: vec![],
        table: customer(),
        identity: None,
    };

    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter().any(
            |x| x.property == "phone" && matches!(x.reason, BindViolationReason::MissingColumn)
        )
    );
    assert!(v.iter().any(
        |x| x.property == "id" && matches!(x.reason, BindViolationReason::TypeMismatch { .. })
    ));
    assert!(v.iter().any(|x| x.property == "amount"
        && matches!(x.reason, BindViolationReason::UnknownLogicalType(_))));
    assert!(
        v.iter().any(|x| x.property == "email"
            && matches!(x.reason, BindViolationReason::NullabilityViolation))
    );
    // score: required `Long` over a nullable int32 column -> BOTH a type mismatch
    // and a nullability violation (independent checks).
    assert!(
        v.iter().any(|x| x.property == "score"
            && matches!(x.reason, BindViolationReason::TypeMismatch { .. }))
    );
    assert!(
        v.iter().any(|x| x.property == "score"
            && matches!(x.reason, BindViolationReason::NullabilityViolation))
    );

    let missing = cp.get_type(&TypeName("Bad".into())).await;
    assert!(
        matches!(missing, Err(ControlPlaneError::NotFound(_))),
        "a rejected type must not be persisted; got {missing:?}"
    );
}

#[tokio::test]
async fn bind_rejects_an_unknown_table() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await; // empty catalog, no tables

    let type_def = ObjectType {
        name: TypeName("Ghost".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "ghost".into(),
        },
        identity: None,
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    assert!(matches!(err, BindError::TableNotFound(_)), "got {err:?}");
}
