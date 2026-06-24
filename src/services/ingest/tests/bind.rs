//! bind validation against the real catalog: a conforming type binds and persists;
//! a non-conforming type is rejected with ALL violations and nothing is persisted.

use control_plane_core::{
    Aggregation, Cardinality, ControlPlaneError, DerivedPropertyDef, LinkBacking, LinkDef,
    ObjectType, Ontology, PageReq, PropertyDef, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{BindError, BindViolationReason, bind, bind_link};

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
async fn bind_accepts_identity_naming_a_required_property() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    // `id` is a required property (int64, non-null) -> a valid identity.
    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("email", "EmailAddress", false),
        ],
        derived: vec![],
        table: customer(),
        identity: Some("id".into()),
    };
    bind(&cp, &cp, type_def).await.unwrap();
}

#[tokio::test]
async fn bind_rejects_identity_naming_unknown_property() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    // `nope` is not a declared property -> BadIdentity.
    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: customer(),
        identity: Some("nope".into()),
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| matches!(x.reason, BindViolationReason::BadIdentity(_))),
        "expected a BadIdentity violation, got {v:?}"
    );
}

#[tokio::test]
async fn bind_rejects_identity_naming_non_required_property() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    // `email` is a declared but non-required (nullable) property -> a PK can't be
    // nullable -> BadIdentity.
    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("email", "EmailAddress", false),
        ],
        derived: vec![],
        table: customer(),
        identity: Some("email".into()),
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| matches!(x.reason, BindViolationReason::BadIdentity(_))),
        "expected a BadIdentity violation, got {v:?}"
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

// Seed main.reserved: id BIGINT NOT NULL, _x BIGINT NULL.
// The physical column `_x` exists so the only violation is the reserved name.
async fn seed_reserved(writer: &DuckLakeWriter) {
    writer
        .seed(
            "main",
            "reserved",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("_x".into(), "BIGINT".into(), true),
            ],
            &[1],
        )
        .await;
}

fn reserved_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "reserved".into(),
    }
}

#[tokio::test]
async fn bind_rejects_a_property_name_starting_with_underscore() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_reserved(&writer).await;

    // `_x` exists as a physical column, so the only violation is the reserved name.
    let type_def = ObjectType {
        name: TypeName("Reserved".into()),
        properties: vec![prop("id", "Long", true), prop("_x", "Long", false)],
        derived: vec![],
        table: reserved_table(),
        identity: None,
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| x.property == "_x" && matches!(x.reason, BindViolationReason::ReservedName)),
        "expected a ReservedName violation for '_x', got {v:?}"
    );
}

#[tokio::test]
async fn bind_rejects_a_derived_property_name_starting_with_underscore() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;

    // Base conforming type, but derived property name begins with `_`.
    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![DerivedPropertyDef {
            name: "_y".into(),
            ty: "long".into(),
            link: "whatever".into(),
            agg: Aggregation::Count,
        }],
        table: customer(),
        identity: None,
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| x.property == "_y" && matches!(x.reason, BindViolationReason::ReservedName)),
        "expected a ReservedName violation for '_y', got {v:?}"
    );
}

// ---- postgres parity: derived-property + bind_link over the real DuckLake catalog ----
//
// The exhaustive violation matrix lives in the in-memory `bind_validation` suite;
// these cases prove the SAME validators run against the real catalog, whose adapter
// maps physical DuckLake types to loom logical types that feed the validator.

fn purchase_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "purchase".into(),
    }
}

// Seed main.purchase: id BIGINT NOT NULL, customer_id BIGINT NULL, cost INTEGER NULL.
async fn seed_purchase(writer: &DuckLakeWriter) {
    writer
        .seed(
            "main",
            "purchase",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("customer_id".into(), "BIGINT".into(), true),
                ("cost".into(), "INTEGER".into(), true),
            ],
            &[2],
        )
        .await;
}

// Define the base Customer + Purchase types and a `Customer.purchases -> Purchase`
// FK link (`customer.id = purchase.customer_id`), so a derived property over
// `purchases` resolves.
async fn define_purchase_graph(cp: &impl Ontology) {
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: customer(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Purchase".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: purchase_table(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "purchases".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Purchase".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bind_accepts_valid_derived_properties_over_the_real_catalog() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;
    seed_purchase(&writer).await;
    define_purchase_graph(&cp).await;

    // Count -> Long, and Sum over the INTEGER `cost` column -> Long (numeric). The
    // DuckLake adapter maps BIGINT/INTEGER to loom logical types the validator reads.
    let derived = vec![
        DerivedPropertyDef {
            name: "purchaseCount".into(),
            ty: "Long".into(),
            link: "purchases".into(),
            agg: Aggregation::Count,
        },
        DerivedPropertyDef {
            name: "totalCost".into(),
            ty: "Long".into(),
            link: "purchases".into(),
            agg: Aggregation::Sum("cost".into()),
        },
    ];
    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: derived.clone(),
        table: customer(),
        identity: None,
    };
    bind(&cp, &cp, type_def).await.unwrap();

    let got = cp.get_type(&TypeName("Customer".into())).await.unwrap();
    assert_eq!(got.derived, derived);
}

#[tokio::test]
async fn bind_rejects_a_bad_derived_reference_over_the_real_catalog() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;
    seed_purchase(&writer).await;
    define_purchase_graph(&cp).await;

    // `cost` is INTEGER -> Sum is applicable, but "ghost" is not a column on
    // purchase -> MissingAggColumn; and "noSuchLink" is undefined -> UnknownDerivedLink.
    let type_def = ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![
            DerivedPropertyDef {
                name: "ghostSum".into(),
                ty: "Long".into(),
                link: "purchases".into(),
                agg: Aggregation::Sum("ghost".into()),
            },
            DerivedPropertyDef {
                name: "dangler".into(),
                ty: "Long".into(),
                link: "noSuchLink".into(),
                agg: Aggregation::Count,
            },
        ],
        table: customer(),
        identity: None,
    };
    let err = bind(&cp, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter().any(|x| x.property == "ghostSum"
            && matches!(x.reason, BindViolationReason::MissingAggColumn))
    );
    assert!(v.iter().any(|x| x.property == "dangler"
        && matches!(&x.reason, BindViolationReason::UnknownDerivedLink(l) if l == "noSuchLink")));
}

#[tokio::test]
async fn bind_link_validates_backing_columns_over_the_real_catalog() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    seed_customer(&writer).await;
    seed_purchase(&writer).await;
    // Endpoint types only (no link yet — bind_link creates it).
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: customer(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Purchase".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: purchase_table(),
        identity: None,
    })
    .await
    .unwrap();

    let good = LinkDef {
        name: "purchases".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Purchase".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    };
    bind_link(&cp, &cp, good).await.unwrap();
    let links = cp
        .links(&TypeName("Customer".into()), PageReq::unbounded())
        .await
        .unwrap();
    assert!(links.items.iter().any(|l| l.name == "purchases"));

    // A backing column that does not exist on the to-table -> MissingColumn.
    let bad = LinkDef {
        name: "broken".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Purchase".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "nope".into(),
        },
    };
    let err = bind_link(&cp, &cp, bad).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| x.property == "nope"
                && matches!(x.reason, BindViolationReason::MissingColumn))
    );
}
