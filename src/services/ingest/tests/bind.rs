//! bind validation against the real (Iceberg mirror) catalog: a conforming type
//! binds and persists; a non-conforming type is rejected with ALL violations and
//! nothing is persisted. Seeding is via the Iceberg writer (real Parquet -> mirror
//! projection); `bind` reads the physical schema through the mirror-backed
//! `IcebergCatalog` while the ontology stays on the shared Postgres tables (`cp`).

use control_plane_core::{
    Aggregation, Cardinality, ControlPlaneError, DerivedPropertyDef, LinkDef, ObjectType, Ontology,
    PageReq, PropertyDef, TypeName,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use ingest::{BindError, BindViolationReason, bind, bind_link};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    let p = PropertyDef::new(name, ty);
    if required { p.required() } else { p }
}

fn customer() -> (&'static str, &'static str) {
    ("main", "customer")
}

/// Build an Iceberg writer over the fixture db (its own pool + libpq DSN for the
/// vendored catalog).
async fn writer_for(fx: &PgFixture, db: &str) -> IcebergWriter {
    let pool = fx.pool_for(db).await;
    IcebergWriter::new(pool, fx.pg_dsn(db))
}

// Seed main.customer with logical columns chosen so the bind validator sees the
// SAME outcomes as the Iceberg-seeded table produces:
//   id     long  NN   (a valid required Long / identity)
//   email  string NULL (String matches; required-over-nullable -> NullabilityViolation)
//   amount long  NULL  (only exercised as `Money` -> UnknownLogicalType; physical irrelevant)
//   score  string NULL (declared Long -> TypeMismatch; required-over-nullable -> NullabilityViolation)
// (The SeedCol API carries only long/string/etc.; the validator compares loom logical
// types, so substituting string for the original int32 columns preserves every assertion.)
async fn seed_customer(writer: &IcebergWriter) {
    writer
        .seed_arrays(
            "main",
            "customer",
            &[
                ("id".into(), "long".into(), false),
                ("email".into(), "string".into(), true),
                ("amount".into(), "long".into(), true),
                ("score".into(), "string".into(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2]),
                SeedCol::Str(vec!["a@x", "b@x"]),
                SeedCol::NullableLong(vec![Some(10), Some(20)]),
                SeedCol::Str(vec!["s1", "s2"]),
            ],
        )
        .await;
}

#[tokio::test]
async fn bind_accepts_conforming_type_and_persists_it() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    let type_def = ObjectType::build("Customer", customer())
        .add_prop(prop("id", "Long", true)) // long, non-null -> ok
        .add_prop(prop("email", "EmailAddress", false)) // string, optional -> ok
        .done();
    bind(&cat, &cp, type_def.clone()).await.unwrap();

    let got = cp.get_type(&TypeName("Customer".into())).await.unwrap();
    assert_eq!(got, type_def);
}

#[tokio::test]
async fn bind_collects_all_violations_and_persists_nothing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // phone: not a column (MissingColumn)
    // id: Integer over long column (TypeMismatch)
    // amount: Money is unknown (UnknownLogicalType)
    // email: String required over a nullable column (NullabilityViolation)
    // score: required Long over a nullable string column (TypeMismatch + NullabilityViolation)
    let type_def = ObjectType::build("Bad", customer())
        .add_prop(prop("phone", "String", false))
        .add_prop(prop("id", "Integer", false))
        .add_prop(prop("amount", "Money", false))
        .add_prop(prop("email", "String", true))
        .add_prop(prop("score", "Long", true))
        .done();

    let err = bind(&cat, &cp, type_def).await.unwrap_err();
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
    // score: required `Long` over a nullable non-long column -> BOTH a type mismatch
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
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // `id` is a required property (long, non-null) -> a valid identity.
    let type_def = ObjectType::build("Customer", customer())
        .add_prop(prop("id", "Long", true))
        .add_prop(prop("email", "EmailAddress", false))
        .identity("id")
        .done();
    bind(&cat, &cp, type_def).await.unwrap();
}

#[tokio::test]
async fn bind_rejects_identity_naming_unknown_property() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // `nope` is not a declared property -> BadIdentity.
    let type_def = ObjectType::build("Customer", customer())
        .add_prop(prop("id", "Long", true))
        .identity("nope")
        .done();
    let err = bind(&cat, &cp, type_def).await.unwrap_err();
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
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // `email` is a declared but non-required (nullable) property -> a PK can't be
    // nullable -> BadIdentity.
    let type_def = ObjectType::build("Customer", customer())
        .add_prop(prop("id", "Long", true))
        .add_prop(prop("email", "EmailAddress", false))
        .identity("email")
        .done();
    let err = bind(&cat, &cp, type_def).await.unwrap_err();
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
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    // No seeding: empty catalog, no tables.
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    let type_def = ObjectType::build("Ghost", ("main", "ghost"))
        .add_prop(prop("id", "Long", true))
        .done();
    let err = bind(&cat, &cp, type_def).await.unwrap_err();
    assert!(matches!(err, BindError::TableNotFound(_)), "got {err:?}");
}

// Seed main.reserved: id long NN, _x long NULL.
// The physical column `_x` exists so the only violation is the reserved name.
async fn seed_reserved(writer: &IcebergWriter) {
    writer
        .seed_arrays(
            "main",
            "reserved",
            &[
                ("id".into(), "long".into(), false),
                ("_x".into(), "long".into(), true),
            ],
            &[SeedCol::Long(vec![1]), SeedCol::NullableLong(vec![Some(1)])],
        )
        .await;
}

fn reserved_table() -> (&'static str, &'static str) {
    ("main", "reserved")
}

#[tokio::test]
async fn bind_rejects_a_property_name_starting_with_underscore() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_reserved(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // `_x` exists as a physical column, so the only violation is the reserved name.
    let type_def = ObjectType::build("Reserved", reserved_table())
        .add_prop(prop("id", "Long", true))
        .add_prop(prop("_x", "Long", false))
        .done();
    let err = bind(&cat, &cp, type_def).await.unwrap_err();
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
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // Base conforming type, but derived property name begins with `_`.
    let type_def = ObjectType::build("Customer", customer())
        .add_prop(prop("id", "Long", true))
        .derived(DerivedPropertyDef::new(
            "_y",
            "long",
            "whatever",
            Aggregation::Count,
        ))
        .done();
    let err = bind(&cat, &cp, type_def).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| x.property == "_y" && matches!(x.reason, BindViolationReason::ReservedName)),
        "expected a ReservedName violation for '_y', got {v:?}"
    );
}

// ---- postgres parity: derived-property + bind_link over the real Iceberg catalog ----
//
// The exhaustive violation matrix lives in the in-memory `bind_validation` suite;
// these cases prove the SAME validators run against the real catalog, whose adapter
// maps physical Iceberg types to loom logical types that feed the validator.

fn purchase_table() -> (&'static str, &'static str) {
    ("main", "purchase")
}

// Seed main.purchase: id long NN, customer_id long NULL, cost long NULL.
// (`cost` is a numeric column so Sum applies; long preserves the original INTEGER
// intent — both map to a numeric loom logical type the validator accepts.)
async fn seed_purchase(writer: &IcebergWriter) {
    writer
        .seed_arrays(
            "main",
            "purchase",
            &[
                ("id".into(), "long".into(), false),
                ("customer_id".into(), "long".into(), true),
                ("cost".into(), "long".into(), true),
            ],
            &[
                SeedCol::Long(vec![1, 2]),
                SeedCol::NullableLong(vec![Some(1), Some(1)]),
                SeedCol::NullableLong(vec![Some(5), Some(7)]),
            ],
        )
        .await;
}

// Define the base Customer + Purchase types and a `Customer.purchases -> Purchase`
// FK link (`customer.id = purchase.customer_id`), so a derived property over
// `purchases` resolves.
async fn define_purchase_graph(cp: &impl Ontology) {
    cp.define_type(
        ObjectType::build("Customer", customer())
            .add_prop(prop("id", "Long", true))
            .done(),
    )
    .await
    .unwrap();
    cp.define_type(
        ObjectType::build("Purchase", purchase_table())
            .add_prop(prop("id", "Long", true))
            .done(),
    )
    .await
    .unwrap();
    cp.define_link(LinkDef::fk(
        "purchases",
        "Customer",
        "Purchase",
        Cardinality::Many,
        "id",
        "customer_id",
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn bind_accepts_valid_derived_properties_over_the_real_catalog() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    seed_purchase(&writer).await;
    define_purchase_graph(&cp).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // Count -> Long, and Sum over the numeric `cost` column -> Long (numeric). The
    // Iceberg adapter maps the physical types to loom logical types the validator reads.
    let derived = vec![
        DerivedPropertyDef::new("purchaseCount", "Long", "purchases", Aggregation::Count),
        DerivedPropertyDef::new(
            "totalCost",
            "Long",
            "purchases",
            Aggregation::Sum("cost".into()),
        ),
    ];
    let type_def = derived
        .clone()
        .into_iter()
        .fold(
            ObjectType::build("Customer", customer()).add_prop(prop("id", "Long", true)),
            |b, d| b.derived(d),
        )
        .done();
    bind(&cat, &cp, type_def).await.unwrap();

    let got = cp.get_type(&TypeName("Customer".into())).await.unwrap();
    assert_eq!(got.derived, derived);
}

#[tokio::test]
async fn bind_rejects_a_bad_derived_reference_over_the_real_catalog() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    seed_purchase(&writer).await;
    define_purchase_graph(&cp).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);

    // `cost` is numeric -> Sum is applicable, but "ghost" is not a column on
    // purchase -> MissingAggColumn; and "noSuchLink" is undefined -> UnknownDerivedLink.
    let type_def = ObjectType::build("Customer", customer())
        .add_prop(prop("id", "Long", true))
        .derived(DerivedPropertyDef::new(
            "ghostSum",
            "Long",
            "purchases",
            Aggregation::Sum("ghost".into()),
        ))
        .derived(DerivedPropertyDef::new(
            "dangler",
            "Long",
            "noSuchLink",
            Aggregation::Count,
        ))
        .done();
    let err = bind(&cat, &cp, type_def).await.unwrap_err();
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
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let writer = writer_for(fx, &db).await;
    seed_customer(&writer).await;
    seed_purchase(&writer).await;
    let cat = IcebergCatalog::new(fx.pool_for(&db).await);
    // Endpoint types only (no link yet — bind_link creates it).
    cp.define_type(
        ObjectType::build("Customer", customer())
            .add_prop(prop("id", "Long", true))
            .done(),
    )
    .await
    .unwrap();
    cp.define_type(
        ObjectType::build("Purchase", purchase_table())
            .add_prop(prop("id", "Long", true))
            .done(),
    )
    .await
    .unwrap();

    let good = LinkDef::fk(
        "purchases",
        "Customer",
        "Purchase",
        Cardinality::Many,
        "id",
        "customer_id",
    );
    bind_link(&cat, &cp, good).await.unwrap();
    let links = cp
        .links(&TypeName("Customer".into()), PageReq::unbounded())
        .await
        .unwrap();
    assert!(links.items.iter().any(|l| l.name == "purchases"));

    // A backing column that does not exist on the to-table -> MissingColumn.
    let bad = LinkDef::fk(
        "broken",
        "Customer",
        "Purchase",
        Cardinality::Many,
        "id",
        "nope",
    );
    let err = bind_link(&cat, &cp, bad).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v
        .iter()
        .any(|x| x.property == "nope" && matches!(x.reason, BindViolationReason::MissingColumn)));
}
