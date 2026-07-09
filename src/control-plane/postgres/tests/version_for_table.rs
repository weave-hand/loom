//! `version_for_table` reverse-lookup: the version/sequence column name for the
//! object type stored at `table`. Mirrors `identity_for_table`.
//! loom_fixture_test (Postgres).

use control_plane_core::{ObjectType, Ontology, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::ontology::version_for_table;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_for_table_round_trips_and_defaults_none() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let with_version = ObjectType::build("Widget", ("main", "widget"))
        .prop_req("id", "Long")
        .prop_req("seq", "Long")
        .identity("id")
        .version("seq")
        .done();
    cp.define_type(with_version).await.unwrap();

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let got = version_for_table(&pool, &table).await.expect("lookup");
    assert_eq!(
        got.as_deref(),
        Some("seq"),
        "declared version column resolves"
    );

    let without = ObjectType::build("Other", ("main", "other"))
        .prop_req("id", "Long")
        .identity("id")
        .done();
    cp.define_type(without).await.unwrap();
    let none = version_for_table(
        &pool,
        &TableRef {
            schema: "main".into(),
            name: "other".into(),
        },
    )
    .await
    .expect("lookup");
    assert!(none.is_none(), "a type with no version property reads None");

    // A table with no bound type also reads None (no row).
    let absent = version_for_table(
        &pool,
        &TableRef {
            schema: "main".into(),
            name: "nope".into(),
        },
    )
    .await
    .expect("lookup");
    assert!(absent.is_none(), "an unbound table reads None");
}
