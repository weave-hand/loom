//! `IcebergCatalog::schema` hides `loom_`-prefixed reserved columns from the
//! logical schema, while the physical read still sees them.
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{
    ProjectedColumn, ensure_table, next_snapshot, project_columns,
};

fn col(order: i64, name: &str, ty: &str, nullable: bool) -> ProjectedColumn {
    ProjectedColumn {
        order,
        name: name.into(),
        iceberg_type: ty.into(),
        nullable,
    }
}

#[tokio::test]
async fn schema_hides_reserved_columns() {
    let fixture = PgFixture::shared();
    let (cp, _db) = fixture.fresh_db().await;
    let pool = cp.pool().clone();
    let table = TableRef {
        schema: "s".to_string(),
        name: "t".to_string(),
    };

    // Seed a table whose mirror carries a user column AND a synthetic reserved
    // `loom_` column at the same snapshot.
    let mut conn = pool.acquire().await.expect("acquire");
    let at = next_snapshot(&mut conn, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut conn, &table.schema, &table.name, at)
        .await
        .expect("ensure_table");
    project_columns(
        &mut conn,
        tid,
        at,
        &[
            col(1, "amount", "long", false),
            col(2, "loom_offset", "long", true),
        ],
    )
    .await
    .expect("project_columns");
    drop(conn);

    let ice = IcebergCatalog::new(pool.clone());

    let logical = ice.schema(&table, at).await.expect("schema");
    let names: Vec<_> = logical.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"amount"), "user column present: {names:?}");
    assert!(
        !names.contains(&"loom_offset"),
        "reserved column hidden: {names:?}"
    );

    // The physical (unfiltered) read still sees it -- flush relies on this.
    let physical = ice.physical_columns(tid, at).await.expect("physical");
    let pnames: Vec<_> = physical.iter().map(|c| c.name.as_str()).collect();
    assert!(
        pnames.contains(&"loom_offset"),
        "physical sees reserved: {pnames:?}"
    );
}
