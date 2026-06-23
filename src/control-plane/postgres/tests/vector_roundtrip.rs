//! De-risk (road-vector-column-type, task 1): confirm a `list<float>` column —
//! Arrow `List<Float32>` — round-trips value-exact through the arrow-57 iceberg writer
//! + Parquet + read-back, and that the vector dimension `N` stashed in the list field's
//! `doc` survives the metadata round-trip. No loom type-system wiring yet; this proves
//! the foundation the rest of the slice builds on. Research basis: iceberg-rust 0.9
//! supports variable `List` (not `FixedSizeList`); Parquet has no fixed-size list.

use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::DataType;
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::iceberg_writer::append_batches;
use control_plane_postgres::read_files_as_batches;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema as IceSchema, Type};
use iceberg::{Catalog as IceCatalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = std::collections::HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Write `{id: long, embedding: list<float>}` (the list field's `doc` carries the
/// dimension `vector(4)`), then read the Parquet back and assert the floats are
/// value-exact and the `doc` survived the append's metadata round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_float_column_roundtrips_through_iceberg() {
    let fx = PgFixture::start();
    let (_pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    // Iceberg schema with a required list<float> column; N stashed in the field doc.
    let schema = IceSchema::builder()
        .with_fields([
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(
                NestedField::required(
                    2,
                    "embedding",
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        3,
                        Type::Primitive(PrimitiveType::Float),
                        true,
                    )))),
                )
                .with_doc("vector(4)"),
            ),
        ])
        .build()
        .expect("schema");

    let ns = NamespaceIdent::new("main".to_string());
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("ns");
    let creation = TableCreation::builder()
        .name("vec".to_string())
        .schema(schema)
        .build();
    catalog.create_table(&ns, creation).await.expect("create");
    let ident = TableIdent::new(ns, "vec".to_string());
    let table = catalog.load_table(&ident).await.expect("load");

    // Build a RecordBatch against the iceberg-derived arrow schema so the list element
    // field (name "element", non-null, field-id metadata) matches exactly.
    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .expect("arrow schema"),
    );
    let DataType::List(child) = ice_arrow
        .field_with_name("embedding")
        .expect("embedding field")
        .data_type()
        .clone()
    else {
        panic!("embedding is not a List");
    };
    let mut b = ListBuilder::new(Float32Builder::new()).with_field(child);
    b.values().append_slice(&[0.1, 0.2, 0.3, 0.4]);
    b.append(true);
    b.values().append_slice(&[0.5, 0.6, 0.7, 0.8]);
    b.append(true);
    let embedding = b.finish();
    let id = Int64Array::from(vec![1i64, 2]);
    let batch = RecordBatch::try_new(ice_arrow.clone(), vec![Arc::new(id), Arc::new(embedding)])
        .expect("batch");

    append_batches(&catalog, &table, vec![batch])
        .await
        .expect("append");

    // Read the written Parquet back through the loom read path.
    let t = TableRef {
        schema: "main".into(),
        name: "vec".into(),
    };
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&t).await.expect("snapshot");
    let files = ice.files_with_stats(&t, snap.id).await.expect("files");
    let paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let (_schema, batches) = read_files_as_batches(&catalog, &t, &paths)
        .await
        .expect("read back");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "two rows round-tripped");
    let emb = batches[0]
        .column_by_name("embedding")
        .expect("embedding col")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list array");
    let row0 = emb.value(0);
    let f0 = row0
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("float32");
    assert_eq!(
        f0.values(),
        &[0.1f32, 0.2, 0.3, 0.4],
        "vector floats are value-exact after the Parquet round-trip"
    );

    // The dimension stashed in the list field's `doc` survives the append's metadata
    // rewrite — the mechanism the mirror will use to recover N.
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    let embedding_field = reloaded
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .find(|f| f.name == "embedding")
        .expect("embedding field");
    assert_eq!(
        embedding_field.doc.as_deref(),
        Some("vector(4)"),
        "the vector dimension survives in the list field doc"
    );
}
