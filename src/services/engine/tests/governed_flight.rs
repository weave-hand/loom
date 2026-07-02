//! e2e: the engine's do_get dispatches a GovernedStatementQuery ticket to the governed
//! path, applying the row filter/deny/mask regardless of the client SQL.

use arrow_array::{Array, Int64Array};
use arrow_flight::Ticket;
use arrow_flight::flight_service_server::FlightService;
use control_plane_core::{
    CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef,
};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine_wire::flight::GovernedStatementQuery;
use futures::TryStreamExt;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use std::sync::Arc;
use tonic::Request;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn do_get_governed_applies_policy() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn.clone());
    writer.seed("s", "orders", &cols, &[5]).await; // ids 0..4

    let file_catalog = make_catalog(dsn, &wh.path().display().to_string()).await;
    let svc = FlightDataService {
        catalog: file_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    // Policy: only rows with id >= 2 are visible on `s.orders`.
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: TableRef {
                schema: "s".into(),
                name: "orders".into(),
            },
            row_filters: vec![RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Ge,
                value: ScalarValue::Int(2),
            }],
            denied: vec![],
            masked: vec![],
        }],
    };
    let ticket = GovernedStatementQuery {
        sql: r#"SELECT "id" FROM "s"."orders" ORDER BY "id""#.to_string(),
        catalog: cat,
    };

    let resp = svc
        .do_get(Request::new(Ticket {
            ticket: ticket.encode().into(),
        }))
        .await
        .expect("do_get governed ticket");

    let stream = resp
        .into_inner()
        .map_err(arrow_flight::error::FlightError::from);
    let batches: Vec<_> =
        arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream)
            .try_collect()
            .await
            .expect("collect governed stream");

    let mut ids = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    assert_eq!(
        ids,
        vec![2, 3, 4],
        "governed row filter applied through the do_get dispatch, regardless of client SQL"
    );
}
