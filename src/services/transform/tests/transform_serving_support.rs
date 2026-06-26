//! `InProcessServingEngine` for the typed-transform e2e: a `dyn ServingEngine` backed
//! by `engine_serving::execute_query` over an `IcebergCatalog`, so query-api's
//! `read_object` reads the typed transform output with no gRPC hop. Mirrors
//! `query_api` e2e_support's `InProcessServingEngine`. Kept in its own module so the
//! other transform e2e tests don't pull the query-api dependency.

use control_plane_postgres::iceberg_catalog::IcebergCatalog;

pub struct InProcessServingEngine {
    catalog: IcebergCatalog,
}

impl InProcessServingEngine {
    pub fn new(catalog: IcebergCatalog) -> Self {
        Self { catalog }
    }
}

#[async_trait::async_trait]
impl query_api::serving::ServingEngine for InProcessServingEngine {
    async fn fetch_rows(
        &self,
        sql: &str,
        params: &[query_api::serving::SqlValue],
    ) -> Result<query_api::serving::Rows, query_api::serving::ServingError> {
        let inlined = query_api::serving::inline_params(sql, params);
        let batches = engine_serving::execute_query(&self.catalog, &inlined, None)
            .await
            .map_err(|e| query_api::serving::ServingError::Engine(e.to_string()))?;
        Ok(query_api::serving_datafusion::batches_to_rows(batches))
    }
    fn dialect(&self) -> &'static dyn query_api::sql::SqlDialect {
        &query_api::sql::DataFusionDialect
    }
}
