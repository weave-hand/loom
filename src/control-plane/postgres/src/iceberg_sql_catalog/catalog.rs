// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, MetadataLocation, Namespace, NamespaceIdent, Result,
    TableCommit, TableCreation, TableIdent,
};
use sqlx::postgres::{PgPoolOptions, PgQueryResult, PgRow};
use sqlx::{AssertSqlSafe, PgPool, Postgres, Row, Transaction};

use control_plane_core::{LineageEvent, SnapshotId};

use crate::iceberg_mirror::{ProjectedColumn, ProjectedFile};
use crate::lineage::pg_emit;

use super::error::{
    from_sqlx_error, no_such_namespace_err, no_such_table_err, table_already_exists_err,
};

/// catalog URI
pub const SQL_CATALOG_PROP_URI: &str = "uri";
/// catalog warehouse location
pub const SQL_CATALOG_PROP_WAREHOUSE: &str = "warehouse";
/// catalog sql bind style
pub const SQL_CATALOG_PROP_BIND_STYLE: &str = "sql_bind_style";

static CATALOG_TABLE_NAME: &str = "iceberg_tables";
static CATALOG_FIELD_CATALOG_NAME: &str = "catalog_name";
static CATALOG_FIELD_TABLE_NAME: &str = "table_name";
static CATALOG_FIELD_TABLE_NAMESPACE: &str = "table_namespace";
static CATALOG_FIELD_METADATA_LOCATION_PROP: &str = "metadata_location";
static CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP: &str = "previous_metadata_location";
static CATALOG_FIELD_RECORD_TYPE: &str = "iceberg_type";
static CATALOG_FIELD_TABLE_RECORD_TYPE: &str = "TABLE";

static NAMESPACE_TABLE_NAME: &str = "iceberg_namespace_properties";
static NAMESPACE_FIELD_NAME: &str = "namespace";
static NAMESPACE_FIELD_PROPERTY_KEY: &str = "property_key";
static NAMESPACE_FIELD_PROPERTY_VALUE: &str = "property_value";

static NAMESPACE_LOCATION_PROPERTY_KEY: &str = "location";

static MAX_CONNECTIONS: u32 = 10; // Default the SQL pool to 10 connections if not provided
static IDLE_TIMEOUT: u64 = 10; // Default the maximum idle timeout per connection to 10s before it is closed
static TEST_BEFORE_ACQUIRE: bool = true; // Default the health-check of each connection to enabled prior to returning

/// Builder for [`SqlCatalog`]
#[derive(Debug)]
pub struct SqlCatalogBuilder {
    config: SqlCatalogConfig,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    runtime: Option<iceberg::Runtime>,
}

impl Default for SqlCatalogBuilder {
    fn default() -> Self {
        Self {
            config: SqlCatalogConfig {
                uri: "".to_string(),
                name: "".to_string(),
                warehouse_location: "".to_string(),
                props: HashMap::new(),
            },
            storage_factory: None,
            runtime: None,
        }
    }
}

impl SqlCatalogBuilder {
    /// Configure the database URI
    ///
    /// If `SQL_CATALOG_PROP_URI` has a value set in `props` during `SqlCatalogBuilder::load`,
    /// that value takes precedence, and the value specified by this method will not be used.
    pub fn uri(mut self, uri: impl Into<String>) -> Self {
        self.config.uri = uri.into();
        self
    }

    /// Configure the warehouse location
    ///
    /// If `SQL_CATALOG_PROP_WAREHOUSE` has a value set in `props` during `SqlCatalogBuilder::load`,
    /// that value takes precedence, and the value specified by this method will not be used.
    pub fn warehouse_location(mut self, location: impl Into<String>) -> Self {
        self.config.warehouse_location = location.into();
        self
    }

    /// Configure the any properties
    ///
    /// If the same key has values set in `props` during `SqlCatalogBuilder::load`,
    /// those values will take precedence.
    pub fn props(mut self, props: HashMap<String, String>) -> Self {
        for (k, v) in props {
            self.config.props.insert(k, v);
        }
        self
    }

    /// Set a new property on the property to be configured.
    /// When multiple methods are executed with the same key,
    /// the later-set value takes precedence.
    ///
    /// If the same key has values set in `props` during `SqlCatalogBuilder::load`,
    /// those values will take precedence.
    pub fn prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.props.insert(key.into(), value.into());
        self
    }
}

impl CatalogBuilder for SqlCatalogBuilder {
    type C = SqlCatalog;

    fn with_storage_factory(mut self, storage_factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(storage_factory);
        self
    }

    /// iceberg main added an explicit `Runtime` (separate IO/CPU tokio handles) that
    /// `Table::builder().build()` now *requires*. Store it; `load` defaults a caller
    /// who never sets one to `Runtime::current()` (the ambient tokio runtime), so loom's
    /// behaviour is unchanged from pre-`main`.
    fn with_runtime(mut self, runtime: iceberg::Runtime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    fn load(
        mut self,
        name: impl Into<String>,
        props: HashMap<String, String>,
    ) -> impl Future<Output = Result<Self::C>> + Send {
        for (k, v) in props {
            self.config.props.insert(k, v);
        }

        if let Some(uri) = self.config.props.remove(SQL_CATALOG_PROP_URI) {
            self.config.uri = uri;
        }
        if let Some(warehouse_location) = self.config.props.remove(SQL_CATALOG_PROP_WAREHOUSE) {
            self.config.warehouse_location = warehouse_location;
        }

        let name = name.into();

        // Loom always targets Postgres, so the SQL bind style is fixed to `$1..$N`.
        // A `SQL_CATALOG_PROP_BIND_STYLE` value supplied in props is ignored.
        self.config.props.remove(SQL_CATALOG_PROP_BIND_STYLE);

        let valid_name = !name.trim().is_empty();

        async move {
            if !valid_name {
                Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Catalog name cannot be empty",
                ))
            } else {
                self.config.name = name;
                let runtime = self.runtime.unwrap_or_else(iceberg::Runtime::current);
                SqlCatalog::new(self.config, self.storage_factory, runtime).await
            }
        }
    }
}

/// A struct representing the SQL catalog configuration.
///
/// This struct contains various parameters that are used to configure a SQL catalog,
/// such as the database URI, warehouse location, and file I/O settings.
///
/// Loom targets PostgreSQL exclusively, so SQL statements are always bound using
/// `$1`, `$2`, ... placeholders.
#[derive(Debug)]
struct SqlCatalogConfig {
    uri: String,
    name: String,
    warehouse_location: String,
    props: HashMap<String, String>,
}

#[derive(Debug)]
/// Sql catalog implementation.
pub struct SqlCatalog {
    name: String,
    connection: PgPool,
    warehouse_location: String,
    fileio: FileIO,
    /// iceberg main requires a `Runtime` on every `Table::builder()`; threaded in here
    /// from the builder (defaulting to `Runtime::current()`).
    runtime: iceberg::Runtime,
}

/// Side-effects to run inside the one `do_update_table` commit tx, alongside the
/// pointer-CAS + mirror projection. Both are optional and independent.
#[derive(Default)]
pub struct CommitExtras<'a> {
    /// Emit this lineage event in the commit tx (landing / flush provenance).
    pub lineage: Option<&'a LineageEvent>,
    /// Retire these inline rows at the commit's snapshot (flush compaction).
    pub end_cap: Option<InlineEndCap<'a>>,
    /// Overwrite/replace mode: end-cap every currently-live data file for the table
    /// at the commit's snapshot (before projecting the new files), so the new set is
    /// the sole live set while prior files stay reachable by time travel. The Iceberg
    /// twin of DuckLake's `Tx::replace_files`. `false` (the `Default`) is append.
    pub overwrite: bool,
}

/// Mark inline rows `loom_row_id = ANY(row_ids)` of `iceberg_mirror.inline_<table_id>`
/// as ended at the commit's snapshot.
pub struct InlineEndCap<'a> {
    /// The `iceberg_mirror` table id (from `inline_<table_id>`).
    pub table_id: i64,
    /// The `loom_row_id` values to retire.
    pub row_ids: &'a [i64],
}

impl SqlCatalog {
    /// Create new sql catalog instance
    async fn new(
        config: SqlCatalogConfig,
        storage_factory: Option<Arc<dyn StorageFactory>>,
        runtime: iceberg::Runtime,
    ) -> Result<Self> {
        let factory = storage_factory.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "StorageFactory must be provided for SqlCatalog. Use `with_storage_factory` to configure it.",
            )
        })?;
        let fileio = FileIOBuilder::new(factory).build();

        let max_connections: u32 = config
            .props
            .get("pool.max-connections")
            .map(|v| {
                v.parse::<u32>().map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("invalid pool.max-connections: {e}"),
                    )
                })
            })
            .transpose()?
            .unwrap_or(MAX_CONNECTIONS);
        let idle_timeout: u64 = config
            .props
            .get("pool.idle-timeout")
            .map(|v| {
                v.parse::<u64>().map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("invalid pool.idle-timeout: {e}"),
                    )
                })
            })
            .transpose()?
            .unwrap_or(IDLE_TIMEOUT);
        let test_before_acquire: bool = config
            .props
            .get("pool.test-before-acquire")
            .map(|v| {
                v.parse::<bool>().map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("invalid pool.test-before-acquire: {e}"),
                    )
                })
            })
            .transpose()?
            .unwrap_or(TEST_BEFORE_ACQUIRE);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .idle_timeout(Duration::from_secs(idle_timeout))
            .test_before_acquire(test_before_acquire)
            .connect(&config.uri)
            .await
            .map_err(from_sqlx_error)?;

        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE IF NOT EXISTS {CATALOG_TABLE_NAME} (
                {CATALOG_FIELD_CATALOG_NAME} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_TABLE_NAMESPACE} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_TABLE_NAME} VARCHAR(255) NOT NULL,
                {CATALOG_FIELD_METADATA_LOCATION_PROP} VARCHAR(1000),
                {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} VARCHAR(1000),
                {CATALOG_FIELD_RECORD_TYPE} VARCHAR(5),
                PRIMARY KEY ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}))"
        )))
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        sqlx::query(AssertSqlSafe(format!(
            "CREATE TABLE IF NOT EXISTS {NAMESPACE_TABLE_NAME} (
                {CATALOG_FIELD_CATALOG_NAME} VARCHAR(255) NOT NULL,
                {NAMESPACE_FIELD_NAME} VARCHAR(255) NOT NULL,
                {NAMESPACE_FIELD_PROPERTY_KEY} VARCHAR(255),
                {NAMESPACE_FIELD_PROPERTY_VALUE} VARCHAR(1000),
                PRIMARY KEY ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}))"
        )))
        .execute(&pool)
        .await
        .map_err(from_sqlx_error)?;

        Ok(SqlCatalog {
            name: config.name.to_owned(),
            connection: pool,
            warehouse_location: config.warehouse_location,
            fileio,
            runtime,
        })
    }

    /// Rewrite the `?` placeholders used by the upstream SQL into the Postgres
    /// `$1..$N` positional form.
    fn replace_placeholders(&self, query: &str) -> String {
        let mut count = 1;
        query
            .chars()
            .fold(String::with_capacity(query.len()), |mut acc, c| {
                if c == '?' {
                    acc.push('$');
                    acc.push_str(&count.to_string());
                    count += 1;
                } else {
                    acc.push(c);
                }
                acc
            })
    }

    /// Fetch a vec of rows from a given query
    async fn fetch_rows(&self, query: &str, args: Vec<Option<&str>>) -> Result<Vec<PgRow>> {
        let query_with_placeholders = self.replace_placeholders(query);

        let mut sqlx_query = sqlx::query(AssertSqlSafe(query_with_placeholders));
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        sqlx_query
            .fetch_all(&self.connection)
            .await
            .map_err(from_sqlx_error)
    }

    /// Execute statements in a transaction, provided or not
    async fn execute(
        &self,
        query: &str,
        args: Vec<Option<&str>>,
        transaction: Option<&mut Transaction<'_, Postgres>>,
    ) -> Result<PgQueryResult> {
        let query_with_placeholders = self.replace_placeholders(query);

        let mut sqlx_query = sqlx::query(AssertSqlSafe(query_with_placeholders));
        for arg in args {
            sqlx_query = sqlx_query.bind(arg);
        }

        match transaction {
            Some(t) => sqlx_query.execute(&mut **t).await.map_err(from_sqlx_error),
            None => {
                let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
                let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error);
                drop(tx.commit().await.map_err(from_sqlx_error));
                result
            }
        }
    }

    /// Physically delete an object-store file by its absolute URL (e.g. a `file://`
    /// or `s3://` Parquet path). Idempotent: a missing object is not an error —
    /// idempotency is provided by the backend (`LocalFsStorage`/S3 both no-op on
    /// absence), not enforced here. This is the only object-store *delete*
    /// capability on the catalog — used by GC to reclaim the Parquet of end-capped
    /// data files; read/write paths are untouched.
    pub async fn delete_file(&self, path: &str) -> control_plane_core::Result<()> {
        self.fileio
            .delete(path)
            .await
            .map_err(|e| control_plane_core::ControlPlaneError::Backend(Box::new(e)))
    }

    /// Write the mirror rows for an already-committed table state, in the caller's
    /// tx, from **precomputed** inputs only. Takes no `&Table` / `FileIO`, so it is
    /// type-level incapable of reading object storage inside the transaction — the
    /// property `iss-iceberg-tx-objectstore` requires (enforced by the signature,
    /// not by convention). The object-store read (`added_files_of`) and schema read
    /// (`columns_of`) are done by the caller before `begin()`.
    ///
    /// Returns the mirror snapshot it allocated so callers can use it for further
    /// in-tx work (e.g. end-capping inline rows at the same snapshot).
    async fn write_mirror(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ident: &TableIdent,
        staged_snap: Option<i64>,
        columns: &[ProjectedColumn],
        files: &[ProjectedFile],
        overwrite: bool,
    ) -> control_plane_core::Result<SnapshotId> {
        use crate::iceberg_mirror::{
            end_cap_live_data_files, ensure_table, next_snapshot, project_files,
            reconcile_and_project, stamp_schema_version,
        };

        let ns = ident.namespace().join(".");
        let name = ident.name();

        let conn = &mut **tx;
        let at = next_snapshot(conn, staged_snap).await?;
        let tid = ensure_table(conn, &ns, name, at).await?;
        // Overwrite/replace: end-cap the pre-existing live files at `at` BEFORE
        // projecting the new ones. The new `project_files` rows are written after this
        // (also `end_snapshot is null`, also `table_id = tid`), so they stay live; only
        // the prior files get `end_snapshot = at`. Ordering is load-bearing — end-capping
        // after `project_files` would wrongly retire the just-projected files too.
        if overwrite {
            end_cap_live_data_files(conn, tid, at).await?;
        }
        reconcile_and_project(conn, tid, at, columns).await?;
        project_files(conn, tid, at, files).await?;
        stamp_schema_version(conn, tid, at).await?;
        Ok(at)
    }

    /// The real commit: pointer CAS + mirror projection (+ optional lineage and/or
    /// inline end-cap), all in one Postgres transaction. `update_table` calls this
    /// with `CommitExtras::default()`; the loom landing path passes a lineage event
    /// so it commits or rolls back together with the snapshot it describes; the
    /// flush path additionally passes an end-cap to retire inline rows at the
    /// same snapshot the new Parquet file becomes live.
    pub(crate) async fn do_update_table(
        &self,
        commit: TableCommit,
        extras: CommitExtras<'_>,
    ) -> Result<Table> {
        let table_ident = commit.identifier().clone();
        let current_table = self.load_table(&table_ident).await?;
        let current_metadata_location = current_table.metadata_location_result()?.to_string();

        let staged_table = commit.apply(current_table)?;
        let staged_metadata_location = staged_table.metadata_location_result()?;
        // iceberg main's `TableMetadata::write_to` takes a typed `&MetadataLocation`
        // (was `&str`); parse the location string commit.apply already computed. The
        // string itself is still used below as the CAS pointer value.
        let staged_ml: MetadataLocation = staged_metadata_location.parse()?;

        staged_table
            .metadata()
            .write_to(staged_table.file_io(), &staged_ml)
            .await?;

        // Object-store reads happen here, BEFORE begin(): load the new snapshot's
        // manifests + Parquet footers and snapshot the staged schema. The manifests
        // are immutable and already persisted (fast_append wrote them; write_to wrote
        // the staged metadata above), so reading them pre-tx is identical to reading
        // them in-tx — no read-after-write hazard, and the CAS still guards the
        // pointer. The transaction below therefore holds only fast local PG work.
        let mirror_files = crate::iceberg_mirror::added_files_of(&staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let mirror_columns = crate::iceberg_mirror::columns_of(&staged_table)
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        let staged_snap = staged_table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());

        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;

        let update_result = self
            .execute(
                &format!(
                    "UPDATE {CATALOG_TABLE_NAME}
                     SET {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?, {CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP} = ?
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )
                      AND {CATALOG_FIELD_METADATA_LOCATION_PROP} = ?"
                ),
                vec![
                    Some(staged_metadata_location),
                    Some(current_metadata_location.as_str()),
                    Some(&self.name),
                    Some(table_ident.name()),
                    Some(&table_ident.namespace().join(".")),
                    Some(current_metadata_location.as_str()),
                ],
                Some(&mut tx),
            )
            .await?;

        if update_result.rows_affected() == 0 {
            drop(tx.rollback().await);
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Commit conflicted for table: {table_ident}"),
            )
            .with_retryable(true));
        }

        let at = self
            .write_mirror(
                &mut tx,
                &table_ident,
                staged_snap,
                &mirror_columns,
                &mirror_files,
                extras.overwrite,
            )
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        if let Some(cap) = &extras.end_cap {
            // Retire the flushed inline rows at the same snapshot the new data file
            // becomes live, so reads never double-serve or drop them.
            let sql = format!(
                "update {} set end_snapshot = {} \
                 where loom_row_id = any($1) and end_snapshot is null",
                crate::iceberg_inline::inline_table_name(cap.table_id),
                at.0,
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(cap.row_ids)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }

        if let Some(ev) = extras.lineage {
            pg_emit(&mut *tx, ev)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(staged_table)
    }
}

#[async_trait]
impl Catalog for SqlCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        // UNION will remove duplicates.
        let all_namespaces_stmt = format!(
            "SELECT {CATALOG_FIELD_TABLE_NAMESPACE}
             FROM {CATALOG_TABLE_NAME}
             WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
             UNION
             SELECT {NAMESPACE_FIELD_NAME}
             FROM {NAMESPACE_TABLE_NAME}
             WHERE {CATALOG_FIELD_CATALOG_NAME} = ?"
        );

        let namespace_rows = self
            .fetch_rows(
                &all_namespaces_stmt,
                vec![Some(&self.name), Some(&self.name)],
            )
            .await?;

        let mut namespaces = HashSet::<NamespaceIdent>::with_capacity(namespace_rows.len());

        if let Some(parent) = parent {
            if self.namespace_exists(parent).await? {
                let parent_str = parent.join(".");

                for row in namespace_rows.iter() {
                    let nsp = row.try_get::<String, _>(0).map_err(from_sqlx_error)?;
                    // if parent = a, then we only want to see a.b, a.c returned.
                    if nsp != parent_str && nsp.starts_with(&parent_str) {
                        namespaces.insert(NamespaceIdent::from_strs(nsp.split("."))?);
                    }
                }

                Ok(namespaces.into_iter().collect::<Vec<NamespaceIdent>>())
            } else {
                no_such_namespace_err(parent)
            }
        } else {
            for row in namespace_rows.iter() {
                let nsp = row.try_get::<String, _>(0).map_err(from_sqlx_error)?;
                let mut levels = nsp.split(".").collect::<Vec<&str>>();
                if !levels.is_empty() {
                    let first_level = levels.drain(..1).collect::<Vec<&str>>();
                    namespaces.insert(NamespaceIdent::from_strs(first_level)?);
                }
            }

            Ok(namespaces.into_iter().collect::<Vec<NamespaceIdent>>())
        }
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let exists = self.namespace_exists(namespace).await?;

        if exists {
            return Err(Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("Namespace {namespace:?} already exists"),
            ));
        }

        let namespace_str = namespace.join(".");
        let insert = format!(
            "INSERT INTO {NAMESPACE_TABLE_NAME} ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}, {NAMESPACE_FIELD_PROPERTY_VALUE})
             VALUES (?, ?, ?, ?)");
        if !properties.is_empty() {
            let mut insert_properties = properties.clone();
            insert_properties.insert("exists".to_string(), "true".to_string());

            let mut query_args = Vec::with_capacity(insert_properties.len() * 4);
            let mut insert_stmt = insert.clone();
            for (index, (key, value)) in insert_properties.iter().enumerate() {
                query_args.extend_from_slice(&[
                    Some(self.name.as_str()),
                    Some(namespace_str.as_str()),
                    Some(key.as_str()),
                    Some(value.as_str()),
                ]);
                if index > 0 {
                    insert_stmt.push_str(", (?, ?, ?, ?)");
                }
            }

            self.execute(&insert_stmt, query_args, None).await?;

            Ok(Namespace::with_properties(
                namespace.clone(),
                insert_properties,
            ))
        } else {
            // set a default property of exists = true
            self.execute(
                &insert,
                vec![
                    Some(&self.name),
                    Some(&namespace_str),
                    Some("exists"),
                    Some("true"),
                ],
                None,
            )
            .await?;
            Ok(Namespace::with_properties(namespace.clone(), properties))
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let namespace_props = self
                .fetch_rows(
                    &format!(
                        "SELECT
                            {NAMESPACE_FIELD_NAME},
                            {NAMESPACE_FIELD_PROPERTY_KEY},
                            {NAMESPACE_FIELD_PROPERTY_VALUE}
                            FROM {NAMESPACE_TABLE_NAME}
                            WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                            AND {NAMESPACE_FIELD_NAME} = ?"
                    ),
                    vec![Some(&self.name), Some(&namespace.join("."))],
                )
                .await?;

            let mut properties = HashMap::with_capacity(namespace_props.len());

            for row in namespace_props {
                let key = row
                    .try_get::<String, _>(NAMESPACE_FIELD_PROPERTY_KEY)
                    .map_err(from_sqlx_error)?;
                let value = row
                    .try_get::<String, _>(NAMESPACE_FIELD_PROPERTY_VALUE)
                    .map_err(from_sqlx_error)?;

                properties.insert(key, value);
            }

            Ok(Namespace::with_properties(namespace.clone(), properties))
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        let namespace_str = namespace.join(".");

        let table_namespaces = self
            .fetch_rows(
                &format!(
                    "SELECT 1 FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                     LIMIT 1"
                ),
                vec![Some(&self.name), Some(&namespace_str)],
            )
            .await?;

        if !table_namespaces.is_empty() {
            Ok(true)
        } else {
            let namespaces = self
                .fetch_rows(
                    &format!(
                        "SELECT 1 FROM {NAMESPACE_TABLE_NAME}
                         WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                          AND {NAMESPACE_FIELD_NAME} = ?
                         LIMIT 1"
                    ),
                    vec![Some(&self.name), Some(&namespace_str)],
                )
                .await?;
            if !namespaces.is_empty() {
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let existing_properties = self.get_namespace(namespace).await?.properties().clone();
            let namespace_str = namespace.join(".");

            let mut updates = vec![];
            let mut inserts = vec![];

            for (key, value) in properties.iter() {
                if existing_properties.contains_key(key) {
                    if existing_properties.get(key) != Some(value) {
                        updates.push((key, value));
                    }
                } else {
                    inserts.push((key, value));
                }
            }

            let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
            let update_stmt = format!(
                "UPDATE {NAMESPACE_TABLE_NAME} SET {NAMESPACE_FIELD_PROPERTY_VALUE} = ?
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                 AND {NAMESPACE_FIELD_NAME} = ?
                 AND {NAMESPACE_FIELD_PROPERTY_KEY} = ?"
            );

            let insert_stmt = format!(
                "INSERT INTO {NAMESPACE_TABLE_NAME} ({CATALOG_FIELD_CATALOG_NAME}, {NAMESPACE_FIELD_NAME}, {NAMESPACE_FIELD_PROPERTY_KEY}, {NAMESPACE_FIELD_PROPERTY_VALUE})
                 VALUES (?, ?, ?, ?)"
            );

            for (key, value) in updates {
                self.execute(
                    &update_stmt,
                    vec![
                        Some(value),
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                    ],
                    Some(&mut tx),
                )
                .await?;
            }

            for (key, value) in inserts {
                self.execute(
                    &insert_stmt,
                    vec![
                        Some(&self.name),
                        Some(&namespace_str),
                        Some(key),
                        Some(value),
                    ],
                    Some(&mut tx),
                )
                .await?;
            }

            let _ = tx.commit().await.map_err(from_sqlx_error)?;

            Ok(())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            // if there are tables in the namespace, don't allow drop.
            let tables = self.list_tables(namespace).await?;
            if !tables.is_empty() {
                return Err(Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Namespace {:?} is not empty. {} tables exist.",
                        namespace,
                        tables.len()
                    ),
                ));
            }

            self.execute(
                &format!(
                    "DELETE FROM {NAMESPACE_TABLE_NAME}
                     WHERE {NAMESPACE_FIELD_NAME} = ?
                      AND {CATALOG_FIELD_CATALOG_NAME} = ?"
                ),
                vec![Some(&namespace.join(".")), Some(&self.name)],
                None,
            )
            .await?;

            Ok(())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let exists = self.namespace_exists(namespace).await?;
        if exists {
            let rows = self
                .fetch_rows(
                    &format!(
                        "SELECT {CATALOG_FIELD_TABLE_NAME},
                                {CATALOG_FIELD_TABLE_NAMESPACE}
                         FROM {CATALOG_TABLE_NAME}
                         WHERE {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                          AND {CATALOG_FIELD_CATALOG_NAME} = ?
                          AND (
                                {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                                OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                          )",
                    ),
                    vec![Some(&namespace.join(".")), Some(&self.name)],
                )
                .await?;

            let mut tables = HashSet::<TableIdent>::with_capacity(rows.len());

            for row in rows.iter() {
                let tbl = row
                    .try_get::<String, _>(CATALOG_FIELD_TABLE_NAME)
                    .map_err(from_sqlx_error)?;
                let ns_strs = row
                    .try_get::<String, _>(CATALOG_FIELD_TABLE_NAMESPACE)
                    .map_err(from_sqlx_error)?;
                let ns = NamespaceIdent::from_strs(ns_strs.split("."))?;
                tables.insert(TableIdent::new(ns, tbl));
            }

            Ok(tables.into_iter().collect::<Vec<TableIdent>>())
        } else {
            no_such_namespace_err(namespace)
        }
    }

    async fn table_exists(&self, identifier: &TableIdent) -> Result<bool> {
        let namespace = identifier.namespace().join(".");
        let table_name = identifier.name();
        let table_counts = self
            .fetch_rows(
                &format!(
                    "SELECT 1
                     FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )"
                ),
                vec![Some(&namespace), Some(&self.name), Some(table_name)],
            )
            .await?;

        if !table_counts.is_empty() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn drop_table(&self, identifier: &TableIdent) -> Result<()> {
        if !self.table_exists(identifier).await? {
            return no_such_table_err(identifier);
        }

        // Pointer delete + mirror drop in one transaction (same atomicity contract
        // as update_table).
        let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
        self.execute(
            &format!(
                "DELETE FROM {CATALOG_TABLE_NAME}
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                  AND (
                    {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                    OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                  )"
            ),
            vec![
                Some(&self.name),
                Some(identifier.name()),
                Some(&identifier.namespace().join(".")),
            ],
            Some(&mut tx),
        )
        .await?;

        // Only end the mirror (and spend a snapshot id) when there is live mirror
        // state to end. A table created via the catalog but never appended has no
        // mirror row — dropping it is just the pointer delete, with no snapshot
        // allocated (which would otherwise leave an orphan snapshot row).
        let ns = identifier.namespace().join(".");
        let unexpected = |e: control_plane_core::ControlPlaneError| {
            Error::new(ErrorKind::Unexpected, e.to_string())
        };
        if crate::iceberg_mirror::live_table_id(&mut tx, &ns, identifier.name())
            .await
            .map_err(unexpected)?
            .is_some()
        {
            let at = crate::iceberg_mirror::next_snapshot(&mut tx, None)
                .await
                .map_err(unexpected)?;
            crate::iceberg_mirror::mark_dropped(&mut tx, &ns, identifier.name(), at)
                .await
                .map_err(unexpected)?;
        }

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(())
    }

    async fn load_table(&self, identifier: &TableIdent) -> Result<Table> {
        if !self.table_exists(identifier).await? {
            return no_such_table_err(identifier);
        }

        let rows = self
            .fetch_rows(
                &format!(
                    "SELECT {CATALOG_FIELD_METADATA_LOCATION_PROP}
                     FROM {CATALOG_TABLE_NAME}
                     WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAME} = ?
                      AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                      AND (
                        {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                        OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                      )"
                ),
                vec![
                    Some(&self.name),
                    Some(identifier.name()),
                    Some(&identifier.namespace().join(".")),
                ],
            )
            .await?;

        if rows.is_empty() {
            return no_such_table_err(identifier);
        }

        let row = rows
            .first()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "expected at least one row"))?;
        let tbl_metadata_location = row
            .try_get::<String, _>(CATALOG_FIELD_METADATA_LOCATION_PROP)
            .map_err(from_sqlx_error)?;

        let metadata = TableMetadata::read_from(&self.fileio, &tbl_metadata_location).await?;

        Ok(Table::builder()
            .file_io(self.fileio.clone())
            .runtime(self.runtime.clone())
            .identifier(identifier.clone())
            .metadata_location(tbl_metadata_location)
            .metadata(metadata)
            .build()?)
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        if !self.namespace_exists(namespace).await? {
            return no_such_namespace_err(namespace);
        }

        let tbl_name = creation.name.clone();
        let tbl_ident = TableIdent::new(namespace.clone(), tbl_name.clone());

        if self.table_exists(&tbl_ident).await? {
            return table_already_exists_err(&tbl_ident);
        }

        let (tbl_creation, location) = match creation.location.clone() {
            Some(location) => (creation, location),
            None => {
                // fall back to namespace-specific location
                // and then to warehouse location
                let nsp_properties = self.get_namespace(namespace).await?.properties().clone();
                let nsp_location = match nsp_properties.get(NAMESPACE_LOCATION_PROPERTY_KEY) {
                    Some(location) => location.clone(),
                    None => {
                        format!(
                            "{}/{}",
                            self.warehouse_location.clone(),
                            namespace.join("/")
                        )
                    }
                };

                let tbl_location = format!("{}/{}", nsp_location, tbl_ident.name());

                (
                    TableCreation {
                        location: Some(tbl_location.clone()),
                        ..creation
                    },
                    tbl_location,
                )
            }
        };

        let tbl_metadata = TableMetadataBuilder::from_table_creation(tbl_creation)?
            .build()?
            .metadata;
        // iceberg main's `write_to` takes a typed `&MetadataLocation`, and
        // `new_with_table_location` is deprecated in favour of `new_with_metadata`
        // (which derives the compression codec from table properties). Build it once;
        // the string form is reused for the SQL insert + the Table builder below.
        let tbl_ml = MetadataLocation::new_with_metadata(location.clone(), &tbl_metadata);
        let tbl_metadata_location = tbl_ml.to_string();

        tbl_metadata.write_to(&self.fileio, &tbl_ml).await?;

        self.execute(&format!(
            "INSERT INTO {CATALOG_TABLE_NAME}
             ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}, {CATALOG_FIELD_METADATA_LOCATION_PROP}, {CATALOG_FIELD_RECORD_TYPE})
             VALUES (?, ?, ?, ?, ?)
            "), vec![Some(&self.name), Some(&namespace.join(".")), Some(&tbl_name.clone()), Some(&tbl_metadata_location), Some(CATALOG_FIELD_TABLE_RECORD_TYPE)], None).await?;

        Ok(Table::builder()
            .file_io(self.fileio.clone())
            .runtime(self.runtime.clone())
            .metadata_location(tbl_metadata_location)
            .identifier(tbl_ident)
            .metadata(tbl_metadata)
            .build()?)
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        if src == dest {
            return Ok(());
        }

        if !self.table_exists(src).await? {
            return no_such_table_err(src);
        }

        if !self.namespace_exists(dest.namespace()).await? {
            return no_such_namespace_err(dest.namespace());
        }

        if self.table_exists(dest).await? {
            return table_already_exists_err(dest);
        }

        self.execute(
            &format!(
                "UPDATE {CATALOG_TABLE_NAME}
                 SET {CATALOG_FIELD_TABLE_NAME} = ?, {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                 WHERE {CATALOG_FIELD_CATALOG_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAME} = ?
                  AND {CATALOG_FIELD_TABLE_NAMESPACE} = ?
                  AND (
                    {CATALOG_FIELD_RECORD_TYPE} = '{CATALOG_FIELD_TABLE_RECORD_TYPE}'
                    OR {CATALOG_FIELD_RECORD_TYPE} IS NULL
                )"
            ),
            vec![
                Some(dest.name()),
                Some(&dest.namespace().join(".")),
                Some(&self.name),
                Some(src.name()),
                Some(&src.namespace().join(".")),
            ],
            None,
        )
        .await?;

        Ok(())
    }

    async fn register_table(
        &self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<Table> {
        if self.table_exists(table_ident).await? {
            return table_already_exists_err(table_ident);
        }

        let metadata = TableMetadata::read_from(&self.fileio, &metadata_location).await?;

        let namespace = table_ident.namespace();
        let tbl_name = table_ident.name().to_string();

        self.execute(&format!(
            "INSERT INTO {CATALOG_TABLE_NAME}
             ({CATALOG_FIELD_CATALOG_NAME}, {CATALOG_FIELD_TABLE_NAMESPACE}, {CATALOG_FIELD_TABLE_NAME}, {CATALOG_FIELD_METADATA_LOCATION_PROP}, {CATALOG_FIELD_RECORD_TYPE})
             VALUES (?, ?, ?, ?, ?)
            "), vec![Some(&self.name), Some(&namespace.join(".")), Some(&tbl_name), Some(&metadata_location), Some(CATALOG_FIELD_TABLE_RECORD_TYPE)], None).await?;

        Ok(Table::builder()
            .identifier(table_ident.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .file_io(self.fileio.clone())
            .runtime(self.runtime.clone())
            .build()?)
    }

    /// Updates an existing table within the SQL catalog. Lineage-free path:
    /// the loom landing path uses `do_update_table(.., CommitExtras { lineage: Some(ev), .. })`
    /// to attach a lineage event atomically (see `iceberg_writer::append_batches_with_lineage`).
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.do_update_table(commit, CommitExtras::default()).await
    }

    /// iceberg main added `purge_table` (drop + physically delete data files) to the
    /// `Catalog` trait. loom reclaims physical bytes through its own mirror-driven GC
    /// (see road-iceberg-gc), not through the catalog, so purge here drops the catalog
    /// entry exactly like `drop_table` — data files are reclaimed by GC, not inline.
    async fn purge_table(&self, identifier: &TableIdent) -> Result<()> {
        self.drop_table(identifier).await
    }
}
