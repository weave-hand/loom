//! Postgres adapter for the control-plane traits, backed by sqlx.
//!
//! `PgControlPlane` wraps a sqlx `PgPool`; `begin()` opens a real Postgres
//! transaction. The [`fixture`] module boots an ephemeral, hermetic Postgres for
//! tests.
//!
//! SQL is issued through sqlx's runtime query API (no compile-time `query!`
//! macros / `.sqlx` offline metadata yet).

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Cardinality, Catalog, ColumnDef, ControlPlane, ControlPlaneError, FileRef, Job, JobId, LinkDef,
    NewJob, ObjectType, Ontology, PropertyDef, Queue, Result, RetryPolicy, Snapshot, SnapshotId,
    TableRef, TableSchema, Tx, TypeName,
};
use sqlx::{PgPool, Postgres, Row as _};
use time::OffsetDateTime;
use uuid::Uuid;

pub mod fixture;

/// Postgres-backed control plane over a sqlx connection pool.
#[derive(Clone)]
pub struct PgControlPlane {
    pool: PgPool,
    lock_timeout: Duration,
}

impl PgControlPlane {
    /// Wrap an existing connection pool. Callers own pool setup (the test fixture
    /// builds one per fresh database; services will build one at startup).
    pub fn new(pool: PgPool, lock_timeout: Duration) -> Self {
        Self { pool, lock_timeout }
    }
}

/// Apply pending migrations from `migrations_dir` (tracked in `_sqlx_migrations`).
pub async fn run_migrations(pool: &PgPool, migrations_dir: &Path) -> Result<()> {
    let migrator = sqlx::migrate::Migrator::new(migrations_dir)
        .await
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    migrator
        .run(pool)
        .await
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    Ok(())
}

async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<JobId> {
    let id = Uuid::new_v4();
    // Insert and fire the wakeup in a single statement: pg_notify inside a
    // transaction is buffered until commit, so a rolled-back enqueue is silent.
    // Channel is per-kind so workers only wake for kinds they handle.
    sqlx::query(
        "with ins as ( \
             insert into queue.jobs (id, kind, payload, state, run_at, priority) \
             values ($1, $2, $3, 'available', coalesce($4, now()), $5) \
             returning kind) \
         select pg_notify('loom_queue:' || kind, '') from ins",
    )
    .bind(id)
    .bind(&job.kind)
    .bind(&job.payload)
    .bind(job.run_at)
    .bind(job.priority)
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(JobId(id))
}

fn row_to_job(row: &sqlx::postgres::PgRow) -> Job {
    Job {
        id: JobId(row.get("id")),
        kind: row.get("kind"),
        payload: row.get("payload"),
        attempts: row.get("attempts"),
        run_at: row.get("run_at"),
    }
}

#[async_trait]
impl Queue for PgControlPlane {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        pg_insert(&self.pool, &job).await
    }

    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>> {
        let cutoff = OffsetDateTime::now_utc() - self.lock_timeout;
        let row = sqlx::query(
            "update queue.jobs set state='running', locked_at=now(), locked_by=$1, \
                 attempts=attempts+1, updated_at=now() \
             where id = ( \
                 select id from queue.jobs \
                 where kind = any($2) and run_at <= now() \
                   and (state='available' or (state='running' and locked_at < $3)) \
                 order by priority desc, run_at asc \
                 for update skip locked limit 1) \
             returning id, kind, payload, attempts, run_at",
        )
        .bind(worker)
        .bind(kinds)
        .bind(cutoff)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(row_to_job))
    }

    async fn complete(&self, id: JobId) -> Result<()> {
        sqlx::query("delete from queue.jobs where id = $1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()> {
        match policy {
            RetryPolicy::Retry { delay } => {
                let run_at = OffsetDateTime::now_utc() + delay;
                sqlx::query(
                    "update queue.jobs set state='available', run_at=$2, locked_at=null, \
                         locked_by=null, last_error=$3, updated_at=now() where id=$1",
                )
                .bind(id.0)
                .bind(run_at)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
            RetryPolicy::Abandon => {
                sqlx::query(
                    "update queue.jobs set state='failed', locked_at=null, locked_by=null, \
                         last_error=$2, updated_at=now() where id=$1",
                )
                .bind(id.0)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        sqlx::query("update queue.jobs set locked_at=now() where id=$1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn await_jobs(&self, kinds: &[String], timeout: Duration) -> Result<()> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(backend)?;
        let channels: Vec<String> = kinds.iter().map(|k| format!("loom_queue:{k}")).collect();
        listener
            .listen_all(channels.iter().map(String::as_str))
            .await
            .map_err(backend)?;
        // A notification, or the polling-fallback timeout — whichever first.
        let _ = tokio::time::timeout(timeout, listener.recv()).await;
        Ok(())
    }
}

#[async_trait]
impl ControlPlane for PgControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        let tx = self.pool.begin().await.map_err(backend)?;
        Ok(Box::new(PgTx { tx }))
    }
}

struct PgTx {
    tx: sqlx::Transaction<'static, Postgres>,
}

#[async_trait]
impl Tx for PgTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.tx.commit().await.map_err(backend)
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        self.tx.rollback().await.map_err(backend)
    }
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        pg_insert(&mut *self.tx, &job).await
    }
}

#[async_trait]
impl Catalog for PgControlPlane {
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let row = sqlx::query(
            "select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
             from ducklake_snapshot sn \
             where exists ( \
                 select 1 from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
                 where s.schema_name = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
        )
        .bind(&table.schema)
        .bind(&table.name)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(row_to_snapshot(&row))
    }

    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>> {
        let rows = sqlx::query(
            "select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
             from ducklake_snapshot sn \
             where exists ( \
                 select 1 from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
                 where s.schema_name = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id",
        )
        .bind(&table.schema)
        .bind(&table.name)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{}",
                table.schema, table.name
            )));
        }
        Ok(rows.iter().map(row_to_snapshot).collect())
    }

    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query(
            "select path, record_count, file_size_bytes from ducklake_data_file \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by data_file_id",
        )
        .bind(tid)
        .bind(at.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| FileRef {
                path: r.get("path"),
                record_count: r.get("record_count"),
                file_size_bytes: r.get("file_size_bytes"),
            })
            .collect())
    }

    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query(
            "select column_order, column_name, column_type, nulls_allowed from ducklake_column \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by column_order",
        )
        .bind(tid)
        .bind(at.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(TableSchema {
            columns: rows
                .iter()
                .map(|r| ColumnDef {
                    order: r.get("column_order"),
                    name: r.get("column_name"),
                    ty: r.get("column_type"),
                    nullable: r.get("nulls_allowed"),
                })
                .collect(),
        })
    }
}

impl PgControlPlane {
    /// Resolve the `table_id` of `table` live at snapshot `at`, or `NotFound`.
    async fn resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            "select t.table_id from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 \
               and t.begin_snapshot <= $3 and (t.end_snapshot is null or t.end_snapshot > $3)",
        )
        .bind(&table.schema)
        .bind(&table.name)
        .bind(at.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
        })
    }
}

fn row_to_snapshot(row: &sqlx::postgres::PgRow) -> Snapshot {
    Snapshot {
        id: SnapshotId(row.get("snapshot_id")),
        time: row.get("snapshot_time"),
        schema_version: row.get("schema_version"),
    }
}

fn backend(e: sqlx::Error) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}

fn cardinality_to_str(c: Cardinality) -> &'static str {
    match c {
        Cardinality::One => "one",
        Cardinality::Many => "many",
    }
}

fn cardinality_from_str(s: &str) -> Cardinality {
    match s {
        "many" => Cardinality::Many,
        _ => Cardinality::One,
    }
}

#[async_trait]
impl Ontology for PgControlPlane {
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query(
            "insert into ontology.object_type (name, table_schema, table_name) \
             values ($1, $2, $3) \
             on conflict (name) do update set table_schema = excluded.table_schema, \
                 table_name = excluded.table_name",
        )
        .bind(&ty.name.0)
        .bind(&ty.table.schema)
        .bind(&ty.table.name)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query("delete from ontology.property where type_name = $1")
            .bind(&ty.name.0)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        for (i, p) in ty.properties.iter().enumerate() {
            sqlx::query(
                "insert into ontology.property (type_name, ordinal, name, ty, required) \
                 values ($1, $2, $3, $4, $5)",
            )
            .bind(&ty.name.0)
            .bind(i as i32)
            .bind(&p.name)
            .bind(&p.ty)
            .bind(p.required)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn define_link(&self, link: LinkDef) -> Result<()> {
        for endpoint in [&link.from, &link.to] {
            let exists: bool = sqlx::query_scalar(
                "select exists (select 1 from ontology.object_type where name = $1)",
            )
            .bind(&endpoint.0)
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;
            if !exists {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        sqlx::query(
            "insert into ontology.link (name, from_type, to_type, cardinality) \
             values ($1, $2, $3, $4) \
             on conflict (name, from_type) do update set to_type = excluded.to_type, \
                 cardinality = excluded.cardinality",
        )
        .bind(&link.name)
        .bind(&link.from.0)
        .bind(&link.to.0)
        .bind(cardinality_to_str(link.cardinality))
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        let row = sqlx::query(
            "select table_schema, table_name from ontology.object_type where name = $1",
        )
        .bind(&name.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        let props = sqlx::query(
            "select name, ty, required from ontology.property \
             where type_name = $1 order by ordinal",
        )
        .bind(&name.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(ObjectType {
            name: name.clone(),
            table: TableRef {
                schema: row.get("table_schema"),
                name: row.get("table_name"),
            },
            properties: props
                .iter()
                .map(|r| PropertyDef {
                    name: r.get("name"),
                    ty: r.get("ty"),
                    required: r.get("required"),
                })
                .collect(),
        })
    }

    async fn list_types(&self) -> Result<Vec<ObjectType>> {
        let names: Vec<String> = sqlx::query_scalar("select name from ontology.object_type")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(self.get_type(&TypeName(n)).await?);
        }
        Ok(out)
    }

    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>> {
        let exists: bool = sqlx::query_scalar(
            "select exists (select 1 from ontology.object_type where name = $1)",
        )
        .bind(&name.0)
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        if !exists {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query(
            "select name, from_type, to_type, cardinality from ontology.link where from_type = $1",
        )
        .bind(&name.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| LinkDef {
                name: r.get("name"),
                from: TypeName(r.get("from_type")),
                to: TypeName(r.get("to_type")),
                cardinality: cardinality_from_str(r.get::<String, _>("cardinality").as_str()),
            })
            .collect())
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        let row = sqlx::query(
            "select table_schema, table_name from ontology.object_type where name = $1",
        )
        .bind(&name.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        Ok(TableRef {
            schema: row.get("table_schema"),
            name: row.get("table_name"),
        })
    }
}
