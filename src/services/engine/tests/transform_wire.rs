//! Engine wire test for `CommitTransform` + `ListFiles.columns_json`: drive the
//! transform commit RPC over the UDS (append, replace, decode errors) and assert
//! `list_files` reports the declared schema — present for live tables (even
//! zero-file ones), absent for unknown tables.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DataFile, DatasetRef, EventType, LineageEvent, OutputMode,
    PageReq, RunId, RunState, RunTrigger, SnapshotId, TableControlPlane, TableRef, TransformBody,
    TransformDef, TransformName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use datafusion_io::{WriteConfig, absolute_data_files, write_dataset};
use engine_wire::client::GrpcQueueClient;
use engine_wire::pb;
use engine_wire::pb::engine_control_client::EngineControlClient;
use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use store_config::{ObjectStoreConfig, WriteStore, build_write_store};
use tonic::transport::{Endpoint, Uri};

// ---- helpers ---------------------------------------------------------------

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn batch(ids: &[i64]) -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch");
    (schema, batch)
}

fn event(inputs: &[TableRef], output: &TableRef, sql: &str) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: inputs.iter().map(DatasetRef::from).collect(),
        outputs: vec![DatasetRef::from(output)],
        payload: serde_json::json!({ "sql": sql }),
    }
}

/// A `WriteStore` rooted at the shared warehouse dir (the same physical root the
/// engine serves), mirroring the worker's store construction in `compact_e2e.rs`.
fn write_store(wh_str: &str) -> WriteStore {
    let mut env_map = HashMap::new();
    env_map.insert("LOOM_WAREHOUSE_URI".to_string(), format!("file://{wh_str}"));
    let cfg = ObjectStoreConfig::parse_from_env(&env_map).expect("store config");
    build_write_store(&cfg).expect("write store")
}

/// Write `ids` as real Parquet under `{schema}/{name}/<run>/` in the shared
/// warehouse and promote to absolute `DataFile`s — exactly what the worker's
/// transform handler will hand `commit_transform`.
async fn write_files(write: &WriteStore, table: &TableRef, ids: &[i64]) -> Vec<DataFile> {
    let (schema, b) = batch(ids);
    let run = uuid::Uuid::new_v4().to_string();
    let written = write_dataset(
        write.store.clone(),
        &format!("{}/{}/{run}", table.schema, table.name),
        schema,
        &[b],
        &WriteConfig::default(),
    )
    .await
    .expect("write_dataset");
    absolute_data_files(written, &write.root_url, &table.schema, &table.name)
}

fn paths(files: &[DataFile]) -> HashSet<String> {
    files.iter().map(|f| f.path.clone()).collect()
}

/// Create an input table with a schema but NO data files (current_snapshot exists,
/// `files` is empty, `schema` resolves) — the empty-input fixture the edge-2 cases need.
async fn create_empty_table(cp: &IcebergControlPlane, table: &TableRef, columns: &[ColumnSpec]) {
    let mut tx = cp.begin_table().await.unwrap();
    tx.create_table(table, columns).await.unwrap();
    // Register the table in the mirror with an EMPTY file list: the table/columns/schema
    // are projected at the snapshot (so `current_snapshot`/`schema` resolve), but no data
    // files exist — the "live input with zero files" fixture. A create-only commit would
    // allocate a snapshot without a mirror `table` row, so the input would read as NotFound.
    tx.append_files(table, &[]).await.unwrap();
    tx.commit().await.unwrap();
}

// ---- tests -----------------------------------------------------------------

/// Append path: write real Parquet into the shared warehouse, commit it over
/// `CommitTransform`, and assert the snapshot resolves, the live set names the
/// committed paths, and the lineage event round-trips (upstream edge + the exact
/// `{"sql": ...}` payload).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_transform_appends_and_emits_lineage() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    let write = write_store(&wh_str);
    let out = tref("main", "t_out");
    let src = tref("main", "src");
    let files = write_files(&write, &out, &[1, 2, 3]).await;
    let ev = event(std::slice::from_ref(&src), &out, "SELECT id FROM src");

    let snap = client
        .commit_transform(
            "main".into(),
            "t_out".into(),
            &columns(),
            &files,
            &ev,
            false,
            None,
        )
        .await
        .expect("commit_transform")
        .expect("snapshot id");

    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&out).await.expect("current snapshot");
    assert_eq!(cur.id.0, snap, "current snapshot is the committed one");
    let live = ice
        .files_with_stats(&out, cur.id)
        .await
        .expect("live files");
    let live_paths: HashSet<String> = live.iter().map(|f| f.path.clone()).collect();
    assert_eq!(
        live_paths,
        paths(&files),
        "live set is exactly the committed files"
    );

    // Lineage round-trips: the input is upstream of the output...
    let ups = cp
        .lineage()
        .upstream(&DatasetRef::from(&out), 1, PageReq::unbounded())
        .await
        .expect("upstream");
    assert!(
        ups.items.iter().any(|d| d.name == "main.src"),
        "upstream of t_out includes the input, got {:?}",
        ups.items
    );

    // ...and the emitted payload is byte-identical to what was sent.
    let payload: serde_json::Value = sqlx::query_scalar(
        "select e.payload from lineage.event e \
         join lineage.event_dataset d on d.event_id = e.event_id and d.direction = 'output' \
         where d.name = $1",
    )
    .bind("main.t_out")
    .fetch_one(&pool)
    .await
    .expect("lineage payload");
    assert_eq!(
        payload,
        serde_json::json!({ "sql": "SELECT id FROM src" }),
        "lineage payload round-trips byte-identically"
    );
}

/// Data-trigger e2e: a downstream physical def whose input is `t_out` is
/// defined before the transform runs; `CommitTransform`'s commit seam (inside
/// `IcebergTx::commit`) matches the committed output against the
/// data-triggered set and queues a run for the downstream def.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_transform_fires_downstream_data_trigger() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    let write = write_store(&wh_str);
    let out = tref("main", "t_out");
    let src = tref("main", "src");

    // Downstream data-triggered def: fires when the transform commits t_out.
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("downstream".into()),
            body: TransformBody::Physical {
                inputs: vec![out.clone()],
                output: tref("main", "downstream_out"),
                sql: "select 1".into(),
                output_mode: OutputMode::Append,
            },
            schedule: None,
            on_input_commit: true,
        })
        .await
        .unwrap();

    let files = write_files(&write, &out, &[1, 2, 3]).await;
    let ev = event(std::slice::from_ref(&src), &out, "SELECT id FROM src");

    let snap = client
        .commit_transform(
            "main".into(),
            "t_out".into(),
            &columns(),
            &files,
            &ev,
            false,
            None,
        )
        .await
        .expect("commit_transform")
        .expect("snapshot id");

    let ice = IcebergCatalog::new(pool);
    let cur = ice.current_snapshot(&out).await.expect("current snapshot");
    assert_eq!(cur.id.0, snap, "current snapshot is the committed one");

    let runs = cp
        .transforms()
        .list_runs(
            Some(&TransformName("downstream".into())),
            PageReq::default(),
        )
        .await
        .unwrap()
        .items;
    assert_eq!(runs.len(), 1, "downstream def has exactly one run");
    assert_eq!(runs[0].trigger, RunTrigger::DataTrigger);
    assert_eq!(runs[0].state, RunState::Queued);
}

/// Replace path: append once, then `replace=true` with a new file. The live set
/// becomes the new file only; the prior snapshot's file list is unchanged
/// (time travel).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_transform_replace_expires_prior_live_set() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    let write = write_store(&wh_str);
    let out = tref("main", "t_replace");
    let src = tref("main", "src");

    let first = write_files(&write, &out, &[1, 2, 3]).await;
    let snap1 = client
        .commit_transform(
            "main".into(),
            "t_replace".into(),
            &columns(),
            &first,
            &event(std::slice::from_ref(&src), &out, "q1"),
            false,
            None,
        )
        .await
        .expect("append commit")
        .expect("append snapshot id");

    let second = write_files(&write, &out, &[4]).await;
    let snap2 = client
        .commit_transform(
            "main".into(),
            "t_replace".into(),
            &columns(),
            &second,
            &event(std::slice::from_ref(&src), &out, "q2"),
            true,
            None,
        )
        .await
        .expect("replace commit")
        .expect("replace snapshot id");
    assert!(snap2 > snap1, "replace allocated a newer snapshot");

    let ice = IcebergCatalog::new(pool);
    let live = ice
        .files_with_stats(&out, SnapshotId(snap2))
        .await
        .expect("live files");
    let live_paths: HashSet<String> = live.iter().map(|f| f.path.clone()).collect();
    assert_eq!(
        live_paths,
        paths(&second),
        "replace made the new file the only live one"
    );

    let then = ice
        .files_with_stats(&out, SnapshotId(snap1))
        .await
        .expect("files at prior snapshot");
    let then_paths: HashSet<String> = then.iter().map(|f| f.path.clone()).collect();
    assert_eq!(
        then_paths,
        paths(&first),
        "the prior snapshot still time-travels to the original files"
    );
}

/// Decode errors are `invalid_argument`: garbage `lineage_json` via the raw pb
/// client (the wrapper client only sends well-formed lineage).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_transform_bad_lineage_json_is_invalid_argument() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;

    let sock = eng.sock.clone();
    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let sock = sock.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(sock).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .expect("connect uds");
    let mut raw = EngineControlClient::new(channel);

    let err = match raw
        .commit_transform(pb::CommitTransformRequest {
            schema: "main".into(),
            name: "t_bad".into(),
            columns_json: serde_json::to_string(&columns()).expect("cols json"),
            write_json: vec![],
            lineage_json: "not lineage".into(),
            replace: false,
            run_id: None,
        })
        .await
    {
        Ok(_) => panic!("garbage lineage_json must be rejected"),
        Err(s) => s,
    };
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().contains("bad lineage_json"),
        "error names the bad field, got: {}",
        err.message()
    );
}

/// `columns_json` is the table-exists discriminator: present (the declared schema)
/// for a seeded table AND for a live zero-file table; absent for an unknown table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_files_reports_columns_and_absence() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;
    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    // Seeded table: one real Parquet file (inline_byte_limit = 0).
    let seeded = tref("main", "seeded");
    let (schema, b) = batch(&[1, 2, 3]);
    land(
        &pool,
        &catalog,
        &seeded,
        &columns(),
        schema,
        vec![b],
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        event(&[], &seeded, "seed"),
        None,
    )
    .await
    .expect("land");

    let listed = client
        .list_files("main".into(), "seeded".into())
        .await
        .expect("list seeded");
    assert_eq!(listed.files.len(), 1, "one landed file");
    assert_eq!(
        listed.columns,
        Some(columns()),
        "seeded table reports its declared columns"
    );

    // Unknown table: empty files, absent columns.
    let unknown = client
        .list_files("main".into(), "nope".into())
        .await
        .expect("list unknown");
    assert!(unknown.files.is_empty(), "unknown table lists no files");
    assert_eq!(unknown.columns, None, "unknown table reports no columns");

    // Zero-file table: columns present, files empty — distinguishable from unknown.
    let icp = IcebergControlPlane::new(cp, local_sql_catalog(fx.pg_dsn(&db), &wh_str).await);
    let empty = tref("main", "empty_in");
    create_empty_table(&icp, &empty, &columns()).await;

    let listed = client
        .list_files("main".into(), "empty_in".into())
        .await
        .expect("list empty");
    assert!(listed.files.is_empty(), "zero-file table lists no files");
    assert_eq!(
        listed.columns,
        Some(columns()),
        "zero-file table still reports its declared columns"
    );
}
