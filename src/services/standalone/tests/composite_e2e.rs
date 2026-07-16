//! End-to-end: the standalone composite boots embedded PG, serves engine+ingest+
//! query-api+worker together, round-trips a dataset (ingest POST -> query-api GET
//! over the engine UDS), drains a queued job through its in-process worker, and
//! shuts down cleanly on signal.
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{NewJob, Queue};
use e2e_support::{define_widget, grant_read, session_token, subject_with_role};
use standalone::StandaloneAddrs;

/// Build an embedded-mode Config pointed at the fixture PG binaries + a temp data
/// dir. `POSTGRES_BIN_DIR` / `POSTGRES_LD_LIBRARY_PATH` are injected by the
/// `loom_fixture_test` macro. `Config::from_map` derives the embedded data dir as
/// `<LOOM_DATA_PATH>/pgdata` and the socket dir as `<LOOM_DATA_PATH>/pgrun`
/// (verified in `src/services/runtime/src/lib.rs:193-194`) — so `LOOM_DB_HOST`
/// MUST be `<LOOM_DATA_PATH>/pgrun` (there are no `LOOM_PG_DATA_DIR`/
/// `LOOM_PG_SOCKET_DIR` env vars; do not invent them).
fn embedded_config(data_path: &std::path::Path) -> service_runtime::Config {
    use std::collections::HashMap;
    let bin_dir = std::env::var("POSTGRES_BIN_DIR").unwrap();
    let ld = std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap();
    let mut v: HashMap<String, String> = HashMap::new();
    v.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:0".into()); // unused by the composite
    v.insert(
        "LOOM_DB_HOST".into(),
        data_path.join("pgrun").display().to_string(),
    );
    v.insert("LOOM_DB_PORT".into(), "5432".into());
    v.insert("LOOM_DB_USER".into(), "postgres".into());
    v.insert("LOOM_DB_PASSWORD".into(), "postgres".into());
    v.insert("LOOM_DB_NAME".into(), "loom".into());
    v.insert("LOOM_DATA_PATH".into(), data_path.display().to_string());
    v.insert(
        "LOOM_WAREHOUSE_URI".into(),
        format!("file://{}", data_path.join("warehouse").display()),
    );
    v.insert("LOOM_PG_MODE".into(), "embedded".into());
    v.insert("LOOM_PG_BIN_DIR".into(), bin_dir);
    v.insert("LOOM_PG_LD_LIBRARY_PATH".into(), ld);
    service_runtime::Config::from_map(&v).expect("config")
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// Arrow IPC stream for two `main.widget` rows (id, name, qty). Mirrors the
/// `ipc_bytes(sample_batch())` pattern in `src/services/ingest/tests/http_model.rs`.
fn widget_ipc() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10_i64, 20])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// POST the widget batch at the composite's ingest port and assert it lands —
/// proving the ingest composition (HTTP -> materializer -> Iceberg write ->
/// snapshot commit on the shared PG).
async fn land_widgets_via_ingest(
    client: &reqwest::Client,
    ingest: std::net::SocketAddr,
    token: &str,
) {
    let land = client
        .post(format!("http://{ingest}/datasets/main/widget"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/vnd.apache.arrow.stream")
        .body(widget_ipc())
        .send()
        .await
        .expect("ingest POST");
    assert!(
        land.status().is_success(),
        "ingest landing failed: {}",
        land.status()
    );
}

/// GET `/objects/Widget` from the composite's query-api port and assert both
/// landed rows come back — proving the query-api -> engine UDS serving wiring
/// reads what ingest wrote through the one shared catalog.
async fn assert_reads_back_widgets(
    client: &reqwest::Client,
    qapi: std::net::SocketAddr,
    token: &str,
) {
    let read = client
        .get(format!("http://{qapi}/objects/Widget"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("query-api GET");
    assert_eq!(read.status(), reqwest::StatusCode::OK, "read status");
    let body: serde_json::Value = read.json().await.expect("json body");
    let objects = body
        .get("objects")
        .and_then(|o| o.as_array())
        .expect("objects array");
    assert_eq!(objects.len(), 2, "expected 2 landed widgets, got {body}");
}

/// Enqueue a `flush_table` job for the landed `main.widget` and assert the
/// composite's in-process worker drains it.
///
/// "Drained" is "the row is gone": only `Queue::complete` deletes it
/// (postgres/src/queue.rs:108) — Retry leaves `state='available'` and Abandon
/// leaves `state='failed'` (:120-140) — so an abandoned or retry-looping job
/// fails this rather than passing it.
///
/// The flush itself is a NO-OP: ingest's POST goes through the landing
/// materializer to Parquet and never writes the inline tier, so `main.widget` has
/// no live inline rows and `flush_locked` returns `Ok(None)`. That is enough —
/// the point is that the loop dequeues, dispatches, and completes at all.
async fn assert_queued_job_drains(cp: &impl Queue, pool: &sqlx::PgPool) {
    let job = cp
        .enqueue(NewJob {
            kind: control_plane_core::FLUSH_JOB_KIND.to_string(),
            payload: serde_json::json!({ "schema": "main", "name": "widget" }),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue flush_table job");

    let drained = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let n: i64 = sqlx::query_scalar("select count(*) from queue.jobs where id = $1")
                .bind(job.0)
                .fetch_one(pool)
                .await
                .expect("count queued job");
            if n == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if drained.is_err() {
        let row: Option<(String, i32, Option<String>)> =
            sqlx::query_as("select state, attempts, last_error from queue.jobs where id = $1")
                .bind(job.0)
                .fetch_optional(pool)
                .await
                .expect("read job state");
        panic!(
            "the flush_table job was never drained in 60s; (state, attempts, last_error) = {row:?} \
             — state='available' with attempts=0 means nothing dequeued it (no worker composed); \
             state='failed' means a worker ran it and abandoned it"
        );
    }
}

#[tokio::test]
async fn composite_round_trips_and_shuts_down_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("warehouse")).unwrap();

    // `cfg` moves into the composite; clone it so the test can open its own direct
    // control-plane pool to the same embedded PG for out-of-band ontology + auth seeding.
    let cfg = embedded_config(tmp.path());
    let cfg_direct = cfg.clone();

    let engine_socket = tmp.path().join("engine.sock").display().to_string();
    let addrs = StandaloneAddrs {
        query_api: format!("127.0.0.1:{}", free_port().await).parse().unwrap(),
        ingest: format!("127.0.0.1:{}", free_port().await).parse().unwrap(),
        engine_socket,
    };
    let qapi = addrs.query_api;
    let ingest = addrs.ingest;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let tuning =
        standalone::StandaloneTuning::from_map(&std::collections::HashMap::new()).expect("tuning");
    let handle = tokio::spawn(async move {
        standalone::run(
            cfg,
            addrs,
            tuning,
            async move {
                drop(shutdown_rx.await);
            },
            ready_tx,
        )
        .await
    });

    // Composite is up once every listener is bound + the engine is serving.
    tokio::time::timeout(Duration::from_secs(90), ready_rx)
        .await
        .expect("composite did not become ready in 90s")
        .expect("ready channel dropped");

    // Direct control-plane pool to the same embedded PG for seeding auth + ontology.
    let pool = service_runtime::build_pool(&cfg_direct.db)
        .await
        .expect("direct pool");
    let cp = service_runtime::control_plane(pool.clone(), cfg_direct.lock_timeout);
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    let token = session_token(&cp, "reader").await;

    let client = reqwest::Client::new();

    // (1) Ingest POST lands `main.widget` over HTTP — proves the ingest composition
    //     (HTTP -> materializer -> Iceberg write -> snapshot commit on the shared PG).
    land_widgets_via_ingest(&client, ingest, &token).await;

    // (2) Define the Widget ontology type over the landed `main.widget` table + grant read.
    define_widget(&cp).await;
    grant_read(&cp, &role, "Widget").await;

    // (3) query-api GET /objects/Widget — proves the query-api -> engine UDS serving
    //     wiring, reading back the rows ingest just landed through the one shared catalog.
    assert_reads_back_widgets(&client, qapi, &token).await;

    // (3.5) The composite runs a worker in-process, so a queued job drains.
    assert_queued_job_drains(&cp, &pool).await;

    // (4) Graceful shutdown: signal -> composite returns Ok, embedded PG stopped cleanly.
    shutdown_tx.send(()).unwrap();
    let res = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("composite did not shut down within 30s");
    assert!(res.unwrap().is_ok());

    // Data dir survives (no re-initdb next boot) and PG stopped cleanly (no orphan postmaster).
    assert!(
        tmp.path().join("pgdata").join("PG_VERSION").exists(),
        "data dir gone"
    );
    assert!(
        !tmp.path().join("pgdata").join("postmaster.pid").exists(),
        "postmaster.pid left behind — PG not stopped cleanly"
    );
}
