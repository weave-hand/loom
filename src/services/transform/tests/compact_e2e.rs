//! Compaction e2e: land three small files into one table, compact_table coalesces them
//! into a single file (DuckDB read-back sees the same rows), the pre-compaction snapshot
//! still time-travels to the three originals, and a second compaction is a no-op (one
//! file left -> fewer than two small files -> None).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{Catalog, DatasetRef, EventType, LineageEvent, PageReq, RunId, TableRef};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use time::OffsetDateTime;
use transform::{CompactConfig, WriteConfig, compact_table};
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

/// Land one batch into `table` under a distinct file prefix (one append -> one data file).
async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
    prefix: &str,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: prefix,
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_coalesces_small_files_and_preserves_time_travel() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    let acc = tref("main", "acc");
    let row = |id: i64, label: &str| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(label.to_string())])),
            ],
        )
        .unwrap()
    };

    // Three appends -> three small data files.
    land(&cp, &store, &acc, schema.clone(), row(1, "a"), "run-1").await;
    land(&cp, &store, &acc, schema.clone(), row(2, "b"), "run-2").await;
    land(&cp, &store, &acc, schema.clone(), row(3, "c"), "run-3").await;

    let before = cp.current_snapshot(&acc).await.unwrap().id;
    let files_before = cp.files(&acc, before, PageReq::unbounded()).await.unwrap();
    assert_eq!(files_before.len(), 3, "three small files before compaction");

    // Compact: 10 MiB threshold (all three qualify), default 128 MiB output target -> 1 file.
    let cfg = CompactConfig {
        small_file_threshold_bytes: 10 * 1024 * 1024,
        write: WriteConfig::default(),
    };
    let snap = compact_table(&cp, store.clone(), &format!("file://{}", writer.data_path().display()), "compact-1", &acc, &cfg)
        .await
        .unwrap()
        .expect("compaction produced a snapshot");

    let files_after = cp.files(&acc, snap, PageReq::unbounded()).await.unwrap();
    assert_eq!(files_after.len(), 1, "three small files coalesced into one");

    // DuckDB read-back: the row set is unchanged.
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.acc;")
        .await;
    assert_eq!(count, "3", "compaction preserves the row set");
    let labels = writer
        .query_scalar("SELECT string_agg(label, ',' ORDER BY id) FROM lake.main.acc;")
        .await;
    assert_eq!(labels, "a,b,c", "values intact after rewrite");

    // Time travel: the pre-compaction snapshot still lists the three originals.
    let files_then = cp.files(&acc, before, PageReq::unbounded()).await.unwrap();
    assert_eq!(
        files_then.len(),
        3,
        "prior snapshot retains the original three files (time travel)"
    );

    // No-op: one coalesced file remains (< 2 small files) -> None, no new snapshot.
    let again = compact_table(&cp, store.clone(), &format!("file://{}", writer.data_path().display()), "compact-2", &acc, &cfg)
        .await
        .unwrap();
    assert!(again.is_none(), "fewer than two small files -> no-op");
    let head = cp.current_snapshot(&acc).await.unwrap().id;
    assert_eq!(head, snap, "no-op created no new snapshot");
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_leaves_large_files_untouched() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));

    let mixed = tref("main", "mixed");
    let row = |id: i64, label: &str| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(label.to_string())])),
            ],
        )
        .unwrap()
    };

    // Three 1-row (small) files plus one 200-row (large) file.
    land(&cp, &store, &mixed, schema.clone(), row(1, "a"), "s1").await;
    land(&cp, &store, &mixed, schema.clone(), row(2, "b"), "s2").await;
    land(&cp, &store, &mixed, schema.clone(), row(3, "c"), "s3").await;
    let big = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..200).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(
                (0..200).map(|i| Some(format!("x{i}"))).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    land(&cp, &store, &mixed, schema.clone(), big, "big").await;

    // Set the threshold to the largest file's exact size so the 200-row file is NOT a
    // candidate (`file_size_bytes < threshold` is false at equality) while the three
    // 1-row files are. Reading sizes from the catalog keeps the test robust to parquet
    // size variance rather than hard-coding byte counts.
    let before = cp.current_snapshot(&mixed).await.unwrap().id;
    let live = cp
        .files(&mixed, before, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(live.len(), 4, "four files before compaction");
    let large = live
        .items
        .iter()
        .max_by_key(|f| f.file_size_bytes)
        .unwrap()
        .clone();

    let cfg = CompactConfig {
        small_file_threshold_bytes: large.file_size_bytes,
        write: WriteConfig::default(),
    };
    let snap = compact_table(&cp, store.clone(), &format!("file://{}", writer.data_path().display()), "mix-1", &mixed, &cfg)
        .await
        .unwrap()
        .expect("the three small files were compacted");

    // After: the large file is still live (path unchanged) plus exactly one coalesced file.
    let after = cp.files(&mixed, snap, PageReq::unbounded()).await.unwrap();
    assert_eq!(after.len(), 2, "large file untouched + one coalesced file");
    assert!(
        after.items.iter().any(|f| f.path == large.path),
        "the large file is left live with its path unchanged"
    );

    // Row set preserved: 3 small + 200 large = 203.
    let count = writer
        .query_scalar("SELECT count(*) FROM lake.main.mixed;")
        .await;
    assert_eq!(
        count, "203",
        "compaction over a mixed set preserves all rows"
    );
}
