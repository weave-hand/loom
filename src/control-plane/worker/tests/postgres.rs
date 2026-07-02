use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use control_plane_core::{NewJob, Queue};
use control_plane_postgres::fixture::PgFixture;
use control_plane_worker::Worker;
use tokio_util::sync::CancellationToken;

fn job(kind: &str) -> NewJob {
    NewJob {
        kind: kind.into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    }
}

const KIND: &str = "t";

// N enqueued jobs are all completed by a worker running against real Postgres.
#[tokio::test]
async fn worker_drains_postgres_jobs() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    for _ in 0..5 {
        cp.enqueue(job(KIND)).await.unwrap();
    }
    let count = Arc::new(AtomicU32::new(0));
    let c = count.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    let worker = Worker::new(cp.clone(), "w1", Duration::from_millis(300))
        .with_poll_interval(Duration::from_millis(100));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    for _ in 0..100 {
        if count.load(Ordering::SeqCst) == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 5);
}

// NOTIFY (not the poll fallback) delivers a job: with a long poll interval, a job
// enqueued after the worker is idle still completes quickly.
#[tokio::test]
async fn notify_delivers_before_poll_timeout() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    let done = Arc::new(AtomicU32::new(0));
    let d = done.clone();
    let token = CancellationToken::new();
    let t = token.clone();

    // Poll fallback is 30s; only NOTIFY can make this finish promptly.
    let worker = Worker::new(cp.clone(), "w1", Duration::from_millis(300))
        .with_poll_interval(Duration::from_secs(30));
    let handle = tokio::spawn(async move {
        worker
            .run(&[KIND.to_string()], t, move |_job| {
                let d = d.clone();
                async move {
                    d.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    let start = std::time::Instant::now();
    cp.enqueue(job(KIND)).await.unwrap();
    for _ in 0..100 {
        if done.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(done.load(Ordering::SeqCst), 1, "job was processed");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "NOTIFY delivered the job well before the 30s poll, took {:?}",
        start.elapsed()
    );
}
