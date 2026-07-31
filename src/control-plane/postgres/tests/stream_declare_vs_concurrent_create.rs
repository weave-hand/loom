//! A stream-declaring `land` must not convert a table a CONCURRENT writer created
//! as a batch table (root cause behind iss-stream-first-declare-race-kind's
//! reachability).
//!
//! `ensure_table` silently resolves a lost unique-index race to the winner's
//! table_id, so a witness taken before it says "brand new" about a table another
//! transaction just created, and the batch->stream conversion guard
//! (`reconcile_stream_mode`'s "cannot convert existing batch table") would never
//! fire. The honest witness comes from `ensure_table_witnessed`.
//!
//! Deterministic, no sleeps — the `pg_stat_activity` barrier in
//! `stream_race_support::losing_stream_declare`: connection A holds an uncommitted
//! `iceberg_mirror.table` insert for `s.t`; the landing writer blocks on it; once it
//! is observably blocked we commit A, so the writer takes exactly the lost-race path.
//! loom_fixture_test (Postgres).

use control_plane_core::{ControlPlaneError, TableRef};
use control_plane_postgres::fixture::PgFixture;
use stream_race_support::{harness, losing_stream_declare};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_declare_losing_the_create_race_cannot_convert_the_winners_table() {
    let fx = PgFixture::shared();
    let (_cp, _db, _wh, catalog, pool) = harness(fx).await;
    let table = TableRef {
        schema: "s".into(),
        name: "t".into(),
    };

    let res = losing_stream_declare(&pool, &catalog, &table, 2).await;
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("cannot convert existing batch table")),
        "a stream declare that LOSES the create race must see the winner's table as \
         pre-existing and refuse the batch->stream conversion, got {res:?}"
    );
}
