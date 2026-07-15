use control_plane_core::{ControlPlaneError, MvWatermarks, WatermarkAdvance};
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_mv_watermarks_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::mv_watermarks_contract(&cp).await;
}

/// A watermark row sitting physically AT 0 must still refuse an advance that makes no forward
/// progress (iss-mv-register-below-reclaimed-floor).
///
/// This is the state the `from == 0` branch of the CAS used to trust its caller in: its upsert
/// (`... on conflict ... do update set next_offset = $4 where next_offset = 0`) MATCHED such a
/// row, so `{from: 0, to: 0}` reported success while the watermark stayed at 0 — with the MV's
/// output committing in the same transaction, so every later run re-read from 0 and re-appended
/// (exactly-once broken). `{from: 0, to: -1}` was worse in principle (a strict rewind), and in
/// practice only bounced off the table's `next_offset >= 0` CHECK as an opaque Backend error.
///
/// The state is reachable: `mv_bootstrap` inserts a row at 0 for an MV whose source's offsets all
/// survive. It is postgres-only (memory has no bootstrap, and an absent row already reads as 0 —
/// the shared contract covers that variant), so it is pinned here rather than in the testkit.
#[tokio::test]
async fn a_watermark_row_at_zero_refuses_a_non_advancing_advance() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    let (mv, tid) = ("s.out", 7_i64);

    // Plant the bootstrapped-at-zero row directly — `mv_bootstrap`'s outcome, without its setup.
    sqlx::query(sqlx::AssertSqlSafe(
        "insert into stream.mv_watermark (mv, source_table_id, bucket, next_offset) \
         values ($1, $2, 0, 0)",
    ))
    .bind(mv)
    .bind(tid)
    .execute(cp.pool())
    .await
    .expect("plant a watermark row at 0");

    for to in [0_i64, -1] {
        let refused = cp
            .advance_mv_watermark(
                mv,
                tid,
                &[WatermarkAdvance {
                    bucket: 0,
                    from: 0,
                    to,
                }],
            )
            .await;
        assert!(
            matches!(refused, Err(ControlPlaneError::Validation(_))),
            "a from=0 advance to {to} against a row at 0 is malformed (Validation), \
             not a silent no-op; got {refused:?}"
        );
        assert_eq!(
            cp.mv_watermarks(mv, tid).await.expect("read").get(&0),
            Some(&0),
            "the refused advance (to = {to}) left the watermark at 0"
        );
    }

    // A strictly-forward advance from the same row still lands.
    cp.advance_mv_watermark(
        mv,
        tid,
        &[WatermarkAdvance {
            bucket: 0,
            from: 0,
            to: 1,
        }],
    )
    .await
    .expect("a strictly-forward advance off a row at 0 is accepted");
    assert_eq!(
        cp.mv_watermarks(mv, tid).await.expect("read").get(&0),
        Some(&1)
    );
}
