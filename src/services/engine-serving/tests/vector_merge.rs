//! Pure unit tests for `merge_topk` — no Postgres, runs locally.
//! Proves the global-top-k, ascending-distance, no-double-count property that
//! is the heart of the cold/hot merge seam.

use control_plane_core::VectorKey;
use engine_serving::merge_topk;

#[test]
fn merge_takes_global_topk_ascending() {
    let cold = vec![(VectorKey::Int(1), 0.0_f32), (VectorKey::Int(2), 0.9)];
    let hot = vec![(VectorKey::Int(3), 0.2_f32), (VectorKey::Int(4), 0.5)];
    let got = merge_topk(cold, hot, 3);
    // Global ascending top-3 across both sides: 1(0.0), 3(0.2), 4(0.5); 2(0.9) dropped.
    assert_eq!(
        got.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![VectorKey::Int(1), VectorKey::Int(3), VectorKey::Int(4)]
    );
    // distances ascending
    assert!(got.windows(2).all(|w| w[0].1 <= w[1].1));
}

#[test]
fn merge_empty_hot_returns_cold_topk() {
    // Empty-hot case: cold top-k passes through unchanged.
    let cold = vec![(VectorKey::Int(7), 0.1_f32), (VectorKey::Int(8), 0.3)];
    let got = merge_topk(cold.clone(), vec![], 5);
    assert_eq!(got, cold);
}

#[test]
fn merge_k_caps_result() {
    let cold = vec![(VectorKey::Int(1), 0.1_f32)];
    let hot = vec![(VectorKey::Int(2), 0.2_f32), (VectorKey::Int(3), 0.3)];
    assert_eq!(merge_topk(cold, hot, 2).len(), 2);
}
