//! Deterministic k-means for the IVF-Flat build.

use super::{Metric, SplitMix64, distance, row_slice, row_slice_mut};

/// "loomvec1" — the fixed seed `IvfFlatIndex::build` feeds `SplitMix64` so the
/// k-means result (hence the serialized blob) is reproducible.
pub(super) const KMEANS_SEED: u64 = 0x6C6F_6F6D_7665_6331;
pub(super) const KMEANS_MAX_ITERS: usize = 20;

/// Deterministic k-means: k-means++ seeded init (metric-consistent) + Lloyd
/// iterations. Returns `(centroids [k*d], assignments [n])`. Requires
/// `1 <= k <= n` and `n >= 1`.
#[expect(
    clippy::needless_range_loop,
    reason = "stride loops over parallel packed arrays (data/centroids/sums/counts) \
              are clearer indexed than zipped"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "loop bounds (i<n, c<k) guarantee every index is valid"
)]
pub(super) fn kmeans(
    data: &[f32],
    d: usize,
    n: usize,
    k: usize,
    metric: Metric,
    rng: &mut SplitMix64,
) -> (Vec<f32>, Vec<u32>) {
    let mut centroids = vec![0.0f32; k * d];
    // --- k-means++ init ---
    let first = rng.next_usize(n);
    row_slice_mut(&mut centroids, d, 0).copy_from_slice(row_slice(data, d, first));
    let mut dist2 = vec![f32::INFINITY; n];
    for c in 1..k {
        let prev = row_slice(&centroids, d, c - 1);
        for i in 0..n {
            let dd = distance(metric, row_slice(data, d, i), prev);
            let dd2 = dd * dd;
            if dd2 < dist2[i] {
                dist2[i] = dd2;
            }
        }
        let sum: f64 = dist2.iter().map(|&x| f64::from(x)).sum();
        let mut target = rng.next_f64() * sum;
        let mut chosen = n - 1;
        for i in 0..n {
            target -= f64::from(dist2[i]);
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        row_slice_mut(&mut centroids, d, c).copy_from_slice(row_slice(data, d, chosen));
    }

    // --- Lloyd iterations ---
    let mut assignments = vec![0u32; n];
    for iter in 0..KMEANS_MAX_ITERS {
        let mut changed = false;
        for i in 0..n {
            let p = row_slice(data, d, i);
            let mut best = 0u32;
            let mut bestd = f32::INFINITY;
            for c in 0..k {
                let dd = distance(metric, p, row_slice(&centroids, d, c));
                if dd < bestd {
                    bestd = dd;
                    best = c as u32;
                }
            }
            if assignments[i] != best {
                changed = true;
                assignments[i] = best;
            }
        }
        if !changed && iter > 0 {
            break;
        }
        // Recompute centroids as the mean of assigned points; empty clusters keep
        // their previous centroid.
        let mut sums = vec![0.0f32; k * d];
        let mut counts = vec![0u32; k];
        for i in 0..n {
            let c = assignments[i] as usize;
            counts[c] += 1;
            let p = row_slice(data, d, i);
            let dst = row_slice_mut(&mut sums, d, c);
            for (s, &x) in dst.iter_mut().zip(p.iter()) {
                *s += x;
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                let cnt = counts[c] as f32;
                let src_sum = row_slice(&sums, d, c).to_vec();
                let dst = row_slice_mut(&mut centroids, d, c);
                for (cv, s) in dst.iter_mut().zip(src_sum.iter()) {
                    *cv = s / cnt;
                }
            }
        }
    }
    (centroids, assignments)
}
