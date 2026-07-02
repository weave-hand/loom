//! `add_role_inheritance` must keep the inheritance graph acyclic even when two
//! opposite-direction edges race. The cycle check + insert run inside one
//! advisory-locked transaction, so of two concurrent `A->B` / `B->A` calls exactly
//! one commits and the other is rejected with `Conflict` — they can never both
//! commit and form a cycle. Regression guard for iss-acl-role-cycle-atomic.
use control_plane_core::{Acl, ControlPlaneError, RoleId};
use control_plane_postgres::fixture::PgFixture;

fn rid(s: &str) -> RoleId {
    RoleId(s.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_opposite_edges_cannot_both_commit() {
    let fx = PgFixture::shared();
    let cp = fx.fresh_control_plane().await;

    // Many fresh role pairs: pre-fix the check/insert race fires intermittently
    // (some iteration sees both edges commit -> a cycle -> assert fails). Post-fix
    // the advisory lock serializes the pair, so every iteration is (1 ok, 1 conflict).
    const ITERS: usize = 25;
    for i in 0..ITERS {
        let a = rid(&format!("a_{i}"));
        let b = rid(&format!("b_{i}"));
        cp.define_role(&a).await.expect("define a");
        cp.define_role(&b).await.expect("define b");

        let cp1 = cp.clone();
        let cp2 = cp.clone();
        let (a1, b1) = (a.clone(), b.clone());
        let (a2, b2) = (a.clone(), b.clone());
        let h1 = tokio::spawn(async move { cp1.add_role_inheritance(&a1, &b1).await });
        let h2 = tokio::spawn(async move { cp2.add_role_inheritance(&b2, &a2).await });
        let r1 = h1.await.expect("join 1");
        let r2 = h2.await.expect("join 2");

        let mut oks = 0;
        let mut conflicts = 0;
        for r in [&r1, &r2] {
            match r {
                Ok(()) => oks += 1,
                Err(ControlPlaneError::Conflict(_)) => conflicts += 1,
                Err(e) => panic!("iter {i}: unexpected error {e:?}"),
            }
        }
        assert_eq!(
            (oks, conflicts),
            (1, 1),
            "iter {i}: exactly one edge commits and one is rejected as a cycle; \
             got r1={r1:?} r2={r2:?}"
        );
    }
}
