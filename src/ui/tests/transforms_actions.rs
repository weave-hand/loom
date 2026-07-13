//! The Transforms drawer-action state transitions (Run saved / Delete outcomes)
//! — the DOM-free core of iss-ui-transforms-drawer-errors. External rust_test
//! (no inline #[cfg(test)]) — see CLAUDE.md.

use loom_ui_core::{DrawerActionEffect, bump_epoch, delete_action_effect, run_action_effect};

#[test]
fn run_success_opens_runs_tab_and_refetches() {
    let eff = run_action_effect(Ok(()));
    assert_eq!(
        eff,
        DrawerActionEffect {
            error: None,
            open_runs_tab: true,
            refetch_runs: true,
            clear_selection: false,
        },
        "a successful Run clears the error, opens Runs, and forces a refetch \
         (epoch bump) even when the Runs tab is already active"
    );
}

#[test]
fn run_failure_surfaces_the_error_and_stays_on_definition() {
    let eff = run_action_effect(Err("bad transform: no such input".to_string()));
    assert_eq!(
        eff.error.as_deref(),
        Some("bad transform: no such input"),
        "the server message passes through verbatim"
    );
    assert!(
        !eff.open_runs_tab,
        "a failed Run must NOT flip to the Runs tab — the error renders beside \
         the Definition tab's action buttons (behavior change vs the old \
         silent tab flip)"
    );
    assert!(!eff.refetch_runs, "nothing ran; no refetch");
    assert!(!eff.clear_selection, "selection untouched");
}

#[test]
fn delete_success_clears_the_selection() {
    let eff = delete_action_effect(Ok(()));
    assert_eq!(
        eff,
        DrawerActionEffect {
            error: None,
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: true,
        },
        "a successful Delete clears selection + def and reloads the list"
    );
}

#[test]
fn delete_failure_is_loud_and_otherwise_a_no_op() {
    let eff = delete_action_effect(Err("server error (404)".to_string()));
    assert_eq!(
        eff,
        DrawerActionEffect {
            error: Some("server error (404)".to_string()),
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: false,
        },
        "a failed Delete keeps the row + drawer and surfaces the message \
         (behavior change vs the old silent no-op)"
    );
}

// --- The runs-fetch epoch (BUG: the bump must not be derived from a render snapshot) ---
//
// `tf_runs_epoch` is a yew `use_state` that participates in the runs effect's dep
// tuple. A `UseStateHandle` derefs to the value captured at the render that built
// the callback, so `epoch.set(*epoch + 1)` is snapshot-derived: two Run clicks from
// the SAME render (the Run button is not disabled in-flight) both compute `E + 1`,
// the dep tuple does not change on the second response, the runs effect never
// refires, and the Runs tab is left empty with no refetch — i.e. exactly the edge
// this issue fixed, back again.
//
// `bump_epoch` takes the AUTHORITATIVE counter cell (`&mut`), not a snapshot, so
// the fix is encoded in the signature: successive bumps are strictly increasing
// regardless of how many callers hold a stale copy of the rendered value.

#[test]
fn bump_epoch_returns_the_next_value_and_stores_it() {
    let mut epoch = 0u64;
    assert_eq!(bump_epoch(&mut epoch), 1, "the bumped value is returned");
    assert_eq!(epoch, 1, "and written back to the authoritative counter");
}

#[test]
fn two_bumps_from_the_same_render_snapshot_are_distinct() {
    // Both callbacks were built at the render where the epoch read `7`; each Run
    // response bumps the shared counter. A snapshot-derived `*handle + 1` would
    // yield 8 twice — the dep tuple would not change and the runs effect would not
    // refire. The authoritative counter must yield 8 then 9.
    let mut epoch = 7u64;
    let first = bump_epoch(&mut epoch);
    let second = bump_epoch(&mut epoch);
    assert_eq!((first, second), (8, 9));
    assert_ne!(
        first, second,
        "a second Run must invalidate the runs-effect dep tuple, not repeat the \
         value the first Run already set"
    );
}

#[test]
fn bump_epoch_is_monotonic_over_many_bumps() {
    let mut epoch = 0u64;
    let mut prev = epoch;
    for _ in 0..1_000 {
        let next = bump_epoch(&mut epoch);
        assert!(next > prev, "each bump strictly increases the epoch");
        assert_eq!(next, epoch, "the returned value is the stored value");
        prev = next;
    }
}
