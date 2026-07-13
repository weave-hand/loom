//! The Transforms drawer-action state transitions (Run saved / Delete outcomes)
//! — the DOM-free core of iss-ui-transforms-drawer-errors. External rust_test
//! (no inline #[cfg(test)]) — see CLAUDE.md.

use loom_ui_core::{DrawerActionEffect, delete_action_effect, run_action_effect};

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
