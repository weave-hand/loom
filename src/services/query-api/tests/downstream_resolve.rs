//! Pure-logic test for `resolve_downstream`: turning `JobTemplate`s into concrete
//! `NewJob`s by substituting `@self.<prop>` / `@<prop>` payload leaves from the
//! primary step's resolved row. No fixture.

use control_plane_core::{JobTemplate, NewJob};
use query_api::downstream::resolve_downstream;
use query_api::serving::SqlValue;
use serde_json::json;

fn row() -> Vec<(String, SqlValue)> {
    vec![
        ("id".into(), SqlValue::Int(42)),
        ("name".into(), SqlValue::Text("x".into())),
    ]
}

#[test]
fn resolves_self_prop_refs() {
    let t = vec![JobTemplate {
        kind: "transform".into(),
        payload: json!({ "orderId": "@self.id", "n": "@name" }),
    }];
    let jobs = resolve_downstream(&t, &row());
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].kind, "transform");
    assert_eq!(jobs[0].payload, json!({ "orderId": 42, "n": "x" }));
    assert!(jobs[0].run_at.is_none());
}

#[test]
fn literals_pass_through() {
    let t = vec![JobTemplate {
        kind: "flush_table".into(),
        payload: json!({ "k": 7, "s": "lit" }),
    }];
    let jobs = resolve_downstream(&t, &row());
    assert_eq!(jobs[0].payload, json!({ "k": 7, "s": "lit" }));
}

#[test]
fn empty_downstream_yields_no_jobs() {
    assert!(resolve_downstream(&[], &row()).is_empty());
}

#[test]
fn unresolved_ref_is_left_as_literal_string() {
    // Define-time validation should prevent this, but the resolver must not panic:
    // it sees only `self_row`, which may lack a referenced column. Leave the leaf
    // as-is so the failure is observable downstream rather than a hard crash.
    let t = vec![JobTemplate {
        kind: "transform".into(),
        payload: json!({ "missing": "@self.nope" }),
    }];
    let jobs = resolve_downstream(&t, &row());
    assert_eq!(jobs[0].payload, json!({ "missing": "@self.nope" }));
}

#[test]
fn refs_resolve_inside_nested_arrays_and_objects() {
    let t = vec![JobTemplate {
        kind: "typed_transform".into(),
        payload: json!({
            "batch": [ "@id", { "inner": "@name" } ],
            "meta": { "who": "@name", "count": 3 }
        }),
    }];
    let jobs = resolve_downstream(&t, &row());
    assert_eq!(
        jobs[0].payload,
        json!({
            "batch": [ 42, { "inner": "x" } ],
            "meta": { "who": "x", "count": 3 }
        })
    );
}

#[test]
fn emits_default_run_at_and_priority() {
    let t = vec![JobTemplate {
        kind: "flush_table".into(),
        payload: json!({}),
    }];
    let jobs: Vec<NewJob> = resolve_downstream(&t, &row());
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].run_at.is_none());
    assert_eq!(jobs[0].priority, 0);
}
