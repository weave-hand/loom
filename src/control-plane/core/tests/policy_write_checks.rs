//! Pure write-time checks for ACL targets, shared by the memory and postgres
//! adapters (which supply the existence/property lookups).

use std::collections::HashSet;

use control_plane_core::{
    CompareOp, ControlPlaneError, Policy, PolicyTarget, RowFilter, ScalarValue, TableRef, TypeName,
    check_grant_target, check_policy_write,
};

fn ttype(n: &str) -> PolicyTarget {
    PolicyTarget::Type(TypeName(n.into()))
}

fn ttable() -> PolicyTarget {
    PolicyTarget::Table(TableRef {
        schema: "main".into(),
        name: "raw".into(),
    })
}

fn filter(prop: &str) -> RowFilter {
    RowFilter::Compare {
        property: prop.into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("x".into()),
    }
}

fn policy(target: PolicyTarget, row_filter: Option<RowFilter>) -> Policy {
    Policy {
        target,
        row_filter,
        deny_columns: vec![],
        mask_columns: vec![],
    }
}

#[test]
fn key_parts_encodes_both_target_kinds() {
    assert_eq!(
        ttype("T").key_parts(),
        ("type", "T".to_string(), String::new())
    );
    assert_eq!(
        ttable().key_parts(),
        ("table", "main".to_string(), "raw".to_string())
    );
}

#[test]
fn grant_target_checks() {
    assert!(check_grant_target(&ttype("T"), true).is_ok());
    let err = check_grant_target(&ttype("Nope"), false).unwrap_err();
    let ControlPlaneError::Validation(msg) = err else {
        panic!("expected Validation");
    };
    assert_eq!(msg, "grant references unknown type `Nope`");
    // Table targets stay unvalidated (deferred) — exists flag ignored.
    assert!(check_grant_target(&ttable(), false).is_ok());
}

#[test]
fn policy_write_checks() {
    let props: HashSet<String> = ["col".to_string()].into_iter().collect();
    // known type, no filter / valid filter
    assert!(check_policy_write(&policy(ttype("T"), None), Some(&props)).is_ok());
    assert!(check_policy_write(&policy(ttype("T"), Some(filter("col"))), Some(&props)).is_ok());
    // unknown type (always checked, filter or not)
    let err = check_policy_write(&policy(ttype("Nope"), None), None).unwrap_err();
    let ControlPlaneError::Validation(msg) = err else {
        panic!("expected Validation");
    };
    assert_eq!(msg, "policy references unknown type Nope");
    // known type, filter on unknown property
    assert!(matches!(
        check_policy_write(&policy(ttype("T"), Some(filter("ghost"))), Some(&props)),
        Err(ControlPlaneError::Validation(_))
    ));
    // Table target: structural filter validation only, props ignored
    assert!(check_policy_write(&policy(ttable(), Some(filter("anything"))), None).is_ok());
}
