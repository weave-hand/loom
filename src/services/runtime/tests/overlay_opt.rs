//! The `overlay_opt` helper is re-exported from loom-config; exercise it via service_runtime.
use std::collections::HashMap;

#[test]
fn service_runtime_reexports_overlay_opt() {
    let mut vars: HashMap<String, String> = HashMap::new();
    vars.insert("LOOM_N".into(), "5".into());
    let mut slot: u32 = 1;
    service_runtime::overlay_opt(&vars, "LOOM_N", &mut slot).unwrap();
    assert_eq!(slot, 5);
    let err = {
        vars.insert("LOOM_N".into(), "x".into());
        service_runtime::overlay_opt(&vars, "LOOM_N", &mut slot).unwrap_err()
    };
    assert!(matches!(err, service_runtime::ConfigError::Invalid { ref var, .. } if var == "LOOM_N"));
}
