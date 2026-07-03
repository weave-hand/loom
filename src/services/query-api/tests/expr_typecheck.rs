//! Bottom-up type inference over the computed-assignment grammar + the property-assignability
//! rule. Rejection matrix: unknown refs, forward/undeclared property refs, operator/arg type
//! errors, unknown-function arity.

use std::collections::{HashMap, HashSet};

use control_plane_core::BaseType;
use query_api::expr::{TypeEnv, TypeError, assignable, parse_expr, typecheck};

struct Env {
    params: HashMap<String, BaseType>,
    props: HashMap<String, BaseType>,
    resolved: HashSet<String>,
}
impl TypeEnv for Env {
    fn param_type(&self, name: &str) -> Option<BaseType> {
        self.params.get(name).copied()
    }
    fn prop_type(&self, name: &str) -> Option<BaseType> {
        self.props.get(name).copied()
    }
    fn prop_resolved(&self, name: &str) -> bool {
        self.resolved.contains(name)
    }
}

fn env() -> Env {
    Env {
        params: HashMap::from([
            ("qty".into(), BaseType::Long),
            ("unitPrice".into(), BaseType::Double),
            ("name".into(), BaseType::String),
            ("active".into(), BaseType::Boolean),
        ]),
        props: HashMap::from([
            ("total".into(), BaseType::Double),
            ("tier".into(), BaseType::String),
            ("later".into(), BaseType::Long),
        ]),
        resolved: HashSet::from(["total".into()]),
    }
}

fn ty(src: &str) -> Result<BaseType, TypeError> {
    typecheck(&parse_expr(src).unwrap(), &env())
}

#[test]
fn arithmetic_widens_to_double() {
    assert_eq!(ty("qty * unitPrice").unwrap(), BaseType::Double);
    assert_eq!(ty("qty + 1").unwrap(), BaseType::Long);
}

#[test]
fn comparison_and_conditional() {
    assert_eq!(ty("@total > 100").unwrap(), BaseType::Boolean);
    assert_eq!(
        ty("if @total > 100 then \"gold\" else \"std\"").unwrap(),
        BaseType::String
    );
}

#[test]
fn string_and_functions() {
    assert_eq!(ty("upper(name) ++ \"!\"").unwrap(), BaseType::String);
    assert_eq!(ty("length(name)").unwrap(), BaseType::Long);
    assert_eq!(ty("now()").unwrap(), BaseType::Timestamp);
    assert_eq!(ty("substr(name, 1, 3)").unwrap(), BaseType::String);
    assert_eq!(ty("cast(qty as double)").unwrap(), BaseType::Double);
    assert_eq!(ty("coalesce(name, \"x\")").unwrap(), BaseType::String);
}

#[test]
fn unknown_param_rejected() {
    assert!(matches!(ty("nope + 1"), Err(TypeError::UnknownParam(p)) if p == "nope"));
}

#[test]
fn unknown_property_rejected() {
    assert!(matches!(ty("@ghost"), Err(TypeError::UnknownProp(p)) if p == "ghost"));
}

#[test]
fn forward_property_ref_rejected() {
    // `later` is a real property but not resolved earlier in the order.
    assert!(matches!(ty("@later + 1"), Err(TypeError::ForwardProp(p)) if p == "later"));
}

#[test]
fn operator_type_mismatch_rejected() {
    assert!(matches!(ty("name + 1"), Err(TypeError::Mismatch(_))));
    assert!(matches!(ty("qty ++ \"x\""), Err(TypeError::Mismatch(_))));
    assert!(matches!(ty("if qty then 1 else 2"), Err(TypeError::Mismatch(_))));
    assert!(matches!(ty("if active then 1 else \"x\""), Err(TypeError::Mismatch(_))));
}

#[test]
fn function_arity_rejected() {
    assert!(matches!(ty("now(1)"), Err(TypeError::Arity(_))));
    assert!(matches!(ty("substr(name, 1)"), Err(TypeError::Arity(_))));
    assert!(matches!(ty("length(name, 1)"), Err(TypeError::Arity(_))));
    assert!(matches!(ty("coalesce()"), Err(TypeError::Arity(_))));
    // substr indices must be integer, not double
    assert!(matches!(ty("substr(name, unitPrice, 3)"), Err(TypeError::Mismatch(_))));
}

#[test]
fn assignability_is_widening_only() {
    assert!(assignable(BaseType::Long, BaseType::Double)); // widen ok
    assert!(assignable(BaseType::Integer, BaseType::Long)); // widen ok
    assert!(assignable(BaseType::String, BaseType::String)); // exact
    assert!(!assignable(BaseType::Double, BaseType::Long)); // narrowing rejected
    assert!(!assignable(BaseType::String, BaseType::Long)); // cross-category rejected
}
