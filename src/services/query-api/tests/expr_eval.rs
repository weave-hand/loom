//! Pure evaluation of computed-assignment expressions to a `SqlValue`. Arithmetic/int-double
//! coercion, string/conditional, `now()` injection, null propagation, and runtime faults
//! (div-by-zero, out-of-range substr, bad cast).

use std::collections::HashMap;

use query_api::expr::{EvalError, ValueEnv, eval, parse_expr};
use query_api::serving::SqlValue;

struct Env {
    params: HashMap<String, SqlValue>,
    props: HashMap<String, SqlValue>,
}
impl ValueEnv for Env {
    fn param(&self, name: &str) -> Option<SqlValue> {
        self.params.get(name).cloned()
    }
    fn prop(&self, name: &str) -> Option<SqlValue> {
        self.props.get(name).cloned()
    }
}

fn now() -> time::PrimitiveDateTime {
    time::PrimitiveDateTime::new(
        time::Date::from_calendar_date(2026, time::Month::July, 3).unwrap(),
        time::Time::from_hms(12, 0, 0).unwrap(),
    )
}

fn ev(src: &str, env: &Env) -> Result<SqlValue, EvalError> {
    eval(&parse_expr(src).unwrap(), env, now())
}

fn env() -> Env {
    Env {
        params: HashMap::from([
            ("qty".into(), SqlValue::Int(4)),
            ("unitPrice".into(), SqlValue::Double(2.5)),
            ("first".into(), SqlValue::Text("ada".into())),
            ("missing".into(), SqlValue::Null),
        ]),
        props: HashMap::from([("total".into(), SqlValue::Double(10.0))]),
    }
}

#[test]
fn arithmetic_int_and_double() {
    assert_eq!(
        ev("qty * unitPrice", &env()).unwrap(),
        SqlValue::Double(10.0)
    );
    assert_eq!(ev("qty + 1", &env()).unwrap(), SqlValue::Int(5));
    assert_eq!(ev("10 / 3", &env()).unwrap(), SqlValue::Int(3)); // integer division
    assert_eq!(ev("qty % 3", &env()).unwrap(), SqlValue::Int(1));
}

#[test]
fn conditional_and_string() {
    assert_eq!(
        ev("if @total > 100 then \"gold\" else \"std\"", &env()).unwrap(),
        SqlValue::Text("std".into())
    );
    assert_eq!(
        ev("upper(first) ++ \"!\"", &env()).unwrap(),
        SqlValue::Text("ADA!".into())
    );
    assert_eq!(ev("length(first)", &env()).unwrap(), SqlValue::Int(3));
    assert_eq!(
        ev("substr(first, 1, 2)", &env()).unwrap(),
        SqlValue::Text("ad".into())
    );
}

#[test]
fn now_is_injected() {
    assert_eq!(ev("now()", &env()).unwrap(), SqlValue::Timestamp(now()));
}

#[test]
fn cast_double_to_long_truncates() {
    assert_eq!(
        ev("cast(unitPrice as long)", &env()).unwrap(),
        SqlValue::Int(2)
    );
    assert_eq!(
        ev("cast(qty as double)", &env()).unwrap(),
        SqlValue::Double(4.0)
    );
}

#[test]
fn null_propagates_and_coalesce() {
    assert_eq!(ev("missing + 1", &env()).unwrap(), SqlValue::Null);
    assert_eq!(
        ev("coalesce(missing, \"fallback\")", &env()).unwrap(),
        SqlValue::Text("fallback".into())
    );
}

#[test]
fn runtime_faults() {
    assert!(matches!(ev("qty / 0", &env()), Err(EvalError::DivByZero)));
    assert!(matches!(ev("qty % 0", &env()), Err(EvalError::DivByZero)));
    assert!(matches!(
        ev("substr(first, 5, 2)", &env()),
        Err(EvalError::Substr(_))
    ));
}
