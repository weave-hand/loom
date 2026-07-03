//! Pure evaluation of a typed computed-assignment expression to a backend-neutral `SqlValue`.
//! Total: every input either yields a value or an `EvalError` (mapped to a 422 at the call
//! site). Deterministic except `now()`, which is injected by the caller.

use control_plane_core::BaseType;

use super::{BinOp, Expr, Func, UnOp};
use crate::serving::SqlValue;

/// A resolved value environment: param values by param name, property values by property name
/// (both accumulate in declared order upstream). An omitted optional param is `SqlValue::Null`.
pub trait ValueEnv {
    fn param(&self, name: &str) -> Option<SqlValue>;
    fn prop(&self, name: &str) -> Option<SqlValue>;
}

/// A runtime evaluation fault the type system cannot rule out. Surfaces as a 422.
#[derive(Clone, Debug, PartialEq)]
pub enum EvalError {
    DivByZero,
    Cast(String),
    Substr(String),
    Ref(String),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::DivByZero => write!(f, "division or modulo by zero"),
            EvalError::Cast(m) => write!(f, "cast failed: {m}"),
            EvalError::Substr(m) => write!(f, "substr out of range: {m}"),
            EvalError::Ref(m) => write!(f, "unresolved reference: {m}"),
        }
    }
}

/// A numeric view of a `SqlValue`: `Int` or `Double`, else not numeric.
enum Num {
    I(i64),
    D(f64),
}

fn as_num(v: &SqlValue) -> Option<Num> {
    match v {
        SqlValue::Int(i) => Some(Num::I(*i)),
        SqlValue::Double(d) => Some(Num::D(*d)),
        _ => None,
    }
}

pub fn eval(
    expr: &Expr,
    env: &dyn ValueEnv,
    now: time::PrimitiveDateTime,
) -> Result<SqlValue, EvalError> {
    match expr {
        Expr::Int(n) => Ok(SqlValue::Int(*n)),
        Expr::Double(d) => Ok(SqlValue::Double(*d)),
        Expr::Str(s) => Ok(SqlValue::Text(s.clone())),
        Expr::Bool(b) => Ok(SqlValue::Bool(*b)),
        Expr::Param(name) => env
            .param(name)
            .ok_or_else(|| EvalError::Ref(format!("param `{name}`"))),
        Expr::Prop(name) => env
            .prop(name)
            .ok_or_else(|| EvalError::Ref(format!("property `@{name}`"))),
        Expr::Unary(op, inner) => {
            let v = eval(inner, env, now)?;
            if matches!(v, SqlValue::Null) {
                return Ok(SqlValue::Null);
            }
            match op {
                UnOp::Neg => match as_num(&v) {
                    Some(Num::I(i)) => Ok(SqlValue::Int(i.wrapping_neg())),
                    Some(Num::D(d)) => Ok(SqlValue::Double(-d)),
                    None => Ok(SqlValue::Null),
                },
                UnOp::Not => match v {
                    SqlValue::Bool(b) => Ok(SqlValue::Bool(!b)),
                    _ => Ok(SqlValue::Null),
                },
            }
        }
        Expr::Binary(op, a, b) => eval_binary(*op, a, b, env, now),
        Expr::If(c, a, b) => {
            let cond = eval(c, env, now)?;
            if matches!(cond, SqlValue::Bool(true)) {
                eval(a, env, now)
            } else {
                eval(b, env, now)
            }
        }
        Expr::Cast(inner, target) => {
            let v = eval(inner, env, now)?;
            eval_cast(&v, *target)
        }
        Expr::Call(func, args) => eval_call(*func, args, env, now),
    }
}

fn eval_binary(
    op: BinOp,
    a: &Expr,
    b: &Expr,
    env: &dyn ValueEnv,
    now: time::PrimitiveDateTime,
) -> Result<SqlValue, EvalError> {
    // `and`/`or` are the only short-circuit-friendly ops but we keep it simple & total:
    // evaluate both, propagate Null.
    let va = eval(a, env, now)?;
    let vb = eval(b, env, now)?;
    if matches!(va, SqlValue::Null) || matches!(vb, SqlValue::Null) {
        // Boolean and/or with a Null operand still propagate Null here.
        return Ok(SqlValue::Null);
    }
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => arith(op, &va, &vb),
        BinOp::Concat => match (&va, &vb) {
            (SqlValue::Text(x), SqlValue::Text(y)) => Ok(SqlValue::Text(format!("{x}{y}"))),
            _ => Ok(SqlValue::Null),
        },
        BinOp::Eq => Ok(SqlValue::Bool(
            cmp(&va, &vb) == Some(std::cmp::Ordering::Equal),
        )),
        BinOp::Ne => Ok(SqlValue::Bool(
            cmp(&va, &vb) != Some(std::cmp::Ordering::Equal),
        )),
        BinOp::Lt => Ok(SqlValue::Bool(
            cmp(&va, &vb) == Some(std::cmp::Ordering::Less),
        )),
        BinOp::Le => Ok(SqlValue::Bool(matches!(
            cmp(&va, &vb),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ))),
        BinOp::Gt => Ok(SqlValue::Bool(
            cmp(&va, &vb) == Some(std::cmp::Ordering::Greater),
        )),
        BinOp::Ge => Ok(SqlValue::Bool(matches!(
            cmp(&va, &vb),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ))),
        BinOp::And => Ok(bool_op(&va, &vb, |x, y| x && y)),
        BinOp::Or => Ok(bool_op(&va, &vb, |x, y| x || y)),
    }
}

fn bool_op(a: &SqlValue, b: &SqlValue, f: impl Fn(bool, bool) -> bool) -> SqlValue {
    match (a, b) {
        (SqlValue::Bool(x), SqlValue::Bool(y)) => SqlValue::Bool(f(*x, *y)),
        _ => SqlValue::Null,
    }
}

/// Arithmetic with int/double promotion. Both operands are non-null numerics (checked upstream)
/// or the result is Null.
fn arith(op: BinOp, a: &SqlValue, b: &SqlValue) -> Result<SqlValue, EvalError> {
    match (as_num(a), as_num(b)) {
        (Some(Num::I(x)), Some(Num::I(y))) => {
            let r = match op {
                BinOp::Add => x.wrapping_add(y),
                BinOp::Sub => x.wrapping_sub(y),
                BinOp::Mul => x.wrapping_mul(y),
                BinOp::Div => {
                    if y == 0 {
                        return Err(EvalError::DivByZero);
                    }
                    x.wrapping_div(y)
                }
                BinOp::Mod => {
                    if y == 0 {
                        return Err(EvalError::DivByZero);
                    }
                    x.wrapping_rem(y)
                }
                _ => return Ok(SqlValue::Null),
            };
            Ok(SqlValue::Int(r))
        }
        (Some(na), Some(nb)) => {
            let x = to_f64(&na);
            let y = to_f64(&nb);
            let r = match op {
                BinOp::Add => x + y,
                BinOp::Sub => x - y,
                BinOp::Mul => x * y,
                BinOp::Div => {
                    if y == 0.0 {
                        return Err(EvalError::DivByZero);
                    }
                    x / y
                }
                #[expect(
                    clippy::modulo_arithmetic,
                    reason = "float modulo mirrors the int Mod arm above; IEEE 754 defines the sign"
                )]
                BinOp::Mod => {
                    if y == 0.0 {
                        return Err(EvalError::DivByZero);
                    }
                    x % y
                }
                _ => return Ok(SqlValue::Null),
            };
            Ok(SqlValue::Double(r))
        }
        _ => Ok(SqlValue::Null),
    }
}

fn to_f64(n: &Num) -> f64 {
    match n {
        #[expect(
            clippy::cast_precision_loss,
            reason = "int->double promotion for mixed arithmetic; acceptable for computed values"
        )]
        Num::I(i) => *i as f64,
        Num::D(d) => *d,
    }
}

/// Total ordering across matching categories; `None` when incomparable (yields false compares).
fn cmp(a: &SqlValue, b: &SqlValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (SqlValue::Int(x), SqlValue::Int(y)) => Some(x.cmp(y)),
        (SqlValue::Text(x), SqlValue::Text(y)) => Some(x.cmp(y)),
        (SqlValue::Bool(x), SqlValue::Bool(y)) => Some(x.cmp(y)),
        (SqlValue::Date(x), SqlValue::Date(y)) => Some(x.cmp(y)),
        (SqlValue::Timestamp(x), SqlValue::Timestamp(y)) => Some(x.cmp(y)),
        _ => match (as_num(a), as_num(b)) {
            (Some(na), Some(nb)) => to_f64(&na).partial_cmp(&to_f64(&nb)),
            _ => None,
        },
    }
}

fn eval_cast(v: &SqlValue, target: BaseType) -> Result<SqlValue, EvalError> {
    if matches!(v, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    match target {
        BaseType::Long => match v {
            SqlValue::Int(i) => Ok(SqlValue::Int(*i)),
            #[expect(
                clippy::cast_possible_truncation,
                reason = "explicit cast(x as long) truncates toward zero by contract"
            )]
            SqlValue::Double(d) => {
                if d.is_finite() && *d >= i64::MIN as f64 && *d <= i64::MAX as f64 {
                    Ok(SqlValue::Int(*d as i64))
                } else {
                    Err(EvalError::Cast(format!("{d} out of i64 range")))
                }
            }
            SqlValue::Text(s) => s
                .trim()
                .parse::<i64>()
                .map(SqlValue::Int)
                .map_err(|e| EvalError::Cast(format!("`{s}` as integer: {e}"))),
            _ => Err(EvalError::Cast("value not castable to integer".into())),
        },
        BaseType::Integer => match v {
            SqlValue::Int(i) => i32::try_from(*i)
                .map(|_| SqlValue::Int(*i))
                .map_err(|e| EvalError::Cast(format!("{i} out of i32 range: {e}"))),
            #[expect(
                clippy::cast_possible_truncation,
                reason = "explicit cast(x as integer) truncates toward zero by contract"
            )]
            SqlValue::Double(d) => {
                if d.is_finite() && *d >= f64::from(i32::MIN) && *d <= f64::from(i32::MAX) {
                    Ok(SqlValue::Int(*d as i64))
                } else {
                    Err(EvalError::Cast(format!("{d} out of i32 range")))
                }
            }
            SqlValue::Text(s) => s
                .trim()
                .parse::<i32>()
                .map(|n| SqlValue::Int(i64::from(n)))
                .map_err(|e| EvalError::Cast(format!("`{s}` as integer: {e}"))),
            _ => Err(EvalError::Cast("value not castable to integer".into())),
        },
        BaseType::Double => match v {
            SqlValue::Double(d) => Ok(SqlValue::Double(*d)),
            #[expect(
                clippy::cast_precision_loss,
                reason = "int->double cast; acceptable for computed values"
            )]
            SqlValue::Int(i) => Ok(SqlValue::Double(*i as f64)),
            SqlValue::Text(s) => s
                .trim()
                .parse::<f64>()
                .map(SqlValue::Double)
                .map_err(|e| EvalError::Cast(format!("`{s}` as double: {e}"))),
            _ => Err(EvalError::Cast("value not castable to double".into())),
        },
        BaseType::String => Ok(SqlValue::Text(render_scalar(v))),
        BaseType::Boolean => match v {
            SqlValue::Bool(b) => Ok(SqlValue::Bool(*b)),
            _ => Err(EvalError::Cast("value not castable to boolean".into())),
        },
        BaseType::Date => match v {
            SqlValue::Date(d) => Ok(SqlValue::Date(*d)),
            SqlValue::Text(s) => {
                let fmt = time::macros::format_description!("[year]-[month]-[day]");
                time::Date::parse(s, &fmt)
                    .map(SqlValue::Date)
                    .map_err(|e| EvalError::Cast(format!("`{s}` as date: {e}")))
            }
            _ => Err(EvalError::Cast("value not castable to date".into())),
        },
        BaseType::Timestamp => match v {
            SqlValue::Timestamp(t) => Ok(SqlValue::Timestamp(*t)),
            SqlValue::Text(s) => {
                let fmt = time::macros::format_description!(
                    "[year]-[month]-[day]T[hour]:[minute]:[second]"
                );
                time::PrimitiveDateTime::parse(s, &fmt)
                    .map(SqlValue::Timestamp)
                    .map_err(|e| EvalError::Cast(format!("`{s}` as timestamp: {e}")))
            }
            _ => Err(EvalError::Cast("value not castable to timestamp".into())),
        },
        BaseType::Vector(_) => Err(EvalError::Cast("cannot cast to a vector".into())),
    }
}

fn render_scalar(v: &SqlValue) -> String {
    match v {
        SqlValue::Text(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Date(d) => crate::serving::iso_date(d),
        SqlValue::Timestamp(t) => crate::serving::iso_timestamp(t),
        SqlValue::Null => String::new(),
    }
}

fn eval_call(
    func: Func,
    args: &[Expr],
    env: &dyn ValueEnv,
    now: time::PrimitiveDateTime,
) -> Result<SqlValue, EvalError> {
    let vals: Vec<SqlValue> = args
        .iter()
        .map(|a| eval(a, env, now))
        .collect::<Result<_, _>>()?;
    match func {
        Func::Now => Ok(SqlValue::Timestamp(now)),
        Func::Coalesce => Ok(vals
            .into_iter()
            .find(|v| !matches!(v, SqlValue::Null))
            .unwrap_or(SqlValue::Null)),
        Func::Upper | Func::Lower | Func::Length | Func::Substr => {
            // string funcs: a Null first arg propagates Null.
            let first = vals.first().cloned().unwrap_or(SqlValue::Null);
            let SqlValue::Text(s) = first else {
                return Ok(SqlValue::Null);
            };
            match func {
                Func::Upper => Ok(SqlValue::Text(s.to_uppercase())),
                Func::Lower => Ok(SqlValue::Text(s.to_lowercase())),
                Func::Length => {
                    let n = i64::try_from(s.chars().count())
                        .map_err(|e| EvalError::Cast(format!("string too long for length: {e}")))?;
                    Ok(SqlValue::Int(n))
                }
                Func::Substr => substr(&s, vals.get(1), vals.get(2)),
                _ => Ok(SqlValue::Null),
            }
        }
    }
}

/// 1-based `substr(s, start, len)` over Unicode scalar values. Any Null index → Null; a start
/// < 1, a negative len, or a window past the end → `EvalError::Substr`.
fn substr(
    s: &str,
    start: Option<&SqlValue>,
    len: Option<&SqlValue>,
) -> Result<SqlValue, EvalError> {
    let idx = |v: Option<&SqlValue>| match v {
        Some(SqlValue::Int(i)) => Some(*i),
        _ => None,
    };
    let (Some(start), Some(len)) = (idx(start), idx(len)) else {
        return Ok(SqlValue::Null);
    };
    if start < 1 || len < 0 {
        return Err(EvalError::Substr(format!("start={start}, len={len}")));
    }
    let chars: Vec<char> = s.chars().collect();
    let from = usize::try_from(start - 1).unwrap_or(usize::MAX);
    let take = usize::try_from(len).unwrap_or(usize::MAX);
    let end = from.saturating_add(take);
    // `.get(from..end)` returns None if the window is out of range — index-free (no
    // `clippy::indexing_slicing`), and the None arm becomes the runtime fault.
    match chars.get(from..end) {
        Some(slice) => Ok(SqlValue::Text(slice.iter().collect())),
        None => Err(EvalError::Substr(format!(
            "window {start}..{} exceeds length {}",
            start.saturating_add(len),
            chars.len()
        ))),
    }
}
