//! Bottom-up type inference for computed-assignment expressions. Pure. Every ref must resolve
//! (param, or property already resolved earlier in declared order); every operator/function
//! arg-type and result-type is computed and checked. Feeds the invoke-time conformance gate.

use control_plane_core::BaseType;

use super::{BinOp, Expr, Func, UnOp};

/// The typing context an expression is checked against: the action's params, the target's
/// properties, and which properties have been resolved *earlier* in declared assignment order
/// (a `@prop` ref to a not-yet-resolved property is a forward-ref error).
pub trait TypeEnv {
    fn param_type(&self, name: &str) -> Option<BaseType>;
    fn prop_type(&self, name: &str) -> Option<BaseType>;
    fn prop_resolved(&self, name: &str) -> bool;
}

#[derive(Clone, Debug, PartialEq)]
pub enum TypeError {
    UnknownParam(String),
    UnknownProp(String),
    ForwardProp(String),
    Mismatch(String),
    Arity(String),
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypeError::UnknownParam(p) => write!(f, "unknown parameter `{p}`"),
            TypeError::UnknownProp(p) => write!(f, "unknown property `{p}`"),
            TypeError::ForwardProp(p) => write!(
                f,
                "property `@{p}` is referenced before it is assigned (no forward refs)"
            ),
            TypeError::Mismatch(m) => write!(f, "{m}"),
            TypeError::Arity(m) => write!(f, "{m}"),
        }
    }
}

fn is_numeric(t: BaseType) -> bool {
    matches!(t, BaseType::Integer | BaseType::Long | BaseType::Double)
}

/// Numeric rank in the widening tower Integer < Long < Double. Non-numeric → None.
fn rank(t: BaseType) -> Option<u8> {
    match t {
        BaseType::Integer => Some(0),
        BaseType::Long => Some(1),
        BaseType::Double => Some(2),
        _ => None,
    }
}

fn numeric_join(a: BaseType, b: BaseType) -> Option<BaseType> {
    match (rank(a), rank(b)) {
        (Some(ra), Some(rb)) => Some(if ra >= rb { a } else { b }),
        _ => None,
    }
}

/// Is `result` assignable to a property of type `target`? Exact, or numeric widening only.
#[must_use]
pub fn assignable(result: BaseType, target: BaseType) -> bool {
    if result == target {
        return true;
    }
    match (rank(result), rank(target)) {
        (Some(rr), Some(rt)) => rr <= rt,
        _ => false,
    }
}

/// Same comparison category (for `=`/`!=`): both numeric, or the same non-numeric base.
fn same_category(a: BaseType, b: BaseType) -> bool {
    (is_numeric(a) && is_numeric(b)) || a == b
}

pub fn typecheck(expr: &Expr, env: &dyn TypeEnv) -> Result<BaseType, TypeError> {
    match expr {
        Expr::Int(_) => Ok(BaseType::Long),
        Expr::Double(_) => Ok(BaseType::Double),
        Expr::Str(_) => Ok(BaseType::String),
        Expr::Bool(_) => Ok(BaseType::Boolean),
        Expr::Param(name) => env
            .param_type(name)
            .ok_or_else(|| TypeError::UnknownParam(name.clone())),
        Expr::Prop(name) => {
            let ty = env
                .prop_type(name)
                .ok_or_else(|| TypeError::UnknownProp(name.clone()))?;
            if env.prop_resolved(name) {
                Ok(ty)
            } else {
                Err(TypeError::ForwardProp(name.clone()))
            }
        }
        Expr::Unary(op, inner) => {
            let t = typecheck(inner, env)?;
            match op {
                UnOp::Neg if is_numeric(t) => Ok(t),
                UnOp::Neg => Err(TypeError::Mismatch(format!(
                    "unary `-` needs a numeric operand, got {}",
                    t.canonical_name()
                ))),
                UnOp::Not if t == BaseType::Boolean => Ok(BaseType::Boolean),
                UnOp::Not => Err(TypeError::Mismatch(format!(
                    "`not` needs a boolean operand, got {}",
                    t.canonical_name()
                ))),
            }
        }
        Expr::Binary(op, a, b) => typecheck_binary(*op, a, b, env),
        Expr::If(c, a, b) => {
            let ct = typecheck(c, env)?;
            if ct != BaseType::Boolean {
                return Err(TypeError::Mismatch(format!(
                    "`if` condition must be boolean, got {}",
                    ct.canonical_name()
                )));
            }
            let at = typecheck(a, env)?;
            let bt = typecheck(b, env)?;
            join_branch(at, bt)
        }
        Expr::Cast(inner, target) => {
            typecheck(inner, env)?;
            Ok(*target)
        }
        Expr::Call(func, args) => typecheck_call(*func, args, env),
    }
}

fn typecheck_binary(
    op: BinOp,
    a: &Expr,
    b: &Expr,
    env: &dyn TypeEnv,
) -> Result<BaseType, TypeError> {
    let at = typecheck(a, env)?;
    let bt = typecheck(b, env)?;
    let numeric = |x: BaseType, y: BaseType, what: &str| {
        numeric_join(x, y).ok_or_else(|| {
            TypeError::Mismatch(format!(
                "{what} needs numeric operands, got {} and {}",
                x.canonical_name(),
                y.canonical_name()
            ))
        })
    };
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            numeric(at, bt, "arithmetic")
        }
        BinOp::Concat => {
            if at == BaseType::String && bt == BaseType::String {
                Ok(BaseType::String)
            } else {
                Err(TypeError::Mismatch(format!(
                    "`++` needs string operands, got {} and {}",
                    at.canonical_name(),
                    bt.canonical_name()
                )))
            }
        }
        BinOp::Eq | BinOp::Ne => {
            if same_category(at, bt) {
                Ok(BaseType::Boolean)
            } else {
                Err(TypeError::Mismatch(format!(
                    "comparison needs same-category operands, got {} and {}",
                    at.canonical_name(),
                    bt.canonical_name()
                )))
            }
        }
        BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            let ordered = |t: BaseType| {
                is_numeric(t)
                    || matches!(t, BaseType::String | BaseType::Date | BaseType::Timestamp)
            };
            if ordered(at) && ordered(bt) && same_category(at, bt) {
                Ok(BaseType::Boolean)
            } else {
                Err(TypeError::Mismatch(format!(
                    "ordering comparison needs same ordered category, got {} and {}",
                    at.canonical_name(),
                    bt.canonical_name()
                )))
            }
        }
        BinOp::And | BinOp::Or => {
            if at == BaseType::Boolean && bt == BaseType::Boolean {
                Ok(BaseType::Boolean)
            } else {
                Err(TypeError::Mismatch(format!(
                    "`and`/`or` need boolean operands, got {} and {}",
                    at.canonical_name(),
                    bt.canonical_name()
                )))
            }
        }
    }
}

fn join_branch(a: BaseType, b: BaseType) -> Result<BaseType, TypeError> {
    if a == b {
        return Ok(a);
    }
    numeric_join(a, b).ok_or_else(|| {
        TypeError::Mismatch(format!(
            "`if` branches have incompatible types {} and {}",
            a.canonical_name(),
            b.canonical_name()
        ))
    })
}

fn typecheck_call(func: Func, args: &[Expr], env: &dyn TypeEnv) -> Result<BaseType, TypeError> {
    let arg_types: Vec<BaseType> = args
        .iter()
        .map(|a| typecheck(a, env))
        .collect::<Result<_, _>>()?;
    let want_string = |t: BaseType, pos: usize| {
        if t == BaseType::String {
            Ok(())
        } else {
            Err(TypeError::Mismatch(format!(
                "argument {pos} must be a string, got {}",
                t.canonical_name()
            )))
        }
    };
    let want_int = |t: BaseType, pos: usize| {
        if matches!(t, BaseType::Integer | BaseType::Long) {
            Ok(())
        } else {
            Err(TypeError::Mismatch(format!(
                "argument {pos} must be an integer, got {}",
                t.canonical_name()
            )))
        }
    };
    // Slice patterns keep this index-free (no `clippy::indexing_slicing`) and fold arity into the
    // match: a wrong count falls through to the `_ =>` Arity arm.
    match (func, arg_types.as_slice()) {
        (Func::Now, []) => Ok(BaseType::Timestamp),
        (Func::Now, _) => Err(TypeError::Arity("now() takes no arguments".into())),
        (Func::Upper | Func::Lower, [s]) => {
            want_string(*s, 1)?;
            Ok(BaseType::String)
        }
        (Func::Upper | Func::Lower, _) => {
            Err(TypeError::Arity("upper/lower takes 1 argument".into()))
        }
        (Func::Length, [s]) => {
            want_string(*s, 1)?;
            Ok(BaseType::Long)
        }
        (Func::Length, _) => Err(TypeError::Arity("length takes 1 argument".into())),
        (Func::Substr, [s, a, b]) => {
            want_string(*s, 1)?;
            want_int(*a, 2)?;
            want_int(*b, 3)?;
            Ok(BaseType::String)
        }
        (Func::Substr, _) => Err(TypeError::Arity("substr takes 3 arguments".into())),
        (Func::Coalesce, []) => Err(TypeError::Arity("coalesce needs at least 1 argument".into())),
        (Func::Coalesce, [first, rest @ ..]) => {
            let mut acc = *first;
            for t in rest {
                acc = join_branch(acc, *t)?;
            }
            Ok(acc)
        }
    }
}
