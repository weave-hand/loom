//! Pratt/precedence parser for the computed-assignment grammar: literals, param/property refs,
//! arithmetic/comparison/boolean precedence, `if/then/else`, whitelisted calls, and `cast(x as T)`.

use control_plane_core::BaseType;
use query_api::expr::{BinOp, Expr, Func, UnOp, parse_expr};

#[test]
fn literals_and_refs() {
    assert_eq!(parse_expr("42").unwrap(), Expr::Int(42));
    assert_eq!(parse_expr("3.5").unwrap(), Expr::Double(3.5));
    assert_eq!(parse_expr("\"hi\"").unwrap(), Expr::Str("hi".into()));
    assert_eq!(parse_expr("true").unwrap(), Expr::Bool(true));
    assert_eq!(parse_expr("qty").unwrap(), Expr::Param("qty".into()));
    assert_eq!(parse_expr("@total").unwrap(), Expr::Prop("total".into()));
}

#[test]
fn arithmetic_precedence() {
    // qty + unitPrice * 2  ==  qty + (unitPrice * 2)
    let e = parse_expr("qty + unitPrice * 2").unwrap();
    assert_eq!(
        e,
        Expr::Binary(
            BinOp::Add,
            Box::new(Expr::Param("qty".into())),
            Box::new(Expr::Binary(
                BinOp::Mul,
                Box::new(Expr::Param("unitPrice".into())),
                Box::new(Expr::Int(2)),
            )),
        )
    );
}

#[test]
fn parens_override_precedence() {
    let e = parse_expr("(qty + unitPrice) * 2").unwrap();
    assert_eq!(
        e,
        Expr::Binary(
            BinOp::Mul,
            Box::new(Expr::Binary(
                BinOp::Add,
                Box::new(Expr::Param("qty".into())),
                Box::new(Expr::Param("unitPrice".into())),
            )),
            Box::new(Expr::Int(2)),
        )
    );
}

#[test]
fn unary_minus_and_not() {
    assert_eq!(
        parse_expr("-5").unwrap(),
        Expr::Unary(UnOp::Neg, Box::new(Expr::Int(5)))
    );
    assert_eq!(
        parse_expr("not active").unwrap(),
        Expr::Unary(UnOp::Not, Box::new(Expr::Param("active".into())))
    );
}

#[test]
fn comparison_and_boolean() {
    // total > 100 and active
    let e = parse_expr("total > 100 and active").unwrap();
    assert_eq!(
        e,
        Expr::Binary(
            BinOp::And,
            Box::new(Expr::Binary(
                BinOp::Gt,
                Box::new(Expr::Param("total".into())),
                Box::new(Expr::Int(100)),
            )),
            Box::new(Expr::Param("active".into())),
        )
    );
}

#[test]
fn not_binds_looser_than_comparison_tighter_than_and() {
    // `not a > b` == `not (a > b)` (not is looser than comparison)
    assert_eq!(
        parse_expr("not qty > 5").unwrap(),
        Expr::Unary(
            UnOp::Not,
            Box::new(Expr::Binary(
                BinOp::Gt,
                Box::new(Expr::Param("qty".into())),
                Box::new(Expr::Int(5)),
            )),
        )
    );
    // `not a and b` == `(not a) and b` (not is tighter than and/or)
    assert_eq!(
        parse_expr("not active and ready").unwrap(),
        Expr::Binary(
            BinOp::And,
            Box::new(Expr::Unary(
                UnOp::Not,
                Box::new(Expr::Param("active".into()))
            )),
            Box::new(Expr::Param("ready".into())),
        )
    );
}

#[test]
fn if_then_else() {
    let e = parse_expr("if total > 100 then \"gold\" else \"std\"").unwrap();
    assert_eq!(
        e,
        Expr::If(
            Box::new(Expr::Binary(
                BinOp::Gt,
                Box::new(Expr::Param("total".into())),
                Box::new(Expr::Int(100)),
            )),
            Box::new(Expr::Str("gold".into())),
            Box::new(Expr::Str("std".into())),
        )
    );
}

#[test]
fn concat_and_calls() {
    assert_eq!(
        parse_expr("upper(first) ++ \"-\" ++ last").unwrap(),
        Expr::Binary(
            BinOp::Concat,
            Box::new(Expr::Binary(
                BinOp::Concat,
                Box::new(Expr::Call(Func::Upper, vec![Expr::Param("first".into())])),
                Box::new(Expr::Str("-".into())),
            )),
            Box::new(Expr::Param("last".into())),
        )
    );
    assert_eq!(parse_expr("now()").unwrap(), Expr::Call(Func::Now, vec![]));
    assert_eq!(
        parse_expr("substr(name, 1, 3)").unwrap(),
        Expr::Call(
            Func::Substr,
            vec![Expr::Param("name".into()), Expr::Int(1), Expr::Int(3)]
        )
    );
}

#[test]
fn cast_resolves_logical_type() {
    assert_eq!(
        parse_expr("cast(qty as double)").unwrap(),
        Expr::Cast(Box::new(Expr::Param("qty".into())), BaseType::Double)
    );
}

#[test]
fn malformed_is_rejected() {
    assert!(parse_expr("qty +").is_err());
    assert!(parse_expr("(qty").is_err());
    assert!(parse_expr("cast(qty as bogus)").is_err());
    assert!(parse_expr("").is_err());
    assert!(parse_expr("1 2 3").is_err());
}
