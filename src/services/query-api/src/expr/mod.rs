//! The closed, total expression grammar for computed action assignments (slice 2).
//! A bounded language over an action's params/earlier-resolved properties: arithmetic,
//! comparison, boolean, string concat, `if`, and a whitelisted function set. No user code,
//! no I/O, no loops — deterministic except `now()`. Parsed here, type-checked in
//! `typecheck` (Task 2), evaluated to a `SqlValue` in `eval` (Task 3).
//!
//! This module grows one submodule per task: Task 1 declares only `parse`; Task 2 adds
//! `pub mod typecheck;`; Task 3 adds `pub mod eval;`. Declaring a submodule before its file
//! exists is an `error[E0583]` that breaks the whole crate.

pub mod parse;
pub mod typecheck;

pub use parse::{ParseError, parse_expr};
pub use typecheck::{TypeEnv, TypeError, assignable, typecheck};

pub mod eval;

pub use eval::{EvalError, ValueEnv, eval};

use control_plane_core::BaseType;

/// A parsed expression node. `Int` literals are carried as `i64` and typed `Long`; `@name`
/// is a property ref (`Prop`), a bare identifier is a param ref (`Param`).
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Int(i64),
    Double(f64),
    Str(String),
    Bool(bool),
    Param(String),
    Prop(String),
    Unary(UnOp, Box<Expr>),
    Binary(BinOp, Box<Expr>, Box<Expr>),
    If(Box<Expr>, Box<Expr>, Box<Expr>),
    Call(Func, Vec<Expr>),
    Cast(Box<Expr>, BaseType),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnOp {
    Neg,
    Not,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Func {
    Now,
    Upper,
    Lower,
    Substr,
    Length,
    Coalesce,
}
