# Custom-logic actions slice 2 — computed assignments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an ontology action property be set by a bounded, closed-grammar expression over the action's inputs (e.g. `total = qty * unitPrice`, `createdAt = now()`, `tier = if total > 100 then "gold" else "std"`), evaluated in query-api before the ACL/constraint gates.

**Architecture:** A small hand-written Pratt parser + pure type-checker + pure evaluator live in a new `query-api/src/expr/` module and produce a `SqlValue`. The ontology model generalizes `ConstAssignment { property, value }` to `Assignment { property, source: Const(Value) | Expr(String) }` (core), persisted via a forward-only Postgres migration (nullable `expr` column) and mirrored by the memory fake (stored by value — no shape change). The type-checker slots into the existing invoke-time `check_conformance` (authoring errors → `Misconfigured` → HTTP 500, matching today's conformance behavior); the evaluator slots into `resolve_action_row`, which gains a `now` parameter (runtime arithmetic faults → `ParamError` → `BadParams` → HTTP 422). The computed row is then **unchanged** downstream — same `write_filter` ACL gate, same constraint validator, same atomic commit.

**Tech Stack:** Rust (edition 2024), buck2, `time` crate (`PrimitiveDateTime` for `now()`), `serde_json` (const values), sqlx compile-time macros (postgres adapter), no new third-party dependency (hand-written parser).

## Global Constraints

- **Tests are `rust_test` integration targets only** — NOT inline `#[cfg(test)] mod tests`. Each new test file is a sibling `tests/<name>.rs` wired as its own `rust_test` target in the crate's `BUCK` (the `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` inside `src/**.rs`).
- **Fixture-backed tests use the `loom_fixture_test` macro** (not bare `rust_test`) so they route to local execution and pass the postgres-boot env. Pure-logic tests use `rust_test`.
- **Clippy is strict** (`pedantic` + `restriction`): production code MUST NOT use `unwrap`/`expect`/`panic!`/`todo!`/`unimplemented!`/`dbg!`/indexing that can panic/`unreachable!`. Return `Result` instead. Use `#[expect(lint, reason = "...")]` for a justified local exception (bare `#[allow]` trips `allow_attributes_without_reason`). Test code is exempted from the panic-safety lints by the `loom_rust_test`/`loom_fixture_test` wrappers.
- **After changing any `query!`/`query_scalar!` SQL or a migration**, regenerate the committed `.sqlx` cache: `tools/sqlx-prepare.sh`, and commit the result. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness.
- **buck2 usage:** build with `buck2 build -M none //src/control-plane/... //src/services/query-api/...` (cloud disk cap — never a bare whole-tree build). Don't pipe `buck2 test`/`bxl` through `tail`/`head` — redirect to a file and grep it: `buck2 test <targets> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Conventional Commits** on every commit message (`feat:`/`refactor:`/`test:`/`fix:` …); the `conventional-commit` commit-msg hook enforces it.
- **Markdown files** end with exactly one trailing newline, no trailing whitespace (prek `end-of-file-fixer`/`trim trailing whitespace`).

---

## File Structure

**New files (query-api expression interpreter — auto-globbed into the `query-api` lib):**
- `src/services/query-api/src/expr/mod.rs` — the `Expr` AST, `UnOp`/`BinOp`/`Func` enums, module re-exports, and the two env traits (`TypeEnv`, `ValueEnv`).
- `src/services/query-api/src/expr/parse.rs` — the tokenizer + Pratt/precedence parser: `parse_expr(&str) -> Result<Expr, ParseError>`.
- `src/services/query-api/src/expr/typecheck.rs` — the pure bottom-up type inferencer: `typecheck(&Expr, &dyn TypeEnv) -> Result<BaseType, TypeError>`.
- `src/services/query-api/src/expr/eval.rs` — the pure evaluator: `eval(&Expr, &dyn ValueEnv, now) -> Result<SqlValue, EvalError>`.

**New test files (each needs a BUCK target):**
- `src/services/query-api/tests/expr_parse.rs` — parser unit tests (`rust_test`).
- `src/services/query-api/tests/expr_typecheck.rs` — type-checker unit tests incl. rejection matrix (`rust_test`).
- `src/services/query-api/tests/expr_eval.rs` — evaluator unit tests incl. runtime faults (`rust_test`).
- `src/services/query-api/tests/action_computed_e2e.rs` — full e2e matrix through postgres+engine (`loom_fixture_test`).
- `src/control-plane/postgres/migrations/0027_action_assignment_expr.sql` — the migration.

**Modified files:**
- `src/control-plane/core/src/ontology.rs` — `ConstAssignment` → `Assignment` + `AssignmentSource`; builder `.assign()` (const) + new `.assign_expr()`.
- `src/control-plane/core/src/lib.rs` — export rename.
- `src/control-plane/postgres/src/ontology.rs` — insert/select of the `expr` column; map rows to `Assignment`.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (2 queries rehash).
- `src/control-plane/testkit/src/lib.rs` — extend the action round-trip contract with an `Expr` assignment.
- `src/services/query-api/src/params.rs` — `resolve_action_row` gains a `now` param; assignment leg branches on `source` (Const vs Expr → evaluate).
- `src/services/query-api/src/action.rs` — thread `now` into the two `resolve_action_row` calls; extend `check_assignments_and_binds` to type-check `Expr` assignments; count `Expr` assignments in required-property coverage.
- `src/services/query-api/src/lib.rs` — add `pub mod expr;`.
- `src/services/query-api/BUCK` — new `rust_test`/`loom_fixture_test` targets.
- Test construction-site updates: `core/tests/{action_mapping,ontology_builder}.rs`, `query-api/tests/{params,action_conformance,mutate_conformance,action_mapping_e2e}.rs` (mechanical `ConstAssignment { property, value }` → `Assignment::constant(property, value)`).

---

## The closed expression grammar (reference for all tasks)

**Operands:** integer literal `42` (typed `Long`), double literal `3.14` (`Double`), string literal `"txt"` (double-quoted, `String`), bool literal `true`/`false` (`Boolean`); **param ref** = bare identifier naming a `ParamDef`; **property ref** = `@name` naming a target property resolved *earlier* in declared assignment order.

**Operators (precedence low→high):** `or` < `and` < `not` (unary) < comparison (`= != < <= > >=`) < `++` (string concat) < `+ -` < `* / %` < unary `-` < calls/parens.

**Functions (whitelisted):** `now()` → `Timestamp`; `upper(s)`/`lower(s)` → `String`; `substr(s, start, len)` → `String` (start/len integer); `length(s)` → `Long`; `coalesce(a, b, …)` (≥1 arg) → common type; `cast(x as <logical-type>)` → the named type.

**Type rules (bottom-up):**
- Numeric tower `Integer < Long < Double`. `numeric_join(a,b)` = the wider; both must be numeric.
- Arithmetic `+ - * / %`: both numeric → `numeric_join`.
- `++`: `String ++ String` → `String`.
- Comparison `= !=`: operands same category (both numeric, both `String`, both `Boolean`, both `Date`, both `Timestamp`) → `Boolean`. Ordering `< <= > >=`: numeric / `String` / `Date` / `Timestamp` (not `Boolean`) → `Boolean`.
- `and`/`or`: both `Boolean` → `Boolean`. `not`: `Boolean` → `Boolean`. unary `-`: numeric → same numeric type.
- `if c then a else b`: `c` `Boolean`; `a`,`b` join (numeric_join if both numeric, else must be equal) → joined type.
- **Assignability to the property** (final step in conformance): result type is assignable to the property's `BaseType` iff equal, OR both numeric and result ≤ property in the tower (widening only; narrowing e.g. `Double`→`Long` is a type error).

**Runtime semantics (evaluator):**
- Numeric ops promote to `f64` when any operand is `Double`, else stay `i64`. `/` and `%` by zero → error (both int and float paths). Integer `/` truncates toward zero.
- **Null propagation:** any `Null` operand to a unary/binary op or to `upper/lower/substr/length` yields `Null`; `coalesce` returns the first non-null arg (or `Null` if all null); `if` with a `Null` condition takes the `else` branch.
- `now()` returns the `now` passed into `eval` (a `time::PrimitiveDateTime`) — deterministic per request; the only non-constant source.
- `cast(x as T)`: numeric↔numeric reinterpret (`Double`→`Long` truncates; out-of-`i64`-range → error), numeric→`String` renders, `String`→numeric parses (parse failure → error), `String`→`Date`/`Timestamp` parse ISO (failure → error), same-type is identity. Unsupported cast pair → error.

---

## Task 1: Expression AST + Pratt parser

**Files:**
- Create: `src/services/query-api/src/expr/mod.rs`
- Create: `src/services/query-api/src/expr/parse.rs`
- Modify: `src/services/query-api/src/lib.rs` (add `pub mod expr;`)
- Modify: `src/services/query-api/BUCK` (add `expr-parse` test target)
- Test: `src/services/query-api/tests/expr_parse.rs`

**Interfaces:**
- Produces:
  - `pub enum Expr { Int(i64), Double(f64), Str(String), Bool(bool), Param(String), Prop(String), Unary(UnOp, Box<Expr>), Binary(BinOp, Box<Expr>, Box<Expr>), If(Box<Expr>, Box<Expr>, Box<Expr>), Call(Func, Vec<Expr>), Cast(Box<Expr>, control_plane_core::BaseType) }`
  - `pub enum UnOp { Neg, Not }`
  - `pub enum BinOp { Add, Sub, Mul, Div, Mod, Concat, Eq, Ne, Lt, Le, Gt, Ge, And, Or }`
  - `pub enum Func { Now, Upper, Lower, Substr, Length, Coalesce }`
  - `pub fn parse_expr(src: &str) -> Result<Expr, ParseError>`
  - `pub struct ParseError(pub String)` with `impl std::fmt::Display`.

- [ ] **Step 1: Write the module skeleton (`mod.rs`)**

Create `src/services/query-api/src/expr/mod.rs`:

```rust
//! The closed, total expression grammar for computed action assignments (slice 2).
//! A bounded language over an action's params/earlier-resolved properties: arithmetic,
//! comparison, boolean, string concat, `if`, and a whitelisted function set. No user code,
//! no I/O, no loops — deterministic except `now()`. Parsed here, type-checked in
//! `typecheck`, evaluated to a `SqlValue` in `eval`.

pub mod eval;
pub mod parse;
pub mod typecheck;

pub use eval::{EvalError, ValueEnv, eval};
pub use parse::{ParseError, parse_expr};
pub use typecheck::{TypeEnv, TypeError, typecheck};

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
```

- [ ] **Step 2: Write the failing parser test file**

Create `src/services/query-api/tests/expr_parse.rs`:

```rust
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
            Box::new(Expr::Unary(UnOp::Not, Box::new(Expr::Param("active".into())))),
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
```

- [ ] **Step 3: Wire the lib module + test target**

In `src/services/query-api/src/lib.rs`, add `pub mod expr;` alongside the other `pub mod` declarations (keep alphabetical if the file is ordered).

In `src/services/query-api/BUCK`, add (mirror the pure-logic `rust_test` targets, e.g. the existing conformance target):

```python
rust_test(
    name = "expr-parse",
    crate = "expr_parse",
    srcs = ["tests/expr_parse.rs"],
    crate_root = "tests/expr_parse.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:expr-parse > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `parse.rs` / `parse_expr` does not exist yet (compile error).

- [ ] **Step 5: Implement the tokenizer + Pratt parser (`parse.rs`)**

Create `src/services/query-api/src/expr/parse.rs`. Implement a small tokenizer (numbers, double-quoted strings with `\"`/`\\` escapes, identifiers, `@ident`, the operator symbols `+ - * / % = != < <= > >= ( ) ,`, the `++` concat, and keywords `and or not if then else true false cast as`), then a Pratt parser with the precedence table above. Resolve `cast(x as <type>)` via `control_plane_core::resolve_logical`, erroring on an unknown type. Return `ParseError(String)` on any lexing/parse failure (unexpected token, unterminated string, trailing tokens, unknown cast type, unknown function name). Full reference implementation:

```rust
//! Tokenizer + Pratt parser for the computed-assignment grammar. Hand-written (no parser
//! dependency); total — every input either parses or returns a `ParseError`.

use control_plane_core::resolve_logical;

use super::{BinOp, Expr, Func, UnOp};

/// A parse/lex failure, carrying a human message used verbatim in the conformance violation.
#[derive(Clone, Debug, PartialEq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Int(i64),
    Double(f64),
    Str(String),
    Ident(String),
    Prop(String),
    // keywords
    And,
    Or,
    Not,
    If,
    Then,
    Else,
    True,
    False,
    Cast,
    As,
    // symbols
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    Comma,
}

// NOTE: index-free by construction — all character access goes through `at()` (`.get().copied()`),
// never `chars[i]`/`chars[a..b]`, so the enforced `clippy::indexing_slicing` lint never fires.
fn lex(src: &str) -> Result<Vec<Tok>, ParseError> {
    let chars: Vec<char> = src.chars().collect();
    let at = |k: usize| chars.get(k).copied();
    let mut i = 0;
    let mut out = Vec::new();
    let err = |m: &str| ParseError(m.to_string());
    while let Some(c) = at(i) {
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => { out.push(Tok::LParen); i += 1; }
            ')' => { out.push(Tok::RParen); i += 1; }
            ',' => { out.push(Tok::Comma); i += 1; }
            '*' => { out.push(Tok::Star); i += 1; }
            '/' => { out.push(Tok::Slash); i += 1; }
            '%' => { out.push(Tok::Percent); i += 1; }
            '-' => { out.push(Tok::Minus); i += 1; }
            '+' => {
                if at(i + 1) == Some('+') {
                    out.push(Tok::Concat);
                    i += 2;
                } else {
                    out.push(Tok::Plus);
                    i += 1;
                }
            }
            '=' => { out.push(Tok::Eq); i += 1; }
            '!' => {
                if at(i + 1) == Some('=') {
                    out.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err(err("unexpected '!'"));
                }
            }
            '<' => {
                if at(i + 1) == Some('=') {
                    out.push(Tok::Le);
                    i += 2;
                } else {
                    out.push(Tok::Lt);
                    i += 1;
                }
            }
            '>' => {
                if at(i + 1) == Some('=') {
                    out.push(Tok::Ge);
                    i += 2;
                } else {
                    out.push(Tok::Gt);
                    i += 1;
                }
            }
            '"' => {
                let mut s = String::new();
                i += 1;
                loop {
                    let Some(ch) = at(i) else {
                        return Err(err("unterminated string literal"));
                    };
                    match ch {
                        '"' => { i += 1; break; }
                        '\\' => {
                            let Some(esc) = at(i + 1) else {
                                return Err(err("dangling escape in string literal"));
                            };
                            match esc {
                                '"' => s.push('"'),
                                '\\' => s.push('\\'),
                                'n' => s.push('\n'),
                                't' => s.push('\t'),
                                other => {
                                    return Err(ParseError(format!("bad string escape: \\{other}")));
                                }
                            }
                            i += 2;
                        }
                        other => { s.push(other); i += 1; }
                    }
                }
                out.push(Tok::Str(s));
            }
            '@' => {
                i += 1;
                let mut name = String::new();
                while let Some(ch) = at(i) {
                    if ch.is_alphanumeric() || ch == '_' {
                        name.push(ch);
                        i += 1;
                    } else {
                        break;
                    }
                }
                if name.is_empty() {
                    return Err(err("expected identifier after '@'"));
                }
                out.push(Tok::Prop(name));
            }
            c if c.is_ascii_digit() => {
                let mut text = String::new();
                let mut seen_dot = false;
                while let Some(ch) = at(i) {
                    if ch == '.' {
                        if seen_dot {
                            break;
                        }
                        seen_dot = true;
                        text.push(ch);
                        i += 1;
                    } else if ch.is_ascii_digit() {
                        text.push(ch);
                        i += 1;
                    } else {
                        break;
                    }
                }
                if seen_dot {
                    let n: f64 = text.parse().map_err(|_| err("bad number literal"))?;
                    out.push(Tok::Double(n));
                } else {
                    let n: i64 = text.parse().map_err(|_| err("integer literal out of range"))?;
                    out.push(Tok::Int(n));
                }
            }
            c if c.is_alphabetic() || c == '_' => {
                let mut word = String::new();
                while let Some(ch) = at(i) {
                    if ch.is_alphanumeric() || ch == '_' {
                        word.push(ch);
                        i += 1;
                    } else {
                        break;
                    }
                }
                out.push(match word.as_str() {
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "if" => Tok::If,
                    "then" => Tok::Then,
                    "else" => Tok::Else,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    "cast" => Tok::Cast,
                    "as" => Tok::As,
                    _ => Tok::Ident(word),
                });
            }
            other => return Err(ParseError(format!("unexpected character: {other}"))),
        }
    }
    Ok(out)
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }
    fn eat(&mut self, want: &Tok) -> Result<(), ParseError> {
        if self.peek() == Some(want) {
            self.pos += 1;
            Ok(())
        } else {
            Err(ParseError(format!("expected {want:?}")))
        }
    }

    /// Pratt loop: parse a prefix, then fold left-associative binary operators whose
    /// binding power is >= `min_bp`.
    fn expr_bp(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.prefix()?;
        while let Some(op) = self.peek().and_then(binop) {
            let (lbp, rbp) = binding_power(op);
            if lbp < min_bp {
                break;
            }
            self.pos += 1;
            let rhs = self.expr_bp(rbp)?;
            lhs = Expr::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn prefix(&mut self) -> Result<Expr, ParseError> {
        match self.next() {
            Some(Tok::Int(n)) => Ok(Expr::Int(n)),
            Some(Tok::Double(n)) => Ok(Expr::Double(n)),
            Some(Tok::Str(s)) => Ok(Expr::Str(s)),
            Some(Tok::True) => Ok(Expr::Bool(true)),
            Some(Tok::False) => Ok(Expr::Bool(false)),
            Some(Tok::Prop(p)) => Ok(Expr::Prop(p)),
            Some(Tok::Minus) => {
                let e = self.expr_bp(PREFIX_BP)?;
                Ok(Expr::Unary(UnOp::Neg, Box::new(e)))
            }
            Some(Tok::Not) => {
                // `not` binds looser than comparison (grammar table), so `not a > b` == `not (a > b)`
                // but tighter than and/or, so `not a and b` == `(not a) and b`. Recurse at NOT_BP
                // (== comparison lbp) to fold a comparison into the operand but stop before and/or.
                let e = self.expr_bp(NOT_BP)?;
                Ok(Expr::Unary(UnOp::Not, Box::new(e)))
            }
            Some(Tok::LParen) => {
                let e = self.expr_bp(0)?;
                self.eat(&Tok::RParen)?;
                Ok(e)
            }
            Some(Tok::If) => {
                let cond = self.expr_bp(0)?;
                self.eat(&Tok::Then)?;
                let then = self.expr_bp(0)?;
                self.eat(&Tok::Else)?;
                let els = self.expr_bp(0)?;
                Ok(Expr::If(Box::new(cond), Box::new(then), Box::new(els)))
            }
            Some(Tok::Cast) => {
                self.eat(&Tok::LParen)?;
                let inner = self.expr_bp(0)?;
                self.eat(&Tok::As)?;
                let ty_name = match self.next() {
                    Some(Tok::Ident(w)) => w,
                    _ => return Err(ParseError("expected a type name after 'as'".into())),
                };
                self.eat(&Tok::RParen)?;
                let bt = resolve_logical(&ty_name)
                    .ok_or_else(|| ParseError(format!("cast to unknown type `{ty_name}`")))?;
                Ok(Expr::Cast(Box::new(inner), bt))
            }
            Some(Tok::Ident(name)) => {
                // A function call if immediately followed by '(', else a param ref.
                if self.peek() == Some(&Tok::LParen) {
                    let func = func_by_name(&name)
                        .ok_or_else(|| ParseError(format!("unknown function `{name}`")))?;
                    self.pos += 1; // consume '('
                    let mut args = Vec::new();
                    if self.peek() != Some(&Tok::RParen) {
                        loop {
                            args.push(self.expr_bp(0)?);
                            match self.peek() {
                                Some(&Tok::Comma) => { self.pos += 1; }
                                _ => break,
                            }
                        }
                    }
                    self.eat(&Tok::RParen)?;
                    Ok(Expr::Call(func, args))
                } else {
                    Ok(Expr::Param(name))
                }
            }
            other => Err(ParseError(format!("unexpected token: {other:?}"))),
        }
    }
}

/// Unary `-` binds tighter than every binary op (so `-a * b` == `(-a) * b`).
const PREFIX_BP: u8 = 9;
/// Unary `not` binds at comparison level: looser than comparison/arithmetic (captures them into
/// its operand), tighter than `and`/`or`.
const NOT_BP: u8 = 3;

fn func_by_name(name: &str) -> Option<Func> {
    Some(match name {
        "now" => Func::Now,
        "upper" => Func::Upper,
        "lower" => Func::Lower,
        "substr" => Func::Substr,
        "length" => Func::Length,
        "coalesce" => Func::Coalesce,
        _ => return None,
    })
}

fn binop(t: &Tok) -> Option<BinOp> {
    Some(match t {
        Tok::Or => BinOp::Or,
        Tok::And => BinOp::And,
        Tok::Eq => BinOp::Eq,
        Tok::Ne => BinOp::Ne,
        Tok::Lt => BinOp::Lt,
        Tok::Le => BinOp::Le,
        Tok::Gt => BinOp::Gt,
        Tok::Ge => BinOp::Ge,
        Tok::Concat => BinOp::Concat,
        Tok::Plus => BinOp::Add,
        Tok::Minus => BinOp::Sub,
        Tok::Star => BinOp::Mul,
        Tok::Slash => BinOp::Div,
        Tok::Percent => BinOp::Mod,
        _ => return None,
    })
}

/// Left binding power / right binding power. Left-associative: rbp = lbp + 1.
fn binding_power(op: BinOp) -> (u8, u8) {
    match op {
        BinOp::Or => (1, 2),
        BinOp::And => (2, 3),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => (3, 4),
        BinOp::Concat => (4, 5),
        BinOp::Add | BinOp::Sub => (5, 6),
        BinOp::Mul | BinOp::Div | BinOp::Mod => (6, 7),
    }
}

/// Parse a whole expression string. Errors on any lex/parse failure or trailing tokens.
pub fn parse_expr(src: &str) -> Result<Expr, ParseError> {
    let toks = lex(src)?;
    if toks.is_empty() {
        return Err(ParseError("empty expression".into()));
    }
    let mut p = Parser { toks, pos: 0 };
    let e = p.expr_bp(0)?;
    if p.pos != p.toks.len() {
        return Err(ParseError("trailing tokens after expression".into()));
    }
    Ok(e)
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:expr-parse > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (all `expr_parse` tests).

- [ ] **Step 7: Lint the new module**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty clippy output (clean). Fix any `restriction`/`pedantic` finding with a real change or a justified `#[expect(..., reason = "...")]`.

- [ ] **Step 8: Commit**

```bash
git add src/services/query-api/src/expr/mod.rs src/services/query-api/src/expr/parse.rs \
        src/services/query-api/src/lib.rs src/services/query-api/tests/expr_parse.rs \
        src/services/query-api/BUCK
git commit -m "feat(query-api): computed-assignment expression AST + Pratt parser"
```

---

## Task 2: Expression type-checker

**Files:**
- Create: `src/services/query-api/src/expr/typecheck.rs`
- Modify: `src/services/query-api/BUCK` (add `expr-typecheck` test target)
- Test: `src/services/query-api/tests/expr_typecheck.rs`

**Interfaces:**
- Consumes: `Expr`, `UnOp`, `BinOp`, `Func` (Task 1); `control_plane_core::BaseType`.
- Produces:
  - `pub trait TypeEnv { fn param_type(&self, name: &str) -> Option<BaseType>; fn prop_type(&self, name: &str) -> Option<BaseType>; fn prop_resolved(&self, name: &str) -> bool; }`
  - `pub enum TypeError { UnknownParam(String), UnknownProp(String), ForwardProp(String), Mismatch(String), Arity(String) }` with `impl Display`.
  - `pub fn typecheck(expr: &Expr, env: &dyn TypeEnv) -> Result<BaseType, TypeError>` — the inferred result `BaseType`.
  - `pub fn assignable(result: BaseType, target: BaseType) -> bool` — widening-only numeric compatibility (used by conformance in Task 6).

- [ ] **Step 1: Write the failing type-checker test file**

Create `src/services/query-api/tests/expr_typecheck.rs`:

```rust
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
```

- [ ] **Step 2: Add the BUCK target and run to verify failure**

In `src/services/query-api/BUCK` add:

```python
rust_test(
    name = "expr-typecheck",
    crate = "expr_typecheck",
    srcs = ["tests/expr_typecheck.rs"],
    crate_root = "tests/expr_typecheck.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

Run: `buck2 test //src/services/query-api:expr-typecheck > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `typecheck`/`TypeEnv`/`assignable` do not exist.

- [ ] **Step 3: Implement the type-checker (`typecheck.rs`)**

Create `src/services/query-api/src/expr/typecheck.rs`:

```rust
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:expr-typecheck > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/expr/typecheck.rs \
        src/services/query-api/tests/expr_typecheck.rs src/services/query-api/BUCK
git commit -m "feat(query-api): computed-assignment expression type-checker"
```

---

## Task 3: Expression evaluator

**Files:**
- Create: `src/services/query-api/src/expr/eval.rs`
- Modify: `src/services/query-api/BUCK` (add `expr-eval` test target)
- Test: `src/services/query-api/tests/expr_eval.rs`

**Interfaces:**
- Consumes: `Expr`, `UnOp`, `BinOp`, `Func` (Task 1); `crate::serving::SqlValue`; `time::PrimitiveDateTime`.
- Produces:
  - `pub trait ValueEnv { fn param(&self, name: &str) -> Option<SqlValue>; fn prop(&self, name: &str) -> Option<SqlValue>; }`
  - `pub enum EvalError { DivByZero, Cast(String), Substr(String), Ref(String) }` with `impl Display`.
  - `pub fn eval(expr: &Expr, env: &dyn ValueEnv, now: time::PrimitiveDateTime) -> Result<SqlValue, EvalError>`.

- [ ] **Step 1: Write the failing evaluator test file**

Create `src/services/query-api/tests/expr_eval.rs`:

```rust
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
    assert_eq!(ev("qty * unitPrice", &env()).unwrap(), SqlValue::Double(10.0));
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
    assert_eq!(ev("cast(unitPrice as long)", &env()).unwrap(), SqlValue::Int(2));
    assert_eq!(ev("cast(qty as double)", &env()).unwrap(), SqlValue::Double(4.0));
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
    assert!(matches!(ev("substr(first, 5, 2)", &env()), Err(EvalError::Substr(_))));
}
```

- [ ] **Step 2: Add the BUCK target and run to verify failure**

In `src/services/query-api/BUCK` add:

```python
rust_test(
    name = "expr-eval",
    crate = "expr_eval",
    srcs = ["tests/expr_eval.rs"],
    crate_root = "tests/expr_eval.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:time",
    ],
)
```

Run: `buck2 test //src/services/query-api:expr-eval > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `eval`/`ValueEnv`/`EvalError` do not exist.

- [ ] **Step 3: Implement the evaluator (`eval.rs`)**

Create `src/services/query-api/src/expr/eval.rs`. Implement `eval` per the runtime semantics section. Numeric ops: if either operand `Double` → `f64` math, else `i64`. `Div`/`Mod` by zero → `EvalError::DivByZero`. Null propagation for unary/binary ops and `upper/lower/substr/length`. `substr` uses 1-based `start`; out-of-range → `EvalError::Substr`. `cast` per the table. Reference implementation:

```rust
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
        BinOp::Eq => Ok(SqlValue::Bool(cmp(&va, &vb) == Some(std::cmp::Ordering::Equal))),
        BinOp::Ne => Ok(SqlValue::Bool(cmp(&va, &vb) != Some(std::cmp::Ordering::Equal))),
        BinOp::Lt => Ok(SqlValue::Bool(cmp(&va, &vb) == Some(std::cmp::Ordering::Less))),
        BinOp::Le => Ok(SqlValue::Bool(matches!(
            cmp(&va, &vb),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ))),
        BinOp::Gt => Ok(SqlValue::Bool(cmp(&va, &vb) == Some(std::cmp::Ordering::Greater))),
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
        BaseType::Integer | BaseType::Long => match v {
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
                        .map_err(|_| EvalError::Cast("string too long for length".into()))?;
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
fn substr(s: &str, start: Option<&SqlValue>, len: Option<&SqlValue>) -> Result<SqlValue, EvalError> {
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:expr-eval > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/expr/eval.rs \
        src/services/query-api/tests/expr_eval.rs src/services/query-api/BUCK
git commit -m "feat(query-api): computed-assignment expression evaluator"
```

---

## Task 4: Generalize the core `Assignment` model (const-only, tree stays green)

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (`ConstAssignment` → `Assignment` + `AssignmentSource`; builder)
- Modify: `src/control-plane/core/src/lib.rs` (export rename)
- Modify: `src/control-plane/postgres/src/ontology.rs` (map rows to `Assignment::constant`)
- Modify: `src/control-plane/testkit/src/lib.rs` (construction sites)
- Modify: `src/services/query-api/src/params.rs`, `src/services/query-api/src/action.rs` (branch on `source`; `Expr` arm = temporary "unsupported" error, replaced in Tasks 6–7)
- Modify test construction sites: `src/control-plane/core/tests/{action_mapping,ontology_builder}.rs`, `src/services/query-api/tests/{params,action_conformance,mutate_conformance,action_mapping_e2e}.rs`

**Interfaces:**
- Produces:
  - `pub struct Assignment { pub property: String, pub source: AssignmentSource }` (derives `Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize`).
  - `pub enum AssignmentSource { Const(serde_json::Value), Expr(String) }` (same derives).
  - `impl Assignment { pub fn constant(property: impl Into<String>, value: serde_json::Value) -> Self; pub fn expr(property: impl Into<String>, source: impl Into<String>) -> Self; }`
  - `ActionDefBuilder::assign(property, value)` (const, unchanged name) + `ActionDefBuilder::assign_expr(property, source)` (new).
  - `ActionDef.assignments: Vec<Assignment>`.
- Consumes: nothing new from earlier tasks (this task is model-only; the expr module is not yet wired in).

- [ ] **Step 1: Replace the model in `core/src/ontology.rs`**

Replace the `ConstAssignment` struct (currently ~lines 294–303) with:

```rust
/// The source of a property's [`Assignment`]: either a fixed constant (the JSON wire form of a
/// scalar, coerced to the property's logical type on the write path) or a bounded expression
/// over the action's params / earlier-resolved properties (computed at invocation, slice 2).
/// Not `Eq` because `serde_json::Value` is not `Eq`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AssignmentSource {
    Const(serde_json::Value),
    Expr(String),
}

/// A declared assignment filling a property when no parameter supplies it. `Const` is the
/// default/fixed-value case (e.g. `status = "active"`); `Expr` computes the value from the
/// action's inputs (e.g. `total = qty * unitPrice`). Ordered within `ActionDef.assignments`;
/// an `Expr` may reference a property assigned *earlier* in that order.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Assignment {
    pub property: String,
    pub source: AssignmentSource,
}

impl Assignment {
    /// A fixed-constant assignment (the slice-1 shape).
    pub fn constant(property: impl Into<String>, value: serde_json::Value) -> Self {
        Assignment {
            property: property.into(),
            source: AssignmentSource::Const(value),
        }
    }

    /// A computed-expression assignment (slice 2); `source` is the raw expression string.
    pub fn expr(property: impl Into<String>, source: impl Into<String>) -> Self {
        Assignment {
            property: property.into(),
            source: AssignmentSource::Expr(source.into()),
        }
    }
}
```

Change `ActionDef.assignments` field type (currently `Vec<ConstAssignment>`, ~line 319) to `Vec<Assignment>`.

Update the builder `assign` method (~lines 401–409) and add `assign_expr`:

```rust
    /// Append a declared constant assignment filling `property` with `value` when no
    /// parameter supplies it.
    pub fn assign(mut self, property: impl Into<String>, value: serde_json::Value) -> Self {
        self.inner.assignments.push(Assignment::constant(property, value));
        self
    }

    /// Append a declared computed-expression assignment: `property` is set by evaluating
    /// `source` (the closed grammar) over the action's inputs.
    pub fn assign_expr(mut self, property: impl Into<String>, source: impl Into<String>) -> Self {
        self.inner.assignments.push(Assignment::expr(property, source));
        self
    }
```

- [ ] **Step 2: Update the export in `core/src/lib.rs`**

In the `pub use ontology::{…}` list (line ~54), replace `ConstAssignment` with `Assignment, AssignmentSource`.

- [ ] **Step 3: Update the postgres adapter (map rows to `Assignment::constant`)**

In `src/control-plane/postgres/src/ontology.rs`:
- Import (line 3): replace `ConstAssignment` with `Assignment, AssignmentSource`.
- Write path (`define_action`, the assignment loop ~line 391): the value bound is now `a.source`. Since this task keeps storage const-only, keep binding `value` from the const arm and skip `Expr` (there is no expr column yet — Task 5 adds it). Temporary shape:

```rust
    for (i, a) in action.assignments.iter().enumerate() {
        let value = match &a.source {
            AssignmentSource::Const(v) => v.clone(),
            // Expr persistence lands in migration 0027 (Task 5); until then, no caller
            // produces an Expr through the postgres adapter.
            AssignmentSource::Expr(_) => {
                return Err(ControlPlaneError::Validation(
                    "expression assignments are not yet persisted".into(),
                ));
            }
        };
        sqlx::query!(
            "insert into ontology.action_assignment (action_name, ordinal, property, value) \
             values ($1, $2, $3, $4)",
            action.name.0,
            i as i32,
            a.property,
            value,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }
```

- Read path (`get_action`, the map ~line 451): map each row to a const assignment:

```rust
        assignments: assignment_rows
            .into_iter()
            .map(|r| Assignment::constant(r.property, r.value))
            .collect(),
```

(No `.sqlx` change in this task — the SQL text is unchanged.)

- [ ] **Step 4: Update `testkit/src/lib.rs` and all test construction sites**

Mechanical rename across every `ConstAssignment { property: P, value: V }` literal → `Assignment::constant(P, V)`, and the import `ConstAssignment` → `Assignment`. Sites (from recon):
- `src/control-plane/testkit/src/lib.rs:21` (import), `:1045` (usage).
- `src/control-plane/core/tests/action_mapping.rs`, `src/control-plane/core/tests/ontology_builder.rs`.
- `src/services/query-api/tests/action_conformance.rs` (import at `:162`, usages in the slice-1 block), `mutate_conformance.rs`, `action_mapping_e2e.rs` (import at `:11`, usages).

Use a grep to find every remaining reference and update it:

Run: `grep -rn "ConstAssignment" src/`
Expected after edits: no matches.

- [ ] **Step 5: Guard the query-api consumers on `source`**

In `src/services/query-api/src/params.rs`, `resolve_action_row` constant leg (~lines 69–85): branch on `a.source` (the `now`/expr wiring lands in Task 7; here `Expr` is a temporary error so the tree compiles and no test exercises it):

```rust
    for a in &action.assignments {
        let prop_ty = target
            .properties
            .iter()
            .find(|p| p.name == a.property)
            .map(|p| p.ty.as_str())
            .ok_or_else(|| {
                ParamError::BadValue(a.property.clone(), "assignment names an unknown property".into())
            })?;
        let value = match &a.source {
            control_plane_core::AssignmentSource::Const(v) => parse_value(&a.property, prop_ty, v)?,
            control_plane_core::AssignmentSource::Expr(_) => {
                return Err(ParamError::BadValue(
                    a.property.clone(),
                    "expression assignments are not yet evaluated".into(),
                ));
            }
        };
        out.push((a.property.clone(), value));
    }
```

In `src/services/query-api/src/action.rs`:
- `check_assignments_and_binds` (~line 193): the `validate_const` call now only applies to the `Const` arm; branch on `a.source` (the expr type-check lands in Task 6; here `Expr` is accepted without checking, replaced in Task 6):

```rust
        Some(prop) => match &a.source {
            control_plane_core::AssignmentSource::Const(v) => {
                if let Err(e) = crate::params::validate_const(&a.property, &prop.ty, v) {
                    violations.push(format!("constant for property `{}`: {e}", a.property));
                }
            }
            control_plane_core::AssignmentSource::Expr(_) => {
                // Expression type-checking is wired in Task 6.
            }
        },
```

- Required-property coverage in `check_insert_conformance` (~line 223): the existing `by_constant` binding already counts any assignment by property name regardless of source (`action.assignments.iter().any(|a| a.property == prop.name)`), so an `Expr` assignment correctly covers a required property with no logic change. Rename the local from `by_constant` → `by_assignment` (it now covers both `Const` and `Expr`) so the name isn't a misnomer; leave the expression unchanged.

- [ ] **Step 6: Build the affected crates and run the existing suites**

Run: `buck2 build -M none //src/control-plane/... //src/services/query-api/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`
Expected: BUILD SUCCEEDED.

Run: `buck2 test //src/control-plane/core/... //src/control-plane/testkit/... //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all existing tests still pass (const-only behavior unchanged).

- [ ] **Step 7: Lint the touched crates**

Run: `buck2 build '//src/control-plane/core:core[clippy.txt]' '//src/control-plane/postgres:postgres[clippy.txt]' '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor(ontology): generalize ConstAssignment to Assignment{property,source}"
```

---

## Task 5: Persist `Expr` assignments (migration + postgres adapter + `.sqlx`)

**Files:**
- Create: `src/control-plane/postgres/migrations/0027_action_assignment_expr.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs` (insert/select the `expr` column; map to `Assignment`)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)
- Modify: `src/control-plane/testkit/src/lib.rs` (round-trip an `Expr` assignment)

**Interfaces:**
- Consumes: `Assignment`/`AssignmentSource` (Task 4).
- Produces: `define_action`/`get_action` now round-trip `Expr` assignments through postgres.

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0027_action_assignment_expr.sql`:

```sql
-- Custom-logic actions slice 2: computed (expression-valued) assignments.
--
-- An assignment's source is now either a fixed constant (`value`, the slice-1 shape) or a
-- bounded expression (`expr`, evaluated in query-api at invocation over the action's inputs).
-- Exactly one of (value, expr) is non-null per row. `value` becomes nullable (was NOT NULL);
-- existing rows are all constants (expr NULL) and are unaffected.
alter table ontology.action_assignment
    alter column value drop not null;

alter table ontology.action_assignment
    add column expr text;

alter table ontology.action_assignment
    add constraint action_assignment_source_ck
    check ((value is not null and expr is null) or (value is null and expr is not null));
```

- [ ] **Step 2: Extend the testkit contract with an `Expr` round-trip (failing)**

In `src/control-plane/testkit/src/lib.rs`, in `ontology_contract`'s action section (near the binds+constant round-trip at ~`:996–1059`), add an action that carries an `Expr` assignment and assert full-struct round-trip. Insert after the existing `createGadget` round-trip assertion:

```rust
    // Slice 2: an expression assignment round-trips through storage (define == get), alongside
    // a constant. (Evaluation is a query-api concern; the adapter only persists the source.)
    let computed = ActionDef {
        name: ActionName("computeGadget".into()),
        target: TypeName("Gadget".into()),
        parameters: vec![ParamDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            binds: None,
        }],
        kind: ActionKind::Insert,
        assignments: vec![
            Assignment::constant("status", serde_json::json!("active")),
            Assignment::expr("name", "upper(\"g\")"),
        ],
    };
    ont.define_action(computed.clone()).await.unwrap();
    assert_eq!(
        ont.get_action(&ActionName("computeGadget".into()))
            .await
            .unwrap()
            .unwrap(),
        computed,
        "action round-trips with a computed (expr) assignment"
    );
```

Run (postgres contract exercises this via the fixture suite): `buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — the adapter doesn't persist `expr` yet (the `Expr` arm errors from Task 4), and/or the memory contract passes but postgres fails.

- [ ] **Step 3: Update the postgres adapter to persist/read `expr`**

In `src/control-plane/postgres/src/ontology.rs`:

Write path (replace the Task-4 temporary loop):

```rust
    for (i, a) in action.assignments.iter().enumerate() {
        let (value, expr): (Option<serde_json::Value>, Option<String>) = match &a.source {
            AssignmentSource::Const(v) => (Some(v.clone()), None),
            AssignmentSource::Expr(s) => (None, Some(s.clone())),
        };
        sqlx::query!(
            "insert into ontology.action_assignment (action_name, ordinal, property, value, expr) \
             values ($1, $2, $3, $4, $5)",
            action.name.0,
            i as i32,
            a.property,
            value,
            expr,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    }
```

Read path — update the select and the mapping:

```rust
    let assignment_rows = sqlx::query!(
        "select property, value, expr from ontology.action_assignment \
         where action_name = $1 order by ordinal",
        name.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
```

```rust
        assignments: assignment_rows
            .into_iter()
            .map(|r| Assignment {
                property: r.property,
                source: match (r.value, r.expr) {
                    (_, Some(e)) => AssignmentSource::Expr(e),
                    (Some(v), None) => AssignmentSource::Const(v),
                    // The CHECK constraint guarantees one is set; a NULL/NULL row is a
                    // corrupt catalog — fail loud rather than fabricate a value.
                    (None, None) => AssignmentSource::Const(serde_json::Value::Null),
                },
            })
            .collect(),
```

(The `(None, None)` arm cannot occur given the CHECK constraint; mapping it to `Const(Null)` keeps the map total and lint-clean without a panic.)

- [ ] **Step 4: Regenerate the `.sqlx` cache**

Run: `tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log`
Expected: success; `git status` shows the two action_assignment `query-*.json` files replaced (old hashes removed, new added).

If the cloud environment cannot boot the pinned postgres for `sqlx-prepare.sh`, STOP and surface it — the postgres crate's `query!` macros need the refreshed cache to build. (Fixture tests boot postgres in this environment, so `initdb` should work.)

- [ ] **Step 5: Verify postgres contract passes**

Run: `buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (incl. `sqlx-cache-check` and the extended `ontology_contract`).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/migrations/0027_action_assignment_expr.sql \
        src/control-plane/postgres/src/ontology.rs src/control-plane/postgres/.sqlx \
        src/control-plane/testkit/src/lib.rs
git commit -m "feat(postgres): persist expression (Expr) action assignments via migration 0027"
```

---

## Task 6: Wire the type-checker into invoke-time conformance

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`check_assignments_and_binds` type-checks `Expr` assignments)
- Test: `src/services/query-api/tests/action_conformance.rs` (extend with the computed-assignment rejection matrix)

**Interfaces:**
- Consumes: `crate::expr::{parse_expr, typecheck, assignable, TypeEnv, TypeError}` (Tasks 1–2); `control_plane_core::{resolve_logical, BaseType, AssignmentSource}`.
- Produces: `check_conformance` rejects a malformed/mistyped/forward-ref/unknown-ref/arity expression via `ActionError::Misconfigured`.

- [ ] **Step 1: Write the failing conformance tests**

Append to `src/services/query-api/tests/action_conformance.rs` (reuse its `gadget()`/`insert_action`/`pb`/`is_misconfigured` helpers; note `insert_action` takes `Vec<Assignment>` after Task 4):

```rust
// --- Computed (expression) assignments: define-time typing (slice 2) ---
// NOTE: `Assignment` is already imported at the top of this file (Task 4 renamed the slice-1
// `use control_plane_core::ConstAssignment;` import to `Assignment`). Do NOT re-import it here.

/// Gadget has id (Long, req), name (String), status (String). Add a numeric prop for typing.
fn gadget_with_total() -> ObjectType {
    let mut g = gadget();
    g.properties.push(prop("total", "Double", false));
    g
}

#[test]
fn valid_expression_conforms() {
    // total = id + 1 : Long, assignable to the Double `total` property by widening -> conforms.
    // (Only the required `id` param is declared; no stray param.)
    let a = ActionDef {
        name: ActionName("a".into()),
        target: TypeName("Gadget".into()),
        parameters: vec![pb("id", "Long", true, None)],
        kind: ActionKind::Insert,
        assignments: vec![Assignment::expr("total", "id + 1")],
    };
    check_conformance(&a, &gadget_with_total()).expect("computed assignment conforms");
}

#[test]
fn expr_double_bind_rejected() {
    // `status` is written by both a renamed param and an Expr assignment -> double-bind.
    let a = insert_action(
        vec![
            pb("id", "Long", true, None),
            pb("s", "String", false, Some("status")),
        ],
        vec![Assignment::expr("status", "upper(\"x\")")],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn unknown_reference_rejected() {
    let a = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("status", "nope ++ \"x\"")],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn type_mismatched_expression_rejected() {
    // status is String; an arithmetic (numeric) result is not assignable.
    let a = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("status", "id + 1")],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn narrowing_expression_rejected() {
    // total is Double; assigning a Double result to a Long prop would narrow. Add a Long prop.
    let mut g = gadget();
    g.properties.push(prop("count", "Long", false));
    let a = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("count", "cast(id as double)")],
    );
    assert!(is_misconfigured(check_conformance(&a, &g)));
}

#[test]
fn unknown_function_or_arity_rejected() {
    let a1 = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("status", "bogus(id)")],
    );
    assert!(is_misconfigured(check_conformance(&a1, &gadget())));
    let a2 = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("status", "now(1)")],
    );
    assert!(is_misconfigured(check_conformance(&a2, &gadget())));
}

#[test]
fn forward_property_ref_rejected() {
    // status references @total, but total is assigned AFTER status here (declared order).
    let a = ActionDef {
        name: ActionName("a".into()),
        target: TypeName("Gadget".into()),
        parameters: vec![pb("id", "Long", true, None)],
        kind: ActionKind::Insert,
        assignments: vec![
            Assignment::expr("status", "if @total > 1.0 then \"hi\" else \"lo\""),
            Assignment::expr("total", "id + 1"),
        ],
    };
    assert!(is_misconfigured(check_conformance(&a, &gadget_with_total())));
}

#[test]
fn earlier_property_ref_conforms() {
    // total assigned first, then status references @total — resolved-earlier, OK.
    let a = ActionDef {
        name: ActionName("a".into()),
        target: TypeName("Gadget".into()),
        parameters: vec![pb("id", "Long", true, None)],
        kind: ActionKind::Insert,
        assignments: vec![
            Assignment::expr("total", "id + 1"),
            Assignment::expr("status", "if @total > 1.0 then \"hi\" else \"lo\""),
        ],
    };
    check_conformance(&a, &gadget_with_total()).expect("earlier @prop ref conforms");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:action-conformance > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `Expr` assignments are currently accepted without type-checking (Task 4 stub), so the rejection tests fail.

- [ ] **Step 3: Implement expression conformance in `check_assignments_and_binds`**

In `src/services/query-api/src/action.rs`, build a `TypeEnv` implementation and check each `Expr` assignment against it, threading the resolved-set in declared order. Add a private env struct and rework the assignment loop. First, a `TypeEnv` adapter over the action + target:

```rust
/// A `crate::expr::TypeEnv` view over an action's params and the target's properties, tracking
/// which properties have been resolved *earlier* in declared assignment order (for the
/// forward-`@ref` rule). Params seed the resolved set (they are resolved before any assignment).
struct ConformanceEnv<'a> {
    action: &'a ActionDef,
    target: &'a ObjectType,
    resolved: std::collections::HashSet<String>,
}

impl crate::expr::TypeEnv for ConformanceEnv<'_> {
    fn param_type(&self, name: &str) -> Option<control_plane_core::BaseType> {
        self.action
            .parameters
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| control_plane_core::resolve_logical(&p.ty))
    }
    fn prop_type(&self, name: &str) -> Option<control_plane_core::BaseType> {
        self.target
            .properties
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| control_plane_core::resolve_logical(&p.ty))
    }
    fn prop_resolved(&self, name: &str) -> bool {
        self.resolved.contains(name)
    }
}
```

Then in `check_assignments_and_binds`, seed `resolved` with every param's bound property, and for each assignment (in order) type-check an `Expr` before recording its property as resolved:

```rust
    // Seed the resolved-property set with everything a param binds (params resolve before any
    // assignment); assignments then resolve in declared order.
    let mut env = ConformanceEnv {
        action,
        target,
        resolved: action
            .parameters
            .iter()
            .map(|p| p.binds_property().to_string())
            .collect(),
    };

    for a in &action.assignments {
        match target.properties.iter().find(|prop| prop.name == a.property) {
            None => violations.push(format!(
                "assignment names property `{}`, which is not a property of type `{target_name}`",
                a.property
            )),
            Some(prop) => match &a.source {
                control_plane_core::AssignmentSource::Const(v) => {
                    if let Err(e) = crate::params::validate_const(&a.property, &prop.ty, v) {
                        violations.push(format!("constant for property `{}`: {e}", a.property));
                    }
                }
                control_plane_core::AssignmentSource::Expr(src) => {
                    check_expr_assignment(src, prop, &env, target_name, &a.property, &mut violations);
                }
            },
        }
        if !bound.insert(&a.property) {
            violations.push(dup(&a.property));
        }
        // This property is now resolved for any later `@ref`.
        env.resolved.insert(a.property.clone());
    }
```

And the helper (parse → typecheck → assignability), each failure a distinct violation:

```rust
/// Parse + type-check one expression assignment against `env`, checking the inferred type is
/// assignable to `prop`. Appends a clear violation per failure mode.
fn check_expr_assignment(
    src: &str,
    prop: &control_plane_core::PropertyDef,
    env: &ConformanceEnv<'_>,
    target_name: &str,
    property: &str,
    violations: &mut Vec<String>,
) {
    let expr = match crate::expr::parse_expr(src) {
        Ok(e) => e,
        Err(e) => {
            violations.push(format!("expression for property `{property}`: {e}"));
            return;
        }
    };
    let inferred = match crate::expr::typecheck(&expr, env) {
        Ok(t) => t,
        Err(e) => {
            violations.push(format!("expression for property `{property}`: {e}"));
            return;
        }
    };
    let Some(prop_base) = control_plane_core::resolve_logical(&prop.ty) else {
        violations.push(format!(
            "property `{property}` of type `{target_name}` has unknown logical type `{}`",
            prop.ty
        ));
        return;
    };
    if !crate::expr::assignable(inferred, prop_base) {
        violations.push(format!(
            "expression for property `{property}` has type {} which is not assignable to `{}`",
            inferred.canonical_name(),
            prop.ty
        ));
    }
}
```

Add `use` for `PropertyDef` if not already imported (it is used across action.rs; confirm the import list).

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/query-api:action-conformance > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/action_conformance.rs
git commit -m "feat(query-api): type-check computed expression assignments at conformance"
```

---

## Task 7: Wire the evaluator into the write path (`resolve_action_row` + `now`)

**Files:**
- Modify: `src/services/query-api/src/params.rs` (`resolve_action_row` gains `now`; `Expr` arm evaluates)
- Modify: `src/services/query-api/src/action.rs` (capture `now`, pass to both `resolve_action_row` calls)
- Modify: `src/services/query-api/tests/params.rs` (update call sites for the new `now` param; add computed-assignment unit tests)

**Interfaces:**
- Consumes: `crate::expr::{parse_expr, eval, ValueEnv}` (Tasks 1, 3).
- Produces: `pub fn resolve_action_row(action: &ActionDef, target: &ObjectType, body: &serde_json::Map<String, Value>, now: time::PrimitiveDateTime) -> Result<Vec<(String, SqlValue)>, ParamError>` — the assignment leg evaluates `Expr` sources against an accumulating env; runtime faults → `ParamError::BadValue`.

- [ ] **Step 1: Update the params tests (new `now` arg + computed cases) — failing**

In `src/services/query-api/tests/params.rs` (the real helpers in this file are `gadget()`,
`pb(name, ty, required, binds: Option<&str>)`, `insert(params, assignments)`, and `body(json!)`
— use those, NOT `param`/`insert_action`; `Assignment` is already imported here after Task 4's
rename of the `ConstAssignment` import — do NOT re-import it):

- Add a `now()` helper and pass it to every existing `resolve_action_row(...)` call (append
  `, now()`), i.e. update the calls at `:174`, `:198`, `:215`, `:235`.

```rust
fn now() -> time::PrimitiveDateTime {
    time::PrimitiveDateTime::new(
        time::Date::from_calendar_date(2026, time::Month::July, 3).unwrap(),
        time::Time::from_hms(9, 0, 0).unwrap(),
    )
}

/// `gadget()` + a `total: Double` property, for computed-assignment tests.
fn gadget_with_total() -> ObjectType {
    let mut g = gadget();
    g.properties.push(PropertyDef {
        name: "total".into(),
        ty: "Double".into(),
        required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    });
    g
}
```

- Add computed-assignment cases (using `pb`/`insert` and `gadget_with_total()`):

```rust
#[test]
fn computed_expression_writes_value() {
    // total = id + 1 : Long(8), assignable to the Double `total` property.
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("total", "id + 1")],
    );
    let pairs =
        resolve_action_row(&action, &gadget_with_total(), &body(json!({ "id": "7" })), now())
            .unwrap();
    let total = pairs.iter().find(|(c, _)| c == "total").map(|(_, v)| v.clone());
    assert_eq!(total, Some(SqlValue::Int(8)));
}

#[test]
fn computed_now_uses_injected_clock() {
    // createdAt = now() lands exactly the injected clock (deterministic).
    let mut g = gadget();
    g.properties.push(PropertyDef {
        name: "createdAt".into(),
        ty: "Timestamp".into(),
        required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    });
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("createdAt", "now()")],
    );
    let pairs = resolve_action_row(&action, &g, &body(json!({ "id": "1" })), now()).unwrap();
    let created = pairs.iter().find(|(c, _)| c == "createdAt").map(|(_, v)| v.clone());
    assert_eq!(created, Some(SqlValue::Timestamp(now())));
}

#[test]
fn computed_runtime_fault_is_bad_value() {
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("total", "id / 0")],
    );
    let err =
        resolve_action_row(&action, &gadget_with_total(), &body(json!({ "id": "7" })), now())
            .unwrap_err();
    assert!(matches!(err, ParamError::BadValue(_, _)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/services/query-api:params > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `resolve_action_row` has arity 3 (no `now`), and the `Expr` arm errors (Task 4 stub).

- [ ] **Step 3: Implement `resolve_action_row` evaluation**

In `src/services/query-api/src/params.rs`:
- Change the signature to add `now: time::PrimitiveDateTime`.
- Build a `ValueEnv` that answers param values by param name and property values by property name from the accumulating output, and evaluate `Expr` sources in declared order. Replace the assignment leg:

```rust
pub fn resolve_action_row(
    action: &ActionDef,
    target: &ObjectType,
    body: &serde_json::Map<String, Value>,
    now: time::PrimitiveDateTime,
) -> Result<Vec<(String, SqlValue)>, ParamError> {
    let param_pairs = parse_params(&action.parameters, body)?;

    // param name -> value (for bare-identifier refs in expressions).
    let param_env: std::collections::HashMap<String, SqlValue> = action
        .parameters
        .iter()
        .zip(&param_pairs)
        .map(|(prm, (_, v))| (prm.name.clone(), v.clone()))
        .collect();

    let mut out: Vec<(String, SqlValue)> =
        Vec::with_capacity(param_pairs.len() + action.assignments.len());
    // property name -> value (params' bound properties, then earlier assignments), for @refs.
    let mut prop_env: std::collections::HashMap<String, SqlValue> = std::collections::HashMap::new();
    for (prm, (_, value)) in action.parameters.iter().zip(param_pairs) {
        prop_env.insert(prm.binds_property().to_string(), value.clone());
        out.push((prm.binds_property().to_string(), value));
    }

    for a in &action.assignments {
        let prop_ty = target
            .properties
            .iter()
            .find(|p| p.name == a.property)
            .map(|p| p.ty.as_str())
            .ok_or_else(|| {
                ParamError::BadValue(a.property.clone(), "assignment names an unknown property".into())
            })?;
        let value = match &a.source {
            control_plane_core::AssignmentSource::Const(v) => parse_value(&a.property, prop_ty, v)?,
            control_plane_core::AssignmentSource::Expr(src) => {
                let expr = crate::expr::parse_expr(src).map_err(|e| {
                    ParamError::BadValue(a.property.clone(), format!("expression parse: {e}"))
                })?;
                let env = RowEnv { params: &param_env, props: &prop_env };
                crate::expr::eval(&expr, &env, now)
                    .map_err(|e| ParamError::BadValue(a.property.clone(), e.to_string()))?
            }
        };
        prop_env.insert(a.property.clone(), value.clone());
        out.push((a.property.clone(), value));
    }
    Ok(out)
}

/// A `crate::expr::ValueEnv` over the resolved params + accumulated property values.
struct RowEnv<'a> {
    params: &'a std::collections::HashMap<String, SqlValue>,
    props: &'a std::collections::HashMap<String, SqlValue>,
}
impl crate::expr::ValueEnv for RowEnv<'_> {
    fn param(&self, name: &str) -> Option<SqlValue> {
        self.params.get(name).cloned()
    }
    fn prop(&self, name: &str) -> Option<SqlValue> {
        self.props.get(name).cloned()
    }
}
```

- In `src/services/query-api/src/action.rs`, capture `now` once in `run_insert` and `run_mutate` and pass it in. In `run_insert` (before line 464):

```rust
    let now = {
        let n = time::OffsetDateTime::now_utc();
        time::PrimitiveDateTime::new(n.date(), n.time())
    };
    let pairs = crate::params::resolve_action_row(action, target, body, now)?;
```

Apply the same two-line change in `run_mutate` (before line 714). (Reuse a small private helper `fn request_now() -> time::PrimitiveDateTime` to avoid duplication.)

Add a helper near the top of `action.rs`:

```rust
/// The request-time clock threaded to `resolve_action_row` for `now()` in computed assignments.
fn request_now() -> time::PrimitiveDateTime {
    let n = time::OffsetDateTime::now_utc();
    time::PrimitiveDateTime::new(n.date(), n.time())
}
```

and call `let now = request_now();` in both.

- [ ] **Step 4: Run to verify pass**

Run: `buck2 test //src/services/query-api:params > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/params.rs src/services/query-api/src/action.rs \
        src/services/query-api/tests/params.rs
git commit -m "feat(query-api): evaluate computed expression assignments on the write path"
```

---

## Task 8: End-to-end matrix through postgres + engine

**Files:**
- Create: `src/services/query-api/tests/action_computed_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add `action-computed-e2e` `loom_fixture_test`)

**Interfaces:**
- Consumes: the full stack (Tasks 1–7); the `e2e_support` helpers + `GadgetWriter`-style setup (mirror `action_mapping_e2e.rs`).

- [ ] **Step 1: Write the e2e test file**

Create `src/services/query-api/tests/action_computed_e2e.rs`. **Copy the `setup_gadget_writer`
and `read_gadgets` harness from `action_mapping_e2e.rs` verbatim** (same imports: `ActionDef`,
`ActionKind`, `ActionName`, `Assignment`, `Acl`, `Action`, `Effect`, `ObjectType`, `Policy`,
`PolicyTarget`, `PropertyDef`, `RoleId`, `SubjectId`, `TableRef`, `TypeName`, the `PgControlPlane`
/ `PgFixture` / `IcebergCatalog` / `e2e_support` / `run_action` / `EngineActionClient` /
`read_object` / `objects_to_json` set, and the `prop`/`param` local helpers), changing only the
`define_type` property list to:

```rust
properties: vec![
    prop("id", "Long", true),
    prop("qty", "Long", false),
    prop("unitPrice", "Double", false),
    prop("total", "Double", false),
    prop("tier", "String", false),
    prop("label", "String", false),
    prop("createdAt", "Timestamp", false),
],
// identity: Some("id".into())
```

and the `read_gadgets` `live_tables()` existence check / `type_name` to `"Gadget"` (unchanged
from the source file). Each test builds its own action, runs it via `run_action`, and reads back
via `read_gadgets`. Concrete tests (each `#[tokio::test(flavor = "multi_thread")]`, destructuring
`GadgetWriter` exactly as `action_mapping_e2e.rs` does):

```rust
// 1. Arithmetic: total = qty * unitPrice.
async fn arithmetic_expression_writes_product() {
    // define action `create` (Insert): params id(Long,req), qty(Long) binds qty,
    // unitPrice(Double) binds unitPrice; assignments: [Assignment::expr("total", "qty * unitPrice")].
    // run_action("create", json!({"id":"1","qty":"4","unitPrice":2.5}), ...).expect(...);
    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"][0]["total"], json!(10.0));
}

// 2. Conditional + string: tier via @total (assigned earlier), label via upper/lower/++.
async fn conditional_and_string_expressions() {
    // assignments (declared order):
    //   Assignment::expr("total", "qty * unitPrice"),
    //   Assignment::expr("tier", "if @total > 100.0 then \"gold\" else \"std\""),
    //   Assignment::expr("label", "upper(\"wid\") ++ \"-\" ++ lower(\"GET\")"),
    // run with qty=4, unitPrice=2.5 -> total 10.
    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"][0]["tier"], json!("std")); // 10 <= 100
    assert_eq!(got["objects"][0]["label"], json!("WID-get"));
}

// 3. now(): createdAt = now() lands a Timestamp within the request window.
async fn now_lands_a_timestamp_in_window() {
    // assignments: [Assignment::expr("createdAt", "now()")].
    let before = time::OffsetDateTime::now_utc();
    // run_action(...).expect(...);
    let after = time::OffsetDateTime::now_utc();
    let got = read_gadgets(&cp, &pool, &subj).await;
    // createdAt reads back as an ISO "YYYY-MM-DDThh:mm:ss" string; parse and bound-check to the second.
    let s = got["objects"][0]["createdAt"].as_str().unwrap();
    let fmt = time::macros::format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");
    let parsed = time::PrimitiveDateTime::parse(s, &fmt).unwrap().assume_utc();
    assert!(parsed >= before.replace_nanosecond(0).unwrap() && parsed <= after);
}

// 4. Runtime fault -> 422, nothing committed.
async fn runtime_fault_returns_422_and_writes_nothing() {
    // assignments: [Assignment::expr("total", "qty / 0")]. Conformance passes (Long/Long->Long,
    // widens to Double); eval faults at div-by-zero.
    let err = run_action("create", json!({"id":"1","qty":"4","unitPrice":2.5}).as_object().unwrap(), &subj, &deps)
        .await.unwrap_err();
    assert!(matches!(err, ActionError::BadParams(_)));
    assert_eq!(read_gadgets(&cp, &pool, &subj).await["objects"].as_array().map(Vec::len), Some(0));
}

// 5. Governance: a computed value is deny-column gated identically to a literal.
async fn computed_value_is_governed_identically() {
    // set_policy Write with deny_columns: vec!["total".into()] (mirror action_mapping_e2e.rs:331).
    // action assigns total = qty * unitPrice.
    let err = run_action("create", json!({"id":"1","qty":"4","unitPrice":2.5}).as_object().unwrap(), &subj, &deps)
        .await.unwrap_err();
    assert!(matches!(&err, ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "total"));
    assert_eq!(read_gadgets(&cp, &pool, &subj).await["objects"].as_array().map(Vec::len), Some(0));
}

// 6. Constraint composition: a computed value trips a property constraint -> 422 (same as literal).
async fn computed_value_is_constraint_gated() {
    // define the type with a max constraint on `total` (mirror constraints_action_http.rs's
    // PropertyConstraints usage: e.g. numeric max = 100.0). Compute total = qty * unitPrice = 250
    // (qty=100, unitPrice=2.5) -> exceeds max.
    let err = run_action("create", json!({"id":"1","qty":"100","unitPrice":2.5}).as_object().unwrap(), &subj, &deps)
        .await.unwrap_err();
    assert!(matches!(err, ActionError::ConstraintViolation(_)));
    assert_eq!(read_gadgets(&cp, &pool, &subj).await["objects"].as_array().map(Vec::len), Some(0));
}

// 7. Update mapping: an Update action recomputes the patched value.
async fn update_action_computes_patched_value() {
    // seed one gadget (Insert action with total = qty*unitPrice). Then define an Update action
    // (kind Update): params key(Long,req) binds id, qty(Long) binds qty, unitPrice(Double) binds
    // unitPrice; assignment total = qty * unitPrice. run update with new qty/unitPrice; assert the
    // recomputed total lands (mirror action_mapping_e2e.rs::update_targets_and_patches_via_bound_property).
    let got = read_gadgets(&cp, &pool, &subj).await;
    assert_eq!(got["objects"].as_array().map(Vec::len), Some(1));
    assert_eq!(got["objects"][0]["total"], json!(20.0)); // e.g. qty=8, unitPrice=2.5
}
```

Fill each body with the exact `ActionDef { name, target, parameters, kind, assignments }` sketched
in its comment (using `param(name, ty, required, Some(binds))` for bound params, `Assignment::expr`
for computed ones), the `ActionDeps { cp, action_engine: &engine, serving: &serving }` construction,
and the `run_action` / `read_gadgets` / `objects_to_json` calls exactly as `action_mapping_e2e.rs`.
For test 6, define the `total` property with a max constraint — read `constraints_action_http.rs`
for the `PropertyConstraints` field names before writing (do not guess the shape). Import
`query_api::action::{ActionError, WriteDenialReason}` for the error-matching tests.

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK` (mirror `action-mapping-e2e`'s `loom_fixture_test`):

```python
loom_fixture_test(
    name = "action-computed-e2e",
    crate = "action_computed_e2e",
    srcs = ["tests/action_computed_e2e.rs"],
    crate_root = "tests/action_computed_e2e.rs",
    edition = "2024",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the e2e matrix**

Run: `buck2 test //src/services/query-api:action-computed-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (all six cases).

- [ ] **Step 4: Lint**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log`
Expected: empty.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/tests/action_computed_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): e2e matrix for computed expression assignments"
```

---

## Task 9: Full sweep, register update, docs

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-action-computed-assignments`), `docs/FUTURE.md` (mark `fut-action-computed-assignments` promoted) via `loom-docs-update`.
- Modify: `docs/ISSUES.md` if any relevant item resolved (none expected).

- [ ] **Step 1: Run the affected-crate suites together**

Run: `buck2 test //src/control-plane/... //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass. (This is the iceberg-adjacent + fixture surface; a broader `//src/...` run is optional but disk-heavy — scope to these cells.)

- [ ] **Step 2: Run the prek hooks and commit any fixups**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -20 /tmp/p.log`
Expected: all hooks pass (rustfmt, clippy, file checks, reindeer-in-sync, docs-validate). Commit any in-place fixes.

- [ ] **Step 3: Close the register item**

Use the `loom-docs-update` skill: flip `- [ ]` → `- [x]` on `road-action-computed-assignments`, set `status:done`, add `pr:#<n>` (the PR number, once opened), and mark `fut-action-computed-assignments` `status:promoted`. Stage the register edits.

- [ ] **Step 4: Commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs: close road-action-computed-assignments (computed assignments shipped)"
```

---

## Self-Review

**Spec coverage:**
- Expression grammar (operands, arithmetic, string, comparison, boolean, conditional, whitelisted functions) → Task 1 (parser) + Task 2 (typer) + Task 3 (eval). ✓
- Pratt parser, no new dependency → Task 1. ✓
- Property-reference ordering (single-pass, declared order, no forward refs) → Task 2 (`prop_resolved` + forward-ref error) tested in Task 6; runtime env accumulation → Task 7. ✓
- Evaluate in query-api before the gates → Task 7 (`resolve_action_row`, unchanged downstream). ✓
- Define-time typing + safety (ref resolution, type inference, compatibility, arity, forward/cyclic refs, double-bind) → Task 6 (conformance) + Task 2. Double-bind already covered by existing `bound` set (Task 4 keeps it). ✓
- Runtime faults → 422 → Task 7 (`ParamError::BadValue` → `BadParams` → 422), tested in Tasks 3, 7, 8. ✓
- Storage (`Assignment { property, source }`, migration, both adapters, testkit) → Tasks 4 + 5. ✓
- Applies to Insert + Update; Delete unchanged → Task 7 threads `now` into both `run_insert` and `run_mutate`; Delete conformance already forbids assignments. ✓
- Testing matrix (round-trip, rejection matrix, arithmetic, conditional+string, now, runtime 422, gate composition, update mapping) → Task 5 (round-trip), Task 6 (rejection), Task 8 (e2e 3–8). ✓
- `now()` deterministic injection → Tasks 3, 7. ✓

**Placeholder scan:** The only intentional temporary stubs are the `Expr` arms in Task 4 (postgres write, params, conformance), each explicitly replaced in a later task (5, 7, 6 respectively) — flagged inline. Task 8 test bodies reference "mirror `action_mapping_e2e.rs`" — that file is fully shown in the recon and the concrete assertions are spelled out per case. No `TBD`/`add error handling`/unshown code.

**Type consistency:** `Assignment`/`AssignmentSource` names, `Assignment::constant`/`::expr` constructors, `resolve_action_row(..., now)` arity, `TypeEnv`/`ValueEnv` method names (`param_type`/`prop_type`/`prop_resolved`; `param`/`prop`), `parse_expr`/`typecheck`/`assignable`/`eval` signatures, and `Expr`/`BinOp`/`Func` variants are used identically across Tasks 1–8. `ParamError::BadValue(String, String)`, `ActionError::{Misconfigured, BadParams, WriteDenied}` match the recon.

## Execution Handoff

Plan complete. Recommended execution: **Subagent-Driven** — a fresh subagent per task with the two-stage (spec-compliance, then code-quality) review between tasks, per `superpowers:subagent-driven-development`.
