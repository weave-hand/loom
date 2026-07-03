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
            '(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            '*' => {
                out.push(Tok::Star);
                i += 1;
            }
            '/' => {
                out.push(Tok::Slash);
                i += 1;
            }
            '%' => {
                out.push(Tok::Percent);
                i += 1;
            }
            '-' => {
                out.push(Tok::Minus);
                i += 1;
            }
            '+' => {
                if at(i + 1) == Some('+') {
                    out.push(Tok::Concat);
                    i += 2;
                } else {
                    out.push(Tok::Plus);
                    i += 1;
                }
            }
            '=' => {
                out.push(Tok::Eq);
                i += 1;
            }
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
                        '"' => {
                            i += 1;
                            break;
                        }
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
                                    return Err(ParseError(format!(
                                        "bad string escape: \\{other}"
                                    )));
                                }
                            }
                            i += 2;
                        }
                        other => {
                            s.push(other);
                            i += 1;
                        }
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
                    let n: f64 = text
                        .parse()
                        .map_err(|e| ParseError(format!("bad number literal: {e}")))?;
                    out.push(Tok::Double(n));
                } else {
                    let n: i64 = text
                        .parse()
                        .map_err(|e| ParseError(format!("integer literal out of range: {e}")))?;
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
                                Some(&Tok::Comma) => {
                                    self.pos += 1;
                                }
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
