//! Pure, DOM-free SQL diagnostics engine. The `SqlEditor` component
//! (`loom_ui_components`) recomputes diagnostics on each edit and maps them to
//! Monaco markers; all logic lives here so it is unit-testable without a browser.
//!
//! v1 is deliberately conservative (client-side only, no backend): it flags only
//! (1) an unknown *table* name in `FROM`/`JOIN` position — a simple, unquoted
//! identifier that matches neither a schema table nor a `WITH` CTE name — and
//! (2) unbalanced parentheses. It never flags qualified (`a.b`), double-quoted, or
//! aliased identifiers, never flags columns (alias/scope resolution is the deferred
//! server-side EXPLAIN work), and emits nothing when the schema is empty (not yet
//! loaded). Columns are 1-based char offsets within a line (Monaco's convention),
//! exact for ASCII — the SQL-identifier case.

use crate::CompletionSchema;

/// Severity of a [`Diagnostic`], mapped to a Monaco marker severity by the component.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DiagnosticSeverity {
    Warning,
    Error,
}

/// A single-line editor diagnostic. `line`/`start_col`/`end_col` are 1-based;
/// `end_col` is exclusive (Monaco's `endColumn` convention).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Diagnostic {
    pub line: u32,
    pub start_col: u32,
    pub end_col: u32,
    pub message: String,
    pub severity: DiagnosticSeverity,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TokKind {
    Ident,
    QuotedIdent,
    LParen,
    RParen,
    Comma,
    Dot,
    Other,
}

/// A lexed token with its 1-based start position. Whitespace, comments and
/// string literals are consumed by the tokenizer and never emitted.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Token {
    kind: TokKind,
    text: String,
    line: u32,
    col: u32,
}

impl Token {
    /// Exclusive 1-based end column = start column + char length.
    fn end_col(&self) -> u32 {
        self.col
            .saturating_add(u32::try_from(self.text.chars().count()).unwrap_or(0))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TableState {
    Normal,
    ExpectTable,
    AfterTable,
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_ident_cont(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// SQL clause keywords that can appear in table position on an incomplete query
/// but are never a table name — so they are not flagged.
fn is_reserved(word: &str) -> bool {
    const RESERVED: &[&str] = &[
        "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING", "JOIN", "LEFT", "RIGHT",
        "INNER", "OUTER", "CROSS", "FULL", "ON", "AS", "AND", "OR", "NOT", "IN", "IS", "NULL",
        "LIMIT", "OFFSET", "DISTINCT", "WITH", "UNION", "ALL", "INSERT", "INTO", "VALUES",
        "UPDATE", "DELETE", "SET", "USING", "NATURAL",
    ];
    RESERVED.iter().any(|k| word.eq_ignore_ascii_case(k))
}

/// Precompute the 1-based `(line, col)` of each char index.
fn line_cols(chars: &[char]) -> Vec<(u32, u32)> {
    let mut out = Vec::with_capacity(chars.len());
    let mut line = 1u32;
    let mut col = 1u32;
    for &c in chars {
        out.push((line, col));
        if c == '\n' {
            line = line.saturating_add(1);
            col = 1;
        } else {
            col = col.saturating_add(1);
        }
    }
    out
}

/// Index just past a `delim`-quoted span whose opening `delim` is at `i`; a
/// doubled delimiter (`''` / `""`) is an escape, not a terminator. Returns the
/// input length if the span is unterminated.
fn scan_quoted_span(chars: &[char], i: usize, delim: char) -> usize {
    let n = chars.len();
    let mut j = i + 1;
    while j < n {
        if chars.get(j).copied() == Some(delim) {
            if chars.get(j + 1).copied() == Some(delim) {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    j
}

/// Index just past a `-- ...` line comment whose `--` starts at `i` (stops at the
/// newline, which the caller then treats as whitespace).
fn scan_line_comment(chars: &[char], i: usize) -> usize {
    let n = chars.len();
    let mut j = i + 2;
    while j < n && chars.get(j).copied() != Some('\n') {
        j += 1;
    }
    j
}

/// Index just past a `/* ... */` block comment whose `/*` starts at `i`.
fn scan_block_comment(chars: &[char], i: usize) -> usize {
    let n = chars.len();
    let mut j = i + 2;
    while j < n && !(chars.get(j).copied() == Some('*') && chars.get(j + 1).copied() == Some('/')) {
        j += 1;
    }
    (j + 2).min(n)
}

/// Index just past an identifier `[A-Za-z_][A-Za-z0-9_]*` whose first char is at `i`.
fn scan_ident(chars: &[char], i: usize) -> usize {
    let n = chars.len();
    let mut j = i + 1;
    while j < n && chars.get(j).copied().is_some_and(is_ident_cont) {
        j += 1;
    }
    j
}

/// Classify a single punctuation char into its token kind.
fn punct_kind(c: char) -> TokKind {
    match c {
        '(' => TokKind::LParen,
        ')' => TokKind::RParen,
        ',' => TokKind::Comma,
        '.' => TokKind::Dot,
        _ => TokKind::Other,
    }
}

/// Build a token spanning `chars[s..end]` at the 1-based `(line, col)` position.
fn make_token(chars: &[char], kind: TokKind, s: usize, end: usize, pos: (u32, u32)) -> Token {
    let text: String = chars.get(s..end).unwrap_or_default().iter().collect();
    Token {
        kind,
        text,
        line: pos.0,
        col: pos.1,
    }
}

/// Lex `text` into significant tokens, dropping whitespace, `--` line comments,
/// `/* */` block comments, and `'...'` string literals. Inner scans are delegated
/// to `scan_*` helpers so this stays a flat dispatch loop.
fn tokenize(text: &str) -> Vec<Token> {
    let chars: Vec<char> = text.chars().collect();
    let pos = line_cols(&chars);
    let n = chars.len();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    let start_pos = |k: usize| pos.get(k).copied().unwrap_or((1, 1));

    while i < n {
        let Some(c) = chars.get(i).copied() else {
            break;
        };
        let next = chars.get(i + 1).copied();

        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && next == Some('-') {
            i = scan_line_comment(&chars, i);
        } else if c == '/' && next == Some('*') {
            i = scan_block_comment(&chars, i);
        } else if c == '\'' {
            i = scan_quoted_span(&chars, i, '\'');
        } else if c == '"' {
            let end = scan_quoted_span(&chars, i, '"');
            tokens.push(make_token(
                &chars,
                TokKind::QuotedIdent,
                i,
                end,
                start_pos(i),
            ));
            i = end;
        } else if is_ident_start(c) {
            let end = scan_ident(&chars, i);
            tokens.push(make_token(&chars, TokKind::Ident, i, end, start_pos(i)));
            i = end;
        } else {
            let (line, col) = start_pos(i);
            tokens.push(Token {
                kind: punct_kind(c),
                text: c.to_string(),
                line,
                col,
            });
            i += 1;
        }
    }
    tokens
}

/// Names introduced by `WITH name AS (...)` (CTEs) or derived-table aliases
/// (`... AS (...)`); collected lowercase so table checks treat them as known.
fn collect_cte_names(tokens: &[Token]) -> Vec<String> {
    let mut names = Vec::new();
    let mut idx = 0usize;
    while idx < tokens.len() {
        if let (Some(a), Some(b), Some(c)) =
            (tokens.get(idx), tokens.get(idx + 1), tokens.get(idx + 2))
            && a.kind == TokKind::Ident
            && b.kind == TokKind::Ident
            && b.text.eq_ignore_ascii_case("AS")
            && c.kind == TokKind::LParen
        {
            names.push(a.text.to_ascii_lowercase());
        }
        idx += 1;
    }
    names
}

/// Evaluate the token at `idx`, which is in `FROM`/`JOIN` table position, and
/// return the next state. Flags a simple unknown table; skips qualified, quoted,
/// subquery, reserved, known-table and CTE cases.
fn handle_expect_table(
    schema: &CompletionSchema,
    tokens: &[Token],
    idx: usize,
    ctes: &[String],
    diags: &mut Vec<Diagnostic>,
) -> TableState {
    let Some(t) = tokens.get(idx) else {
        return TableState::Normal;
    };
    match t.kind {
        TokKind::LParen | TokKind::QuotedIdent => TableState::Normal,
        TokKind::Ident => {
            if tokens.get(idx + 1).map(|nx| nx.kind) == Some(TokKind::Dot) {
                return TableState::Normal; // qualified name — skip
            }
            if is_reserved(&t.text) {
                return TableState::Normal; // a keyword, not a table
            }
            let known = schema
                .tables
                .iter()
                .any(|tb| tb.name.eq_ignore_ascii_case(&t.text))
                || ctes.iter().any(|c| c.eq_ignore_ascii_case(&t.text));
            if !known {
                diags.push(Diagnostic {
                    line: t.line,
                    start_col: t.col,
                    end_col: t.end_col(),
                    message: format!("unknown table `{}`", t.text),
                    severity: DiagnosticSeverity::Warning,
                });
            }
            TableState::AfterTable
        }
        _ => TableState::Normal,
    }
}

/// Flag unknown table names in `FROM`/`JOIN` position. No-op on an empty schema.
fn check_tables(schema: &CompletionSchema, tokens: &[Token], diags: &mut Vec<Diagnostic>) {
    if schema.tables.is_empty() {
        return;
    }
    let ctes = collect_cte_names(tokens);
    let mut state = TableState::Normal;
    let mut idx = 0usize;
    while idx < tokens.len() {
        let Some(t) = tokens.get(idx) else { break };
        if t.kind == TokKind::Ident
            && (t.text.eq_ignore_ascii_case("FROM") || t.text.eq_ignore_ascii_case("JOIN"))
        {
            state = TableState::ExpectTable;
            idx += 1;
            continue;
        }
        state = match state {
            TableState::Normal => TableState::Normal,
            TableState::ExpectTable => handle_expect_table(schema, tokens, idx, &ctes, diags),
            TableState::AfterTable => {
                if t.kind == TokKind::Comma {
                    TableState::ExpectTable
                } else {
                    TableState::Normal
                }
            }
        };
        idx += 1;
    }
}

/// Flag unbalanced parentheses: each unmatched `)` at its position, and each
/// still-open `(` at end of input.
fn check_brackets(tokens: &[Token], diags: &mut Vec<Diagnostic>) {
    let mut open: Vec<&Token> = Vec::new();
    for t in tokens {
        match t.kind {
            TokKind::LParen => open.push(t),
            TokKind::RParen if open.pop().is_none() => {
                diags.push(Diagnostic {
                    line: t.line,
                    start_col: t.col,
                    end_col: t.col.saturating_add(1),
                    message: "unexpected `)`".to_owned(),
                    severity: DiagnosticSeverity::Error,
                });
            }
            _ => {}
        }
    }
    for t in open {
        diags.push(Diagnostic {
            line: t.line,
            start_col: t.col,
            end_col: t.col.saturating_add(1),
            message: "unclosed `(`".to_owned(),
            severity: DiagnosticSeverity::Error,
        });
    }
}

/// Compute client-side diagnostics for `text` against `schema`. Total, pure,
/// order: table warnings (source order) then bracket errors.
#[must_use]
pub fn sql_diagnostics(schema: &CompletionSchema, text: &str) -> Vec<Diagnostic> {
    let tokens = tokenize(text);
    let mut diags = Vec::new();
    check_tables(schema, &tokens, &mut diags);
    check_brackets(&tokens, &mut diags);
    diags
}
