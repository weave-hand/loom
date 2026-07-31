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
//!
//! The second half of the module ([`anchor_diagnostic`] / [`parse_server_diagnostics`])
//! is the client side of the *server*-computed diagnostics that `POST /sql/validate`
//! returns: the engine plans the statement (so it does resolve columns) but attaches a
//! position to parser errors only, so a plan error's range has to be recovered against
//! the live editor text — which only the client holds. That half never runs the
//! tokenizer above and does not change what `sql_diagnostics` reports.

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
            let next_kind = tokens.get(idx + 1).map(|nx| nx.kind);
            if next_kind == Some(TokKind::Dot) {
                return TableState::Normal; // qualified name — skip
            }
            if next_kind == Some(TokKind::LParen) {
                return TableState::Normal; // table-valued function call — skip
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
/// `FROM`/`JOIN` only arm table detection at paren depth 0, so an inner `FROM`
/// inside a function call (`EXTRACT(f FROM col)`) or a subquery is not mistaken
/// for a clause keyword.
fn check_tables(schema: &CompletionSchema, tokens: &[Token], diags: &mut Vec<Diagnostic>) {
    if schema.tables.is_empty() {
        return;
    }
    let ctes = collect_cte_names(tokens);
    let mut state = TableState::Normal;
    let mut depth: u32 = 0;
    let mut idx = 0usize;
    while idx < tokens.len() {
        let Some(t) = tokens.get(idx) else { break };
        match t.kind {
            TokKind::LParen => depth = depth.saturating_add(1),
            TokKind::RParen => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && t.kind == TokKind::Ident
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

/// Strip surrounding quotes/backticks/dots and return the LAST dotted segment of a
/// possibly-qualified name: `"wh"."orders"."email"` → `email`, `nope.` → `nope`.
///
/// The split is naive about a dot *inside* a quoted segment (`"my.col"` → `col`), which
/// simply means that pathological name fails to resolve and the caller falls through to
/// the next candidate — never a wrong-but-confident anchor.
fn last_segment(name: &str) -> String {
    name.rsplit('.')
        .find(|s| !s.trim_matches(['"', '`']).is_empty())
        .unwrap_or(name)
        .trim_matches(['"', '`'])
        .to_owned()
}

/// The field name DataFusion reports in `No field named <name>` — the *offending*
/// identifier, so it is tried first. It arrives via `Column::quoted_flat_name`, which
/// quotes a segment only when it needs quoting: `nope` and `"wh"."orders"."email"` are
/// both possible, and only the last segment is a name that appears in the user's SQL.
fn field_name_candidate(message: &str) -> Option<String> {
    const NEEDLE: &str = "No field named ";
    let at = message.find(NEEDLE)?.checked_add(NEEDLE.len())?;
    let rest = message.get(at..)?;
    let raw: String = rest.chars().take_while(|c| !c.is_whitespace()).collect();
    let seg = last_segment(raw.trim_end_matches('.'));
    if seg.is_empty() { None } else { Some(seg) }
}

/// The contents of every `delim`-quoted span in `message`, in order of appearance.
fn quoted_spans(message: &str, delim: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = message;
    while let Some(open) = rest.find(delim) {
        let after_open = open.saturating_add(delim.len_utf8());
        let Some(tail) = rest.get(after_open..) else {
            break;
        };
        let Some(close) = tail.find(delim) else { break };
        if let Some(text) = tail.get(..close) {
            out.push(text.to_owned());
        }
        let Some(next) = tail.get(close.saturating_add(delim.len_utf8())..) else {
            break;
        };
        rest = next;
    }
    out
}

/// Identifier candidates from `message`, most-likely first: the offending field name,
/// then every quoted span in the order the message mentions them. Deduplicated,
/// preserving first appearance.
///
/// Order is load-bearing. `No field named "wh"."orders"."email"` mentions `orders`
/// before `email`, and `orders` *does* occur in the SQL — anchoring by raw message order
/// would squiggle the table name instead of the column.
fn message_candidates(message: &str) -> Vec<String> {
    /// Append `s` unless it is empty or already present.
    fn push_unique(out: &mut Vec<String>, s: String) {
        if !s.is_empty() && !out.contains(&s) {
            out.push(s);
        }
    }
    let mut out: Vec<String> = Vec::new();
    if let Some(f) = field_name_candidate(message) {
        push_unique(&mut out, f);
    }
    for span in quoted_spans(message, '"')
        .into_iter()
        .chain(quoted_spans(message, '`'))
    {
        push_unique(&mut out, last_segment(&span));
    }
    out
}

/// The 1-based `(line, start_col, end_col)` of the first whole-word, case-insensitive
/// occurrence of `word` in `sql`. Whole-word means neither neighbour is an identifier
/// character, so `id` does not match inside `idempotent`.
fn find_word_range(sql: &str, word: &str) -> Option<(u32, u32, u32)> {
    let wchars: Vec<char> = word.chars().collect();
    if wchars.is_empty() {
        return None;
    }
    let chars: Vec<char> = sql.chars().collect();
    let pos = line_cols(&chars);
    let n = chars.len();
    let w = wchars.len();
    let eq = |a: char, b: char| a.eq_ignore_ascii_case(&b);
    let mut i = 0usize;
    while i + w <= n {
        let hit = (0..w)
            .all(|k| matches!((chars.get(i + k), wchars.get(k)), (Some(&a), Some(&b)) if eq(a, b)));
        let before_ok = i == 0 || !chars.get(i - 1).copied().is_some_and(is_ident_cont);
        let after_ok = !chars.get(i + w).copied().is_some_and(is_ident_cont);
        if hit && before_ok && after_ok {
            let (line, col) = pos.get(i).copied()?;
            return Some((line, col, col.saturating_add(u32::try_from(w).ok()?)));
        }
        i += 1;
    }
    None
}

/// The full span of the first non-empty line of `sql` — the anchor of last resort, so a
/// message naming nothing we can find still lands somewhere visible rather than nowhere.
fn first_nonempty_line_range(sql: &str) -> Option<(u32, u32, u32)> {
    for (idx, line) in sql.lines().enumerate() {
        if !line.trim().is_empty() {
            let n = u32::try_from(line.chars().count()).unwrap_or(1);
            let lineno = u32::try_from(idx.saturating_add(1)).unwrap_or(1);
            return Some((lineno, 1, n.saturating_add(1)));
        }
    }
    None
}

/// Anchor a position-less engine message to a range in `sql`.
///
/// Server diagnostics carry typed positions only for *parser* errors; a *plan* error
/// (`Schema error: No field named foo`) has none, so the range has to be recovered from
/// the text the user is actually looking at — which only the client has. The rule: try
/// each identifier the message names, most-likely first (see [`message_candidates`]), as
/// a whole word in `sql`; failing that, fall back to the whole of the first non-empty
/// line. `None` only when `sql` has no non-empty line, in which case there is nothing to
/// squiggle.
///
/// Returns `(line, start_col, end_col)`, 1-based, `end_col` exclusive — Monaco's
/// convention, matching [`Diagnostic`].
#[must_use]
pub fn anchor_diagnostic(sql: &str, message: &str) -> Option<(u32, u32, u32)> {
    for cand in message_candidates(message) {
        if let Some(range) = find_word_range(sql, &cand) {
            return Some(range);
        }
    }
    first_nonempty_line_range(sql)
}

/// Read one diagnostic object from a `POST /sql/validate` response, resolving its range:
/// the server's typed positions when present, [`anchor_diagnostic`] over `sql` when they
/// are `null`. `None` when the object has no message, or when nothing can be anchored.
fn server_diagnostic(item: &serde_json::Value, sql: &str) -> Option<Diagnostic> {
    let message = item.get("message")?.as_str()?.to_owned();
    let num = |k: &str| -> Option<u32> { u32::try_from(item.get(k)?.as_u64()?).ok() };
    let (line, start_col, end_col) = match (num("line"), num("start_col"), num("end_col")) {
        (Some(l), Some(s), Some(e)) => (l, s, e),
        _ => anchor_diagnostic(sql, &message)?,
    };
    Some(Diagnostic {
        line,
        start_col,
        end_col,
        message,
        // The endpoint's vocabulary is closed: the engine only reports planning errors.
        severity: DiagnosticSeverity::Error,
    })
}

/// Parse a `POST /sql/validate` response body into editor diagnostics, anchored against
/// `sql` (the text that was validated). Total: a malformed or absent `diagnostics` array
/// yields an empty list rather than an error, so a backend shape change degrades to "no
/// squiggles" instead of breaking the editor.
#[must_use]
pub fn parse_server_diagnostics(body: &serde_json::Value, sql: &str) -> Vec<Diagnostic> {
    body.get("diagnostics")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|i| server_diagnostic(i, sql))
                .collect()
        })
        .unwrap_or_default()
}
