//! Pure, DOM-free SQL completion engine. The `SqlEditor` component
//! (`loom_ui_components`) feeds cursor context in and maps the results to
//! Monaco completion items; all behaviour and filtering lives here so it is
//! unit-testable without a browser.

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct CompletionSchema {
    pub tables: Vec<CompletionTable>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CompletionTable {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<CompletionColumn>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CompletionColumn {
    pub name: String,
    pub ty: String,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SuggestionKind {
    Keyword,
    Table,
    Column,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Suggestion {
    pub label: String,
    pub kind: SuggestionKind,
    pub detail: Option<String>,
    pub insert_text: String,
}

/// The static keyword set offered on unqualified completion.
pub const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "JOIN",
    "LEFT JOIN",
    "INNER JOIN",
    "ON",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "LIMIT",
    "DISTINCT",
    "WITH",
    "UNION ALL",
    "INSERT INTO",
    "VALUES",
];

fn starts_with_ci(haystack: &str, prefix: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .starts_with(&prefix.to_ascii_lowercase())
}

/// Suggestions for the given cursor context.
/// - `qualifier = Some(t)` → columns of the table named `t` (case-insensitive),
///   filtered by `prefix`; empty if no such table.
/// - `qualifier = None` → table names + all column names + SQL keywords,
///   each filtered by `prefix`. Order: tables, columns, keywords.
#[must_use]
pub fn sql_completions(
    schema: &CompletionSchema,
    prefix: &str,
    qualifier: Option<&str>,
) -> Vec<Suggestion> {
    if let Some(q) = qualifier {
        let Some(table) = schema
            .tables
            .iter()
            .find(|t| t.name.eq_ignore_ascii_case(q))
        else {
            return Vec::new();
        };
        return table
            .columns
            .iter()
            .filter(|c| starts_with_ci(&c.name, prefix))
            .map(|c| Suggestion {
                label: c.name.clone(),
                kind: SuggestionKind::Column,
                detail: Some(c.ty.clone()),
                insert_text: c.name.clone(),
            })
            .collect();
    }

    let mut out = Vec::new();
    for t in &schema.tables {
        if starts_with_ci(&t.name, prefix) {
            out.push(Suggestion {
                label: t.name.clone(),
                kind: SuggestionKind::Table,
                detail: t.schema.clone(),
                insert_text: t.name.clone(),
            });
        }
    }
    for t in &schema.tables {
        for c in &t.columns {
            if starts_with_ci(&c.name, prefix) {
                out.push(Suggestion {
                    label: c.name.clone(),
                    kind: SuggestionKind::Column,
                    detail: Some(c.ty.clone()),
                    insert_text: c.name.clone(),
                });
            }
        }
    }
    for kw in SQL_KEYWORDS {
        if starts_with_ci(kw, prefix) {
            out.push(Suggestion {
                label: (*kw).to_owned(),
                kind: SuggestionKind::Keyword,
                detail: None,
                insert_text: (*kw).to_owned(),
            });
        }
    }
    out
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Extract `(prefix, qualifier)` at a byte `offset` into `text`.
/// `prefix` is the identifier ending at the cursor; `qualifier` is the
/// identifier immediately before a `.` that precedes the prefix, if present.
/// ASCII-identifier oriented (SQL identifiers); `offset` is clamped to `text`.
#[must_use]
pub fn cursor_context(text: &str, offset: usize) -> (String, Option<String>) {
    let offset = offset.min(text.len());
    let bytes = text.as_bytes();

    // Walk left over identifier chars to find the prefix start.
    let mut start = offset;
    while start > 0
        && bytes
            .get(start - 1)
            .is_some_and(|&b| is_ident_char(b as char))
    {
        start -= 1;
    }
    let prefix = text.get(start..offset).unwrap_or_default().to_owned();

    // If a '.' immediately precedes the prefix, read the qualifier before it.
    let qualifier = if start > 0 && bytes.get(start - 1) == Some(&b'.') {
        let mut qstart = start - 1;
        while qstart > 0
            && bytes
                .get(qstart - 1)
                .is_some_and(|&b| is_ident_char(b as char))
        {
            qstart -= 1;
        }
        let q = text.get(qstart..start - 1).unwrap_or_default();
        if q.is_empty() {
            None
        } else {
            Some(q.to_owned())
        }
    } else {
        None
    };

    (prefix, qualifier)
}
