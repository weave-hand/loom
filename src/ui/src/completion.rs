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
