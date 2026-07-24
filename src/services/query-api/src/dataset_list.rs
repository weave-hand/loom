//! Pure sort/filter for the `GET /datasets` list — socket-free so the ordering and
//! filtering rules are unit-testable without axum or a control plane. `list_datasets`
//! collects its ACL-gated entries into `DatasetSummary`s, then applies these.
//!
//! `sort` is optional: absent preserves the handler's natural `(schema,name)`-then-
//! views order (existing callers unchanged); present reorders. Malformed `sort`/`dir`
//! surface as the 400 message the route returns verbatim.

use crate::query_params::split_reserved;

/// One dataset list entry after ACL gating, before serialization. Unifies tables and
/// views so sort/filter treat them uniformly; `to_json` renders the wire shape (a
/// table has no `base` key; a view carries `base` and `rows: null`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetSummary {
    pub schema: String,
    pub name: String,
    pub project: String,
    pub updated: String,
    pub rows: Option<i64>,
    pub kind: &'static str,
    pub base: Option<(String, String)>,
}

impl DatasetSummary {
    /// The `GET /datasets` entry JSON. `base` is emitted only for views (a table entry
    /// has no `base` key at all, matching the existing route contract).
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        m.insert("schema".into(), self.schema.clone().into());
        m.insert("name".into(), self.name.clone().into());
        m.insert("project".into(), self.project.clone().into());
        m.insert("updated".into(), self.updated.clone().into());
        m.insert(
            "rows".into(),
            self.rows.map_or(serde_json::Value::Null, Into::into),
        );
        m.insert("kind".into(), self.kind.into());
        if let Some((bs, bn)) = &self.base {
            m.insert(
                "base".into(),
                serde_json::json!({ "schema": bs, "name": bn }),
            );
        }
        serde_json::Value::Object(m)
    }
}

/// The column a `sort` request orders by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortKey {
    Name,
    Project,
    Updated,
    Rows,
}

impl SortKey {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "name" => Ok(SortKey::Name),
            "project" => Ok(SortKey::Project),
            "updated" => Ok(SortKey::Updated),
            "rows" => Ok(SortKey::Rows),
            other => Err(format!(
                "unknown sort key '{other}' (allowed: name, project, updated, rows)"
            )),
        }
    }
}

/// Sort direction. `Asc` is the default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

impl SortDir {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "asc" => Ok(SortDir::Asc),
            "desc" => Ok(SortDir::Desc),
            other => Err(format!(
                "unknown sort direction '{other}' (allowed: asc, desc)"
            )),
        }
    }
}

/// The parsed, validated `GET /datasets` list controls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetListParams {
    pub sort: Option<SortKey>,
    pub dir: SortDir,
    pub project: Option<String>,
}

impl DatasetListParams {
    /// Parse raw query pairs. `sort` absent => `None` (order preserved); `dir` absent
    /// => `Asc`; `project` absent or empty => no filter. An unknown `sort`/`dir` value
    /// is the `Err` the route returns as a 400 body. Single-valued keys take the last
    /// occurrence (mirrors `query_params` semantics).
    pub fn from_params(params: &[(String, String)]) -> Result<Self, String> {
        let (reserved, _rest) = split_reserved(params.to_vec(), &["sort", "dir", "project"]);
        let sort = match reserved.last("sort") {
            Some(s) => Some(SortKey::parse(s)?),
            None => None,
        };
        let dir = match reserved.last("dir") {
            Some(s) => SortDir::parse(s)?,
            None => SortDir::Asc,
        };
        let project = reserved
            .last("project")
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);
        Ok(Self { sort, dir, project })
    }
}

/// Filter by exact `project` (if set), then — only when a `sort` key is given —
/// stable-sort by that key/direction with a `(schema, name)` ascending tiebreak that
/// is independent of `dir` (so equal-primary rows keep a deterministic order).
#[must_use]
pub fn apply(mut items: Vec<DatasetSummary>, params: &DatasetListParams) -> Vec<DatasetSummary> {
    if let Some(p) = &params.project {
        items.retain(|d| &d.project == p);
    }
    if let Some(key) = params.sort {
        items.sort_by(|a, b| {
            let primary = match key {
                SortKey::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
                SortKey::Project => a.project.to_lowercase().cmp(&b.project.to_lowercase()),
                SortKey::Updated => a.updated.cmp(&b.updated),
                SortKey::Rows => a.rows.cmp(&b.rows),
            };
            let primary = match params.dir {
                SortDir::Asc => primary,
                SortDir::Desc => primary.reverse(),
            };
            primary.then_with(|| {
                (a.schema.as_str(), a.name.as_str()).cmp(&(b.schema.as_str(), b.name.as_str()))
            })
        });
    }
    items
}
