//! Governed-catalog wire payloads: a caller-resolved set of per-type governance
//! (row filters, denied columns, masked columns) that the engine's external-SQL path
//! applies. Pure data (serde), no DataFusion — engine-serving turns these into an
//! enforcing `TableProvider`; engine-wire carries a `GovernedCatalog` in its ticket.

use serde::{Deserialize, Serialize};

use crate::TableRef;
use crate::acl::RowFilter;

/// One type's governance in a caller-supplied catalog. Vec fields keep it JSON-stable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedTable {
    pub table: TableRef,
    #[serde(default)]
    pub row_filters: Vec<RowFilter>,
    #[serde(default)]
    pub denied: Vec<String>,
    #[serde(default)]
    pub masked: Vec<String>,
}

/// A fully-resolved governed catalog: one `GovernedTable` per type the caller may see.
/// A table with no entry is treated as fully visible — the caller (slice 2) owns
/// deny-by-default at the edge by never listing a type without a grant.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedCatalog {
    pub tables: Vec<GovernedTable>,
}

impl GovernedCatalog {
    /// The catalog entry for `table`, if present.
    #[must_use]
    pub fn table_for(&self, table: &TableRef) -> Option<&GovernedTable> {
        self.tables.iter().find(|gt| &gt.table == table)
    }
}
