//! Live authenticated surfaces rendered inside the `Shell`. Each surface is split
//! into presentational components; the interactive state and load effects are
//! lifted into `Workspace` (`main.rs`), which passes state down and callbacks up.

mod catalog;
mod ontology;

pub use catalog::{CatalogDrawer, CatalogList};
pub use ontology::{LoadStatus, OntologyDrawer, OntologyList};
