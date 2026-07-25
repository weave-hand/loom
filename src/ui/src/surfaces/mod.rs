//! Live authenticated surfaces rendered inside the `Shell`. Each surface is split
//! into presentational components; the interactive state and load effects are
//! lifted into `Workspace` (`main.rs`), which passes state down and callbacks up.

mod catalog;
mod ontology;
mod query;
mod transforms;

pub use catalog::{CatalogControls, CatalogDrawer, CatalogList};
pub use ontology::{LoadStatus, OntologyDrawer, OntologyList, OntologyTypeRow};
pub use query::QueryView;
pub use transforms::{TransformDrawer, TransformEditor, TransformsList};
