//! Live authenticated surfaces rendered inside the `Shell`. Each surface is split
//! into presentational components; the interactive state and load effects are
//! lifted into `Workspace` (`main.rs`), which passes state down and callbacks up.

mod catalog;
mod ontology;
mod transforms;

pub use catalog::{CatalogDrawer, CatalogList};
pub use ontology::{LoadStatus, OntologyDrawer, OntologyList, OntologyTypeRow};
// Unused until Task 9 mounts these into `Workspace`'s render arm; matches the
// module-level `dead_code` allow in `transforms.rs` for the same reason.
#[allow(
    unused_imports,
    reason = "consumed by Task 9's Workspace render arm; the allow is removed there"
)]
pub use transforms::{TransformDrawer, TransformEditor, TransformsList};
