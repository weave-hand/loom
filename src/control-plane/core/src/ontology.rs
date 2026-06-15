//! The ontology concern: loom's user-facing typed model — object types, their
//! logical properties, links between types, and the mapping from a type to its
//! backing DuckLake table. Unlike the catalog, the `ontology` schema is
//! loom-owned: this trait reads AND writes it.
//!
//! Property types are the ontology's own logical vocabulary (e.g. `EmailAddress`),
//! deliberately decoupled from the physical DuckLake column types (which come from
//! [`crate::Catalog::schema`]). [`Ontology::resolve`] bridges a type to its
//! physical table via [`crate::TableRef`].

use async_trait::async_trait;

use crate::TableRef;
use crate::error::Result;
use crate::page::{Page, PageReq};

/// An ontology type name (e.g. "Customer").
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TypeName(pub String);

/// A logical property of an object type. `ty` is the ontology's logical type
/// (loom's vocabulary), NOT the physical DuckLake column type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropertyDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// An ontology object type: a named, propertied view bound to a physical table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectType {
    pub name: TypeName,
    /// Ordered.
    pub properties: Vec<PropertyDef>,
    /// Ordered. Aggregate-over-link computed properties (served alongside `properties`).
    pub derived: Vec<DerivedPropertyDef>,
    pub table: TableRef,
}

/// Link multiplicity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

/// How a link is physically realized as a join. Carried by `LinkDef`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkBacking {
    /// Direct equijoin `from_table.from_column = to_table.to_column`.
    /// Covers one-to-many and many-to-one.
    ForeignKey {
        from_column: String,
        to_column: String,
    },
    /// Many-to-many through a mapping table:
    ///   `from_table.from_key = table.from_column AND table.to_column = to_table.to_key`.
    JoinTable {
        table: TableRef,
        from_key: String,
        from_column: String,
        to_column: String,
        to_key: String,
    },
}

/// A directed link between two types (e.g. `Order.customer -> Customer`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkDef {
    pub name: String,
    pub from: TypeName,
    pub to: TypeName,
    pub cardinality: Cardinality,
    pub backing: LinkBacking,
}

/// How a derived property aggregates over its link's target rows. The `String` is the
/// target-type column to aggregate (COUNT takes none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Aggregation {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

/// A computed property: aggregate `agg` over the rows reachable from this type via the
/// link named `link`. `ty` is the declared logical type of the result (e.g. "Long" for a
/// count, "Double" for an average).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedPropertyDef {
    pub name: String,
    pub ty: String,
    pub link: String,
    pub agg: Aggregation,
}

/// A named ontology action (e.g. "createCustomer").
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActionName(pub String);

/// A typed input to an action. `ty` is the ontology's logical vocabulary (like `PropertyDef.ty`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParamDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// A named ontology operation. Part-1 semantics: insert one new instance of `target`,
/// taking a value for each parameter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionDef {
    pub name: ActionName,
    pub target: TypeName,
    /// Ordered.
    pub parameters: Vec<ParamDef>,
}

#[async_trait]
pub trait Ontology {
    /// Create or replace an object type and its full (ordered) property list. Upsert.
    async fn define_type(&self, ty: ObjectType) -> Result<()>;
    /// Create or replace a link, keyed by `(name, from)`. Both endpoint types must
    /// already exist, else `NotFound`. Upsert.
    async fn define_link(&self, link: LinkDef) -> Result<()>;
    /// Fetch a type by name. `NotFound` if absent.
    async fn get_type(&self, name: &TypeName) -> Result<ObjectType>;
    /// All defined types (order unspecified). The `page` request is accepted but not yet enforced; results
    /// are a single full page.
    async fn list_types(&self, page: PageReq) -> Result<Page<ObjectType>>;
    /// All links whose `from` is `name`. `NotFound` if the type itself is absent. The `page` request is accepted but not yet enforced; results
    /// are a single full page.
    async fn links(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>>;
    /// The physical DuckLake table backing `name`. `NotFound` if the type is absent.
    async fn resolve(&self, name: &TypeName) -> Result<TableRef>;
    /// Create or replace a named action and its ordered parameter list. Upsert.
    async fn define_action(&self, action: ActionDef) -> Result<()>;
    /// Fetch an action by name. `NotFound` if absent.
    async fn get_action(&self, name: &ActionName) -> Result<ActionDef>;
}
