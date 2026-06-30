//! The ontology concern: loom's user-facing typed model — object types, their
//! logical properties, links between types, and the mapping from a type to its
//! backing Iceberg table. Unlike the catalog, the `ontology` schema is
//! loom-owned: this trait reads AND writes it.
//!
//! Property types are the ontology's own logical vocabulary (e.g. `EmailAddress`),
//! deliberately decoupled from the physical column types (which come from
//! [`crate::Catalog::schema`]). [`Ontology::resolve`] bridges a type to its
//! physical table via [`crate::TableRef`].

use async_trait::async_trait;

use crate::TableRef;
use crate::error::Result;
use crate::page::{Page, PageReq};
use crate::vector_index::{IndexSpec, Metric};

/// An ontology type name (e.g. "Customer").
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TypeName(pub String);

/// A logical property of an object type. `ty` is the ontology's logical type
/// (loom's vocabulary), NOT the physical column type.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PropertyDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// An ontology object type: a named, propertied view bound to a physical table.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectType {
    pub name: TypeName,
    /// Ordered.
    pub properties: Vec<PropertyDef>,
    /// Ordered. Aggregate-over-link computed properties (served alongside `properties`).
    pub derived: Vec<DerivedPropertyDef>,
    pub table: TableRef,
    /// The property that is this type's primary key, if declared. Names one of
    /// `properties`. `None` = no declared identity (back-compatible).
    pub identity: Option<String>,
}

/// Link multiplicity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Cardinality {
    One,
    Many,
}

/// How a link is physically realized as a join. Carried by `LinkDef`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

impl LinkBacking {
    /// The backing for traversing this link in the inverse direction (`to` -> `from`).
    /// The column roles are swapped so the same symmetric chain-join compiler reaches
    /// the origin type; the mapping table (for `JoinTable`) is unchanged. An involution:
    /// `b.reversed().reversed() == b`.
    pub fn reversed(&self) -> LinkBacking {
        match self {
            LinkBacking::ForeignKey {
                from_column,
                to_column,
            } => LinkBacking::ForeignKey {
                from_column: to_column.clone(),
                to_column: from_column.clone(),
            },
            LinkBacking::JoinTable {
                table,
                from_key,
                from_column,
                to_column,
                to_key,
            } => LinkBacking::JoinTable {
                table: table.clone(),
                from_key: to_key.clone(),
                from_column: to_column.clone(),
                to_column: from_column.clone(),
                to_key: from_key.clone(),
            },
        }
    }
}

/// A directed link between two types (e.g. `Order.customer -> Customer`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LinkDef {
    pub name: String,
    pub from: TypeName,
    pub to: TypeName,
    pub cardinality: Cardinality,
    pub backing: LinkBacking,
}

/// How a derived property aggregates over its link's target rows. The `String` is the
/// target-type column to aggregate (COUNT takes none).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DerivedPropertyDef {
    pub name: String,
    pub ty: String,
    pub link: String,
    pub agg: Aggregation,
}

/// A named ontology action (e.g. "createCustomer").
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ActionName(pub String);

/// Which kind of mutation an action performs against its target type. `Insert`
/// (part-1) creates a new object; `Update`/`Delete` (A5) mutate or remove one
/// existing object located by the target type's declared `identity`.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
pub enum ActionKind {
    #[default]
    Insert,
    Update,
    Delete,
}

/// A typed input to an action. `ty` is the ontology's logical vocabulary (like `PropertyDef.ty`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParamDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// A named vector index declared on an object type's `vector(N)` property. The
/// declaration is the authoritative source of an index's kind/metric/params;
/// the build primitive resolves it and copies it into the mirror row. Dimension
/// is NOT restated — it is derived from the property's `vector(N)` type. Multiple
/// indexes may exist per property, distinguished by `name` (unique per type).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorIndexDef {
    pub name: String,
    pub type_name: TypeName,
    pub property: String,
    pub metric: Metric,
    pub spec: IndexSpec,
}

/// A named ontology operation. Part-1 semantics: insert one new instance of `target`,
/// taking a value for each parameter.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActionDef {
    pub name: ActionName,
    pub target: TypeName,
    /// Ordered.
    pub parameters: Vec<ParamDef>,
    /// The mutation kind. `Insert` (part-1 default) creates; `Update`/`Delete` mutate
    /// one existing object by `target`'s declared `identity`.
    pub kind: ActionKind,
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
    /// All links whose `to` is `name` — inbound adjacency, the inverse of [`links`].
    /// `NotFound` if the type itself is absent. The `page` request is accepted but not yet
    /// enforced; results are a single full page.
    async fn links_to(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>>;
    /// The physical Iceberg table backing `name`. `NotFound` if the type is absent.
    async fn resolve(&self, name: &TypeName) -> Result<TableRef>;
    /// Create or replace a named action and its ordered parameter list. Upsert.
    async fn define_action(&self, action: ActionDef) -> Result<()>;
    /// Fetch an action by name. `NotFound` if absent.
    async fn get_action(&self, name: &ActionName) -> Result<ActionDef>;
    /// Declare (upsert) a named vector index, keyed by `(type, name)`. Replaces an
    /// existing index of the same key — matching `define_type`'s replace semantics.
    /// Validates that `def.property` exists on `def.type_name` and is a `vector(N)`
    /// type; returns `Validation` otherwise.
    async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()>;
    /// Fetch one named index declaration. `None` if absent.
    async fn get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>>;
    /// All index declarations on `type_name` (order unspecified).
    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>>;
}
