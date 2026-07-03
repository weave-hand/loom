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
use crate::error::{ControlPlaneError, Result};
use crate::page::{Page, PageReq};
use crate::vector_index::{IndexSpec, Metric};

/// An ontology type name (e.g. "Customer").
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TypeName(pub String);

/// A logical property of an object type. `ty` is the ontology's logical type
/// (loom's vocabulary), NOT the physical column type. `constraints` (default empty)
/// declares optional per-value validation rules enforced on every write path.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PropertyDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
    #[serde(
        default,
        skip_serializing_if = "crate::constraints::PropertyConstraints::is_empty"
    )]
    pub constraints: crate::constraints::PropertyConstraints,
}

/// An ontology object type: a named, propertied view bound to a physical table.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

impl ObjectType {
    /// Start a fluent [`ObjectTypeBuilder`] for a type named `name`, backed by the
    /// physical table `(schema, table)`. Plain construction — no validation, no
    /// I/O (that stays with [`Ontology::define_type`], exactly as for a literal).
    ///
    /// ```
    /// use control_plane_core::ObjectType;
    /// let docs = ObjectType::build("Docs", ("wh", "docs"))
    ///     .prop_req("id", "Long")
    ///     .prop("note", "String")
    ///     .identity("id")
    ///     .done();
    /// assert_eq!(docs.identity.as_deref(), Some("id"));
    /// ```
    pub fn build(
        name: impl Into<String>,
        table: (impl Into<String>, impl Into<String>),
    ) -> ObjectTypeBuilder {
        ObjectTypeBuilder {
            inner: ObjectType {
                name: TypeName(name.into()),
                properties: Vec::new(),
                derived: Vec::new(),
                table: TableRef {
                    schema: table.0.into(),
                    name: table.1.into(),
                },
                identity: None,
            },
        }
    }
}

/// Fluent constructor for [`ObjectType`] — see [`ObjectType::build`]. Methods
/// append in call order (`properties`/`derived` are ordered).
#[derive(Clone, Debug)]
pub struct ObjectTypeBuilder {
    inner: ObjectType,
}

impl ObjectTypeBuilder {
    /// Append an optional (`required: false`), unconstrained property.
    pub fn prop(self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.prop_with(
            name,
            ty,
            false,
            crate::constraints::PropertyConstraints::default(),
        )
    }

    /// Append a required, unconstrained property.
    pub fn prop_req(self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.prop_with(
            name,
            ty,
            true,
            crate::constraints::PropertyConstraints::default(),
        )
    }

    /// Append a property with explicit requiredness and constraints — the full
    /// [`PropertyDef`] surface.
    pub fn prop_with(
        mut self,
        name: impl Into<String>,
        ty: impl Into<String>,
        required: bool,
        constraints: crate::constraints::PropertyConstraints,
    ) -> Self {
        self.inner.properties.push(PropertyDef {
            name: name.into(),
            ty: ty.into(),
            required,
            constraints,
        });
        self
    }

    /// Append a derived (aggregate-over-link) property. Passthrough — the
    /// [`DerivedPropertyDef`] literal is already minimal.
    pub fn derived(mut self, def: DerivedPropertyDef) -> Self {
        self.inner.derived.push(def);
        self
    }

    /// Declare `prop` as the type's identity (primary key). Should name one of
    /// the declared properties — NOT validated here or at define time (matching
    /// literal construction): a dangling identity surfaces only when the
    /// identity is used (action/read paths).
    pub fn identity(mut self, prop: impl Into<String>) -> Self {
        self.inner.identity = Some(prop.into());
        self
    }

    /// Finish: the assembled [`ObjectType`].
    pub fn done(self) -> ObjectType {
        self.inner
    }
}

/// Link multiplicity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Cardinality {
    One,
    Many,
}

impl Cardinality {
    /// The persisted wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Cardinality::One => "one",
            Cardinality::Many => "many",
        }
    }
}

impl std::str::FromStr for Cardinality {
    type Err = ControlPlaneError;

    /// Parse the persisted token. Unknown tokens are a loud error (a corrupt
    /// row), never a silent default.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "one" => Ok(Cardinality::One),
            "many" => Ok(Cardinality::Many),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown cardinality '{other}'"
            ))),
        }
    }
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

impl ActionKind {
    /// The persisted wire token.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ActionKind::Insert => "insert",
            ActionKind::Update => "update",
            ActionKind::Delete => "delete",
        }
    }
}

impl std::str::FromStr for ActionKind {
    type Err = ControlPlaneError;

    /// Parse the persisted token. Unknown tokens are a loud error (a corrupt
    /// row), never a silent default.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "insert" => Ok(ActionKind::Insert),
            "update" => Ok(ActionKind::Update),
            "delete" => Ok(ActionKind::Delete),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown action kind '{other}'"
            ))),
        }
    }
}

/// A typed input to an action. `ty` is the ontology's logical vocabulary (like `PropertyDef.ty`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ParamDef {
    pub name: String,
    pub ty: String,
    pub required: bool,
    /// The property this parameter writes. `None` ⇒ the property named `name` (so an action
    /// whose params are named for their properties is unchanged); `Some(p)` renames the
    /// param away from the property `p` it binds.
    #[serde(default)]
    pub binds: Option<String>,
}

impl ParamDef {
    /// The property this parameter writes: its explicit `binds`, else its own `name`.
    #[must_use]
    pub fn binds_property(&self) -> &str {
        self.binds.as_deref().unwrap_or(&self.name)
    }
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

/// A declared constant filling a property when no parameter supplies it (the
/// default/fixed-value case, e.g. `status = "active"`). `value` is the JSON wire form of a
/// scalar — the canonical representation the query-api write path coerces to the property's
/// logical type (the same path parameters take); it is validated against the property type at
/// invocation-time conformance. Not `Eq` because `serde_json::Value` is not `Eq`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConstAssignment {
    pub property: String,
    pub value: serde_json::Value,
}

/// A named ontology operation. Slice-1 semantics: insert/update/delete one instance of
/// `target`. Parameters are mapped onto the target's properties (via `ParamDef.binds`), and
/// `assignments` fill properties with declared constants when no parameter supplies them.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActionDef {
    pub name: ActionName,
    pub target: TypeName,
    /// Ordered.
    pub parameters: Vec<ParamDef>,
    /// The mutation kind. `Insert` (part-1 default) creates; `Update`/`Delete` mutate
    /// one existing object by `target`'s declared `identity`.
    pub kind: ActionKind,
    /// Ordered constant property assignments (the default/fixed-value case).
    #[serde(default)]
    pub assignments: Vec<ConstAssignment>,
}

impl ActionDef {
    /// Start a fluent [`ActionDefBuilder`] for an action named `name` targeting
    /// the type `target`, of mutation kind `kind`. Plain construction — no
    /// validation, no I/O (that stays with [`Ontology::define_action`]).
    ///
    /// ```
    /// use control_plane_core::{ActionDef, ActionKind};
    /// let update = ActionDef::build("updateWidget", "Widget", ActionKind::Update)
    ///     .param_req("id", "Long")
    ///     .param_req("qty", "Long")
    ///     .done();
    /// assert_eq!(update.parameters.len(), 2);
    /// ```
    pub fn build(
        name: impl Into<String>,
        target: impl Into<String>,
        kind: ActionKind,
    ) -> ActionDefBuilder {
        ActionDefBuilder {
            inner: ActionDef {
                name: ActionName(name.into()),
                target: TypeName(target.into()),
                parameters: Vec::new(),
                kind,
                assignments: Vec::new(),
            },
        }
    }
}

/// Fluent constructor for [`ActionDef`] — see [`ActionDef::build`]. Methods
/// append in call order (`parameters`/`assignments` are ordered).
#[derive(Clone, Debug)]
pub struct ActionDefBuilder {
    inner: ActionDef,
}

impl ActionDefBuilder {
    /// Append an optional (`required: false`) parameter binding the property of
    /// the same name (`binds: None`).
    pub fn param(mut self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.inner.parameters.push(ParamDef {
            name: name.into(),
            ty: ty.into(),
            required: false,
            binds: None,
        });
        self
    }

    /// Append a required parameter binding the property of the same name.
    pub fn param_req(mut self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        self.inner.parameters.push(ParamDef {
            name: name.into(),
            ty: ty.into(),
            required: true,
            binds: None,
        });
        self
    }

    /// Append a parameter renamed away from the property it writes
    /// ([`ParamDef::binds`] = `Some(binds)`) — the full [`ParamDef`] surface.
    pub fn param_bound(
        mut self,
        name: impl Into<String>,
        ty: impl Into<String>,
        required: bool,
        binds: impl Into<String>,
    ) -> Self {
        self.inner.parameters.push(ParamDef {
            name: name.into(),
            ty: ty.into(),
            required,
            binds: Some(binds.into()),
        });
        self
    }

    /// Append a declared constant assignment ([`ConstAssignment`]) filling
    /// `property` with `value` when no parameter supplies it.
    pub fn assign(mut self, property: impl Into<String>, value: serde_json::Value) -> Self {
        self.inner.assignments.push(ConstAssignment {
            property: property.into(),
            value,
        });
        self
    }

    /// Finish: the assembled [`ActionDef`].
    pub fn done(self) -> ActionDef {
        self.inner
    }
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
