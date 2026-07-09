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
use crate::logical_type::BaseType;
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
    /// The property that is this type's monotonic version/sequence column, used
    /// as precedence by the `Versioned` CDC merge engine. `None` = no version
    /// column (back-compatible). At most one version property per type.
    pub version: Option<String>,
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
                version: None,
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

    /// Declare `prop` as the type's version/sequence column (used by the
    /// `Versioned` merge engine). Like `identity`, NOT validated here or at
    /// define time: a dangling name surfaces only when the versioned engine
    /// reads it (the declaration surface validates orderability, not existence).
    pub fn version(mut self, prop: impl Into<String>) -> Self {
        self.inner.version = Some(prop.into());
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

/// The result-type expectation of an aggregation, resolved against the target
/// column's base type. Pairs the acceptance predicate ([`ResultExpectation::accepts`])
/// with the human description ([`ResultExpectation::description`]) used in violation
/// messages, so the derived-property validator carries neither inline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultExpectation {
    /// A `Count`: an int64 in disguise — accept `Integer` or `Long`.
    IntegerOrLong,
    /// A `Sum`/`Avg`: any numeric base type.
    Numeric,
    /// A `Min`/`Max`: exactly the target column's own base type (`None` if that type
    /// could not be resolved, which nothing then satisfies).
    ExactColumn(Option<BaseType>),
}

impl ResultExpectation {
    /// Whether a declared result type (resolved to a `BaseType`, or `None` if unknown)
    /// is consistent with this category.
    #[must_use]
    pub fn accepts(&self, declared: Option<BaseType>) -> bool {
        match self {
            ResultExpectation::IntegerOrLong => {
                matches!(declared, Some(BaseType::Integer | BaseType::Long))
            }
            ResultExpectation::Numeric => declared.is_some_and(BaseType::is_numeric),
            ResultExpectation::ExactColumn(col) => declared.is_some() && declared == *col,
        }
    }

    /// A human label for the expected result type, used in violation messages.
    #[must_use]
    pub fn description(&self) -> String {
        match self {
            ResultExpectation::IntegerOrLong => "integer or long".to_string(),
            ResultExpectation::Numeric => "numeric".to_string(),
            ResultExpectation::ExactColumn(Some(b)) => b.canonical_name(),
            ResultExpectation::ExactColumn(None) => "the target column's type".to_string(),
        }
    }
}

impl Aggregation {
    /// The target-type column this aggregation reads, or `None` for `Count` (which
    /// aggregates rows, not a column).
    #[must_use]
    pub fn column(&self) -> Option<&str> {
        match self {
            Aggregation::Count => None,
            Aggregation::Sum(c)
            | Aggregation::Avg(c)
            | Aggregation::Min(c)
            | Aggregation::Max(c) => Some(c),
        }
    }

    /// A human label for this aggregation, used in violation messages.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Aggregation::Count => "Count",
            Aggregation::Sum(_) => "Sum",
            Aggregation::Avg(_) => "Avg",
            Aggregation::Min(_) => "Min",
            Aggregation::Max(_) => "Max",
        }
    }

    /// Whether this aggregation is applicable to a column of base type `col`
    /// (`None` = unresolved). `Sum`/`Avg` need numeric; `Min`/`Max` need ordered;
    /// `Count` takes no column and is always applicable.
    #[must_use]
    pub fn column_applicable(&self, col: Option<BaseType>) -> bool {
        match self {
            Aggregation::Sum(_) | Aggregation::Avg(_) => col.is_some_and(BaseType::is_numeric),
            Aggregation::Min(_) | Aggregation::Max(_) => col.is_some_and(BaseType::is_ordered),
            Aggregation::Count => true,
        }
    }

    /// The result-type expectation for this aggregation given its target column's base
    /// type `col` (used only by `Min`/`Max`).
    #[must_use]
    pub fn result_expectation(&self, col: Option<BaseType>) -> ResultExpectation {
        match self {
            Aggregation::Count => ResultExpectation::IntegerOrLong,
            Aggregation::Sum(_) | Aggregation::Avg(_) => ResultExpectation::Numeric,
            Aggregation::Min(_) | Aggregation::Max(_) => ResultExpectation::ExactColumn(col),
        }
    }
}

/// A named ontology action (e.g. "createCustomer").
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ActionName(pub String);

/// Which kind of mutation an action performs against its target type. `Insert`
/// (part-1) creates a new object; `Update`/`Delete` (A5) mutate or remove one
/// existing object located by the target type's declared `identity`.
///
/// The serde wire token is the lowercase canonical form (`insert`/`update`/`delete`),
/// matching [`ActionKind::as_str`] / [`ActionKind::from_str`] and the postgres
/// `action_steps.kind` column default (`'insert'`, migration `0019`). The prior
/// derived-default PascalCase form was an inconsistency with every other wire surface.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
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

/// The source of a property's [`Assignment`]: either a fixed constant (the JSON wire form of a
/// scalar, coerced to the property's logical type on the write path) or a bounded expression
/// over the action's params / earlier-resolved properties (computed at invocation, slice 2).
/// Not `Eq` because `serde_json::Value` is not `Eq`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AssignmentSource {
    Const(serde_json::Value),
    Expr(String),
    /// A reference to an earlier step's resolved property (`@<bind>.<prop>`). Define-time
    /// validated (the `bind` must name a strictly-earlier bound step and `prop` a real property
    /// of that step's target); resolved at invocation from the prior-step binding environment.
    StepRef {
        bind: String,
        prop: String,
    },
}

/// A declared assignment filling a property when no parameter supplies it. `Const` is the
/// default/fixed-value case (e.g. `status = "active"`); `Expr` computes the value from the
/// action's inputs (e.g. `total = qty * unitPrice`). Ordered within `ActionDef.assignments`;
/// an `Expr` may reference a property assigned *earlier* in that order.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Assignment {
    pub property: String,
    pub source: AssignmentSource,
}

impl Assignment {
    /// A fixed-constant assignment (the slice-1 shape).
    pub fn constant(property: impl Into<String>, value: serde_json::Value) -> Self {
        Assignment {
            property: property.into(),
            source: AssignmentSource::Const(value),
        }
    }

    /// A computed-expression assignment (slice 2); `source` is the raw expression string.
    pub fn expr(property: impl Into<String>, source: impl Into<String>) -> Self {
        Assignment {
            property: property.into(),
            source: AssignmentSource::Expr(source.into()),
        }
    }

    /// A cross-step reference assignment (`property = @<bind>.<prop>`): fill `property` from the
    /// resolved value of `prop` on the earlier step bound as `bind`.
    pub fn step_ref(
        property: impl Into<String>,
        bind: impl Into<String>,
        prop: impl Into<String>,
    ) -> Self {
        Assignment {
            property: property.into(),
            source: AssignmentSource::StepRef {
                bind: bind.into(),
                prop: prop.into(),
            },
        }
    }
}

/// A downstream job an action enqueues atomically with its write (slice 4). `kind`
/// is validated at define time against [`crate::KNOWN_JOB_KINDS`]; `payload` is a JSON
/// object whose string leaves beginning with `@` are `@self.<prop>` references resolved
/// against the written (primary-step) row at invoke time.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JobTemplate {
    pub kind: String,
    #[serde(default = "serde_json::Value::default")]
    pub payload: serde_json::Value,
}

/// One step of an [`ActionDef`]: a single-target mutation. `bind` names the step's
/// output row so later steps can reference its properties (`@order.id`). Slice-1
/// `ParamDef.binds` and slice-2 `Assignment` live inside a step unchanged.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActionStep {
    pub target: TypeName,
    pub kind: ActionKind,
    #[serde(default)]
    pub parameters: Vec<ParamDef>,
    #[serde(default)]
    pub assignments: Vec<Assignment>,
    /// Names this step's resolved row for cross-step references. `None` ⇒ not bindable.
    #[serde(default)]
    pub bind: Option<String>,
}

/// A named action: an ordered list of single-target mutation [`ActionStep`]s committed
/// in one transaction. A single-step action is byte-compatible with the pre-steps flat
/// shape (see the `ActionDefRepr` serde bridge).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(from = "ActionDefRepr", into = "ActionDefRepr")]
pub struct ActionDef {
    pub name: ActionName,
    pub steps: Vec<ActionStep>,
    /// Downstream jobs the action enqueues atomically with its write (slice 4). The
    /// field-level `#[serde(default)]` is documentary — the [`ActionDefRepr`] bridge
    /// carries the real default (empty) on both its `Flat` and `Stepped` variants.
    #[serde(default)]
    pub downstream: Vec<JobTemplate>,
}

/// Wire bridge: a single-step, bind-less action reads/writes the legacy flat JSON;
/// anything else uses the explicit `steps` array. `#[serde(untagged)]` tries `Flat`
/// first on read, so legacy `{name,target,kind,parameters,assignments}` still parses.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum ActionDefRepr {
    Flat {
        name: ActionName,
        target: TypeName,
        #[serde(default)]
        kind: ActionKind,
        #[serde(default)]
        parameters: Vec<ParamDef>,
        #[serde(default)]
        assignments: Vec<Assignment>,
        #[serde(default)]
        downstream: Vec<JobTemplate>,
    },
    Stepped {
        name: ActionName,
        steps: Vec<ActionStep>,
        #[serde(default)]
        downstream: Vec<JobTemplate>,
    },
}

impl From<ActionDefRepr> for ActionDef {
    fn from(r: ActionDefRepr) -> Self {
        match r {
            ActionDefRepr::Flat {
                name,
                target,
                kind,
                parameters,
                assignments,
                downstream,
            } => {
                let mut def = ActionDef::single_step(name, target, kind, parameters, assignments);
                def.downstream = downstream;
                def
            }
            ActionDefRepr::Stepped {
                name,
                steps,
                downstream,
            } => ActionDef {
                name,
                steps,
                downstream,
            },
        }
    }
}

impl From<ActionDef> for ActionDefRepr {
    fn from(a: ActionDef) -> Self {
        // A single bind-less step round-trips to the flat form (byte-compat). `into()`
        // consumes `a`, so move the single step out before matching on its `bind`.
        let mut steps = a.steps;
        let single = if steps.len() == 1 {
            match steps.first() {
                Some(s) if s.bind.is_none() => steps.pop(),
                _ => None,
            }
        } else {
            None
        };
        match single {
            Some(s) => ActionDefRepr::Flat {
                name: a.name,
                target: s.target,
                kind: s.kind,
                parameters: s.parameters,
                assignments: s.assignments,
                downstream: a.downstream,
            },
            None => ActionDefRepr::Stepped {
                name: a.name,
                steps,
                downstream: a.downstream,
            },
        }
    }
}

impl ActionDef {
    /// Build a one-step action from the pre-steps flat shape (the implicit-step migration).
    #[must_use]
    pub fn single_step(
        name: ActionName,
        target: TypeName,
        kind: ActionKind,
        parameters: Vec<ParamDef>,
        assignments: Vec<Assignment>,
    ) -> Self {
        ActionDef {
            name,
            steps: vec![ActionStep {
                target,
                kind,
                parameters,
                assignments,
                bind: None,
            }],
            downstream: Vec::new(),
        }
    }

    /// Replace this action's downstream templates (slice 4). Default is empty (set by
    /// [`ActionDef::single_step`] / [`ActionDef::build`]). A chained setter mirroring
    /// [`ActionDefBuilder::downstream`] for callers that finish with `single_step`.
    #[must_use]
    pub fn downstream(mut self, templates: Vec<JobTemplate>) -> Self {
        self.downstream = templates;
        self
    }

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
    /// assert_eq!(update.steps.len(), 1);
    /// ```
    pub fn build(
        name: impl Into<String>,
        target: impl Into<String>,
        kind: ActionKind,
    ) -> ActionDefBuilder {
        ActionDefBuilder {
            name: ActionName(name.into()),
            steps: vec![ActionStep {
                target: TypeName(target.into()),
                kind,
                parameters: Vec::new(),
                assignments: Vec::new(),
                bind: None,
            }],
            downstream: Vec::new(),
        }
    }
}

/// Fluent constructor for [`ActionDef`] — see [`ActionDef::build`]. Methods
/// append in call order onto the current (last) step; `step` opens a new step.
#[derive(Clone, Debug)]
pub struct ActionDefBuilder {
    name: ActionName,
    /// Always holds at least one open step (seeded by [`ActionDef::build`]).
    steps: Vec<ActionStep>,
    /// Downstream templates (slice 4); empty by default until `.downstream(...)`.
    downstream: Vec<JobTemplate>,
}

impl ActionDefBuilder {
    /// Append an optional (`required: false`) parameter binding the property of
    /// the same name (`binds: None`) to the current step.
    pub fn param(mut self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.parameters.push(ParamDef {
                name: name.into(),
                ty: ty.into(),
                required: false,
                binds: None,
            });
        }
        self
    }

    /// Append a required parameter binding the property of the same name to the current step.
    pub fn param_req(mut self, name: impl Into<String>, ty: impl Into<String>) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.parameters.push(ParamDef {
                name: name.into(),
                ty: ty.into(),
                required: true,
                binds: None,
            });
        }
        self
    }

    /// Append a parameter renamed away from the property it writes
    /// ([`ParamDef::binds`] = `Some(binds)`) to the current step — the full [`ParamDef`] surface.
    pub fn param_bound(
        mut self,
        name: impl Into<String>,
        ty: impl Into<String>,
        required: bool,
        binds: impl Into<String>,
    ) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.parameters.push(ParamDef {
                name: name.into(),
                ty: ty.into(),
                required,
                binds: Some(binds.into()),
            });
        }
        self
    }

    /// Append a declared constant assignment filling `property` with `value` when no
    /// parameter supplies it, on the current step.
    pub fn assign(mut self, property: impl Into<String>, value: serde_json::Value) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.assignments.push(Assignment::constant(property, value));
        }
        self
    }

    /// Append a declared computed-expression assignment on the current step: `property` is set
    /// by evaluating `source` (the closed grammar) over the action's inputs.
    pub fn assign_expr(mut self, property: impl Into<String>, source: impl Into<String>) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.assignments.push(Assignment::expr(property, source));
        }
        self
    }

    /// Append a cross-step reference assignment on the current step: `property` is set to the
    /// value of an **earlier** step's `prop`, read from the step named `bind` (`@bind.prop`,
    /// e.g. a child row's `orderId` = the parent step's minted `id`). The reference is validated
    /// at define time (the bind must name a strictly-earlier step and `prop` a real property of
    /// that step's target).
    pub fn assign_step_ref(
        mut self,
        property: impl Into<String>,
        bind: impl Into<String>,
        prop: impl Into<String>,
    ) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.assignments
                .push(Assignment::step_ref(property, bind, prop));
        }
        self
    }

    /// Open a new step targeting `target` of mutation kind `kind`; subsequent
    /// `param`/`assign` calls append to it.
    #[must_use]
    pub fn step(mut self, target: impl Into<String>, kind: ActionKind) -> ActionDefBuilder {
        self.steps.push(ActionStep {
            target: TypeName(target.into()),
            kind,
            parameters: Vec::new(),
            assignments: Vec::new(),
            bind: None,
        });
        self
    }

    /// Name the current step's resolved row so later steps can reference its properties.
    #[must_use]
    pub fn bind(mut self, name: impl Into<String>) -> Self {
        if let Some(s) = self.steps.last_mut() {
            s.bind = Some(name.into());
        }
        self
    }

    /// Replace the action's downstream templates (slice 4). Default is empty (seeded
    /// by [`ActionDef::build`]). A chained setter; call before [`done`](Self::done).
    #[must_use]
    pub fn downstream(mut self, templates: Vec<JobTemplate>) -> Self {
        self.downstream = templates;
        self
    }

    /// Finish: the assembled [`ActionDef`].
    pub fn done(self) -> ActionDef {
        ActionDef {
            name: self.name,
            steps: self.steps,
            downstream: self.downstream,
        }
    }
}

/// Validate each derived property's aggregate column against its link's target
/// type — best-effort, catalog-decoupled. `resolve_target(link_name)` returns the
/// target `ObjectType` or `None`; an unresolvable link/target is SKIPPED (deferred
/// to the read path). A derived aggregate reads a TARGET-TABLE column, which a type
/// need not declare as a property (a type may declare a subset of its table's
/// columns; the ingest `bind` conformance seam validates against the catalog table),
/// so a column absent from the declared `properties` is likewise SKIPPED here. Only
/// a DECLARED property whose KNOWN logical type is inapplicable to the aggregation
/// (`Sum`/`Avg` need numeric, `Min`/`Max` need ordered) is a `Validation` error.
///
/// `resolve_target` carries an explicit lifetime `'a` (rather than a fully
/// elided `Fn(&str) -> Option<&ObjectType>`) because the elided form desugars
/// to a higher-ranked bound (`for<'r> Fn(&'r str) -> Option<&'r ObjectType>`)
/// that a closure returning a reference into a captured map (e.g.
/// `|ln| targets.get(ln)`, where the output's lifetime comes from `targets`,
/// not from `ln`) cannot satisfy — the borrow checker demands `targets: 'static`.
/// Naming `'a` ties the output to the caller's own borrow instead.
pub fn validate_derived_columns<'a>(
    derived: &[DerivedPropertyDef],
    resolve_target: impl Fn(&str) -> Option<&'a ObjectType>,
) -> Result<()> {
    for d in derived {
        let Some(col) = d.agg.column() else { continue }; // Count: no column
        let Some(target) = resolve_target(&d.link) else {
            continue;
        }; // unresolvable link/target: skip (deferred)
        // Best-effort: a derived aggregate reads a TARGET-TABLE column, which a type need
        // not declare as a property (a type may declare a subset of its table's columns; the
        // ingest `bind` conformance seam validates against the catalog table). So a column
        // absent from the declared properties is SKIPPED here — only a DECLARED property whose
        // KNOWN logical type is inapplicable to the aggregation is rejected.
        let Some(prop) = target.properties.iter().find(|p| p.name == col) else {
            continue;
        };
        let base = crate::logical_type::resolve_logical(&prop.ty);
        if base.is_some() && !d.agg.column_applicable(base) {
            return Err(ControlPlaneError::Validation(format!(
                "derived `{}`: aggregation over column `{col}` (type `{}`) is not applicable \
                 (Sum/Avg need a numeric column; Min/Max need an ordered column)",
                d.name, prop.ty
            )));
        }
    }
    Ok(())
}

#[async_trait]
pub trait Ontology {
    /// Create or replace an object type and its full (ordered) property list. Upsert.
    async fn define_type(&self, ty: ObjectType) -> Result<()>;
    /// Create or replace a link, keyed by `(name, from)`. Both endpoint types must
    /// already exist, else `NotFound`. Upsert.
    async fn define_link(&self, link: LinkDef) -> Result<()>;
    /// Delete a link, keyed by `(from, name)`. Definition only — the physical
    /// backing columns / join tables are untouched. Idempotent: deleting an
    /// absent link is `Ok(())`.
    async fn delete_link(&self, from: &TypeName, name: &str) -> Result<()>;
    /// Names of derived properties on `from` whose link is `name`. Empty if none.
    /// Only derived properties on `from` can be stranded by deleting link
    /// `(from, name)` (a derived property resolves its link among its own type's
    /// outbound links).
    async fn derived_properties_referencing(
        &self,
        from: &TypeName,
        name: &str,
    ) -> Result<Vec<String>>;
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
    /// Delete a named action and its steps/params/assignments. Definition only.
    /// Idempotent: deleting an absent action is `Ok(())`.
    async fn delete_action(&self, name: &ActionName) -> Result<()>;
    /// Fetch an action by name. `NotFound` if absent.
    async fn get_action(&self, name: &ActionName) -> Result<ActionDef>;
    /// Page through every defined action, name-ordered. Adapters return the
    /// full set in a single page (`next: None`); `page` is accepted for future
    /// keyset paging, like [`Ontology::list_types`].
    async fn list_actions(&self, page: PageReq) -> Result<Page<ActionDef>>;
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
