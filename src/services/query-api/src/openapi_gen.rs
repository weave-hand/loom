//! Pure ontology→OpenAPI generation: map an ontology snapshot (types + links + actions) to
//! OpenAPI `Paths` + component `Schemas`. No I/O — the caller reads the live ontology and
//! hands it a snapshot. The type codec is faithful to `crate::render`'s wire rendering (e.g.
//! Long is a decimal string, not a JSON integer — see `crate::render::render_cell`).

use std::collections::{BTreeMap, BTreeSet};

use control_plane_core::{
    ActionDef, ActionKind, ActionStep, BaseType, LinkDef, ObjectType, resolve_logical,
};
use service_runtime::BEARER_SCHEME_NAME;
use utoipa::openapi::path::{
    HttpMethod, Operation, OperationBuilder, Parameter, ParameterBuilder, ParameterIn, PathItem,
    Paths, PathsBuilder,
};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::schema::{
    ArrayBuilder, KnownFormat, ObjectBuilder, SchemaFormat, SchemaType, Type,
};
use utoipa::openapi::security::SecurityRequirement;
use utoipa::openapi::{Content, Ref, RefOr, Required, ResponseBuilder, Schema};

/// A non-nullable single `Type`, or (when `nullable`) the OpenAPI 3.1 type array
/// `[ty, "null"]`.
fn schema_type_of(ty: Type, nullable: bool) -> SchemaType {
    if nullable {
        SchemaType::Array(vec![ty, Type::Null])
    } else {
        SchemaType::Type(ty)
    }
}

/// The OpenAPI schema for one loom base type, `nullable` widening the type to also admit
/// JSON null. Faithful to `crate::render::render_cell`: `Long` is a decimal **string** (the
/// wire encodes int64 as a string to keep precision past 2^53), integers/doubles are JSON
/// numbers, dates/timestamps are formatted strings, and `vector(N)` is a bounded number array.
#[must_use]
pub fn base_type_to_schema(bt: BaseType, nullable: bool) -> RefOr<Schema> {
    let (ty, format): (Type, Option<SchemaFormat>) = match bt {
        BaseType::Integer => (
            Type::Integer,
            Some(SchemaFormat::KnownFormat(KnownFormat::Int32)),
        ),
        // Long renders as a decimal string on the wire (int64 > 2^53 safe range).
        BaseType::Long => (Type::String, None),
        BaseType::Double => (
            Type::Number,
            Some(SchemaFormat::KnownFormat(KnownFormat::Double)),
        ),
        BaseType::Boolean => (Type::Boolean, None),
        BaseType::String => (Type::String, None),
        BaseType::Date => (
            Type::String,
            Some(SchemaFormat::KnownFormat(KnownFormat::Date)),
        ),
        BaseType::Timestamp => (
            Type::String,
            Some(SchemaFormat::KnownFormat(KnownFormat::DateTime)),
        ),
        BaseType::Vector(n) => return vector_schema(n),
    };
    let mut b = ObjectBuilder::new().schema_type(schema_type_of(ty, nullable));
    if let Some(f) = format {
        b = b.format(Some(f));
    }
    if matches!(bt, BaseType::Long) {
        b = b.description(Some("int64 encoded as a decimal string"));
    }
    RefOr::T(Schema::Object(b.build()))
}

/// A dense `vector(N)` embedding → a bounded `array` of `number`(float). Nullability is not
/// modeled on the array (no property is a nullable vector in practice, and the read path
/// always projects the full embedding).
fn vector_schema(n: u32) -> RefOr<Schema> {
    let item = ObjectBuilder::new()
        .schema_type(SchemaType::Type(Type::Number))
        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Float)))
        .build();
    let arr = ArrayBuilder::new()
        .items(RefOr::T(Schema::Object(item)))
        .min_items(Some(n as usize))
        .max_items(Some(n as usize))
        .build();
    RefOr::T(Schema::Array(arr))
}

/// The schema for a property's logical type. An unrecognized logical type falls back to a
/// free-form string (mirrors `render`'s never-fail-a-permitted-read posture). A non-required
/// property is nullable.
pub(crate) fn property_schema(
    ty: &str,
    required: bool,
    description: Option<&str>,
) -> RefOr<Schema> {
    let base = match resolve_logical(ty) {
        Some(bt) => base_type_to_schema(bt, !required),
        None => RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .build(),
        )),
    };
    let Some(desc) = description else { return base };
    // Fold the property's prose into the schema's description, preserving any existing
    // note (e.g. Long's "int64 encoded as a decimal string") in parentheses.
    match base {
        RefOr::T(Schema::Object(mut obj)) => {
            let combined = match obj.description.take() {
                Some(existing) => format!("{desc} ({existing})"),
                None => desc.to_string(),
            };
            obj.description = Some(combined);
            RefOr::T(Schema::Object(obj))
        }
        other => other, // arrays (vector) etc. — no description slot we model; leave as-is
    }
}

fn bearer() -> SecurityRequirement {
    SecurityRequirement::new(BEARER_SCHEME_NAME, Vec::<String>::new())
}

/// `{ "objects": [ $ref #/components/schemas/{type_name} ] }` — the typed read wrapper,
/// mirroring `render::objects_to_json`'s shape.
fn objects_response_schema(type_name: &str) -> RefOr<Schema> {
    let item = RefOr::Ref(Ref::from_schema_name(type_name));
    let array = ArrayBuilder::new().items(item).build();
    let obj = ObjectBuilder::new()
        .schema_type(SchemaType::Type(Type::Object))
        .property("objects", RefOr::T(Schema::Array(array)))
        .build();
    RefOr::T(Schema::Object(obj))
}

fn json_response(schema: RefOr<Schema>, description: &str) -> utoipa::openapi::Response {
    ResponseBuilder::new()
        .description(description)
        .content("application/json", Content::new(Some(schema)))
        .build()
}

/// Mark a property schema `readOnly` — derived/computed columns are served on reads but are
/// never writable. Non-object schemas (none arise for derived scalar aggregates) pass through.
fn read_only_derived(schema: RefOr<Schema>) -> RefOr<Schema> {
    match schema {
        RefOr::T(Schema::Object(mut obj)) => {
            obj.read_only = Some(true);
            RefOr::T(Schema::Object(obj))
        }
        other => other,
    }
}

/// The component (read) schema for a type: properties by codec, identity `required`.
fn type_component_schema(ty: &ObjectType) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    if let Some(d) = &ty.description {
        b = b.description(Some(d.clone()));
    }
    for p in &ty.properties {
        b = b.property(
            p.name.clone(),
            property_schema(&p.ty, p.required, p.description.as_deref()),
        );
    }
    // Derived (aggregate-over-link) properties are computed, served on reads, and never
    // writable — declare them readOnly so the read document matches object-read rows while
    // the write/action schemas (built from action params) never gain them.
    for d in &ty.derived {
        b = b.property(
            d.name.clone(),
            read_only_derived(property_schema(&d.ty, false, d.description.as_deref())),
        );
    }
    if let Some(id) = &ty.identity {
        b = b.required(id.clone());
    }
    RefOr::T(Schema::Object(b.build()))
}

/// The shared filter-predicate grammar, documented once on every per-property filter
/// parameter. Mirrors `crate::filter::coerce_predicate`: split at the first `:` into
/// `op:value`; a bare value is `eq`. Kept in sync with the operator token list there.
const FILTER_GRAMMAR: &str = "Filter predicate `op:value` — op ∈ eq, ne, lt, le, gt, ge, in, \
     nin, between, contains, startswith, endswith, isnull, isnotnull; a bare value (no `op:`) \
     means eq. Set ops (in/nin/between) take comma-separated operands (escape a literal comma \
     as `\\,`). Repeat the key to apply several predicates to one column.";

/// A bare string schema, the wire type of every filter/`_ids`/`_or`/`cursor` value.
fn string_schema() -> RefOr<Schema> {
    RefOr::T(Schema::Object(
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::String))
            .build(),
    ))
}

/// An optional query-string `Parameter` with the given schema and description.
fn query_param(name: &str, schema: RefOr<Schema>, description: &str) -> Parameter {
    ParameterBuilder::new()
        .name(name)
        .parameter_in(ParameterIn::Query)
        .required(Required::False)
        .description(Some(description))
        .schema(Some(schema))
        .build()
}

/// One filter query parameter (`?<key>=op:value`) documenting the shared predicate grammar.
/// `key` is the wire key — a bare property name on a type read, or `<link>.<prop>` for a
/// link's target column; `label` names the column the filter applies to in the description.
fn filter_param(key: &str, label: &str, ty: &str) -> Parameter {
    query_param(
        key,
        string_schema(),
        &format!("Filter on `{label}` (declared type `{ty}`). {FILTER_GRAMMAR}"),
    )
}

/// `GET /objects/{Type}`: the governed typed read. Documents one filter parameter per
/// property (the `op:value` grammar) plus the reserved knobs `_ids` (object-set scoping),
/// `_or` (cross-column OR-groups), and `limit`/`cursor` (keyset pagination) — the same set
/// `get_object` splits out in `http.rs`.
fn get_objects_op(ty: &ObjectType) -> Operation {
    let name = &ty.name.0;
    let mut op = OperationBuilder::new()
        .summary(Some(format!("List {name} objects")))
        .tag(name.clone())
        .security(bearer());
    for p in &ty.properties {
        op = op.parameter(filter_param(&p.name, &p.name, &p.ty));
    }
    let int_schema = RefOr::T(Schema::Object(
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::Integer))
            .build(),
    ));
    op.parameter(query_param(
        "_ids",
        string_schema(),
        "Restrict the read to this comma-separated identity set (object-set scoping). \
         Mutually exclusive with `limit`/`cursor` pagination.",
    ))
    .parameter(query_param(
        "_or",
        string_schema(),
        "OR-group of cross-column predicates: `col:pred,col:pred` (>=2 members). Repeatable; \
         each member uses the same predicate grammar as a plain filter.",
    ))
    .parameter(query_param(
        "limit",
        int_schema,
        "Page size, clamped to [1,200]. Presence (with `cursor`) selects cursor pagination; \
         incompatible with `_ids`.",
    ))
    .parameter(query_param(
        "cursor",
        string_schema(),
        "Opaque keyset cursor from a previous page's `next`. Presence selects cursor pagination.",
    ))
    .response(
        "200",
        json_response(objects_response_schema(name), "Matching objects"),
    )
    .response("400", plain_response("Bad filter, _ids, or pagination"))
    .response("403", plain_response("Forbidden by ACL policy"))
    .response("404", plain_response("Unknown type"))
    .build()
}

/// `GET /objects/{from}/links/{link}`: traverse a single link. Documents the reserved
/// knobs `_direction` (forward|inverse), `_shape` (objects|association), and `_ids`
/// (source object-set), plus a filter parameter per source property (bare key) and per
/// target property (`<link>.<prop>` key) — the keys `resolve_chain_filters` resolves.
fn link_op(
    from_ty: &ObjectType,
    link_name: &str,
    to_ty: &ObjectType,
    description: Option<&str>,
) -> Operation {
    let from = &from_ty.name.0;
    let to = &to_ty.name.0;
    let mut op =
        OperationBuilder::new().summary(Some(format!("Traverse {from}.{link_name} -> {to}")));
    if let Some(d) = description {
        op = op.description(Some(d.to_string()));
    }
    let mut op = op
        .tag(from.clone())
        .security(bearer())
        .parameter(query_param(
            "_direction",
            string_schema(),
            "Hop direction: `forward` (default) follows the link; `inverse` follows it backward.",
        ))
        .parameter(query_param(
            "_shape",
            string_schema(),
            "Response shape: `objects` (default) returns the linked objects; `association` \
             returns the source/target identity pairs.",
        ))
        .parameter(query_param(
            "_ids",
            string_schema(),
            "Restrict the source objects to this comma-separated identity set.",
        ));
    // A bare key filters the source object; a `<link>.<prop>` key filters the linked target.
    for p in &from_ty.properties {
        op = op.parameter(filter_param(&p.name, &format!("{from}.{}", p.name), &p.ty));
    }
    for p in &to_ty.properties {
        op = op.parameter(filter_param(
            &format!("{link_name}.{}", p.name),
            &format!("{to}.{}", p.name),
            &p.ty,
        ));
    }
    op.response(
        "200",
        json_response(objects_response_schema(to), "Linked objects"),
    )
    .response(
        "400",
        plain_response("Bad direction, shape, filter, or _ids"),
    )
    .response("403", plain_response("Forbidden by ACL policy"))
    .response("404", plain_response("Unknown type or link"))
    .build()
}

/// Request schema for an action: one property per parameter across every step,
/// in step order — the handler takes one flat body and projects it per step
/// (`binds` renames the written property, not the wire parameter). `required`
/// from the param flags, deduped (steps may share a parameter name).
fn action_request_schema(action: &ActionDef) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    for p in action.steps.iter().flat_map(|s| &s.parameters) {
        b = b.property(
            p.name.clone(),
            property_schema(&p.ty, p.required, p.description.as_deref()),
        );
    }
    let mut required = BTreeSet::new();
    for p in action.steps.iter().flat_map(|s| &s.parameters) {
        if p.required && required.insert(p.name.as_str()) {
            b = b.required(p.name.clone());
        }
    }
    RefOr::T(Schema::Object(b.build()))
}

/// The lowercase verb for a step kind, for multi-step summaries.
fn kind_verb(kind: ActionKind) -> &'static str {
    match kind {
        ActionKind::Insert => "insert",
        ActionKind::Update => "update",
        ActionKind::Delete => "delete",
    }
}

fn plain_response(description: &str) -> utoipa::openapi::Response {
    ResponseBuilder::new().description(description).build()
}

/// `POST /actions/{name}` for one defined action, tagged by **every** step's
/// target type (deduped, step order) so the op appears in each involved type's
/// docs section. A single-step action's 2xx body documents the affected
/// object itself (a ref to the target type's schema) — that's exactly what
/// `run_action`'s `ActionOutcome::Single` path returns. A multi-step action's
/// 2xx body instead documents the `{steps:[...]}` envelope (a ref to
/// `ActionStepsBody`), one entry per declared step, matching
/// `ActionOutcome::Multi`. The status is kind-true, matching `post_action`
/// (`http.rs`): Insert documents `201 Created`; Update/Delete document
/// `200 OK`. Multi-step actions key on the first (primary) step's kind.
fn action_op(action: &ActionDef, primary: &ActionStep) -> Operation {
    let target = &primary.target.0;
    let (summary, ok_desc, ok_schema) = if let [only] = action.steps.as_slice() {
        let summary = match only.kind {
            ActionKind::Insert => format!("Insert a {target}"),
            ActionKind::Update => format!("Update a {target} (identity-targeted PATCH)"),
            ActionKind::Delete => format!("Delete a {target} by identity"),
        };
        let desc = match only.kind {
            ActionKind::Insert => "Created object",
            ActionKind::Update => "Updated object",
            ActionKind::Delete => "Deleted object (pre-deletion values)",
        };
        (summary, desc, RefOr::Ref(Ref::from_schema_name(target)))
    } else {
        let steps = action
            .steps
            .iter()
            .map(|s| format!("{} {}", kind_verb(s.kind), s.target.0))
            .collect::<Vec<_>>()
            .join(", ");
        (
            format!("Atomically {steps}"),
            "Ordered `steps` envelope: one entry per declared step, each `{bind, target, objects}`",
            RefOr::Ref(Ref::from_schema_name("ActionStepsBody")),
        )
    };
    let body = RequestBodyBuilder::new()
        .content(
            "application/json",
            Content::new(Some(action_request_schema(action))),
        )
        .build();
    let mut op = OperationBuilder::new().summary(Some(summary));
    if let Some(d) = &action.description {
        op = op.description(Some(d.clone()));
    }
    let mut tagged = BTreeSet::new();
    for step in &action.steps {
        if tagged.insert(step.target.0.as_str()) {
            op = op.tag(step.target.0.clone());
        }
    }
    let ok_status = match primary.kind {
        ActionKind::Insert => "201",
        ActionKind::Update | ActionKind::Delete => "200",
    };
    op.security(bearer())
        .request_body(Some(body))
        .response(ok_status, json_response(ok_schema, ok_desc))
        .response(
            "400",
            plain_response("Malformed or undecodable request body"),
        )
        .response("403", plain_response("Write denied by ACL policy"))
        .response("404", plain_response("Unknown action or target object"))
        .response(
            "422",
            plain_response("Constraint violation or bad parameters"),
        )
        .build()
}

/// Map an ontology snapshot to OpenAPI paths + component schemas. Pure. For each type: a
/// component read schema and a `GET /objects/{Type}`, tagged by the type name. For each
/// link: a `GET /objects/{from}/links/{link}` typed to the target. For each action: a
/// `POST /actions/{name}` whose request body is derived from the action's parameters.
#[must_use]
pub fn ontology_openapi(
    types: &[ObjectType],
    links: &[LinkDef],
    actions: &[ActionDef],
) -> (Paths, BTreeMap<String, RefOr<Schema>>) {
    let mut schemas: BTreeMap<String, RefOr<Schema>> = BTreeMap::new();
    let by_name: BTreeMap<&str, &ObjectType> =
        types.iter().map(|t| (t.name.0.as_str(), t)).collect();
    let mut pb = PathsBuilder::new();
    for ty in types {
        let name = &ty.name.0;
        schemas.insert(name.clone(), type_component_schema(ty));
        pb = pb.path(
            format!("/objects/{name}"),
            PathItem::new(HttpMethod::Get, get_objects_op(ty)),
        );
    }
    for l in links {
        // Only emit a link whose endpoint types are both in the snapshot, so the generated
        // response `$ref #/components/schemas/{to}` always resolves — a snapshot skew (a
        // `links` read succeeding while a type read failed) must not yield an invalid document.
        // The type lookups double as that guard (a missing endpoint type skips the link).
        let (Some(from_ty), Some(to_ty)) =
            (by_name.get(l.from.0.as_str()), by_name.get(l.to.0.as_str()))
        else {
            continue;
        };
        pb = pb.path(
            format!("/objects/{}/links/{}", l.from.0, l.name),
            PathItem::new(
                HttpMethod::Get,
                link_op(from_ty, &l.name, to_ty, l.description.as_deref()),
            ),
        );
    }
    for a in actions {
        // Same skew guard as links, extended to steps: the 2xx response `$ref`s
        // the primary (first) step's schema and every step's target is a tag, so
        // ALL step targets must be in the snapshot. A stepless ActionDef is a
        // broken definition — skipped, never an invalid document.
        let Some(primary) = a.steps.first() else {
            continue;
        };
        if a.steps.iter().any(|s| !schemas.contains_key(&s.target.0)) {
            continue;
        }
        pb = pb.path(
            format!("/actions/{}", a.name.0),
            PathItem::new(HttpMethod::Post, action_op(a, primary)),
        );
    }
    (pb.build(), schemas)
}
