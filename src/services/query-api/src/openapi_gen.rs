//! Pure ontology→OpenAPI generation: map an ontology snapshot (types + links + actions) to
//! OpenAPI `Paths` + component `Schemas`. No I/O — the caller reads the live ontology and
//! hands it a snapshot. The type codec is faithful to `crate::render`'s wire rendering (e.g.
//! Long is a decimal string, not a JSON integer — see `crate::render::render_cell`).

use std::collections::BTreeMap;

use control_plane_core::{ActionDef, ActionKind, BaseType, LinkDef, ObjectType, resolve_logical};
use service_runtime::BEARER_SCHEME_NAME;
use utoipa::openapi::path::{
    HttpMethod, Operation, OperationBuilder, PathItem, Paths, PathsBuilder,
};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::schema::{
    ArrayBuilder, KnownFormat, ObjectBuilder, SchemaFormat, SchemaType, Type,
};
use utoipa::openapi::security::SecurityRequirement;
use utoipa::openapi::{Content, Ref, RefOr, ResponseBuilder, Schema};

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
pub(crate) fn property_schema(ty: &str, required: bool) -> RefOr<Schema> {
    match resolve_logical(ty) {
        Some(bt) => base_type_to_schema(bt, !required),
        None => RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .build(),
        )),
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

/// The component (read) schema for a type: properties by codec, identity `required`.
fn type_component_schema(ty: &ObjectType) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    for p in &ty.properties {
        b = b.property(p.name.clone(), property_schema(&p.ty, p.required));
    }
    if let Some(id) = &ty.identity {
        b = b.required(id.clone());
    }
    RefOr::T(Schema::Object(b.build()))
}

fn get_objects_op(type_name: &str) -> Operation {
    OperationBuilder::new()
        .summary(Some(format!("List {type_name} objects")))
        .tag(type_name)
        .security(bearer())
        .response(
            "200",
            json_response(objects_response_schema(type_name), "Matching objects"),
        )
        .build()
}

fn link_op(from: &str, link_name: &str, to: &str) -> Operation {
    OperationBuilder::new()
        .summary(Some(format!("Traverse {from}.{link_name} -> {to}")))
        .tag(from)
        .security(bearer())
        .response(
            "200",
            json_response(objects_response_schema(to), "Linked objects"),
        )
        .build()
}

/// Request schema for an action: one property per parameter (`binds` renames the
/// written property, not the wire parameter), `required` from the param flags.
fn action_request_schema(action: &ActionDef) -> RefOr<Schema> {
    let mut b = ObjectBuilder::new().schema_type(SchemaType::Type(Type::Object));
    for p in &action.parameters {
        b = b.property(p.name.clone(), property_schema(&p.ty, p.required));
    }
    for p in action.parameters.iter().filter(|p| p.required) {
        b = b.required(p.name.clone());
    }
    RefOr::T(Schema::Object(b.build()))
}

fn plain_response(description: &str) -> utoipa::openapi::Response {
    ResponseBuilder::new().description(description).build()
}

/// `POST /actions/{name}` for one defined action, tagged by its target type.
fn action_op(action: &ActionDef) -> Operation {
    let target = &action.target.0;
    let (summary, ok_status, ok_desc) = match action.kind {
        ActionKind::Insert => (format!("Insert a {target}"), "201", "Created object"),
        ActionKind::Update => (
            format!("Update a {target} (identity-targeted PATCH)"),
            "200",
            "Updated object",
        ),
        ActionKind::Delete => (
            format!("Delete a {target} by identity"),
            "200",
            "Deleted object (pre-deletion values)",
        ),
    };
    let body = RequestBodyBuilder::new()
        .content(
            "application/json",
            Content::new(Some(action_request_schema(action))),
        )
        .build();
    OperationBuilder::new()
        .summary(Some(summary))
        .tag(target)
        .security(bearer())
        .request_body(Some(body))
        .response(
            ok_status,
            json_response(RefOr::Ref(Ref::from_schema_name(target)), ok_desc),
        )
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
    let mut pb = PathsBuilder::new();
    for ty in types {
        let name = &ty.name.0;
        schemas.insert(name.clone(), type_component_schema(ty));
        pb = pb.path(
            format!("/objects/{name}"),
            PathItem::new(HttpMethod::Get, get_objects_op(name)),
        );
    }
    for l in links {
        // Only emit a link whose endpoint types are both in the snapshot, so the generated
        // response `$ref #/components/schemas/{to}` always resolves — a snapshot skew (a
        // `links` read succeeding while a type read failed) must not yield an invalid document.
        if !schemas.contains_key(&l.from.0) || !schemas.contains_key(&l.to.0) {
            continue;
        }
        pb = pb.path(
            format!("/objects/{}/links/{}", l.from.0, l.name),
            PathItem::new(HttpMethod::Get, link_op(&l.from.0, &l.name, &l.to.0)),
        );
    }
    for a in actions {
        // Same skew guard as links: the 2xx response `$ref`s the target type's schema.
        if !schemas.contains_key(&a.target.0) {
            continue;
        }
        pb = pb.path(
            format!("/actions/{}", a.name.0),
            PathItem::new(HttpMethod::Post, action_op(a)),
        );
    }
    (pb.build(), schemas)
}
