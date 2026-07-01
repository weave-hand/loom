//! Pure unit tests for the ontology→OpenAPI generator and its type codec, plus the live
//! per-request document. Memory-backed (no postgres): the generator is pure and the liveness
//! handler reads any `ControlPlane`, so `MemoryControlPlane` exercises `list_types`/`links`/
//! `define_type` identically to the postgres path for doc-generation purposes.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{
    BaseType, Cardinality, ControlPlane, LinkBacking, LinkDef, ObjectType, Ontology, PropertyDef,
    TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::openapi_gen::{base_type_to_schema, ontology_openapi};

// ---- codec ---------------------------------------------------------------------------

/// Serialize a schema to JSON for structural assertions (utoipa's typed builders are awkward
/// to pattern-match; the emitted JSON is the contract a client consumes).
fn to_json(schema: &utoipa::openapi::RefOr<utoipa::openapi::Schema>) -> serde_json::Value {
    serde_json::to_value(schema).unwrap()
}

/// The set of type tokens in an OpenAPI `type` node — a bare string, or a 3.1 nullable
/// type array like `["number","null"]`.
fn type_set(ty: &serde_json::Value) -> std::collections::BTreeSet<String> {
    match ty {
        serde_json::Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        serde_json::Value::String(s) => std::iter::once(s.clone()).collect(),
        _ => std::collections::BTreeSet::new(),
    }
}

#[test]
fn integer_maps_to_int32_number() {
    let j = to_json(&base_type_to_schema(BaseType::Integer, false));
    assert_eq!(j["type"], "integer");
    assert_eq!(j["format"], "int32");
}

#[test]
fn long_maps_to_string_not_integer() {
    // The wire renders Long as a decimal string (render.rs NumericString); the doc must match.
    let j = to_json(&base_type_to_schema(BaseType::Long, false));
    assert_eq!(j["type"], "string");
}

#[test]
fn double_maps_to_number() {
    let j = to_json(&base_type_to_schema(BaseType::Double, false));
    assert_eq!(j["type"], "number");
}

#[test]
fn boolean_maps_to_boolean() {
    assert_eq!(
        to_json(&base_type_to_schema(BaseType::Boolean, false))["type"],
        "boolean"
    );
}

#[test]
fn string_maps_to_string() {
    assert_eq!(
        to_json(&base_type_to_schema(BaseType::String, false))["type"],
        "string"
    );
}

#[test]
fn date_and_timestamp_are_formatted_strings() {
    let d = to_json(&base_type_to_schema(BaseType::Date, false));
    assert_eq!(d["type"], "string");
    assert_eq!(d["format"], "date");
    let t = to_json(&base_type_to_schema(BaseType::Timestamp, false));
    assert_eq!(t["type"], "string");
    assert_eq!(t["format"], "date-time");
}

#[test]
fn vector_maps_to_bounded_number_array() {
    let j = to_json(&base_type_to_schema(BaseType::Vector(4), false));
    assert_eq!(j["type"], "array");
    assert_eq!(j["items"]["type"], "number");
    assert_eq!(j["minItems"], 4);
    assert_eq!(j["maxItems"], 4);
}

#[test]
fn nullable_adds_null_to_type() {
    // OpenAPI 3.1: nullability is a type array including "null".
    let j = to_json(&base_type_to_schema(BaseType::String, true));
    let ty = &j["type"];
    let types: Vec<String> = match ty {
        serde_json::Value::Array(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        serde_json::Value::String(s) => vec![s.clone()],
        _ => vec![],
    };
    assert!(
        types.contains(&"null".to_string()),
        "nullable string must include null: {ty}"
    );
    assert!(types.contains(&"string".to_string()));
}

// ---- fixtures ------------------------------------------------------------------------

fn tref(schema: &str, table: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: table.into(),
    }
}

fn customer() -> ObjectType {
    ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "email".into(),
                ty: "string".into(),
                required: false,
            },
            PropertyDef {
                name: "score".into(),
                ty: "double".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: tref("main", "customer"),
        identity: Some("id".into()),
    }
}

fn order() -> ObjectType {
    ObjectType {
        name: TypeName("Order".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "long".into(),
            required: true,
        }],
        derived: vec![],
        table: tref("main", "orders"),
        identity: Some("id".into()),
    }
}

fn orders_link() -> LinkDef {
    LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    }
}

// ---- generator -----------------------------------------------------------------------

/// Flatten (method, path) pairs from generated `Paths` via JSON. `Paths` serializes as a bare
/// path→item map, so fall back to the top-level object when there is no `paths` key.
fn methods_and_paths(
    paths: &utoipa::openapi::path::Paths,
) -> std::collections::BTreeSet<(String, String)> {
    const METHODS: [&str; 8] = [
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];
    let json = serde_json::to_value(paths).unwrap();
    let map = json
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .or_else(|| json.as_object());
    let mut out = std::collections::BTreeSet::new();
    if let Some(obj) = map {
        for (path, item) in obj {
            if let Some(ops) = item.as_object() {
                for m in ops.keys() {
                    if METHODS.contains(&m.as_str()) {
                        out.insert((m.clone(), path.clone()));
                    }
                }
            }
        }
    }
    out
}

#[test]
fn generates_per_type_operations() {
    let (paths, schemas) = ontology_openapi(&[customer(), order()], &[orders_link()]);
    let mp = methods_and_paths(&paths);
    assert!(mp.contains(&("get".into(), "/objects/Customer".into())));
    assert!(mp.contains(&("post".into(), "/objects/Customer".into())));
    assert!(mp.contains(&("get".into(), "/objects/Order".into())));
    assert!(mp.contains(&("get".into(), "/objects/Customer/links/orders".into())));

    // Component schema present with typed properties + identity required.
    let cust = serde_json::to_value(schemas.get("Customer").expect("Customer schema")).unwrap();
    // `id` is required (identity) → a bare, non-nullable string (long encodes as a string).
    assert_eq!(cust["properties"]["id"]["type"], "string");
    // `score` is not required → nullable double, i.e. the 3.1 type array ["number","null"].
    assert!(
        type_set(&cust["properties"]["score"]["type"]).contains("number"),
        "score should be a (nullable) number: {}",
        cust["properties"]["score"]["type"]
    );
    assert!(
        type_set(&cust["properties"]["score"]["type"]).contains("null"),
        "non-required property should be nullable"
    );
    let empty = vec![];
    let required: Vec<String> = cust["required"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert!(
        required.contains(&"id".to_string()),
        "identity must be required"
    );
}

#[test]
fn link_response_targets_the_to_type() {
    let (paths, _schemas) = ontology_openapi(&[customer(), order()], &[orders_link()]);
    let json = serde_json::to_value(&paths).unwrap();
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("/objects/Customer/links/orders"));
    assert!(
        s.contains("Order"),
        "link response should reference the target type schema"
    );
}

// ---- liveness (memory-backed) --------------------------------------------------------

// MemoryControlPlane::new takes a lock_timeout Duration (see memory/src/lib.rs).
fn mem_cp() -> Arc<dyn ControlPlane + Send + Sync> {
    Arc::new(MemoryControlPlane::new(Duration::from_millis(300)))
}

#[tokio::test]
async fn live_doc_reflects_defined_types_without_restart() {
    let cp = mem_cp();
    cp.ontology().define_type(customer()).await.unwrap();

    let doc1 = query_api::live_openapi(cp.clone()).await;
    let j1 = serde_json::to_value(&doc1).unwrap();
    assert!(j1["paths"]["/objects/Customer"]["get"].is_object());
    assert!(j1["components"]["schemas"]["Customer"].is_object());
    assert!(
        j1["paths"]["/objects/Order"].is_null(),
        "Order not defined yet"
    );

    // Define a NEW type; the next generation must include it (per-request liveness).
    cp.ontology().define_type(order()).await.unwrap();
    let doc2 = query_api::live_openapi(cp.clone()).await;
    let j2 = serde_json::to_value(&doc2).unwrap();
    assert!(
        j2["paths"]["/objects/Order"]["get"].is_object(),
        "new type appears live"
    );
}

#[tokio::test]
async fn static_framework_survives_merge() {
    let cp = mem_cp();
    cp.ontology().define_type(customer()).await.unwrap();
    let doc = query_api::live_openapi(cp).await;
    let j = serde_json::to_value(&doc).unwrap();
    // The hand-written template + the OpenAPI 3.1 version survive.
    assert!(
        j["paths"]["/objects/{type_name}"]["get"].is_object(),
        "static template intact"
    );
    assert_eq!(j["openapi"].as_str().unwrap().get(0..3), Some("3.1"));
}

#[tokio::test]
async fn full_catalog_is_generated() {
    let cp = mem_cp();
    cp.ontology().define_type(customer()).await.unwrap();
    cp.ontology().define_type(order()).await.unwrap();
    let doc = query_api::live_openapi(cp).await;
    let j = serde_json::to_value(&doc).unwrap();
    assert!(j["paths"]["/objects/Customer"].is_object());
    assert!(j["paths"]["/objects/Order"].is_object());
}
