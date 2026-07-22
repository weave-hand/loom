//! Pure unit tests for the ontology→OpenAPI generator and its type codec, plus the live
//! per-request document. Memory-backed (no postgres): the generator is pure and the liveness
//! handler reads any `ControlPlane`, so `MemoryControlPlane` exercises `list_types`/`links`/
//! `define_type`/`define_action`/`list_actions` identically to the postgres path for
//! doc-generation purposes.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{
    ActionDef, ActionKind, ActionName, BaseType, Cardinality, ControlPlane, LinkDef, ObjectType,
    ParamDef, PropertyDef, TypeName,
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

fn customer() -> ObjectType {
    ObjectType::build("Customer", ("main", "customer"))
        .prop_req("id", "long")
        .prop("email", "string")
        .prop("score", "double")
        .identity("id")
        .done()
}

fn order() -> ObjectType {
    ObjectType::build("Order", ("main", "orders"))
        .prop_req("id", "long")
        .identity("id")
        .done()
}

fn orders_link() -> LinkDef {
    LinkDef::fk(
        "orders",
        "Customer",
        "Order",
        Cardinality::Many,
        "id",
        "customer_id",
    )
}

/// A well-formed Insert action against `customer()`: one required + one optional parameter.
fn create_customer_action() -> ActionDef {
    ActionDef::single_step(
        ActionName("createCustomer".into()),
        TypeName("Customer".into()),
        ActionKind::Insert,
        vec![
            ParamDef::new("name", "string").required(),
            ParamDef::new("tier", "integer"),
        ],
        vec![],
    )
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

/// One generated operation as JSON: `paths[path][method]`. `Paths` serializes as a bare
/// path→item map, so fall back to the top-level object when there is no `paths` key.
fn op_json(paths: &utoipa::openapi::path::Paths, path: &str, method: &str) -> serde_json::Value {
    let json = serde_json::to_value(paths).unwrap();
    let map = json.get("paths").cloned().unwrap_or(json);
    map[path][method].clone()
}

#[test]
fn generates_per_type_operations() {
    let (paths, schemas) = ontology_openapi(&[customer(), order()], &[orders_link()], &[]);
    let mp = methods_and_paths(&paths);
    assert!(mp.contains(&("get".into(), "/objects/Customer".into())));
    assert!(
        !mp.contains(&("post".into(), "/objects/Customer".into())),
        "the phantom typed-insert POST /objects/{{Type}} must be gone — inserts are \
         real POST /actions/{{name}} operations"
    );
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
fn link_op_documents_direction_shape_ids_and_filters() {
    let (paths, _schemas) = ontology_openapi(&[customer(), order()], &[orders_link()], &[]);
    let op = op_json(&paths, "/objects/Customer/links/orders", "get");
    let params = op["parameters"].as_array().expect("parameters array");
    let names: std::collections::BTreeSet<&str> =
        params.iter().filter_map(|p| p["name"].as_str()).collect();
    // Reserved traversal knobs.
    for reserved in ["_direction", "_shape", "_ids"] {
        assert!(names.contains(reserved), "missing {reserved}: {names:?}");
    }
    // Source filters use the bare property name; target filters use `<link>.<prop>`.
    assert!(names.contains("email"), "source filter (bare): {names:?}");
    assert!(
        names.contains("orders.id"),
        "target filter (prefixed): {names:?}"
    );
    // Every documented param is a query param.
    assert!(params.iter().all(|p| p["in"] == "query"));
}

#[test]
fn link_response_targets_the_to_type() {
    let (paths, _schemas) = ontology_openapi(&[customer(), order()], &[orders_link()], &[]);
    let json = serde_json::to_value(&paths).unwrap();
    let s = serde_json::to_string(&json).unwrap();
    assert!(s.contains("/objects/Customer/links/orders"));
    assert!(
        s.contains("Order"),
        "link response should reference the target type schema"
    );
}

#[test]
fn link_to_a_type_absent_from_the_snapshot_is_skipped() {
    // Order is NOT in the type snapshot, so its $ref would dangle — the link op must be
    // dropped rather than emit an invalid document referencing a missing schema.
    let (paths, _schemas) = ontology_openapi(&[customer()], &[orders_link()], &[]);
    let mp = methods_and_paths(&paths);
    assert!(
        !mp.contains(&("get".into(), "/objects/Customer/links/orders".into())),
        "a link to an absent target type must not be generated"
    );
    // The present type's own operations still generate.
    assert!(mp.contains(&("get".into(), "/objects/Customer".into())));
}

#[test]
fn generates_real_action_operations() {
    let (paths, _schemas) = ontology_openapi(&[customer()], &[], &[create_customer_action()]);
    let mp = methods_and_paths(&paths);
    assert!(mp.contains(&("post".into(), "/actions/createCustomer".into())));

    let op = op_json(&paths, "/actions/createCustomer", "post");
    // Request body schema is derived from the action's parameters, not the type.
    let schema = &op["requestBody"]["content"]["application/json"]["schema"];
    assert!(
        schema["properties"]["name"].is_object(),
        "required param documented: {schema}"
    );
    assert!(
        schema["properties"]["tier"].is_object(),
        "optional param documented: {schema}"
    );
    assert_eq!(
        schema["required"],
        serde_json::json!(["name"]),
        "only required params are required"
    );
    // Insert documents 201 Created with the target type's component schema.
    assert_eq!(
        op["responses"]["201"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Customer"
    );
    // Grouped under the target type, not a flat "actions" bucket.
    assert_eq!(op["tags"], serde_json::json!(["Customer"]));
}

#[test]
fn update_and_delete_actions_document_200() {
    // Kind-true: Update/Delete document 200 OK (not 201); Insert still 201.
    let update = ActionDef::single_step(
        ActionName("updateCustomer".into()),
        TypeName("Customer".into()),
        ActionKind::Update,
        vec![],
        vec![],
    );
    let delete = ActionDef::single_step(
        ActionName("deleteCustomer".into()),
        TypeName("Customer".into()),
        ActionKind::Delete,
        vec![],
        vec![],
    );
    let (paths, _schemas) = ontology_openapi(&[customer()], &[], &[update, delete]);

    let up = op_json(&paths, "/actions/updateCustomer", "post");
    assert!(
        up["responses"]["200"].is_object(),
        "Update documents 200 OK"
    );
    assert!(
        up["responses"]["201"].is_null(),
        "Update no longer documents 201"
    );
    // 2xx body still refs the target type's component schema.
    assert_eq!(
        up["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Customer"
    );

    let del = op_json(&paths, "/actions/deleteCustomer", "post");
    assert!(
        del["responses"]["200"].is_object(),
        "Delete documents 200 OK"
    );
    assert!(
        del["responses"]["201"].is_null(),
        "Delete no longer documents 201"
    );
}

#[test]
fn multi_step_action_documents_union_schema_and_all_target_tags() {
    let action = ActionDef::build("onboardCustomer", "Customer", ActionKind::Insert)
        .param_req("name", "string")
        .step("Order", ActionKind::Insert)
        .param_req("total", "integer")
        .done();
    let (paths, _schemas) = ontology_openapi(&[customer(), order()], &[], &[action]);

    let op = op_json(&paths, "/actions/onboardCustomer", "post");
    // One op in every involved type's section, primary (first step) first.
    assert_eq!(
        op["tags"],
        serde_json::json!(["Customer", "Order"]),
        "tagged by every step target, step order"
    );
    // Union request schema: one flat body carries every step's params.
    let schema = &op["requestBody"]["content"]["application/json"]["schema"];
    assert!(schema["properties"]["name"].is_object());
    assert!(schema["properties"]["total"].is_object());
    assert_eq!(schema["required"], serde_json::json!(["name", "total"]));
    // 2xx body is the `{steps:[...]}` envelope, not the primary step's bare object.
    assert_eq!(
        op["responses"]["201"]["content"]["application/json"]["schema"]["$ref"],
        serde_json::json!("#/components/schemas/ActionStepsBody")
    );
    assert_eq!(
        op["summary"],
        serde_json::json!("Atomically insert Customer, insert Order")
    );
}

#[test]
fn multi_step_action_with_any_absent_step_target_is_skipped() {
    // First step's target is in the snapshot, the second's is not — the whole op
    // is skipped (its tag and any future per-step $ref would dangle).
    let action = ActionDef::build("ghostly", "Customer", ActionKind::Insert)
        .step("Ghost", ActionKind::Insert)
        .done();
    let (paths, _schemas) = ontology_openapi(&[customer()], &[], &[action]);
    assert!(
        !methods_and_paths(&paths)
            .iter()
            .any(|(_, p)| p == "/actions/ghostly"),
        "op with an absent step target must be skipped"
    );
}

#[test]
fn action_targeting_a_type_absent_from_the_snapshot_is_skipped() {
    // Customer is NOT in the type snapshot, so the 2xx $ref would dangle — the action op
    // must be dropped rather than emit an invalid document (same skew guard as links).
    let (paths, _schemas) = ontology_openapi(&[order()], &[], &[create_customer_action()]);
    let mp = methods_and_paths(&paths);
    assert!(
        !mp.contains(&("post".into(), "/actions/createCustomer".into())),
        "an action targeting an absent type must not be generated"
    );
}

#[test]
fn generated_ops_are_tagged_by_type() {
    let (paths, _schemas) = ontology_openapi(&[customer(), order()], &[orders_link()], &[]);
    let get = op_json(&paths, "/objects/Customer", "get");
    assert_eq!(
        get["tags"],
        serde_json::json!(["Customer"]),
        "GET /objects/{{Type}} groups under the type name"
    );
    let link = op_json(&paths, "/objects/Customer/links/orders", "get");
    assert_eq!(
        link["tags"],
        serde_json::json!(["Customer"]),
        "link ops group under the FROM type name"
    );
}

#[test]
fn get_op_documents_filter_and_pagination_params() {
    let (paths, _schemas) = ontology_openapi(&[customer()], &[], &[]);
    let get = op_json(&paths, "/objects/Customer", "get");
    let params = get["parameters"].as_array().expect("parameters array");
    let names: std::collections::BTreeSet<&str> =
        params.iter().filter_map(|p| p["name"].as_str()).collect();
    // One filter param per property...
    assert!(names.contains("id"), "filter param per property: {names:?}");
    assert!(names.contains("email"));
    assert!(names.contains("score"));
    // ...plus the reserved object-set and pagination knobs.
    for reserved in ["_ids", "_or", "limit", "cursor"] {
        assert!(
            names.contains(reserved),
            "missing reserved param {reserved}: {names:?}"
        );
    }
    // Every documented param is a query param; the grammar is described.
    let email = params
        .iter()
        .find(|p| p["name"] == "email")
        .expect("email param");
    assert_eq!(email["in"], "query");
    assert!(
        email["description"]
            .as_str()
            .unwrap_or_default()
            .contains("startswith"),
        "filter grammar documented: {}",
        email["description"]
    );
}

#[test]
fn generated_document_carries_ontology_descriptions() {
    let doc_ty = ObjectType::build("Doc", ("main", "docs"))
        .described("A document in the corpus")
        .add_prop(
            PropertyDef::new("id", "Long")
                .required()
                .described("The document's id"),
        )
        .prop("body", "String")
        .identity("id")
        .done();
    let create = ActionDef::build("createDoc", "Doc", ActionKind::Insert)
        .described("Registers a new document")
        .param_req("id", "Long")
        .done();
    let link = LinkDef::fk("parent", "Doc", "Doc", Cardinality::One, "parent_id", "id")
        .described("The document this one was split from");

    let (paths, schemas) = ontology_openapi(&[doc_ty], &[link], &[create]);

    let doc = serde_json::to_value(schemas.get("Doc").expect("Doc schema")).unwrap();
    assert_eq!(doc["description"], "A document in the corpus");
    // Combined: property prose + the preserved Long encoding note.
    assert_eq!(
        doc["properties"]["id"]["description"],
        "The document's id (int64 encoded as a decimal string)"
    );
    assert!(
        doc["properties"]["body"].get("description").is_none(),
        "an undescribed property carries no description key: {}",
        doc["properties"]["body"]
    );
    assert_eq!(
        op_json(&paths, "/actions/createDoc", "post")["description"],
        "Registers a new document"
    );
    assert_eq!(
        op_json(&paths, "/objects/Doc/links/parent", "get")["description"],
        "The document this one was split from"
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

    // Define a NEW action; the next generation must document it too.
    cp.ontology()
        .define_action(create_customer_action())
        .await
        .unwrap();
    let doc3 = query_api::live_openapi(cp.clone()).await;
    let j3 = serde_json::to_value(&doc3).unwrap();
    assert!(
        j3["paths"]["/actions/createCustomer"]["post"].is_object(),
        "new action appears live"
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

#[test]
fn type_detail_response_documents_derived_and_vector_indexes() {
    let doc = query_api::openapi::build_openapi();
    let j = serde_json::to_value(&doc).unwrap();
    let td = &j["components"]["schemas"]["TypeDetailResponse"]["properties"];
    assert!(
        td["derived"].is_object(),
        "TypeDetailResponse must document `derived`: {td}"
    );
    assert!(
        td["vector_indexes"].is_object(),
        "TypeDetailResponse must document `vector_indexes`: {td}"
    );
    // Both reference their view component schemas.
    assert!(j["components"]["schemas"]["DerivedPropertyView"].is_object());
    assert!(j["components"]["schemas"]["VectorIndexView"].is_object());
}

#[test]
fn derived_properties_are_read_only_in_the_component_schema() {
    let cust = ObjectType::build("Customer", ("main", "customer"))
        .prop_req("id", "long")
        .derived(
            control_plane_core::DerivedPropertyDef::new(
                "orderCount",
                "Long",
                "orders",
                control_plane_core::Aggregation::Count,
            )
            .described("How many orders this customer has."),
        )
        .identity("id")
        .done();
    let create = create_customer_action(); // params: name (req), tier — no derived props
    let (_paths, schemas) = ontology_openapi(&[cust], &[], &[create]);

    let doc = serde_json::to_value(schemas.get("Customer").expect("Customer schema")).unwrap();
    // Derived property is present in the READ component, marked readOnly, with its prose.
    // `orderCount` is declared `ty = "Long"`, so `property_schema` folds the property prose
    // together with `base_type_to_schema(Long)`'s encoding note (same pattern the pre-existing
    // `generated_document_carries_ontology_descriptions` test asserts, openapi_gen.rs:492-495).
    assert_eq!(doc["properties"]["orderCount"]["readOnly"], true);
    assert_eq!(
        doc["properties"]["orderCount"]["description"],
        "How many orders this customer has. (int64 encoded as a decimal string)"
    );
    // A physical property is NOT readOnly.
    assert!(
        doc["properties"]["id"].get("readOnly").is_none(),
        "physical properties must not be readOnly: {}",
        doc["properties"]["id"]
    );
}

#[test]
fn derived_properties_do_not_appear_in_action_request_schemas() {
    let cust = ObjectType::build("Customer", ("main", "customer"))
        .prop_req("id", "long")
        .derived(control_plane_core::DerivedPropertyDef::new(
            "orderCount",
            "Long",
            "orders",
            control_plane_core::Aggregation::Count,
        ))
        .identity("id")
        .done();
    let (paths, _schemas) = ontology_openapi(&[cust], &[], &[create_customer_action()]);
    let op = op_json(&paths, "/actions/createCustomer", "post");
    let schema = &op["requestBody"]["content"]["application/json"]["schema"];
    assert!(
        schema["properties"]["orderCount"].is_null(),
        "derived properties are computed, never writable — must not appear in the action \
         request schema: {schema}"
    );
}
