//! Path-coverage + drift guard for query-api's OpenAPI document. Pure: calls
//! `build_openapi()` and inspects the document — no router/AppState/Postgres.

use std::collections::BTreeSet;

/// The exact (method, path) set this service documents. Adding a route without
/// updating this set (and the `ApiDoc` `paths(...)`) fails this test — utoipa can't
/// introspect axum's route table, so this hardcoded expectation keeps the doc honest.
fn expected() -> BTreeSet<(String, String)> {
    [
        ("get", "/objects/{type_name}"),
        ("get", "/objects/{from_type}/links/{link_name}"),
        ("get", "/objects/{from_type}/links"),
        ("get", "/objects/{type_name}/graph/{link_name}"),
        ("get", "/objects/{type_name}/graph"),
        ("post", "/actions/{action_name}"),
        ("post", "/search/{type_name}/{index_name}"),
        ("post", "/maintenance/gc/{schema}/{table}"),
        ("get", "/lineage/datasets/{namespace}/{name}/upstream"),
        ("get", "/lineage/datasets/{namespace}/{name}/downstream"),
        ("get", "/lineage/runs/{run_id}/events"),
        ("get", "/ontology/types"),
        // Runtime-mounted routes (serve.rs merges the auth, service-account, and
        // admin routers) — documented by the service_runtime OpenAPI fragments.
        ("post", "/auth/login"),
        ("post", "/auth/logout"),
        ("post", "/auth/password"),
        ("post", "/auth/service-accounts"),
        ("get", "/auth/service-accounts"),
        ("post", "/auth/service-accounts/{id}/tokens"),
        ("get", "/auth/service-accounts/{id}/tokens"),
        ("delete", "/auth/service-accounts/{id}/tokens/{token_id}"),
        ("post", "/admin/users"),
        ("get", "/admin/users"),
        ("post", "/admin/users/{username}/disable"),
        ("post", "/admin/users/{username}/enable"),
        ("post", "/admin/users/{username}/password"),
        ("post", "/admin/models"),
        ("post", "/admin/roles"),
        ("get", "/admin/roles"),
        ("post", "/admin/roles/{role}/grants"),
    ]
    .iter()
    .map(|(m, p)| ((*m).to_string(), (*p).to_string()))
    .collect()
}

/// Flatten the document's paths into a (method, path) set.
fn documented(doc: &utoipa::openapi::OpenApi) -> BTreeSet<(String, String)> {
    // Walk the serialized JSON rather than the typed PathItem: utoipa 5's PathItem
    // exposes per-method `Option<Operation>` fields (get/post/...), NOT an operations
    // map. Each path's JSON object has HTTP-method keys plus non-operation keys
    // (summary/description/servers/parameters); keep only the method keys.
    const METHODS: [&str; 8] = [
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];
    let json = serde_json::to_value(doc).unwrap();
    let mut out = BTreeSet::new();
    if let Some(paths) = json["paths"].as_object() {
        for (path, item) in paths {
            if let Some(ops) = item.as_object() {
                for method in ops.keys() {
                    if METHODS.contains(&method.as_str()) {
                        out.insert((method.clone(), path.clone()));
                    }
                }
            }
        }
    }
    out
}

#[test]
fn valid_openapi_document() {
    let doc = query_api::build_openapi();
    // Round-trips through serde and carries an info block.
    let json = serde_json::to_value(&doc).unwrap();
    assert_eq!(json["openapi"].as_str().unwrap().get(0..3), Some("3.1"));
    assert!(json["info"]["title"].is_string());
}

#[test]
fn documents_exactly_the_expected_routes() {
    let doc = query_api::build_openapi();
    assert_eq!(documented(&doc), expected());
}

#[test]
fn post_action_documents_422_and_400() {
    let doc = query_api::build_openapi();
    let json = serde_json::to_value(&doc).unwrap();
    let responses = &json["paths"]["/actions/{action_name}"]["post"]["responses"];
    assert!(
        responses.get("422").is_some(),
        "POST /actions must document 422 (semantic failure), got {responses}"
    );
    assert!(
        responses.get("400").is_some(),
        "POST /actions must document 400 (malformed body), got {responses}"
    );
}
