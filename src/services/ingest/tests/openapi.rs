//! Path-coverage + drift guard for ingest's OpenAPI document. Pure: calls
//! `build_openapi()` and inspects it — no router/AppState/Postgres.

use std::collections::BTreeSet;

fn expected() -> BTreeSet<(String, String)> {
    [
        ("post", "/datasets/{schema}/{table}"),
        ("post", "/models/{type}"),
        ("post", "/tables/{schema}/{table}/compact"),
        // Runtime-mounted routes (serve.rs merges the auth and service-account
        // routers — no admin router on ingest) — documented by the
        // service_runtime OpenAPI fragments.
        ("post", "/auth/login"),
        ("post", "/auth/logout"),
        ("post", "/auth/password"),
        ("post", "/auth/service-accounts"),
        ("get", "/auth/service-accounts"),
        ("post", "/auth/service-accounts/{id}/tokens"),
        ("get", "/auth/service-accounts/{id}/tokens"),
        ("delete", "/auth/service-accounts/{id}/tokens/{token_id}"),
    ]
    .iter()
    .map(|(m, p)| ((*m).to_string(), (*p).to_string()))
    .collect()
}

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
    let doc = ingest::build_openapi();
    let json = serde_json::to_value(&doc).unwrap();
    assert_eq!(json["openapi"].as_str().unwrap().get(0..3), Some("3.1"));
    assert!(json["info"]["title"].is_string());
}

#[test]
fn documents_exactly_the_expected_routes() {
    let doc = ingest::build_openapi();
    assert_eq!(documented(&doc), expected());
}
