//! Route-set + security guards for the runtime's mergeable OpenAPI fragments
//! (auth, service-accounts, admin). Pure: builds each fragment and inspects the
//! document — no router, no AppState, no Postgres.

use std::collections::BTreeSet;

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

#[tokio::test]
async fn auth_fragment_documents_exactly_the_auth_routes() {
    let set = documented(&service_runtime::auth_openapi());
    let expected: BTreeSet<(String, String)> = [
        ("post", "/auth/login"),
        ("post", "/auth/logout"),
        ("post", "/auth/password"),
    ]
    .into_iter()
    .map(|(m, p)| (m.to_string(), p.to_string()))
    .collect();
    assert_eq!(set, expected);
}

#[tokio::test]
async fn service_account_fragment_documents_exactly_the_service_account_routes() {
    let set = documented(&service_runtime::service_account_openapi());
    let expected: BTreeSet<(String, String)> = [
        ("post", "/auth/service-accounts"),
        ("get", "/auth/service-accounts"),
        ("post", "/auth/service-accounts/{id}/tokens"),
        ("get", "/auth/service-accounts/{id}/tokens"),
        ("delete", "/auth/service-accounts/{id}/tokens/{token_id}"),
    ]
    .into_iter()
    .map(|(m, p)| (m.to_string(), p.to_string()))
    .collect();
    assert_eq!(set, expected);
}

#[tokio::test]
async fn admin_fragment_documents_exactly_the_admin_routes() {
    let set = documented(&service_runtime::admin_openapi());
    let expected: BTreeSet<(String, String)> = [
        ("post", "/admin/users"),
        ("get", "/admin/users"),
        ("post", "/admin/users/{username}/disable"),
        ("post", "/admin/users/{username}/enable"),
        ("post", "/admin/users/{username}/password"),
        ("post", "/admin/models"),
        ("post", "/admin/roles"),
        ("get", "/admin/roles"),
        ("post", "/admin/roles/{role}/grants"),
        ("get", "/admin/roles/{role}/grants"),
        ("delete", "/admin/roles/{role}/grants"),
        ("post", "/admin/roles/{role}/policies"),
        ("get", "/admin/roles/{role}/policies"),
        ("delete", "/admin/roles/{role}/policies"),
        ("delete", "/admin/roles/{role}"),
        ("post", "/admin/links"),
        ("delete", "/admin/links/{from}/{name}"),
        ("post", "/admin/actions"),
        ("delete", "/admin/actions/{name}"),
        ("get", "/admin/users/{username}/roles"),
        ("put", "/admin/users/{username}/roles/{role}"),
        ("delete", "/admin/users/{username}/roles/{role}"),
        ("post", "/admin/transforms"),
        ("get", "/admin/transforms"),
        ("get", "/admin/transforms/{name}"),
        ("delete", "/admin/transforms/{name}"),
        ("post", "/admin/transforms/{name}/run"),
        ("post", "/admin/transforms/run"),
        ("get", "/admin/transforms/{name}/runs"),
        ("get", "/admin/runs/{run_id}"),
    ]
    .into_iter()
    .map(|(m, p)| (m.to_string(), p.to_string()))
    .collect();
    assert_eq!(set, expected);
}

/// Every component name `$ref`'d by a documented response body, across all
/// paths/methods/status codes of one fragment document.
fn response_schema_names(json: &serde_json::Value) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    for (_path, item) in json["paths"].as_object().into_iter().flatten() {
        for (_m, op) in item.as_object().into_iter().flatten() {
            for (_code, resp) in op["responses"].as_object().into_iter().flatten() {
                if let Some(r) = resp["content"]["application/json"]["schema"]["$ref"].as_str() {
                    out.insert(r.rsplit('/').next().unwrap().to_string());
                }
            }
        }
    }
    out
}

#[tokio::test]
async fn no_response_schema_echoes_a_secret() {
    for doc in [
        service_runtime::auth_openapi(),
        service_runtime::service_account_openapi(),
        service_runtime::admin_openapi(),
    ] {
        let json = serde_json::to_value(&doc).unwrap();
        // Walk every documented RESPONSE body schema ref, resolve it in
        // components, and reject secret fields. MintTokenResp and LoginResp are
        // the two deliberate exceptions: minting (a service token / a session
        // token at login) is the single moment the raw token is shown.
        // Request DTOs may carry passwords; responses must not.
        let schemas = json["components"]["schemas"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        for name in &response_schema_names(&json) {
            let props = schemas[name]["properties"]
                .as_object()
                .cloned()
                .unwrap_or_default();
            assert!(
                !props.contains_key("password") && !props.contains_key("current"),
                "response schema {name} echoes a password field"
            );
            if name != "MintTokenResp" && name != "LoginResp" {
                assert!(
                    !props.contains_key("token"),
                    "response schema {name} echoes a token"
                );
            }
        }
    }
}

#[tokio::test]
async fn every_op_requires_bearer_except_login() {
    // The scheme COMPONENT is registered by the serve seam
    // (`register_bearer_scheme`, runtime/src/openapi.rs:41), not by the raw
    // fragments — assert the per-op security REQUIREMENT here instead.
    for doc in [
        service_runtime::auth_openapi(),
        service_runtime::service_account_openapi(),
        service_runtime::admin_openapi(),
    ] {
        let json = serde_json::to_value(&doc).unwrap();
        for (path, item) in json["paths"].as_object().into_iter().flatten() {
            for (method, op) in item.as_object().into_iter().flatten() {
                if ![
                    "get", "post", "put", "delete", "patch", "head", "options", "trace",
                ]
                .contains(&method.as_str())
                {
                    continue;
                }
                let requires_bearer = op["security"]
                    .as_array()
                    .is_some_and(|reqs| reqs.iter().any(|r| r.get("bearer_auth").is_some()));
                if path == "/auth/login" {
                    assert!(!requires_bearer, "login must be unauthenticated");
                } else {
                    assert!(requires_bearer, "{method} {path} must require bearer auth");
                }
            }
        }
    }
}
