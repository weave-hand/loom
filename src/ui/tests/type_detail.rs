use loom_ui_core::parse_type_detail;

#[test]
fn parses_properties_and_both_link_directions() {
    let body = serde_json::json!({
        "name": "Order",
        "properties": [
            { "name": "id", "ty": "Long", "required": true },
            { "name": "note", "ty": "String", "required": false }
        ],
        "links": [ { "name": "customer", "from": "Order", "to": "Customer", "cardinality": "one" } ],
        "links_to": []
    });
    let d = parse_type_detail(&body);
    assert_eq!(d.properties.len(), 2);
    assert_eq!(d.properties[0].name, "id");
    assert!(d.properties[0].required);
    assert_eq!(d.properties[1].ty, "String");
    assert_eq!(d.links.len(), 1);
    assert_eq!(d.links[0].to, "Customer");
    assert_eq!(d.links[0].cardinality, "one");
    assert!(d.links_to.is_empty());
}

#[test]
fn missing_fields_default_to_empty() {
    let d = parse_type_detail(&serde_json::json!({}));
    assert!(d.properties.is_empty() && d.links.is_empty() && d.links_to.is_empty());
}
