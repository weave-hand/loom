//! Pure tests for the reserved-query-param splitter + typed extractors the HTTP
//! routes share (query_params.rs): per-route key sets, last-occurrence-wins single
//! values, accumulated `_or`, filter order/repeat preservation, and the exact 400
//! messages the routes serve.
use query_api::query_params::{comma_list, parse_depth, parse_ids, split_reserved};

fn pairs(xs: &[(&str, &str)]) -> Vec<(String, String)> {
    xs.iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn split_keeps_filter_order_and_repeats() {
    let (r, filters) = split_reserved(
        pairs(&[("a", "1"), ("_ids", "7"), ("a", "2"), ("b", "3")]),
        &["_ids"],
    );
    assert_eq!(filters, pairs(&[("a", "1"), ("a", "2"), ("b", "3")]));
    assert_eq!(r.last("_ids"), Some("7"));
}

#[test]
fn single_valued_keys_take_the_last_occurrence() {
    let (r, _) = split_reserved(pairs(&[("depth", "1"), ("depth", "2")]), &["depth"]);
    assert_eq!(r.last("depth"), Some("2"));
}

#[test]
fn or_accumulates_every_occurrence_in_order() {
    let (r, _) = split_reserved(pairs(&[("_or", "a:1,b:2"), ("_or", "c:3")]), &["_or"]);
    assert_eq!(r.all("_or"), ["a:1,b:2".to_string(), "c:3".to_string()]);
}

#[test]
fn unreserved_keys_stay_filters_per_route() {
    // `_direction` is reserved on /links but NOT on /objects — the splitter must
    // never consume a key the route did not reserve.
    let (r, filters) = split_reserved(pairs(&[("_direction", "inverse")]), &["_ids", "_or"]);
    assert!(!r.present("_direction"));
    assert_eq!(filters, pairs(&[("_direction", "inverse")]));
}

#[test]
fn present_tracks_even_empty_values() {
    let (r, _) = split_reserved(pairs(&[("_ids", "")]), &["_ids"]);
    assert!(r.present("_ids"));
    assert_eq!(r.last("_ids"), Some(""));
}

#[test]
fn parse_ids_absent_is_no_scoping() {
    assert_eq!(parse_ids(None).unwrap(), Vec::<String>::new());
}

#[test]
fn parse_ids_splits_and_drops_empty_elements() {
    assert_eq!(
        parse_ids(Some("1,,2")).unwrap(),
        vec!["1".to_string(), "2".to_string()]
    );
}

#[test]
fn parse_ids_empty_is_the_routes_400_message() {
    assert_eq!(
        parse_ids(Some("")).unwrap_err(),
        "_ids requires at least one value"
    );
    assert_eq!(
        parse_ids(Some(",")).unwrap_err(),
        "_ids requires at least one value"
    );
}

#[test]
fn parse_depth_defaults_parses_and_rejects() {
    assert_eq!(parse_depth(None, 5).unwrap(), 5);
    assert_eq!(parse_depth(Some("3"), 5).unwrap(), 3);
    assert_eq!(
        parse_depth(Some("abc"), 5).unwrap_err(),
        "depth must be a positive integer"
    );
    assert_eq!(
        parse_depth(Some("-1"), 5).unwrap_err(),
        "depth must be a positive integer"
    );
}

#[test]
fn comma_list_drops_empties() {
    assert_eq!(comma_list("a,,b,"), vec!["a".to_string(), "b".to_string()]);
}
