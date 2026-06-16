use query_api::chain_filter::{FilterResolveError, resolve_chain_filters};
use query_api::handler::ChainFilter;

fn p(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn sorted(mut fs: Vec<ChainFilter>) -> Vec<(usize, String, String)> {
    let mut out: Vec<(usize, String, String)> = fs
        .drain(..)
        .map(|f| (f.position, f.column, f.raw))
        .collect();
    out.sort();
    out
}

#[test]
fn bare_key_is_a_source_filter() {
    let got = resolve_chain_filters(&["placed".into()], p(&[("active", "true")])).unwrap();
    assert_eq!(sorted(got), vec![(0, "active".into(), "true".into())]);
}

#[test]
fn prefixed_key_maps_to_the_links_position() {
    let path = vec!["placed".to_string(), "contains".to_string()];
    let got = resolve_chain_filters(
        &path,
        p(&[
            ("active", "true"),
            ("placed.status", "open"),
            ("contains.sku", "ABC"),
        ]),
    )
    .unwrap();
    assert_eq!(
        sorted(got),
        vec![
            (0, "active".into(), "true".into()),
            (1, "status".into(), "open".into()),
            (2, "sku".into(), "ABC".into()),
        ]
    );
}

#[test]
fn unknown_prefix_is_an_error() {
    let err = resolve_chain_filters(&["placed".into()], p(&[("nope.x", "1")])).unwrap_err();
    assert_eq!(err, FilterResolveError::UnknownTarget("nope".into()));
}

#[test]
fn repeated_link_prefix_is_rejected() {
    let path = vec!["knows".to_string(), "knows".to_string()];
    let err = resolve_chain_filters(&path, p(&[("knows.name", "X")])).unwrap_err();
    assert_eq!(err, FilterResolveError::AmbiguousLink("knows".into()));
}

#[test]
fn repeated_link_without_a_filter_on_it_is_fine() {
    // Plain self-traversal still resolves (no per-hop filter on the repeated link).
    let path = vec!["knows".to_string(), "knows".to_string()];
    let got = resolve_chain_filters(&path, p(&[("active", "true")])).unwrap();
    assert_eq!(sorted(got), vec![(0, "active".into(), "true".into())]);
}

#[test]
fn column_with_first_dot_split_keeps_remainder() {
    // Split on the FIRST dot only; the remainder is the column.
    let path = vec!["placed".to_string()];
    let got = resolve_chain_filters(&path, p(&[("placed.a", "1")])).unwrap();
    assert_eq!(sorted(got), vec![(1, "a".into(), "1".into())]);
}
