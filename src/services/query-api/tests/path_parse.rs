//! Pure HTTP-edge parsing of traversal direction: `~`-prefixed path elements (multi-hop)
//! and the single-hop `?direction=` value.

use query_api::handler::{Direction, Hop};
use query_api::path_parse::{GraphMode, parse_direction, parse_graph_mode, parse_path_hops};

#[test]
fn bare_names_are_forward_hops() {
    assert_eq!(
        parse_path_hops("orders,lineItems"),
        vec![Hop::from("orders"), Hop::from("lineItems")]
    );
}

#[test]
fn tilde_prefix_is_an_inverse_hop() {
    assert_eq!(
        parse_path_hops("~orders"),
        vec![Hop {
            link: "orders".into(),
            direction: Direction::Inverse,
        }]
    );
}

#[test]
fn mixed_directions_and_whitespace_and_empties() {
    // leading/trailing spaces trimmed (before and after the sigil); empty segments dropped.
    assert_eq!(
        parse_path_hops(" ~orders , lineItems ,, "),
        vec![
            Hop {
                link: "orders".into(),
                direction: Direction::Inverse,
            },
            Hop {
                link: "lineItems".into(),
                direction: Direction::Forward,
            },
        ]
    );
}

#[test]
fn direction_default_and_explicit_forward() {
    assert_eq!(parse_direction(None), Ok(Direction::Forward));
    assert_eq!(parse_direction(Some("forward")), Ok(Direction::Forward));
}

#[test]
fn direction_inverse() {
    assert_eq!(parse_direction(Some("inverse")), Ok(Direction::Inverse));
}

#[test]
fn direction_invalid_is_error() {
    assert!(parse_direction(Some("sideways")).is_err());
    assert!(parse_direction(Some("")).is_err());
}

#[test]
fn graph_path_inherits_the_inverse_grammar() {
    assert_eq!(
        parse_path_hops("memberOf,~memberOf"),
        vec![
            Hop {
                link: "memberOf".into(),
                direction: Direction::Forward,
            },
            Hop {
                link: "memberOf".into(),
                direction: Direction::Inverse,
            },
        ]
    );
}

fn hop(link: &str) -> Hop {
    Hop {
        link: link.to_string(),
        direction: Direction::Forward,
    }
}

fn ihop(link: &str) -> Hop {
    Hop {
        link: link.to_string(),
        direction: Direction::Inverse,
    }
}

#[test]
fn graph_mode_rejects_path_and_links_together() {
    let err = parse_graph_mode(vec![hop("a")], vec!["b".into()], false).unwrap_err();
    assert_eq!(err, "specify either path or links, not both");
}

#[test]
fn graph_mode_union_and_its_tree_rejection() {
    assert!(matches!(
        parse_graph_mode(vec![], vec!["a".into(), "b".into()], false),
        Ok(GraphMode::Union(links)) if links == ["a".to_string(), "b".to_string()]
    ));
    assert_eq!(
        parse_graph_mode(vec![], vec!["a".into()], true).unwrap_err(),
        "tree view is not supported with links (union)"
    );
}

#[test]
fn graph_mode_empty_is_400() {
    assert_eq!(
        parse_graph_mode(vec![], vec![], false).unwrap_err(),
        "path or links requires at least one link"
    );
}

#[test]
fn graph_mode_plain_path_cycle_allows_tree() {
    assert!(matches!(
        parse_graph_mode(vec![hop("knows")], vec![], true),
        Ok(GraphMode::PathCycle(p)) if p.len() == 1
    ));
}

#[test]
fn graph_mode_core_tail_strips_star_and_reemits_inverse_sigil() {
    let got = parse_graph_mode(
        vec![hop("knows*"), hop("worksAt"), ihop("owns")],
        vec![],
        false,
    );
    assert!(matches!(
        got,
        Ok(GraphMode::CoreTail { ref core_link, ref tail_links })
            if core_link == "knows" && *tail_links == ["worksAt".to_string(), "~owns".to_string()]
    ));
}

#[test]
fn graph_mode_star_rules() {
    assert_eq!(
        parse_graph_mode(vec![hop("a*"), hop("b*")], vec![], false).unwrap_err(),
        "at most one path segment may be marked recursive with `*`"
    );
    assert_eq!(
        parse_graph_mode(vec![hop("a"), hop("b*")], vec![], false).unwrap_err(),
        "the recursive `*` segment must be the first path segment"
    );
    assert_eq!(
        parse_graph_mode(vec![hop("*"), hop("b")], vec![], false).unwrap_err(),
        "recursive core link name must not be empty"
    );
    assert_eq!(
        parse_graph_mode(vec![hop("a*"), hop("b")], vec![], true).unwrap_err(),
        "tree view is not supported for a recursive-core (*) path"
    );
    // `~foo*` is NOT a core marker — it stays a path-cycle inverse hop.
    assert!(matches!(
        parse_graph_mode(vec![ihop("foo*")], vec![], false),
        Ok(GraphMode::PathCycle(_))
    ));
}
