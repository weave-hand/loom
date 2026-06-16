//! Pure HTTP-edge parsing of traversal direction: `~`-prefixed path elements (multi-hop)
//! and the single-hop `?direction=` value.

use query_api::handler::{Direction, Hop};
use query_api::path_parse::{parse_direction, parse_path_hops};

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
