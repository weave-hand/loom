//! `Hop` / `Direction` value-type behavior: the `From` conversions default to Forward
//! (this is what keeps existing forward call sites compiling) and Direction's Default.

use query_api::handler::{Direction, Hop};

#[test]
fn from_str_is_a_forward_hop() {
    assert_eq!(
        Hop::from("orders"),
        Hop {
            link: "orders".to_string(),
            direction: Direction::Forward,
        }
    );
}

#[test]
fn from_string_is_a_forward_hop() {
    assert_eq!(
        Hop::from("orders".to_string()),
        Hop {
            link: "orders".to_string(),
            direction: Direction::Forward,
        }
    );
}

#[test]
fn direction_default_is_forward() {
    assert_eq!(Direction::default(), Direction::Forward);
}
