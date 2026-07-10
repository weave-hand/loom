//! Config behaviour of `SqlWireTuning` on the `QueryApiConfig` layered seam:
//! `bind_addr` is opt-in (unset => the external SQL wire listener does not
//! start) and validated as a `SocketAddr` at startup; `max_rows` is the
//! stream-side row cap.
use std::collections::HashMap;

use query_api::config::QueryApiConfig;

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

#[test]
fn empty_env_defaults_bind_addr_none_and_max_rows_1e6() {
    let cfg: QueryApiConfig = loom_config::load(&map(&[])).unwrap();
    assert_eq!(cfg.sql_wire.bind_addr, None);
    assert_eq!(cfg.sql_wire.max_rows, 1_000_000);
}

#[test]
fn env_overlays_both_fields() {
    let cfg: QueryApiConfig = loom_config::load(&map(&[
        ("LOOM_SQL_WIRE_BIND_ADDR", "127.0.0.1:31337"),
        ("LOOM_SQL_WIRE_MAX_ROWS", "500"),
    ]))
    .unwrap();
    assert_eq!(cfg.sql_wire.bind_addr.as_deref(), Some("127.0.0.1:31337"));
    assert_eq!(cfg.sql_wire.max_rows, 500);
}

#[test]
fn malformed_bind_addr_is_error() {
    // `QueryApiConfig` is intentionally not `Debug`, so match rather than `unwrap_err`.
    match loom_config::load::<QueryApiConfig>(&map(&[("LOOM_SQL_WIRE_BIND_ADDR", "not-an-addr")])) {
        Err(e) => assert!(format!("{e}").contains("LOOM_SQL_WIRE_BIND_ADDR")),
        Ok(_) => panic!("expected a validation error for a malformed bind_addr"),
    }
}

#[test]
fn zero_max_rows_is_error() {
    match loom_config::load::<QueryApiConfig>(&map(&[("LOOM_SQL_WIRE_MAX_ROWS", "0")])) {
        Err(e) => assert!(format!("{e}").contains("LOOM_SQL_WIRE_MAX_ROWS")),
        Ok(_) => panic!("expected a validation error for max_rows = 0"),
    }
}
