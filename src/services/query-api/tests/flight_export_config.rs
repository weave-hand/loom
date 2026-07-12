//! Config behaviour of `FlightExportTuning` on the `QueryApiConfig` layered seam:
//! `bind_addr` is opt-in (unset => the Arrow Flight export listener does not
//! start) and validated as a `SocketAddr` at startup; `max_rows` is the
//! per-export row cap. Env names (`LOOM_FLIGHT_BIND_ADDR`, `LOOM_EXPORT_MAX_ROWS`)
//! are unchanged from the pre-seam raw reads — this test pins that contract.
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
    assert_eq!(cfg.flight_export.bind_addr, None);
    assert_eq!(cfg.flight_export.max_rows, 1_000_000);
}

#[test]
fn env_overlays_both_fields() {
    let cfg: QueryApiConfig = loom_config::load(&map(&[
        ("LOOM_FLIGHT_BIND_ADDR", "127.0.0.1:50052"),
        ("LOOM_EXPORT_MAX_ROWS", "500"),
    ]))
    .unwrap();
    assert_eq!(
        cfg.flight_export.bind_addr.as_deref(),
        Some("127.0.0.1:50052")
    );
    assert_eq!(cfg.flight_export.max_rows, 500);
}

#[test]
fn file_layer_sets_both_then_env_overrides() {
    // The headline capability of this item: the knobs now have a file layer
    // (defaults < file < env). Mirrors `loom-config/tests/load.rs`'s file test.
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    write!(
        f,
        r#"{{"flight_export": {{"bind_addr": "0.0.0.0:50052", "max_rows": 500000}}}}"#
    )
    .unwrap();
    f.flush().unwrap();
    let path = f.path().to_str().unwrap().to_string();
    // File only: file values apply over the defaults.
    let from_file: QueryApiConfig =
        loom_config::load(&map(&[("LOOM_CONFIG_FILE", &path)])).unwrap();
    assert_eq!(
        from_file.flight_export.bind_addr.as_deref(),
        Some("0.0.0.0:50052")
    );
    assert_eq!(from_file.flight_export.max_rows, 500_000);
    // File + env: env overlays the file.
    let env_wins: QueryApiConfig = loom_config::load(&map(&[
        ("LOOM_CONFIG_FILE", &path),
        ("LOOM_EXPORT_MAX_ROWS", "42"),
    ]))
    .unwrap();
    assert_eq!(env_wins.flight_export.max_rows, 42, "env overlays file");
    // `f` drops at end of scope, removing the temp file — no manual cleanup.
}

#[test]
fn malformed_bind_addr_is_error() {
    // `QueryApiConfig` is intentionally not `Debug`, so match rather than `unwrap_err`.
    match loom_config::load::<QueryApiConfig>(&map(&[("LOOM_FLIGHT_BIND_ADDR", "not-an-addr")])) {
        Err(e) => assert!(format!("{e}").contains("LOOM_FLIGHT_BIND_ADDR")),
        Ok(_) => panic!("expected a validation error for a malformed bind_addr"),
    }
}

#[test]
fn zero_max_rows_is_error() {
    match loom_config::load::<QueryApiConfig>(&map(&[("LOOM_EXPORT_MAX_ROWS", "0")])) {
        Err(e) => assert!(format!("{e}").contains("LOOM_EXPORT_MAX_ROWS")),
        Ok(_) => panic!("expected a validation error for max_rows = 0"),
    }
}
