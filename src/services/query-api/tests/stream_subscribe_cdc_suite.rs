//! Consolidation suite for the stream/CDC/subscribe e2e family (#668 phase 1).
//! Folding 15 single-file `loom_fixture_test` targets into one crate cuts 15
//! link actions + 15 hermetic-Postgres boots down to 1 of each. Member files
//! below are unmodified siblings pulled in as modules.

mod changelog_rpc_wire;
mod stream_cdc_consolidate;
mod stream_cdc_declare;
mod stream_cdc_e2e;
mod stream_cdc_read_mid_window;
mod stream_feed_torn_read;
mod stream_log_subscribe_e2e;
mod stream_log_subscribe_scan;
mod stream_log_subscribe_wire_e2e;
mod stream_merge_firstrow;
mod stream_merge_versioned;
mod stream_subscribe_e2e;
mod stream_subscribe_gov_e2e;
mod stream_subscribe_scan;
mod stream_subscribe_wire_e2e;
