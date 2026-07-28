//! Consolidation suite for the action/write e2e family (#668 phase 1).
//! Folding 10 single-file `loom_fixture_test` targets into one crate cuts 10
//! link actions + 10 hermetic-Postgres boots down to 1 of each. Member files
//! below are unmodified siblings pulled in as modules.

mod action_client_wire;
mod action_computed_e2e;
mod action_downstream_atomicity;
mod action_e2e;
mod action_mapping_e2e;
mod action_multi_object_e2e;
mod action_response_http;
mod iceberg_action_e2e;
mod multi_step_stream_refuse_http;
mod write_steps_e2e;
