"""Single source of truth for the wasm-bindgen version.

The `wasm-bindgen` *crate* (pinned `=` in src/ui/Cargo.toml) and the
`wasm-bindgen-cli` *tool* (//tools:wasm-bindgen) MUST be the same version, or the
generated JS glue panics at module load. Both `tools/BUCK` (the CLI http_file +
sha256) and `src/ui/BUCK` (the bundle genrule's version assertion) load this
constant so the version lives in exactly one place.

To bump: change WASM_BINDGEN_VERSION here, update the `=` pin in
src/ui/Cargo.toml to match, and refresh the CLI sha256 in tools/BUCK.
"""

WASM_BINDGEN_VERSION = "0.2.100"
