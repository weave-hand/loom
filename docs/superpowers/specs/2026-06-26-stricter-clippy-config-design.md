# Stricter clippy config via toolchain-level lint policy

- **Date:** 2026-06-26
- **Status:** design approved, pending implementation plan
- **Area:** tooling / build

## Problem

loom's first-party Rust is linted by clippy through the buck2-native `[clippy.txt]`
sub-target (`tools/clippy-all.sh`, wired as the `clippy` prek hook and the CI `lint`
job — the enforced gate). Today that gate runs clippy with **default lints only**:
there is no `clippy.toml`, no `[lints]` table is honored, no `rustc_flags` carry lint
config, and the only crate-level lint attribute in the whole tree is a single
`#![deny(missing_docs)]` in `src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs`.

We want substantially stricter, opinionated linting to act as durable guidelines —
adopting the broad allowlist philosophy from
[billylevin.dev/posts/clippy-config](https://billylevin.dev/posts/clippy-config/)
(enable whole groups, then selectively `allow`) informed by the curated
restriction set from
[emschwartz.me/your-clippy-config-should-be-stricter](https://emschwartz.me/your-clippy-config-should-be-stricter/).

The codebase is the smallest it will ever be, so the one-time cleanup cost of going
broad is at its minimum now.

## Constraints discovered

These shaped the design and are load-bearing:

1. **buck2 does not read Cargo `[lints]` / `[workspace.lints]`.** The rust rules read
   only BUCK attributes and `RustToolchainInfo`; nothing bridges Cargo lint tables into
   the build. The root `Cargo.toml` has no `[workspace.lints]`, and adding one would be
   honored *only* by the ad-hoc, ungated `//tools:clippy` (`cargo clippy`) path — the
   enforced `clippy-all.sh` gate would silently ignore it. So the Cargo-workspace-lints
   route is a dead end for the gate.

2. **The enforced gate fails on *any* diagnostic.** `tools/clippy-all.sh` builds each
   target's `[clippy.txt]` sub-target and fails if any diagnostics file is non-empty.
   There is no "warn but don't fail" middle ground today, so `warn`-level lints behave
   as hard failures under the gate. Enabling broad groups therefore requires reaching a
   fully clean tree before landing.

3. **The prelude already supports toolchain-level, clippy-only lint config.**
   `RustToolchainInfo` carries `warn_lints` / `deny_lints` / `allow_lints` / `clippy_toml`
   (`prelude/rust/rust_toolchain.bzl:81-100`). `_lintify`
   (`prelude/rust/build.bzl:954-957`) converts them to flags and **filters
   `"clippy::"`-prefixed lints to the clippy action only** — they are stripped from the
   normal `rustc` build. The same `clippy.toml` is symlinked in and `CLIPPY_CONF_DIR`
   set for the clippy emit (`build.bzl:546-564`). This is a true configure-once knob that
   the gate honors and that leaves plain `rustc` builds clean. The catch: loom's
   `hermetic_rust_toolchain` (`toolchains/rust_dist.bzl`) does not currently forward
   these fields into the `RustToolchainInfo` it builds, so they fall back to the prelude
   defaults (`[]`/`None`).

4. **Tests are separate `rust_test` crates, not `#[cfg(test)]` modules.** clippy's
   "in tests" exemptions (`allow-unwrap-in-tests`, etc.) key off `#[cfg(test)]` /
   `#[test]` detection; whether they fire for loom's separate test crates must be
   verified, with a fallback if they do not.

## Decisions

- **Scope:** broad — enable the whole `clippy::pedantic` and `clippy::restriction`
  groups, then `allow` back the lints that are stylistic, contradictory, or fire on
  nearly every line.
- **Wiring:** source-/toolchain-level via the prelude's `warn_lints` mechanism. One
  config block, inherited by every first-party crate and every `rust_test`, honored by
  the gate, invisible to plain `rustc`.
- **Rollout:** big-bang — a single PR that enables the groups and reaches a green
  `clippy-all.sh` (via real code fixes for the kept lints plus the curated allow-list),
  rather than a ratchet or a temporary report-only gate.

## Design

### 1. Mechanism

Extend `hermetic_rust_toolchain` (`toolchains/rust_dist.bzl`) to accept and forward four
new attributes into the `RustToolchainInfo` it constructs:

- `warn_lints: list[str]`
- `deny_lints: list[str]`
- `allow_lints: list[str]`
- `clippy_toml: source | None`

Then set them once on the `:rust` toolchain in `toolchains/BUCK`:

```python
warn_lints = ["clippy::pedantic", "clippy::restriction"],
allow_lints = [
    # ... curated exceptions, each with a one-line reason (see §2) ...
],
clippy_toml = "//:clippy.toml",
```

No per-BUCK-file edits, no per-crate `#![warn]`, and new crates inherit the policy for
free. `warn` (not `deny`) is used because the gate already converts any diagnostic into
a failure, and `warn` keeps a bare local `buck2 build` non-fatal.

### 2. Lint policy

**Keep on (high signal, manageable volume)** — the restriction lints worth honoring,
roughly the emschwartz curated set:

- *Panic safety:* `unwrap_used`, `expect_used` (TBD by census), `indexing_slicing`,
  `string_slice`, `panic`, `unwrap_in_result`, `panic_in_result_fn`, `get_unwrap`.
- *Error handling:* `let_underscore_must_use`, `let_underscore_future`, `map_err_ignore`.
- *Unsafe:* `undocumented_unsafe_blocks`, `multiple_unsafe_ops_per_block`, `mem_forget`.
- *Async:* `await_holding_lock`, `await_holding_refcell_ref`, `large_futures`.
- *Leftovers:* `dbg_macro`, `todo`, `unimplemented`, `print_stdout`, `print_stderr`.
- *Discipline:* `allow_attributes`, `allow_attributes_without_reason`.

**Allow back (stylistic / contradictory / whole-API-surface)** — fire on nearly every
line and carry little signal for this codebase. Each gets a one-line reason comment:

- *Pervasive style:* `implicit_return`, `missing_docs_in_private_items`,
  `question_mark_used`, `min_ident_chars`, `single_char_lifetime_names`,
  `single_call_fn`, `ref_patterns`, `else_if_without_else`, `pattern_type_mismatch`.
- *Numeric noise:* `arithmetic_side_effects` (~85% false positives per emschwartz),
  `as_conversions`, `integer_division`, `modulo_arithmetic`, `default_numeric_fallback`.
- *Whole-type-surface:* `exhaustive_enums`, `exhaustive_structs`,
  `field_scoped_visibility_modifiers`, `partial_pub_fields`.
- *Contradictory pairs (allow one side):* `mod_module_files` vs `self_named_module_files`;
  `semicolon_inside_block` vs `semicolon_outside_block`; `pub_with_shorthand` vs
  `pub_without_shorthand`; `shadow_reuse` / `shadow_same` / `shadow_unrelated`;
  `separated_literal_suffix` vs `unseparated_literal_suffix`.
- *Not-applicable:* `std_instead_of_core`, `std_instead_of_alloc` (these are std services).
- *Required when enabling the group:* `blanket_clippy_restriction_lints`.

The **final allow-list is set by the census** (§3, Step 2), not by this list — the above
is the governing policy and starting point. Anything kept-on that produces a large fix
volume with low payoff may be demoted to allow during triage, with the reason recorded.

**Test exemptions** via root `clippy.toml`:

```toml
allow-unwrap-in-tests = true
allow-panic-in-tests = true
allow-expect-in-tests = true
allow-indexing-slicing-in-tests = true
allow-dbg-in-tests = true
```

If constraint #4 proves these do not fire for loom's separate `rust_test` crates, the
fallback is to `allow` the affected lints for test targets another way (e.g. a
`loom_rust_test` wrapper passing per-target `allow` flags, or `#![allow(...)]` test-root
attributes). This fork is resolved empirically during the spike/census.

### 3. Big-bang execution

- **Step 0 — spike (de-risk the mechanism):** wire the four toolchain fields, set only
  `warn_lints = ["clippy::pedantic"]`, run `clippy-all.sh` against one crate. Confirm:
  the gate sees the diagnostics, plain `buck2 build` of the same crate stays clean (no
  `-Wclippy::*` reaching rustc), and `clippy.toml` is picked up. Also probe whether the
  test-in-exemptions fire for a `rust_test` crate (resolves constraint #4).
- **Step 1 — census:** set `warn_lints = ["clippy::pedantic", "clippy::restriction"]`,
  run `clippy-all.sh` over `//src/...`, capture the full census (lint → count → files).
- **Step 2 — triage:** split every firing lint into *keep-and-fix* vs *allow-with-reason*
  per §2. Finalize `allow_lints` and the `clippy.toml`.
- **Step 3 — fix:** apply code fixes for the kept lints (mechanical via clippy's
  suggestions where safe, hand-fixed otherwise). Prefer fixing over allowing for the
  high-signal lints.
- **Step 4 — green & document:** `clippy-all.sh` exits 0 over `//src/...` **and**
  `buck2 test //src/...` passes. Add a "Clippy / lint policy" section to `CLAUDE.md`
  documenting the toolchain mechanism and where to edit the allow-list, and update
  `prek.toml` only if the gate wiring changes.

### 4. Verification & scope

- **Done =** `tools/clippy-all.sh` exits 0 over `//src/...` **and** the full
  `buck2 test //src/...` sweep is green.
- **Scope:** first-party `//src/...` only. Third-party (`//third-party`) and `//tools`
  are explicitly out of scope — `clippy-all.sh` already scopes to `root//src/...`.
- **Not in scope:** changing the gate to a warn-only/report mode; touching the ad-hoc
  `//tools:clippy` cargo path; per-crate lint divergence beyond the test-exemption
  fallback.

## Risks

- **Large diff.** Broad `restriction` means a sizable PR dominated by the `allow_lints`
  list plus a spread of small code fixes. Mitigation: group the allow-list with reason
  comments; keep mechanical fixes and hand fixes in separate commits for reviewability.
- **Test-exemption uncertainty (constraint #4).** Resolved by the spike before bulk work;
  fallback documented in §2.
- **rustc-vs-clippy flag routing.** Relying on `_lintify`'s `clippy::` filter rather than
  raw `rustc_flags` specifically to avoid `-Wclippy::*` noise on normal builds; confirmed
  by the spike in Step 0.
- **New lints on toolchain bump.** A future Rust nightly may add lints to these groups and
  redden the gate. Accepted: the fix is to `allow` or address the new lint, same as any
  lint-policy repo; noted in the CLAUDE.md section.
