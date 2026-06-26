# Stricter Clippy Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt broad `clippy::pedantic` + `clippy::restriction` as loom's enforced first-party lint policy, wired once through the buck2 toolchain, enforce a curated high-signal subset (panic-safety + error-handling) on production code, exempt test and test-harness code, and bring the tree to a green `clippy-all.sh`.

**Architecture:** The buck2 prelude's `RustToolchainInfo` carries `warn_lints`/`allow_lints`/`clippy_toml` fields that `_lintify` (`prelude/rust/build.bzl:954-968`) turns into clippy flags. loom's `hermetic_rust_toolchain` (`toolchains/rust_dist.bzl`) now forwards them (Task 1). The groups are enabled once on `:rust`; a large `allow_lints` block (Billy-Levin-style enable-broadly-allowlist) silences the stylistic/contradictory/structural lints; the high-signal panic-safety/error-handling lints stay enforced. Test targets and test-harness libraries are exempted from the panic-safety lints (test code legitimately panics) via a `loom_rust_test` wrapper + the existing `loom_fixture_test` macro + crate/module `#![allow]` on the two harness sources. Then production violations of the enforced set (~160 sites) are fixed for real.

**Tech Stack:** buck2 + vendored prelude, hermetic Rust toolchain (pinned nightly), clippy via the `[clippy.txt]` sub-target, `tools/clippy-all.sh` (the enforced gate, also the `clippy` prek hook + CI `lint` job).

## Global Constraints

- **Enforced gate fails on ANY diagnostic.** `tools/clippy-all.sh` builds each first-party target's `[clippy.txt]` and exits non-zero if any diagnostics file is non-empty. "Green" means `tools/clippy-all.sh` exits 0 over `//src/...`.
- **Scope is first-party only:** `//src/...`. Third-party and `//tools` are out of scope.
- **buck2 does not read Cargo `[lints]`.** All lint config flows through `RustToolchainInfo` / per-target `rustc_flags`. Do NOT add `[workspace.lints]`.
- **Lint fields take plain strings** (`attrs.list(attrs.string())`), values like `"clippy::pedantic"`. Per-target allows are `rustc_flags = ["-Aclippy::<lint>"]` (rustc silently ignores `clippy::*` flags; only the clippy action applies them).
- **Use `warn`, not `deny`** for the toolchain groups.
- **Test/harness code is exempted, not rewritten.** The panic-safety lints are allowed for: all `rust_test` targets (via `loom_rust_test` + `loom_fixture_test`), the `testkit` library (`src/control-plane/testkit/src/lib.rs`, a conformance harness), and `postgres/src/fixture.rs` (the test fixture harness — CLAUDE.md: "not production query paths"). Production `src/` code is fixed for real.
- **ENFORCED production lint set** (the only `clippy::restriction`/leftover lints kept on for `src/`; everything else in the groups is allowed):
  `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `get_unwrap`, `unwrap_in_result`, `panic_in_result_fn`, `map_err_ignore`, `let_underscore_must_use`, `unused_result_ok`, `unreachable`, `format_push_string`, `allow_attributes_without_reason`, `dbg_macro`, `todo`, `unimplemented`, `print_stdout`, `print_stderr`.
- **TEST/HARNESS exemption lint set** (allowed for test targets + the two harness sources):
  `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `get_unwrap`, `unwrap_in_result`, `panic_in_result_fn`, `unreachable`, `assertions_on_result_states`.
- **Don't pipe `buck2 test`/`buck2 bxl` through tail/head** — redirect to a file and grep it. `buck2 build | tail` is fine.
- **ENV (local machine):** inotify is throttled (~4041 slots); `buck-out/` grows to ~8800 dirs after a full gate run and can break the next run. Clean buck-out (`buck2 clean` / remove `buck-out`) if the gate errors with "too many open files"/inotify. CI/RE is unaffected.
- **Markdown hygiene:** any `.md` edit must end with exactly one trailing newline and no trailing whitespace (the `lint` job's `end-of-file-fixer`/`trim trailing whitespace` police all files). Run `buck2 run //tools:prek -- run --all-files` before pushing and commit what it changes.
- **Commits:** Conventional Commits (commit-msg hook) + trailer `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Branch: `clippy-strict-lints`.

---

### Task 1: Wire toolchain lint fields + enable-pedantic spike — ✅ DONE

Threaded `allow_lints`/`deny_lints`/`warn_lints`/`clippy_toml` through `hermetic_rust_toolchain` into `RustToolchainInfo` (commit `17bfa0b`). `clippy_toml` uses `attrs.dep(providers=[DefaultInfo])` + root `BUCK` `export_file` (`root//:clippy-toml`). Root `clippy.toml` has the five `allow-*-in-tests` keys. Verified: pedantic reaches the clippy action; plain `rustc` stays clean; `allow-unwrap-in-tests` fires for `#[test]`-fn code in `rust_test` crates (but NOT test-helper fns — hence the wrapper in Task 3).

### Task 2: Enable `restriction` + census — ✅ DONE (measurement, uncommitted)

`toolchains/BUCK` working-tree edit sets `warn_lints = ["clippy::pedantic", "clippy::restriction"]`, `allow_lints = ["clippy::blanket_clippy_restriction_lints"]`. Census: 109 distinct lints, ~10,591 occurrences, 0 build failures. Triage decisions are recorded in the plan Global Constraints (enforced set / exemption set) and the SDD ledger.

---

### Task 3: Apply lint policy — global allowlist + test/harness exemptions

Brings the tree from "everything red" to "red ONLY on the ~160 production sites of the enforced set." No production code fixes here (Task 4 does those).

**Files:**
- Modify: `toolchains/rust_dist.bzl` (add+forward a `rustc_test_flags` attr — test-only compile flags)
- Modify: `toolchains/BUCK` (the `:rust` `allow_lints` full allowlist + `rustc_test_flags` exemption list)
- Modify: `src/services/query-api/tests/e2e_support.rs` (crate-root `#![allow]` — the one test-support `rust_library`)
- Modify: `src/control-plane/testkit/src/lib.rs` (crate-root `#![allow]` harness exemption)
- Modify: `src/control-plane/postgres/src/fixture.rs` (module `#![allow]` harness exemption)

**Mechanism (why this is small):** The prelude's `RustToolchainInfo.rustc_test_flags` (`rust_toolchain.bzl:58`) is consumed ONLY in `rust_test_impl` (`rust_binary.bzl:555-571`) as `extra_flags` — applied to every `rust_test` target's compile (incl. its clippy emit) and to NOTHING else (not `rust_library`, not `rust_binary`). So one toolchain field exempts ALL test targets — bare `rust_test` AND `loom_fixture_test` (which calls `native.rust_test`) AND their `srcs` (e.g. `transform_e2e_support.rs`, which is a src of fixture tests). No wrapper, no per-target migration, no `defs.bzl` edit. The only test code NOT compiled as a `rust_test` target is: the single `query-api:e2e-support` `rust_library` (`tests/e2e_support.rs`), `testkit` (a `rust_library` harness), and `postgres/src/fixture.rs` (a module in the production lib) — those three get a source-level `#![allow]`.

**Interfaces:**
- Consumes: the wired toolchain (Task 1); the working-tree `warn_lints` edit (Task 2).
- Produces: `:rust` toolchain with `rustc_test_flags` = the `-Aclippy::*` test-exemption list. New `rust_test` targets inherit the exemption automatically.

- [ ] **Step 1: Write the global `allow_lints` in `toolchains/BUCK`**

Set the `:rust` toolchain's `allow_lints` to the full allowlist — every firing lint NOT in the enforced production set, grouped with reason comments. Keep `warn_lints = ["clippy::pedantic", "clippy::restriction"]`. The list (from the census):

```python
    warn_lints = ["clippy::pedantic", "clippy::restriction"],
    allow_lints = [
        # Required when enabling the restriction group broadly.
        "clippy::blanket_clippy_restriction_lints",
        # Structural: loom mandates separate rust_test crates, not #[cfg(test)]
        # modules (CLAUDE.md) — this fires on every test fn.
        "clippy::tests_outside_test_module",
        # Pervasive style — fire on nearly every expression; no signal here.
        "clippy::implicit_return", "clippy::min_ident_chars", "clippy::question_mark_used",
        "clippy::single_call_fn", "clippy::pattern_type_mismatch", "clippy::ref_patterns",
        "clippy::else_if_without_else", "clippy::single_char_lifetime_names",
        "clippy::many_single_char_names", "clippy::similar_names", "clippy::shadow_reuse",
        "clippy::shadow_unrelated", "clippy::shadow_same", "clippy::items_after_statements",
        "clippy::unneeded_field_pattern", "clippy::used_underscore_binding",
        "clippy::used_underscore_items", "clippy::elidable_lifetime_names",
        # Docs — not required across this service tree.
        "clippy::missing_docs_in_private_items", "clippy::doc_markdown", "clippy::missing_errors_doc",
        "clippy::missing_panics_doc", "clippy::doc_paragraphs_missing_punctuation",
        "clippy::missing_inline_in_public_items",
        # Naming / module conventions — opinionated.
        "clippy::module_name_repetitions", "clippy::struct_field_names", "clippy::pub_use",
        "clippy::mod_module_files", "clippy::unused_trait_names", "clippy::enum_glob_use",
        "clippy::absolute_paths", "clippy::arbitrary_source_item_ordering",
        "clippy::field_scoped_visibility_modifiers", "clippy::missing_trait_methods",
        "clippy::renamed_function_params", "clippy::multiple_inherent_impl", "clippy::unused_self",
        # Numeric / cast — high false-positive rate; documented-invariant churn.
        "clippy::arithmetic_side_effects", "clippy::as_conversions", "clippy::default_numeric_fallback",
        "clippy::cast_possible_wrap", "clippy::cast_possible_truncation", "clippy::cast_lossless",
        "clippy::cast_precision_loss", "clippy::cast_sign_loss", "clippy::integer_division_remainder_used",
        "clippy::float_arithmetic", "clippy::decimal_literal_representation", "clippy::little_endian_bytes",
        # std vs core/alloc — these are std services.
        "clippy::std_instead_of_core", "clippy::std_instead_of_alloc",
        # Whole-API-surface / opinionated restriction.
        "clippy::exhaustive_structs", "clippy::exhaustive_enums", "clippy::wildcard_enum_match_arm",
        "clippy::clone_on_ref_ptr", "clippy::impl_trait_in_params", "clippy::let_underscore_untyped",
        "clippy::semicolon_outside_block", "clippy::unseparated_literal_suffix",
        "clippy::separated_literal_suffix", "clippy::pub_with_shorthand", "clippy::missing_assert_message",
        "clippy::str_to_string", "clippy::duration_suboptimal_units", "clippy::non_ascii_literal",
        "clippy::assertions_on_result_states", "clippy::allow_attributes",
        "clippy::case_sensitive_file_extension_comparisons", "clippy::missing_asserts_for_indexing",
        # Subjective pedantic — commonly allowed; low signal / high churn.
        "clippy::must_use_candidate", "clippy::too_many_lines", "clippy::cognitive_complexity",
        "clippy::needless_pass_by_value", "clippy::if_not_else", "clippy::single_match_else",
        "clippy::match_wildcard_for_single_variants", "clippy::implicit_hasher",
        "clippy::default_trait_access", "clippy::return_self_not_must_use", "clippy::trivially_copy_pass_by_ref",
        "clippy::iter_over_hash_type",
        # Cheap-mechanical pedantic — allowed for now; candidates to promote-to-fix later.
        "clippy::map_unwrap_or", "clippy::redundant_closure_for_method_calls", "clippy::implicit_clone",
        "clippy::manual_string_new", "clippy::needless_continue", "clippy::explicit_iter_loop",
        "clippy::stable_sort_primitive", "clippy::redundant_type_annotations", "clippy::unnecessary_semicolon",
        "clippy::semicolon_if_nothing_returned", "clippy::deref_by_slicing", "clippy::single_char_pattern",
        "clippy::manual_assert", "clippy::ignored_unit_patterns", "clippy::match_same_arms",
        "clippy::return_and_then", "clippy::unnecessary_literal_bound", "clippy::needless_continue",
    ],
    clippy_toml = "root//:clippy-toml",
```

Note: this is the full allowlist; if `clippy-all.sh` later reports an ALLOWed lint still firing, it was misspelled — fix it. Lints in the enforced set (see Global Constraints) are deliberately ABSENT here.

- [ ] **Step 2: Add a `rustc_test_flags` attr to `hermetic_rust_toolchain`**

In `toolchains/rust_dist.bzl`, add `"rustc_test_flags": attrs.list(attrs.string(), default = [])` to the rule's `attrs` (alongside the lint fields from Task 1), and forward it into `RustToolchainInfo(...)`:
```python
            rustc_test_flags = ctx.attrs.rustc_test_flags,
```
(The prelude's `RustToolchainInfo` already declares this field — `rust_toolchain.bzl:58` — and consumes it ONLY in `rust_test_impl`.)

- [ ] **Step 3: Set the test exemption on `:rust`**

In `toolchains/BUCK`, add to the `:rust` toolchain (after `allow_lints`):
```python
    # Panic-safety lints allowed for test code only. rustc_test_flags is applied
    # by the prelude solely to rust_test targets (rust_binary.bzl:rust_test_impl) —
    # every rust_test (bare and loom_fixture_test) and its srcs inherit this;
    # rust_library/rust_binary do NOT. Test code legitimately panics on setup.
    rustc_test_flags = [
        "-Aclippy::unwrap_used", "-Aclippy::expect_used", "-Aclippy::indexing_slicing",
        "-Aclippy::panic", "-Aclippy::get_unwrap", "-Aclippy::unwrap_in_result",
        "-Aclippy::panic_in_result_fn", "-Aclippy::unreachable",
        "-Aclippy::assertions_on_result_states",
    ],
```

- [ ] **Step 4: Exempt the three non-test-target test sources**

These are test code NOT compiled as `rust_test` targets, so `rustc_test_flags` does not reach them. Add a crate-root (or module) `#![allow(...)]` at the very top of each, before any items:

`src/services/query-api/tests/e2e_support.rs` (the one test-support `rust_library`), `src/control-plane/testkit/src/lib.rs` (conformance harness lib), and `src/control-plane/postgres/src/fixture.rs` (fixture harness module):
```rust
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "test/fixture harness code, not a production path"
)]
```
Tailor each `reason` (e.g. testkit: "conformance test-harness library"; fixture.rs: "postgres test-fixture harness, not a production query path — see CLAUDE.md"). `allow_attributes` is globally allowed so `#![allow]` is fine; `allow_attributes_without_reason` stays enforced, so the `reason =` is required.

- [ ] **Step 5: Run the gate; confirm only the enforced production set remains red**

Clean buck-out if needed (see ENV), then:
```bash
./tools/clippy-all.sh > /tmp/clippy_t3.txt 2>&1; echo "exit=$?"
sed 's/\x1b\[[0-9;]*m//g' /tmp/clippy_t3.txt | grep -oE 'clippy::[a-z_]+' | sort | uniq -c | sort -rn
sed 's/\x1b\[[0-9;]*m//g' /tmp/clippy_t3.txt | grep -oE '> [^ ]+\.rs' | sort -u | grep -E '/tests/|testkit/src/lib.rs|fixture.rs' || echo "GOOD: no test/harness files remain"
```
Expected: non-zero exit; remaining lints are ONLY the enforced set (`unwrap_used`, `expect_used`, `indexing_slicing`, `map_err_ignore`, `let_underscore_must_use`, `panic`, `format_push_string`, `unused_result_ok`, `unreachable`, `allow_attributes_without_reason`) in `src/` non-harness files. If an allowed lint still appears → misspelled in Step 1, fix it. If a `/tests/` file still appears → either `rustc_test_flags` didn't reach it (a `rust_library` test-support crate other than e2e_support — add a `#![allow]`) or the flag isn't wired (recheck Steps 2-3). If `testkit/src/lib.rs` or `fixture.rs` appears → the Step 4 `#![allow]` is missing/misplaced.

- [ ] **Step 6: Commit the policy (config + exemptions; production still red)**

```bash
git add toolchains/rust_dist.bzl toolchains/BUCK \
  src/services/query-api/tests/e2e_support.rs \
  src/control-plane/testkit/src/lib.rs src/control-plane/postgres/src/fixture.rs
git commit -m "build(lints): enable restriction; allowlist + test exemptions

Enable clippy::restriction alongside pedantic; add the census-driven global
allow_lints (Billy-Levin enable-broadly-allowlist). Exempt test code from the
panic-safety lints via the toolchain's rustc_test_flags (applied by the prelude
to rust_test targets only), plus a #![allow] on the three test sources not
compiled as rust_test targets (query-api e2e-support lib, testkit lib, postgres
fixture). Tree intentionally still red on the ~160 enforced production sites.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Tasks 4a–4d: Fix the enforced-set violations in production, per crate

Each sub-task fixes ONE crate's production violations of the enforced set, keeps that crate's tests green, and commits. Worked independently; order by size. The enforced set and fix idioms are identical across them — only the crate/scope differs.

**Fix idioms (all sub-tasks):**
- `unwrap_used` / `expect_used` / `get_unwrap` → propagate with `?` over the fn's `Result` (add a `thiserror` variant or `.map_err(...)`/`.context(...)` as the surrounding code does); for a genuine, documented invariant that cannot be a `Result`, restructure or — last resort — `#[expect(clippy::unwrap_used, reason = "…")]` on that statement (NOT a blanket file allow).
- `indexing_slicing` → `.get(i).ok_or(...)?` / `.first()` / `.get(range)`.
- `map_err_ignore` → carry the source error (`.map_err(|e| Error::Foo(e))` / `#[from]`), never `|_|`.
- `let_underscore_must_use` → handle the value (`?`, `.await`, explicit `drop()` with reason).
- `panic` / `unreachable` → return an error variant, or `#[expect(..., reason=…)]` for a proven-unreachable arm.
- `unused_result_ok` → replace `.ok()` discard with `?` or explicit handling.
- `format_push_string` → `write!(s, ...)` instead of `s.push_str(&format!(...))`.
- For locks (`.lock().unwrap()` in `memory`): prefer propagating the poison (`.map_err(|_| Error::LockPoisoned)?`) if the fn returns `Result`; if not, `#[expect(clippy::unwrap_used, reason = "std Mutex poisoning is unrecoverable here")]` per call is acceptable.
- Use `#[expect(...)]` not `#[allow(...)]` only where the lint genuinely should fire; otherwise fix. Every suppression carries a `reason =`.

**Per sub-task loop (do for each crate):**
- [ ] List the crate's remaining enforced-set sites from `/tmp/clippy_t3.txt` (or re-lint the crate: `buck2 build '//<crate>:<lib>[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}' | xargs -r cat`).
- [ ] Fix each site with the idiom above.
- [ ] Re-lint the crate's lib/bin `[clippy.txt]` — expect zero enforced-set diagnostics.
- [ ] Run the crate's tests: `buck2 test //<crate>/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — expect `0 failed`. (`?`-refactors change signatures/behavior; this catches regressions.)
- [ ] Commit: `fix(clippy): enforce panic-safety lints in <crate>` + trailer.

**Sub-tasks (by size):**
- [ ] **Task 4a — `src/services/query-api`** (~56: `sql.rs` 26, `handler.rs` 13, `http.rs` 5, `filter.rs` 5, `params.rs` 4, `serving.rs`/`main.rs`/`chain_filter.rs` 3)
- [ ] **Task 4b — `src/control-plane/memory`** (~60: `acl.rs` 16, `queue.rs` 15, `ontology.rs` 8, `auth.rs` 7, `catalog.rs` 5, `lineage.rs` 4, `transaction.rs` 3, `lib.rs` 2 — mostly lock poisoning)
- [ ] **Task 4c — `src/control-plane/postgres` (non-fixture)** (~30: `iceberg_landing.rs` 8, `iceberg_sql_catalog/catalog.rs` 6, `iceberg_schema_evolution.rs` 4, `ontology.rs` 2, `iceberg_sql_catalog/s3_storage.rs` 2, `iceberg_mirror.rs` 2, + 6 singletons). `fixture.rs` is already exempted — do NOT touch it.
- [ ] **Task 4d — small services batch** (~17: `engine` 4, `runtime` 3, `datafusion-io` 3, `worker` 2, `transform` 2, `ingest` 2, `control-plane/worker` 1). One commit for the batch is fine; run each touched crate's tests.

---

### Task 5: Full verification + document the policy

- [ ] **Step 1: Full clippy gate** — clean buck-out if needed, then `./tools/clippy-all.sh; echo "exit=$?"` → expect `exit=0`.
- [ ] **Step 2: Full test sweep** — `buck2 test //src/... > /tmp/full_test.log 2>&1; grep -E "Tests finished|FAIL|error:" /tmp/full_test.log` → expect `0 failed`. (Fixture tests can flake on contention; re-run a clean sweep before treating a failure as real.)
- [ ] **Step 3: CLAUDE.md lint-policy section.** Under the clippy bullet in "Dev tools", add a paragraph documenting: groups enabled tree-wide via `:rust` `warn_lints` (forwarded through `hermetic_rust_toolchain`); the large `allow_lints` allowlist (enable-broadly model); the enforced production subset; that test code is exempted via `loom_rust_test`/`loom_fixture_test` (`src/loom_test.bzl` → `LOOM_TEST_LINT_ALLOWS`) and the two harness sources via `#![allow]`; how to silence globally (add to `allow_lints` with a reason) vs locally (`#[expect(lint, reason=…)]`); and that a nightly bump can add lints that redden the gate.
- [ ] **Step 4: Resolve the spec's test-exemption fork.** In `docs/superpowers/specs/2026-06-26-stricter-clippy-config-design.md` §2, replace the "If constraint #4 proves…" hedge with what happened: in-`#[test]`-fn code is exempted by `allow-*-in-tests`, but test-helper/harness code required the `loom_rust_test` wrapper + harness `#![allow]`s.
- [ ] **Step 5: Record the deferred promote-to-fix list.** Add a FUTURE register item (`docs/FUTURE.md`, via the grammar in CLAUDE.md "Documentation registers") noting the cheap-mechanical pedantic lints currently allowlisted (`map_unwrap_or`, `redundant_closure_for_method_calls`, `cast_lossless`, …) as candidates to promote from allow to fix.
- [ ] **Step 6: prek + commit docs** — `buck2 run //tools:prek -- run --all-files` (commit any hook fixes), then commit CLAUDE.md + spec + FUTURE.md.
- [ ] **Step 7: Push + PR** — `git push -u origin clippy-strict-lints`; `gh pr create` with title "Stricter clippy: enable pedantic + restriction tree-wide" and a body summarizing enforced-set/allowlist/exemptions and linking the spec + plan. Merge on green `affected` + `lint`.

---

## Self-Review

**Spec coverage:** mechanism (Task 1 ✅); groups enabled + allowlist (Task 3 Step 1); test-exemption mechanism, incl. the constraint-#4 fallback that materialized (Task 3 Steps 2-5); production fixes for the enforced high-signal set (Tasks 4a-d); verification + docs + deferred-list (Task 5). The keep/allow split is now concrete (Global Constraints) rather than census-deferred, because the census has been taken.

**Placeholder scan:** The allowlist (Task 3 Step 1) and per-crate site counts (Tasks 4a-d) are concrete from the census. Fix idioms are spelled out. No "TBD"/"handle edge cases" placeholders.

**Type consistency:** `LOOM_TEST_LINT_ALLOWS` (list of `-Aclippy::*` strings) is defined once in `src/loom_test.bzl` and imported by both `loom_rust_test` and `loom_fixture_test`. `allow_lints`/`warn_lints` are plain-string lists matching `RustToolchainInfo` (Task 1). The enforced set (Global Constraints) and the global allowlist (Task 3 Step 1) are complementary — no lint appears in both.
