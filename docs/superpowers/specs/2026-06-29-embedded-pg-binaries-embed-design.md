# Embed + self-extract the PG binaries (slice 2, PR-b)

Status: design. Author: brainstorm session 2026-06-29.

## Why

The single-binary arc wants loom to run with **no files on disk beyond the
binary**. Slice 1 added the embedded-Postgres lifecycle (against a configurable
`bin_dir`); slice 2 PR-a ([`2026-06-28-embedded-migrations-embed-design.md`](2026-06-28-embedded-migrations-embed-design.md))
removed the migrations-on-disk dependency. The remaining on-disk dependency is
the **PostgreSQL distribution itself** — today `LOOM_PG_BIN_DIR` /
`LOOM_PG_LD_LIBRARY_PATH` point at the buck-materialized `:postgres-bin`. PR-b
bakes that distribution into the binary and self-extracts it at startup, so the
embedded path needs no PG binaries pre-staged.

This is the last piece that makes loom a genuine single file (slice 3 then
composes the services in-process against it).

## Scope boundary vs PR-a

PR-a embedded the 76 KB of migrations via `sqlx::migrate!` + `mapped_srcs` — a
compile-time macro reading source files. PR-b is a **different mechanism**: the
PG distribution is an opaque 11.7 MB binary blob, so it is embedded with
`include_bytes!` and extracted to disk at runtime (Postgres must `exec` real
files). The two share no extract primitive, and that is correct — each uses the
right tool.

## What is and isn't embedded

The theseus-rs dist tarball contains `bin/` (initdb, postgres, pg_ctl, …) **and**
`lib/` (its bundled ICU etc.). Both are loom-owned — they ride in the tarball —
so both are embedded and extracted. It does **not** bundle `libxml2.so.2`.

**libxml2 is out of scope and deliberately NOT embedded.** System shared
libraries are the deployment target's responsibility (the OCI/Wolfi image or
host supplies them); the `libxml2-el7.rpm` fetch in `BUCK` exists only to make
the **test fixture / RE workers** hermetic across distro soname drift. See the
project decision recorded in memory (`deploy-target-provides-system-libs`).
Consequence for `ld_library_path`:

- **Production (embedded-extract):** `ld_library_path = <extracted>/lib`; the
  loader finds `libxml2.so.2` via the system path.
- **PR-b test (RE/dev, where `.so.2` is absent):** `ld_library_path =
  <extracted>/lib:<:libxml2 shim dir>` — the test layers the existing `:libxml2`
  shim on top, exactly as the slice-1 fixture does.

## Mechanism

1. **Add a raw-tarball buck input.** `:postgres-bin` is an `http_archive`
   (extracted dir); PR-b needs the single `.tar.gz` blob. Add an `http_file`
   `:postgres-tarball` with the **same** per-arch URL + sha256 pins (reuse the
   existing `PG_VERSION` / `_PG_URL` and the sha `select`), so there is one
   source of truth.
2. **Embed via the `$(location)`→env route** (the same plumbing the `.sqlx`
   cache and PR-a's migrate path use — proven): the embed target sets
   `env = { "LOOM_PG_TARBALL": "$(location :postgres-tarball)", "LOOM_PG_VERSION": PG_VERSION }`.
   `LOOM_PG_TARBALL` is a plain `$(location)` string; `LOOM_PG_VERSION` is the
   plain `PG_VERSION` string constant (e.g. `"17.9.0"`) — **not** a per-arch sha
   `select`, because buck has no precedent for a `select()` value inside an `env`
   dict in this tree and the cache key does not need content-addressing (see
   below). The code reads:
   ```rust
   static PG_TARBALL: &[u8] = include_bytes!(env!("LOOM_PG_TARBALL"));
   const PG_VERSION: &str = env!("LOOM_PG_VERSION");
   ```
3. **Decompress + untar** with `flate2::read::GzDecoder` (flate2 already
   vendored) piped into `tar::Archive` (the one new dependency — pure-Rust, no
   build script).
4. **Extract to a content-addressed cache, lock-free** (see below).

## Design

### `tar` dependency

Add `tar` to the embed crate's `Cargo.toml`, `cargo generate-lockfile`,
`./tools/buckify.sh`. Per CLAUDE.md, after any lock change diff the lock vs the
merge-base for native/`links` crates and run the full `buck2 test //src/...`
(reindeer re-resolution can churn unrelated crates). `tar` is pure-Rust with no
build script, so no fixup is expected.

### Feature/target gating (keep 11.7 MB out of lean binaries)

Embedding `include_bytes!` of 11.7 MB into every `managed-postgres` consumer is
unacceptable — the zero-pool transform worker must stay lean. The embed lives in
a **separate crate** so it is not even compiled into the base crate:

- The base `managed-postgres` crate/rust_library is **unchanged** (no tarball, no
  `tar`).
- A **new crate `managed-postgres-embed`** depends on `managed-postgres` (for the
  config/error types) + `tar` + `flate2`, and holds the `include_bytes!` +
  extract logic + the `LOOM_PG_TARBALL`/`LOOM_PG_VERSION` env on its buck target.
  Only consumers that depend on it (slice 3's binary, the PR-b test) pay the
  11.7 MB. (A *feature variant* of `managed-postgres` was considered and rejected:
  a binary pulling both the lean crate via `service_runtime` and the embed
  variant would link two copies of `managed_postgres` — a duplicate-crate
  conflict. A separate crate sidesteps this entirely.)
- **Plan's first task verifies** the gated crate builds on RE with the
  `$(location)`+env+`include_bytes!` wiring working (the buck shape is the part
  worth de-risking first); the base `managed-postgres` is untouched, so it stays
  lean by construction.

### Extract API (in the gated module)

```rust
/// Where the extracted, ready-to-exec PG distribution lives.
pub struct ExtractedPg {
    pub bin_dir: PathBuf,   // <cache>/pg-<version>/bin
    pub lib_dir: PathBuf,   // <cache>/pg-<version>/lib
}

/// Extract the embedded PG distribution under `cache_root`, reusing an existing
/// extraction. Lock-free and concurrency-safe.
pub fn extract_pg(cache_root: &Path) -> Result<ExtractedPg, EmbedError>;
```

- **Cache key — no runtime hashing.** Directory name is `pg-<version>` where
  `<version>` is `PG_VERSION` (`env!("LOOM_PG_VERSION")`, e.g. `pg-17.9.0`). The
  11.7 MB is never hashed at startup; the key changes when the version pin bumps.
  Version (not a content sha) is sufficient: a single host runs a single arch, a
  version bump drives any content change, and theseus release tarballs are
  immutable per tag — so re-pinning a *different* tarball under the *same*
  version (the only stale-cache risk) is pathological. The tar/extract logic is
  arch-agnostic regardless.
- **Location:** `cache_root` defaults to `<LOOM_DATA_PATH>/cache`, giving
  `<LOOM_DATA_PATH>/cache/pg-<version>/`. Extract once; reused across restarts.
- **Lock-free extraction:**
  1. If `<cache>/pg-<version>/` exists → reuse (return the paths).
  2. Else extract into a sibling temp dir `pg-<version>.tmp.<pid>`, **stripping
     the single top-level `postgresql-<ver>-<triple>/` component** the tarball
     wraps everything in (so `bin/`/`lib/` land at the temp-dir root).
  3. Atomically `rename()` the temp dir onto `pg-<version>/`. The rename is the
     publish step, so the final dir only ever appears fully-extracted.
  4. If the rename loses a race (`EEXIST`/`ENOTEMPTY` — another loom published
     first), discard the temp dir and reuse the winner's. No flock needed (the
     slice-1 single-owner flock guards the *data dir*; the cache is a separate,
     shareable resource).
- **Permissions:** the `bin/` files are extracted executable (tar mode bits are
  preserved by `tar`'s unpack on unix).
- **GC: deferred.** A version bump leaves the prior `pg-<version>/` as a dead
  dir — a bounded disk leak, not a correctness issue. Note in FUTURE alongside
  `fut-embedded-postgres-pg-upgrade`.

### Integration deferred to slice 3 (NOT in PR-b)

PR-b's deliverable is the **embed + extract capability**: the `managed-postgres-embed`
crate exposing `extract_pg()`, proven by a test that boots `EmbeddedPg` from the
extracted assets with no external bin dir. It does **not** touch `service_runtime`.

This is deliberate and required by the gating: `build_pool_managed` lives in the
*shared* `service_runtime`, depended on by every service binary (ingest,
query-api, …). If it called `extract_pg`, `service_runtime` would depend on
`managed-postgres-embed` and the 11.7 MB would land in **every** binary —
defeating the gating above. So the integration (call `extract_pg` in the
all-in-one binary's `main`, feed the resulting `bin_dir`/`ld_library_path` into
the `EmbeddedPgConfig` that `build_pool_managed` already accepts from slice 1)
belongs to **slice 3's binary**, which is the sole consumer that opts into the
embed crate. `EmbeddedPgConfig` already carries `bin_dir` + `ld_library_path`, so
no runtime change is needed in PR-b.

## Testing

- **Fixture test** (`loom_fixture_test`, hermetic PG), in the gated target:
  `extract_pg(tmp)` → assert `bin_dir`/`lib_dir` exist and `initdb`/`postgres`
  are present + executable; then boot `EmbeddedPg` with `bin_dir` from the
  extraction and `ld_library_path = <lib_dir>:<:libxml2 shim>` (the shim from the
  fixture env, since RE/Arch lack system `.so.2`) → assert the cluster boots and
  accepts a connection. Re-run `extract_pg` on the same `cache_root` → assert it
  reuses (no re-extract: the `pg-<sha8>` dir's mtime/inode is unchanged).
  **This proves boot-from-embedded-bytes with no external `LOOM_PG_BIN_DIR`.**
- **Pure-logic unit** (RE-eligible, no DB): `pg-<sha8>` key derivation from a
  sample sha; the temp→rename race resolution given a pre-existing final dir
  (call `extract_pg` twice; second is a no-op reuse).
- **Lean-crate guard:** the base `managed-postgres` is untouched (no dep on the
  embed crate, no `tar`/tarball), so it stays lean by construction — confirmed by
  its unchanged `BUCK`/`Cargo.toml` rather than a size assertion.
- Full `buck2 test //src/...` green is the acceptance gate.

## Risks / open questions

- **buck embed-crate wiring.** The `$(location)`+env+`include_bytes!` shape on a
  new crate is the least-trodden part; the plan de-risks it in task 1 (scaffold
  the crate, embed the tarball, build on RE) before building extract logic.
- **Binary size.** The gated binary grows ~11.7 MB; acceptable for a
  single-file local deploy and contained to the gated target.
- **macOS.** Per-arch tarball + sha already exist via `select`; extraction is
  host-agnostic. Socket-dir length limit already documented in slice 1.
- **First-boot latency.** First embedded boot pays a one-time ~11.7 MB
  decompress+untar; subsequent boots reuse the cache. Acceptable.

## Register note

On completion: flip `fut-embedded-postgres-embed-extract` to done (both halves
shipped) or close it, add a ROADMAP item for slice-2 PR-b with this spec as
`spec:` and slice 3 (`fut-embedded-postgres-all-in-one`) as a `[[link]]`, and add
a FUTURE GC item (dead `pg-<sha8>` dirs on pin bump) linked to
`fut-embedded-postgres-pg-upgrade`.
