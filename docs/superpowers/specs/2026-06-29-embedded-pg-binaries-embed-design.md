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
   `env = { "LOOM_PG_TARBALL": "$(location :postgres-tarball)", "LOOM_PG_SHA": PG_SHA }`
   (where `PG_SHA` is the per-arch sha `select`, hoisted to a shared constant and
   referenced by both `:postgres-tarball` and the env). The code reads:
   ```rust
   static PG_TARBALL: &[u8] = include_bytes!(env!("LOOM_PG_TARBALL"));
   const PG_SHA: &str = env!("LOOM_PG_SHA");
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
unacceptable — the zero-pool transform worker must stay lean. The embed lives
behind a **dedicated buck target** so it is not even compiled into the base
crate:

- The base `managed-postgres` rust_library is unchanged (no tarball, no `tar`).
- A new module (e.g. `src/embed.rs`) holds the `include_bytes!` + extract logic
  and is compiled **only** into a gated target. Concretely: a `managed-postgres`
  cargo feature `embed-assets` guards the module (`#[cfg(feature = "embed-assets")]`),
  and a second buck `rust_library` target builds the crate with that feature
  **and** the `LOOM_PG_TARBALL`/`LOOM_PG_SHA` env (the env is only needed when
  the module compiles). Slice 3's binary and the PR-b test depend on the gated
  target; all existing consumers keep depending on the lean target.
- **Plan's first task verifies** both that the gated target builds on RE and
  that the lean target's rlib does **not** contain the tarball bytes (size
  check), since the buck shape for "same crate, two feature variants, env only
  on one" is the part worth de-risking first.

### Extract API (in the gated module)

```rust
/// Where the extracted, ready-to-exec PG distribution lives.
pub struct ExtractedPg {
    pub bin_dir: PathBuf,   // <cache>/pg-<sha8>/bin
    pub lib_dir: PathBuf,   // <cache>/pg-<sha8>/lib
}

/// Extract the embedded PG distribution under `cache_root`, reusing an existing
/// extraction. Lock-free and concurrency-safe.
pub fn extract_pg(cache_root: &Path) -> Result<ExtractedPg, EmbedError>;
```

- **Cache key — no runtime hashing.** Directory name is `pg-<sha8>` where
  `sha8` is the first 8 chars of `PG_SHA` (the build-time-known tarball sha). The
  11.7 MB is never re-hashed at startup; the key changes automatically when the
  pin bumps.
- **Location:** `cache_root` defaults to `<LOOM_DATA_PATH>/cache`, giving
  `<LOOM_DATA_PATH>/cache/pg-<sha8>/`. Extract once; reused across restarts.
- **Lock-free extraction:**
  1. If `<cache>/pg-<sha8>/` exists → reuse (return the paths).
  2. Else extract into a sibling temp dir `pg-<sha8>.tmp.<pid>`.
  3. Atomically `rename()` the temp dir onto `pg-<sha8>/`. The rename is the
     publish step, so the final dir only ever appears fully-extracted.
  4. If the rename loses a race (`EEXIST`/`ENOTEMPTY` — another loom published
     first), discard the temp dir and reuse the winner's. No flock needed (the
     slice-1 single-owner flock guards the *data dir*; the cache is a separate,
     shareable resource).
- **Permissions:** the `bin/` files are extracted executable (preserve tar mode
  bits, or chmod `0755`).
- **GC: deferred.** A pin bump leaves the prior `pg-<sha8>/` as a dead dir — a
  bounded disk leak, not a correctness issue. Note in FUTURE alongside
  `fut-embedded-postgres-pg-upgrade`.

### `service_runtime` wiring

Under `LOOM_PG_MODE=embedded`, when the embed feature is in play (slice 3's
binary), `build_pool_managed` calls `extract_pg(<LOOM_DATA_PATH>/cache)` and
fills `EmbeddedPgConfig.bin_dir` / `ld_library_path` from the result instead of
reading `LOOM_PG_BIN_DIR` / `LOOM_PG_LD_LIBRARY_PATH`. Those two env vars become
optional in embedded mode (an override seam for the non-embedded "managed
against an external bin dir" path slice 1 shipped, which stays working).
`EmbeddedPgConfig` is unchanged (already carries `bin_dir` + `ld_library_path`).

PR-b's deliverable is the **embed + extract capability** wired so an embedded
boot needs no external bin dir. The in-process all-in-one composition is slice 3.

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
- **Lean-crate guard** (plan task 1): the base `managed-postgres` rlib does not
  embed the tarball (size assertion / absence of the symbol).
- Full `buck2 test //src/...` green is the acceptance gate.

## Risks / open questions

- **buck "two feature variants of one crate, env on one" shape.** The gating is
  the least-trodden part; the plan de-risks it in task 1 (build the gated target
  + confirm the lean target stays lean) before building extract logic.
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
