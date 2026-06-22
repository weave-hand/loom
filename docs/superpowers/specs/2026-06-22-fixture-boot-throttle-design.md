# Hermetic-Postgres fixture boot throttle — Design

> Closes `iss-fixture-boot-contention`. A full `buck2 test //src/...` sweep boots
> every `loom_fixture_test` target's Postgres cluster at once — ~60 `initdb`
> invocations in a single window — and they fail en masse at initdb's bootstrap
> backend (`PgFixture::start`, `fixture.rs:88`, `assert!(initdb_ok, "initdb failed")`).
> The cause is kernel-resource exhaustion under mass-concurrent cluster boot
> (SysV semaphore sets — Linux `SEMMNI` default is 128; dozens of live clusters,
> each reserving several sets, blow past it). Single targets and small batches pass
> because few clusters exist at once. This slice adds a cross-process+cross-thread
> bound on the number of **simultaneously-alive fixture clusters**, self-contained
> in the fixture, with no build-system or dependency changes.

## Symptom & root cause (observed)

Pushing a docs-only branch tripped the pre-push `buck2-test` hook twice with a
*shifting* 50–60-target failure set; a direct `buck2 test //src/...` reproduced it:
`Pass 68, Fail 60`, every failure a `loom_fixture_test` whose stderr shows
`initdb` reaching `running bootstrap script ...` then the process panicking at
`fixture.rs:88` with `initdb failed`, all clustered in one ~1s window. A single
target (`//src/control-plane/postgres:lineage-roundtrip`) run alone passes. The
failures are independent of any source diff — they reproduce on `main`. Two
concurrency axes stack:

1. **In-process** — Rust's test harness runs a target's `#[test]`s on parallel
   threads, so a multi-test fixture target boots several clusters at once.
2. **Cross-target** — buck2 runs many `loom_fixture_test` *processes*
   concurrently (all are local-only via the `loom_fixture_test` macro).

The product is dozens of clusters booting/living simultaneously. The exhausted
resource is held for each cluster's **lifetime** (a running `postgres` holds its
semaphore sets until it exits), so the bound must be on **live clusters**, not
merely concurrent boots.

## Goal

Bound the number of hermetic-Postgres fixture clusters alive at once across the
whole `buck2 test //src/...` run to a small, configurable `K`, so the suite stops
exhausting kernel resources — with no change to the build rules, no new
dependency, and no behavior change for a single test.

## Mechanism — a file-lock slot semaphore in `PgFixture`

A process-global semaphore of `K` slots, each backed by an advisory **`flock`** on
a marker file in a host-stable shared directory. A fixture acquires a slot before
`initdb` and holds it until the cluster is torn down (`Drop`), so at most `K`
clusters are alive at once across all test processes and threads.

`flock` is the right primitive: a lock is tied to the open file description and is
**auto-released when the fd closes or the process dies** — so a panicking/killed
test never leaks a slot (a `create_new`-marker lock would need stale-PID
reclamation). Rust 1.96 (the pinned nightly, `toolchains/BUCK` →
`RUST_NIGHTLY = 2026-03-28`) has **stable std file locking** — `File::try_lock`,
`File::unlock` — so this needs **no third-party crate** (no `Cargo.lock` /
`third-party/BUCK` churn, and so the duckdb-downgrade footgun is not in play).

Confirmed std API on this toolchain:

```rust
// std::fs::File
fn try_lock(&self) -> Result<(), std::fs::TryLockError>;  // Ok(())=acquired;
                                                          // Err(WouldBlock)=held elsewhere
fn unlock(&self) -> std::io::Result<()>;                  // (implicit on Drop too)
// std::fs::TryLockError = { WouldBlock, Error(std::io::Error) }
```

### The primitive (in `fixture.rs`, `pub` test-support like the rest of the file)

```rust
use std::fs::{File, OpenOptions, TryLockError};
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Bounds the number of concurrently-alive fixture Postgres clusters across all
/// test processes/threads. `slots` marker files live under `dir`; an acquired
/// slot is an exclusive `flock` held for the cluster's lifetime.
pub struct BootThrottle {
    dir: PathBuf,
    slots: usize,
}

/// An acquired boot slot. The `flock` is released when this `File` drops (or the
/// process exits), freeing the slot for another cluster.
pub struct SlotGuard {
    _file: File,
}

impl BootThrottle {
    pub fn new(dir: PathBuf, slots: usize) -> Self {
        std::fs::create_dir_all(&dir).expect("create fixture slot dir");
        Self { dir, slots: slots.max(1) }
    }

    /// Block until a slot is free, then return a guard holding it. Polls each of
    /// the `K` marker files with a non-blocking exclusive `flock`; sleeps briefly
    /// when all are busy. Panics after a generous deadline (test-only) so a wedged
    /// suite fails loudly instead of hanging forever.
    pub fn acquire(&self) -> SlotGuard {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            for i in 0..self.slots {
                let path = self.dir.join(format!("slot-{i}"));
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&path)
                    .expect("open fixture slot file");
                match file.try_lock() {
                    Ok(()) => return SlotGuard { _file: file },
                    Err(TryLockError::WouldBlock) => continue, // taken — try next slot
                    Err(TryLockError::Error(e)) => panic!("flock slot {i}: {e}"),
                }
            }
            if Instant::now() >= deadline {
                panic!("could not acquire a pg fixture boot slot within 120s \
                        (LOOM_PG_FIXTURE_SLOTS too low, or slots leaked?)");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}
```

Each `acquire` attempt **opens its own fd** (own open file description), so two
threads of the *same* process contend correctly — `flock` via distinct fds
conflicts even within one process. Marker files are never deleted (empty, reused
across runs); leaving them avoids a create/unlink race.

### Wiring into `PgFixture`

- A process-global throttle, configured once from the environment:

  ```rust
  use std::sync::OnceLock;
  static THROTTLE: OnceLock<BootThrottle> = OnceLock::new();

  fn boot_throttle() -> &'static BootThrottle {
      THROTTLE.get_or_init(|| {
          let slots = std::env::var("LOOM_PG_FIXTURE_SLOTS")
              .ok().and_then(|s| s.parse::<usize>().ok()).filter(|&n| n > 0)
              .unwrap_or(8);
          let dir = std::env::var_os("LOOM_PG_FIXTURE_SLOT_DIR")
              .map(PathBuf::from)
              .unwrap_or_else(|| PathBuf::from("/tmp/loom-pg-fixture-slots"));
          BootThrottle::new(dir, slots)
      })
  }
  ```

- Add a `_slot: SlotGuard` field to `PgFixture`, **declared last** so it drops
  after the others. Acquire it as the **first** line of `start()`:

  ```rust
  pub fn start() -> Self {
      let _slot = boot_throttle().acquire();   // <-- gate before initdb
      // ... existing initdb / postgres spawn / wait_ready ...
      Self { _data_dir, socket_dir, server, bin, ld_library_path, _slot }
  }
  ```

  The existing `impl Drop for PgFixture` already `kill()`s + `wait()`s the server
  before any field drops, so the cluster's semaphores are freed **before** the
  `_slot` field releases the lock — the next waiter only proceeds once this
  cluster is truly gone.

### Why a host-stable slot dir (not `$TMPDIR`)

The default dir is the **fixed literal `/tmp/loom-pg-fixture-slots`**, not
`std::env::temp_dir()`. buck2's local executor can hand each test action a
per-action `TMPDIR`; if the slot dir keyed off `$TMPDIR`, every process would get
a *different* dir and the cross-target axis would not be throttled at all. A fixed
shared path under `/tmp` (which buck2 does not mount-isolate) guarantees all test
processes on the host share the same `K` slots. To make this explicit and
override-safe, **`loom_fixture_test` sets `LOOM_PG_FIXTURE_SLOT_DIR` in the
fixture env** to that same path; the code default matches so a direct `cargo test`
behaves identically. (Cross-run collision on a shared host merely shares the slot
budget — a throttle, never a correctness bug.)

`K` defaults to **8** (≤ 8 live clusters keeps total semaphore sets far under
`SEMMNI`=128), overridable via `LOOM_PG_FIXTURE_SLOTS` for a big CI box. The macro
does **not** pin `K`, so ambient env wins.

## What this does NOT change

- **Single-test behavior** — one fixture acquires slot 0 immediately; no
  observable change, no added latency.
- **The `loom_fixture_test` local-only routing** — unchanged; this is orthogonal
  to RE-vs-local placement.
- **`DuckLakeWriter` / `IcebergWriter`** — they connect to an already-booted
  fixture cluster (and spawn transient `duckdb` CLIs that don't reserve postgres
  semaphores); they boot no cluster, so they are **not** gated. Only
  `PgFixture::start` acquires a slot.
- **No new dependency, no `Cargo.lock` / `third-party/BUCK` change, no prelude
  edit, no new build rule.** (A buck2 local-resource pool was considered and
  rejected: `rust_test`'s `ExternalRunnerTestInfo` exposes no
  `local_resources`/`required_local_resources`, so that path would require forking
  the vendored prelude — clobbered on every submodule bump — and would still miss
  the in-process axis.)

## Testing

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`).

- **Throttle bound** (`postgres` `tests/boot_throttle.rs`, a **plain `rust_test`**
  — pure `flock` on temp files, no postgres/duckdb, so RE-eligible, *not*
  `loom_fixture_test`): construct `BootThrottle::new(tempdir_path, 2)`; spawn 6
  threads, each `acquire()`ing a slot, then — while holding it — `fetch_add` a
  shared `AtomicUsize` live-counter, update an `AtomicUsize` observed-max with a
  `fetch_max`, sleep ~50 ms, `fetch_sub`, and drop the guard. Join all and assert:
  - observed max concurrent holders **≤ 2** (the bound holds), and
  - all 6 threads completed (slots are **reused** — 6 acquisitions through 2
    slots, proving release-on-drop frees the slot).

  This exercises the exact `flock`-contention path (distinct fds in one process
  contend identically to the cross-process case) deterministically and fast,
  without standing up a cluster.

- **Existing fixture tests stay green unchanged** — they now each take a slot, but
  a single test or a small batch acquires immediately.

- **Integration validation (manual / CI, not a unit test):** the work agent runs
  the **full** `buck2 test //src/...` and confirms the `initdb failed` storm is
  gone (was `Fail 60`; expect `Fail 0` for the fixture targets), since the
  environmental failure only manifests under the whole-suite boot. Record the
  before/after `Tests finished:` line in the PR.

## Out of scope

- **A buck2-scheduler resource pool** — needs a vendored-prelude fork (see above);
  explicitly not done. If `rust_test` ever gains a `local_resources` attribute
  upstream, revisit (mint a `fut-` item then, not now).
- **`RUST_TEST_THREADS=1`** as the mechanism — rejected: it caps only the
  in-process axis and serializes every fixture target internally; the slot
  semaphore caps both axes with more parallelism.
- **Tuning `K` per-host automatically** (e.g. from `SEMMNI`/core count) — the
  fixed default + env override is sufficient; no autodetection.
- **Non-Linux hosts** — fixtures already require Linux postgres/duckdb; the `/tmp`
  default and `flock` are fine there.

## Files

- Modify: `src/control-plane/postgres/src/fixture.rs` — add `BootThrottle` +
  `SlotGuard` + the `boot_throttle()` `OnceLock`; add the `_slot: SlotGuard` field
  (declared last) to `PgFixture`; acquire it first in `start()`.
- Modify: `src/control-plane/postgres/defs.bzl` — set
  `LOOM_PG_FIXTURE_SLOT_DIR = "/tmp/loom-pg-fixture-slots"` in `loom_fixture_test`'s
  `fixture_env` (shared, override-safe).
- Create: `src/control-plane/postgres/tests/boot_throttle.rs` + its plain
  `rust_test` target in `src/control-plane/postgres/BUCK` (deps: the postgres lib +
  `//third-party:tempfile`; RE-eligible, *not* `loom_fixture_test`).
- Modify: `docs/ISSUES.md` — close `iss-fixture-boot-contention`
  (`[x] status:fixed pr:#<n>`); it already points at this design.
- No `Cargo.toml`, `Cargo.lock`, `third-party/BUCK`, prelude, or core change.
