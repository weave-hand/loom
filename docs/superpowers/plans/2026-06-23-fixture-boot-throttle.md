# Hermetic-Postgres fixture boot throttle — Implementation Plan

> **For agentic workers:** implement task-by-task under TDD (test first, watch it
> fail, make it pass). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the number of hermetic-Postgres fixture clusters alive at once
across a whole `buck2 test //src/...` run to a small, configurable `K`, so the
suite stops exhausting kernel SysV-semaphore resources (the `initdb failed`
storm, `iss-fixture-boot-contention`) — with no build-rule, dependency, or
single-test behavior change.

**Architecture:** A process-global file-lock slot semaphore in `fixture.rs`. A
`BootThrottle` of `K` marker files under a host-stable shared dir
(`/tmp/loom-pg-fixture-slots`); `acquire()` takes an exclusive advisory `flock`
on the first free marker (Rust-stable `File::try_lock`/`File::unlock`, no
third-party crate) and returns a `SlotGuard` that holds it. `PgFixture` gains a
`_slot: SlotGuard` field (declared last → dropped last) acquired as the first
line of `start()`, so at most `K` clusters live simultaneously across all test
processes and threads. The lock is tied to the open fd and auto-released on fd
close / process death, so a panicking test never leaks a slot.

**Tech Stack:** Rust (nightly `2026-03-28`, stable `File::try_lock`), buck2,
hermetic Postgres fixture, `loom_fixture_test` + plain `rust_test` integration
targets.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-22-fixture-boot-throttle-design.md`.
  Closes `iss-fixture-boot-contention`.
- **Tests are integration targets only** — `rust_test` / `loom_fixture_test` in
  `BUCK`, never inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails on
  any `#[test]`/`#[tokio::test]` in `src/**.rs`).
- **The throttle test is a PLAIN `rust_test`, NOT `loom_fixture_test`** — it is
  pure `flock` on temp files (no postgres/duckdb), so it is RE-eligible. Routing
  it through `loom_fixture_test` would only pin it local for no reason.
- **No `Cargo.toml`, `Cargo.lock`, `third-party/BUCK`, prelude, or core change.**
  The std file-locking API (`File::try_lock`, `File::unlock`,
  `TryLockError::{WouldBlock, Error}`) is confirmed present on the pinned
  toolchain — no new dependency, so the duckdb-downgrade footgun is not in play.
- **Never pipe `buck2 test` through `tail`/`head`** — redirect to a file and
  grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **`DuckLakeWriter` / `IcebergWriter` are NOT gated** — they connect to an
  already-booted cluster and spawn transient `duckdb` CLIs (no postgres
  semaphores). Only `PgFixture::start` acquires a slot.
- Commit messages end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
  Conventional Commits enforced by the `conventional-commit` hook.

## File Structure

- **Modify `src/control-plane/postgres/src/fixture.rs`** — add the `BootThrottle`
  + `SlotGuard` primitive and the `boot_throttle()` `OnceLock`; add the
  `_slot: SlotGuard` field (declared **last**) to `PgFixture`; acquire it as the
  **first** line of `start()`.
- **Create `src/control-plane/postgres/tests/boot_throttle.rs`** — a plain
  `rust_test` exercising the `flock`-contention path deterministically (no
  cluster).
- **Modify `src/control-plane/postgres/BUCK`** — add the `boot-throttle` plain
  `rust_test` target (deps: `:postgres`, `//third-party:tempfile`).
- **Modify `src/control-plane/postgres/defs.bzl`** — set
  `LOOM_PG_FIXTURE_SLOT_DIR = "/tmp/loom-pg-fixture-slots"` in
  `loom_fixture_test`'s `fixture_env` (shared, override-safe).
- **Modify `docs/ISSUES.md`** — close `iss-fixture-boot-contention`
  (`[x] status:fixed pr:#<N>`).

---

## Task 1: The `BootThrottle` / `SlotGuard` primitive + its test (TDD)

**Files:**
- Create: `src/control-plane/postgres/tests/boot_throttle.rs`
- Modify: `src/control-plane/postgres/BUCK` (add the `boot-throttle` target)
- Modify: `src/control-plane/postgres/src/fixture.rs` (add the primitive)

**Interfaces:**
- Produces: `pub struct BootThrottle { dir: PathBuf, slots: usize }` with
  `pub fn new(dir: PathBuf, slots: usize) -> Self` and
  `pub fn acquire(&self) -> SlotGuard`; `pub struct SlotGuard { _file: File }`.
  All `pub` test-support, reachable as
  `control_plane_postgres::fixture::{BootThrottle, SlotGuard}`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/boot_throttle.rs` (a plain `rust_test`,
mirroring the spec's "Throttle bound" test): construct
`BootThrottle::new(tempdir_path, 2)`, spawn 6 threads each `acquire()`ing a slot
then — while holding it — bumping a shared live-counter, recording an observed
max via `fetch_max`, sleeping ~50 ms, decrementing, and dropping the guard.
Assert observed max concurrent holders ≤ 2 (bound holds) and all 6 threads
completed (slots are reused — release-on-drop frees them).

```rust
//! Bound check for the `BootThrottle` file-lock slot semaphore (no cluster).
//! Plain `rust_test` — pure `flock` on temp files, RE-eligible.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use control_plane_postgres::fixture::BootThrottle;

#[test]
fn throttle_bounds_concurrent_holders_and_reuses_slots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let throttle = Arc::new(BootThrottle::new(dir.path().to_path_buf(), 2));

    let live = Arc::new(AtomicUsize::new(0));
    let observed_max = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..6 {
        let throttle = Arc::clone(&throttle);
        let live = Arc::clone(&live);
        let observed_max = Arc::clone(&observed_max);
        let completed = Arc::clone(&completed);
        handles.push(thread::spawn(move || {
            let guard = throttle.acquire();
            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
            observed_max.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(50));
            live.fetch_sub(1, Ordering::SeqCst);
            completed.fetch_add(1, Ordering::SeqCst);
            drop(guard);
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }

    assert!(
        observed_max.load(Ordering::SeqCst) <= 2,
        "throttle exceeded its bound: observed max {} > 2",
        observed_max.load(Ordering::SeqCst)
    );
    assert_eq!(
        completed.load(Ordering::SeqCst),
        6,
        "not all threads completed — slots were not reused on drop"
    );
}
```

Add the BUCK target to `src/control-plane/postgres/BUCK` (place it next to the
other plain `rust_test`s, e.g. after `iceberg-inline-types` at ≈L206):

```python
rust_test(
    name = "boot-throttle",
    crate = "boot_throttle",
    srcs = ["tests/boot_throttle.rs"],
    crate_root = "tests/boot_throttle.rs",
    edition = "2024",
    deps = [":postgres", "//third-party:tempfile"],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:boot-throttle > /tmp/t.log 2>&1; grep -E "error\[|cannot find|unresolved|Tests finished|FAIL" /tmp/t.log`
Expected: compile error — `unresolved import control_plane_postgres::fixture::BootThrottle` (the primitive does not exist yet).

- [ ] **Step 3: Implement `BootThrottle` + `SlotGuard` in `fixture.rs`**

In `src/control-plane/postgres/src/fixture.rs`, add the imports needed
(`std::fs::{File, OpenOptions, TryLockError}`, `std::time::{Duration, Instant}`
— `Duration` and `PathBuf` are already imported; add what's missing) and, after
the `DB_COUNTER` static (≈L38), add the primitive verbatim from the spec:

```rust
/// Bounds the number of concurrently-alive fixture Postgres clusters across all
/// test processes/threads. `slots` marker files live under `dir`; an acquired
/// slot is an exclusive `flock` held for the cluster's lifetime. Test-support.
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
    /// Create a throttle of `slots` (min 1) marker files under `dir`.
    pub fn new(dir: PathBuf, slots: usize) -> Self {
        std::fs::create_dir_all(&dir).expect("create fixture slot dir");
        Self {
            dir,
            slots: slots.max(1),
        }
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
                panic!(
                    "could not acquire a pg fixture boot slot within 120s \
                     (LOOM_PG_FIXTURE_SLOTS too low, or slots leaked?)"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}
```

NOTE: `File::try_lock` returns `Result<(), TryLockError>` on this toolchain
(confirmed: `Ok(())` = acquired, `Err(TryLockError::WouldBlock)` = held
elsewhere, `Err(TryLockError::Error(_))` = real error). Each `acquire` attempt
opens its **own** fd, so two threads of the same process contend correctly.
Marker files are never deleted (empty, reused across runs).

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/postgres:boot-throttle > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (`Tests finished: Pass 1. Fail 0.`).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/tests/boot_throttle.rs src/control-plane/postgres/BUCK
git commit -m "feat(postgres-fixture): add BootThrottle file-lock slot semaphore

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Wire the throttle into `PgFixture` + the shared slot dir

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs` (`boot_throttle()` OnceLock;
  `_slot` field; acquire in `start()`)
- Modify: `src/control-plane/postgres/defs.bzl` (`LOOM_PG_FIXTURE_SLOT_DIR`)

**Interfaces:**
- Consumes: `BootThrottle`/`SlotGuard` (Task 1).
- Produces: every `PgFixture::start()` acquires a slot before `initdb` and holds
  it for the cluster's lifetime; the slot dir is shared across test processes.

- [ ] **Step 1: Add the process-global throttle accessor**

In `fixture.rs`, add `use std::sync::OnceLock;` to the imports, and add the
accessor near the `DB_COUNTER`/`BootThrottle` definitions:

```rust
static THROTTLE: OnceLock<BootThrottle> = OnceLock::new();

/// The process-global fixture-boot throttle. `K` slots come from
/// `LOOM_PG_FIXTURE_SLOTS` (default 8); the slot dir from
/// `LOOM_PG_FIXTURE_SLOT_DIR` (default the fixed `/tmp/loom-pg-fixture-slots`, so
/// all test processes on the host share the same `K` slots — see the design).
fn boot_throttle() -> &'static BootThrottle {
    THROTTLE.get_or_init(|| {
        let slots = std::env::var("LOOM_PG_FIXTURE_SLOTS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(8);
        let dir = std::env::var_os("LOOM_PG_FIXTURE_SLOT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/loom-pg-fixture-slots"));
        BootThrottle::new(dir, slots)
    })
}
```

- [ ] **Step 2: Add the `_slot` field to `PgFixture` (declared last)**

In the `pub struct PgFixture { … }` (≈L41-48), add `_slot` as the **last**
field so it drops after the server is killed:

```rust
pub struct PgFixture {
    // Dropped last; keeps the data/socket dirs alive while the server runs.
    _data_dir: TempDir,
    socket_dir: TempDir,
    server: Child,
    bin: PathBuf,
    ld_library_path: String,
    // Declared last so it drops AFTER the server is killed/reaped (Drop runs
    // fields in declaration order): the cluster's semaphores are freed before the
    // slot lock is released, so the next waiter only proceeds once this cluster is
    // truly gone.
    _slot: SlotGuard,
}
```

- [ ] **Step 3: Acquire the slot first in `start()` and populate the field**

In `PgFixture::start()` (≈L65), make the slot acquisition the **first** line,
and add `_slot` to the `Self { … }` constructor (≈L102-108):

```rust
    pub fn start() -> Self {
        let _slot = boot_throttle().acquire(); // gate before initdb (bounds live clusters)
        let bin = PathBuf::from(
            std::env::var("POSTGRES_BIN_DIR")
                .expect("POSTGRES_BIN_DIR must point at the postgres bin/ directory"),
        );
        // ... unchanged initdb / postgres spawn ...

        let fixture = Self {
            _data_dir: data_dir,
            socket_dir,
            server,
            bin,
            ld_library_path,
            _slot,
        };
        fixture.wait_ready();
        fixture
    }
```

Leave `impl Drop for PgFixture` unchanged — it already `kill()`s + `wait()`s the
server before any field drops, so the semaphores are freed before `_slot`
releases its lock.

- [ ] **Step 4: Set the shared slot dir in `loom_fixture_test`**

In `src/control-plane/postgres/defs.bzl`, add to the `fixture_env` dict (after
the `LOOM_MIGRATIONS_DIR` line, ≈L26):

```python
        # Host-stable shared dir so the boot throttle's K slots are shared across
        # ALL fixture-test processes (buck2 may hand each action a per-action
        # TMPDIR; keying off that would un-throttle the cross-target axis). The
        # fixture code defaults to this same literal, so a direct cargo test
        # behaves identically. Override-safe: `env` (and ambient env) win.
        "LOOM_PG_FIXTURE_SLOT_DIR": "/tmp/loom-pg-fixture-slots",
```

(It is in `fixture_env` before `fixture_env.update(env)`, so a per-target `env`
override still wins.)

- [ ] **Step 5: Build the library + run a couple of fixture tests to confirm no regression**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "error|BUILD SUCCEEDED|Build ID" /tmp/b.log; echo built`
Then a small fixture batch:
`buck2 test //src/control-plane/postgres:lineage-roundtrip //src/control-plane/postgres:queue > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: builds clean; both targets PASS (a small batch acquires slots immediately).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/defs.bzl
git commit -m "feat(postgres-fixture): gate PgFixture boot on the slot throttle

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Whole-suite validation (the actual fix)

**Files:** none (validation only).

**Interfaces:** consumes the wired throttle (Tasks 1-2). The environmental
`initdb failed` storm only manifests under the whole-suite boot, so this is where
the fix is proven.

- [ ] **Step 1: Run the full `buck2 test //src/...` sweep and record the result**

Run: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL|initdb failed" /tmp/full.log | tail -40`
Expected: zero `loom_fixture_test` failures from `initdb failed` (was `Fail 60`
on the unthrottled suite; expect `Fail 0` for the fixture targets). Capture the
`Tests finished:` line for the PR body.

- [ ] **Step 2: If any fixture target still fails on `initdb failed`**

Lower the default — but first confirm it is genuinely the storm (stderr shows
`initdb` reaching `running bootstrap script …` then panicking at `fixture.rs`).
The default `K`=8 keeps semaphore sets far under `SEMMNI`=128; if the host is
unusually constrained, re-run with `LOOM_PG_FIXTURE_SLOTS=4` via the env to
confirm the throttle is the lever, and note it in the PR (do NOT lower the code
default below 8 without evidence). Re-run the full sweep until the storm is gone.

---

## Task 4: Close the issue

**Files:**
- Modify: `docs/ISSUES.md` (close `iss-fixture-boot-contention`)

**Interfaces:** consumes nothing (documentation only). This is the register half
of loom-work-checkout step 4; `loom-docs-update` may also run at finish — keep
consistent.

- [ ] **Step 1: Close `iss-fixture-boot-contention` in `docs/ISSUES.md`**

Change the entry's checkbox to `[x]`, set `status:fixed`, add `pr:#<N>` (filled
at finish), and update the prose to describe the shipped throttle. Mirror the
file's existing closed-item style (see the `iss-quote-ident-panic` /
`iss-multi-file-limit-misread` entries).

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate > /tmp/v.log 2>&1; cat /tmp/v.log`
Expected: no errors (ids unique, vocab valid, `[[links]]` resolve, `spec:` slug
resolves on disk).

- [ ] **Step 3: Commit**

```bash
git add docs/ISSUES.md
git commit -m "docs(issues): close iss-fixture-boot-contention

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

(The `pr:#<N>` number is filled at finish, once the PR is opened.)

---

## Final Verification (after all tasks)

- [ ] **Full sweep green** (already run in Task 3; re-confirm after the docs commit):
  `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log`
- [ ] **Clippy on the changed crate** (the prek `clippy` hook gate):
  `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1` then read the
  `[clippy.txt]` output — expect empty (clean). Also lint the new test target.
- [ ] **rustfmt** the changed source:
  `buck2 run //tools:rustfmt -- --check src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/tests/boot_throttle.rs`
  — reformat if it reports diffs.
- [ ] **prek hooks** on the whole tree (markdown EOF/whitespace, etc.):
  `buck2 run //tools:prek -- run --all-files` — commit anything the hooks fix.

---

## Self-Review

**1. Spec coverage:**
- "Bound live clusters to a small configurable `K` across the whole sweep" →
  `BootThrottle` + `_slot` held for the cluster lifetime (Tasks 1-2). ✅
- "`flock` primitive, auto-released on fd close/process death, no third-party
  crate" → stable `File::try_lock`/`File::unlock`, confirmed on the toolchain;
  no `Cargo.*`/`third-party` change (Task 1). ✅
- "Each `acquire` opens its own fd so same-process threads contend" → per-attempt
  `OpenOptions::open` (Task 1) + the multithread bound test (Task 1 Step 1). ✅
- "Process-global `OnceLock`, `K` from `LOOM_PG_FIXTURE_SLOTS` (default 8), dir
  from `LOOM_PG_FIXTURE_SLOT_DIR` (default `/tmp/loom-pg-fixture-slots`)" →
  `boot_throttle()` (Task 2 Step 1). ✅
- "`_slot` declared LAST, acquired FIRST in `start()`; Drop already kills server
  before fields drop" → Task 2 Steps 2-3. ✅
- "Host-stable shared dir, set in `loom_fixture_test` env, code default matches" →
  Task 2 Step 4 + the `boot_throttle()` default. ✅
- "Throttle-bound test: plain `rust_test`, ≤ K concurrent, 6 acquisitions reuse 2
  slots" → Task 1 Step 1 (`throttle_bounds_concurrent_holders_and_reuses_slots`). ✅
- "Existing fixture tests stay green; full-sweep storm gone (Fail 60 → Fail 0),
  record before/after" → Task 2 Step 5 + Task 3. ✅
- "Does NOT change single-test behavior, the local-only routing, the
  writers (not gated), or any build rule/dep" → only `fixture.rs` + `defs.bzl`
  touched; writers untouched; no `Cargo.*`/`third-party`/prelude edit. ✅
- "Close `iss-fixture-boot-contention`" → Task 4. ✅

**2. Placeholder scan:** No "TBD"/"handle edge cases". The `<N>` PR-number in
Task 4 is intentional (filled at finish). The Task 3 Step 2 contingency is a
real, bounded fallback (env override, no code-default change without evidence),
not hidden logic.

**3. Type consistency:** `BootThrottle::new(dir: PathBuf, slots: usize) -> Self`,
`BootThrottle::acquire(&self) -> SlotGuard`, `SlotGuard { _file: File }`,
`boot_throttle() -> &'static BootThrottle` are used with consistent signatures
across Tasks 1-2 and the test. `File::try_lock() -> Result<(), TryLockError>`
matches the confirmed toolchain API. No existing `PgFixture` public method
signature changes — only an added private field — so no call-site ripple.
