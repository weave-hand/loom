# road-test-shared-pg-fixture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One hermetic Postgres cluster per test **process** instead of one per
test function — `PgFixture::shared()` — cutting ~344 `initdb`+`postgres` boots
per full sweep (117 files) to ~117 and retiring the boot-slot starvation flake
class.

**Architecture:** A `OnceLock<PgFixture>` static in `fixture.rs`. The lifecycle
problem: a static never runs `Drop`, so without countermeasures the postgres
child outlives the test binary (orphaned per target) and its tempdirs leak.
Two mechanisms close it:

1. **PDEATHSIG, `shared()`-only, from a dedicated thread.** `PR_SET_PDEATHSIG`
   fires when the **spawning thread** dies — not the process (prctl(2)
   footgun) — and tokio worker threads die per-test, so the shared fixture is
   booted on a dedicated `pg-fixture-shared` thread that parks forever (dies
   only at process death, including SIGKILL from a buck2 timeout). The public
   `start()` is untouched: per-test fixtures are covered by `Drop`, and giving
   them PDEATHSIG from short-lived tokio threads would kill live clusters.
2. **An `atexit` reaper for normal exit.** libtest terminates via
   `process::exit`, which skips static drops but runs atexit handlers: the
   handler SIGKILLs + `waitpid`s the shared server and best-effort removes its
   data/socket dirs.

Isolation stays database-level via the existing `fresh_db()` (unique
`loom_test_<pid>_<n>` names). Accepted shared-cluster semantics (documented on
`shared()`): PostgreSQL advisory locks are cluster-wide, so same-key advisory
locks in concurrently-running tests of one binary serialize across databases —
all loom uses are `pg_advisory_xact_lock` (transaction-scoped), so timing only,
never correctness; `pg_notify` channels are per-database, so queue tests stay
isolated. Two capacity knobs change: **`max_connections=200`** on all fixture
clusters (one cluster now serves a whole binary's concurrent tests — e.g. 8
libtest threads × ~15 pooled conns exceeds PG's default 100; total live
clusters stay throttle-bounded at 8, so SysV-semaphore headroom is a 2×
per-cluster increase, not a cluster-count increase), and the boot-slot
**acquire deadline 120s→300s** (a slot is now held for a whole binary's
lifetime, so ninth-and-later concurrent binaries legitimately wait longer than
today; deadline stays a backstop, not a scheduler).

**Tech Stack:** Rust (`std::process`, `std::thread`, `libc`), buck2, reindeer.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md` (`road-test-shared-pg-fixture`).
- `clippy::undocumented_unsafe_blocks` and `clippy::multiple_unsafe_ops_per_block` are **enforced** (not in `CLIPPY_ALLOWS`): every `unsafe` block contains ONE unsafe operation and carries its own `// SAFETY:` comment directly above; an unsafe fn called inside a closure needs an unsafe block **inside** that closure (E0133 — the outer block doesn't reach it).
- `fixture.rs`'s module allow-block covers only the panic-family lints (`expect` is fine there); this change introduces the tree's first first-party `unsafe` — keep it minimal and contained to fixture.rs.
- After the sweep: `buck2 test //src/... -j 8` must pass; also run once WITHOUT `-j 8` and record both wall-clocks + any slot-deadline behavior in the PR body (the bare run is the real test of the "obsoletes -j 8" claim).
- `buck2 run //tools:prek -- run --all-files` and commit what it fixes BEFORE every push (rustfmt bit PR #296).
- Conventional-commit messages.

---

### Task 1: Add the `libc` dependency to the postgres crate (reindeer flow)

**Files:**
- Modify: `src/control-plane/postgres/Cargo.toml` (add `libc = "0.2"` to `[dependencies]` with comment: `# fixture.rs: shared-cluster lifecycle (PDEATHSIG + atexit reaper)`)
- Regenerate: `Cargo.lock`, `third-party/BUCK` (via buckify — libc 0.2.186 is already in the graph transitively with its buildscript configured; going direct just emits the public alias)
- Modify: `src/control-plane/postgres/BUCK` — add `"//third-party:libc"` to the `:postgres` library `deps`

**Interfaces:**
- Produces: `//third-party:libc` public alias (Tasks 2-3 use `libc::{prctl, atexit, kill, waitpid, PR_SET_PDEATHSIG, SIGKILL, c_int}`).

- [ ] **Step 1: Add the dep and regenerate**

```bash
eval "$(./tools/env.sh)"
# edit src/control-plane/postgres/Cargo.toml as above
cargo generate-lockfile
./tools/buckify.sh
```

- [ ] **Step 2: Guard against silent downgrades (CLAUDE.md rule)**

Run: `git diff origin/main -- Cargo.lock | grep -E '^[+-](name|version)' | grep -B1 -E 'zstd-sys|ring'`
Expected: empty (no native/`links` crate moved).

- [ ] **Step 3: Build check**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b1.log 2>&1; tail -3 /tmp/b1.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/Cargo.toml src/control-plane/postgres/BUCK Cargo.lock third-party/BUCK
git commit -m "build(postgres): add libc dep for fixture process-lifecycle control"
```

### Task 2: Capacity knobs — max_connections + slot deadline

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs:202-221` (server spawn args), `:111-115` (the `BootThrottle::acquire` deadline)

**Interfaces:**
- No API change. All fixture clusters (shared and per-test) boot with `max_connections=200`; slot waiters panic at 300s instead of 120s.

- [ ] **Step 1: Add the spawn arg**

In `start()`'s server command, after the `dynamic_shared_memory_type=mmap` arg:

```rust
            // One cluster now serves a whole test binary's concurrently-running
            // tests (PgFixture::shared): N libtest threads × ~15 pooled conns
            // exceeds the default 100. Live clusters stay throttle-bounded, so
            // the SysV-semaphore cost is a bounded per-cluster 2x, not a
            // cluster-count increase.
            .args(["-c", "max_connections=200"])
```

- [ ] **Step 2: Raise the acquire deadline**

In `BootThrottle::acquire`, change the 120s constant to 300s and extend its
comment: a slot is held for a whole test binary's lifetime under
`PgFixture::shared()`, so ninth-and-later concurrent binaries legitimately wait
for a binary to finish, not just for a boot; 300s stays a backstop against a
wedged holder.

- [ ] **Step 3: Run a representative fixture test + commit**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS.

```bash
git add src/control-plane/postgres/src/fixture.rs
git commit -m "test(postgres): fixture capacity for shared clusters (max_connections=200, 300s slot deadline)"
```

### Task 3: `PgFixture::shared()` — dedicated boot thread, PDEATHSIG, atexit reaper

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs` (a `start_with` private seam; `shared()`; reaper fn; statics)
- Test: new `src/control-plane/postgres/tests/fixture_shared.rs` + `loom_fixture_test` target `fixture-shared` in `src/control-plane/postgres/BUCK` (deps: `":postgres"`, `"//third-party:sqlx"`, `"//third-party:tokio"`)

**Interfaces:**
- Produces: `pub fn shared() -> &'static PgFixture` — the default for tests; `start()` unchanged, kept for cluster-isolation needs.

- [ ] **Step 1: Write the failing test**

`tests/fixture_shared.rs`:

```rust
//! PgFixture::shared(): one cluster per test process, database-level isolation.
//! loom_fixture_test (Postgres).

use control_plane_postgres::fixture::PgFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_returns_one_cluster_with_isolated_databases() {
    let a = PgFixture::shared();
    let b = PgFixture::shared();
    assert!(
        std::ptr::eq(a, b),
        "shared() must return the same fixture instance"
    );

    let (_cp1, db1) = a.fresh_db().await;
    let (_cp2, db2) = b.fresh_db().await;
    assert_ne!(db1, db2, "fresh_db stays per-call isolated on the shared cluster");

    let pool = a.pool_for(&db1).await;
    let one: i64 = sqlx::query_scalar("select 1")
        .fetch_one(&pool)
        .await
        .expect("query on shared cluster");
    assert_eq!(one, 1);
}

/// Regression guard for the PDEATHSIG thread-semantics footgun: the cluster
/// must survive the death of earlier tests' tokio worker threads. (prctl's
/// PDEATHSIG fires on SPAWNING-THREAD death — which is why shared() boots from
/// a dedicated parked thread, and why this second, later-scheduled test
/// exists.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_survives_other_tests_runtimes() {
    // Force sequencing after the sibling test has likely completed at least
    // once: do our own full round-trip regardless of libtest scheduling.
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let one: i64 = sqlx::query_scalar("select 1")
        .fetch_one(&pool)
        .await
        .expect("shared cluster still alive");
    assert_eq!(one, 1);
    assert!(db.starts_with("loom_test_"));
}
```

BUCK target (mirror an adjacent `loom_fixture_test`):

```python
loom_fixture_test(
    name = "fixture-shared",
    crate = "fixture_shared",
    srcs = ["tests/fixture_shared.rs"],
    crate_root = "tests/fixture_shared.rs",
    deps = [
        ":postgres",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Verify it fails to build** (no `shared()` yet)

Run: `buck2 test //src/control-plane/postgres:fixture-shared > /tmp/t2.log 2>&1; grep -E "cannot find|not found|Build failure" /tmp/t2.log`
Expected: build error — no `shared` on `PgFixture`.

- [ ] **Step 3: Implement**

(a) Split the spawn seam. Extract the body of `start()` into
`fn start_with(pdeathsig: bool) -> Self`; `pub fn start()` becomes
`Self::start_with(false)`. When `pdeathsig` is true, add the `pre_exec` hook to
the **server** command only (initdb/pg_isready run to completion):

```rust
        if pdeathsig {
            use std::os::unix::process::CommandExt;
            // SAFETY: pre_exec runs in the forked child before exec; the
            // closure below performs no allocation and takes no locks.
            unsafe {
                server_cmd.pre_exec(|| {
                    // SAFETY: prctl(PR_SET_PDEATHSIG) is async-signal-safe;
                    // arms the kernel to SIGKILL this child when the thread
                    // that spawned it dies (which, for shared(), is the
                    // process-lifetime pg-fixture-shared thread).
                    let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) };
                    if rc == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
```

(restructure the existing chained builder into a `let mut server_cmd = …;`
statement form so the conditional hook can be applied, preserving every
existing arg including the new `max_connections`).

(b) Statics + reaper at module scope (near `THROTTLE`; add `AtomicI32` to the
existing `std::sync::atomic` import — only `AtomicU64` is imported today):

```rust
static SHARED: OnceLock<PgFixture> = OnceLock::new();
static SHARED_PID: AtomicI32 = AtomicI32::new(0);
static SHARED_DIRS: OnceLock<[PathBuf; 2]> = OnceLock::new();

/// Reap the shared cluster on NORMAL process exit: libtest terminates via
/// `process::exit`, which skips static destructors but runs atexit handlers.
/// Abnormal exits (SIGKILL, e.g. a buck2 test timeout) are covered by the
/// server's PDEATHSIG instead.
extern "C" fn reap_shared_cluster() {
    let pid = SHARED_PID.load(Ordering::SeqCst);
    if pid > 0 {
        // SAFETY: pid is our own child's, recorded at spawn; signalling one's
        // own child is sound, and SIGKILL cannot be mis-handled by the target.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let mut status: libc::c_int = 0;
        // SAFETY: waitpid on our own child reaps the zombie created above;
        // the status pointer is a valid local.
        unsafe {
            libc::waitpid(pid, &raw mut status, 0);
        }
    }
    if let Some(dirs) = SHARED_DIRS.get() {
        for d in dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}
```

(c) `shared()` on `impl PgFixture`:

```rust
    /// The process-wide shared cluster. Prefer this over `start()`: isolation
    /// is database-level via [`fresh_db`](Self::fresh_db), and one cluster per
    /// test process cuts whole-suite boots ~3x while holding a single
    /// boot-throttle slot (for the process lifetime — strictly fewer live
    /// clusters than per-test fixtures). Use `start()` only for cluster-level
    /// isolation.
    ///
    /// Lifecycle: never dropped. Reaped on normal exit by an atexit handler
    /// (libtest exits via `process::exit`, skipping static drops) and on
    /// abnormal exit by PDEATHSIG — which fires on SPAWNING-THREAD death
    /// (prctl(2)), so the boot happens on a dedicated thread parked for the
    /// process lifetime, never a per-test tokio worker.
    ///
    /// Shared-cluster semantics: advisory locks are cluster-wide (all loom
    /// uses are transaction-scoped, so cross-test contention affects timing
    /// only); `pg_notify` channels are per-database and stay isolated.
    pub fn shared() -> &'static PgFixture {
        SHARED.get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::sync_channel(0);
            std::thread::Builder::new()
                .name("pg-fixture-shared".into())
                .spawn(move || {
                    let fx = PgFixture::start_with(true);
                    tx.send(fx).expect("deliver shared fixture");
                    // Keep this thread alive for the process lifetime: it is
                    // the PDEATHSIG anchor. park() can wake spuriously — loop.
                    loop {
                        std::thread::park();
                    }
                })
                .expect("spawn pg-fixture-shared thread");
            let fx = rx.recv().expect("receive shared fixture");
            SHARED_PID.store(
                i32::try_from(fx.server.id()).expect("pid fits i32"),
                Ordering::SeqCst,
            );
            let _ = SHARED_DIRS.set([
                fx._data_dir.path().to_path_buf(),
                fx.socket_dir.path().to_path_buf(),
            ]);
            // SAFETY: registers an extern "C" fn; the handler only reads
            // statics initialized above and syscalls on our own child.
            unsafe {
                libc::atexit(reap_shared_cluster);
            }
            fx
        })
    }
```

(`&raw mut status` needs no feature on the pinned nightly; if the toolchain
rejects it, use `std::ptr::addr_of_mut!(status)`.)

- [ ] **Step 4: Run the new test**

Run: `buck2 test //src/control-plane/postgres:fixture-shared > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS (2 tests).

- [ ] **Step 5: Verify no orphan postgres after the run**

Run: `sleep 2; pgrep -a postgres | grep -c 'pg_dynshmem\|-D /tmp' || echo clean`
Expected: `clean` (no fixture server outliving its binary; ignore any user-level postgres).

- [ ] **Step 6: prek, then commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/p0.log 2>&1; grep -c Failed /tmp/p0.log  # expect 0
git add src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/tests/fixture_shared.rs src/control-plane/postgres/BUCK
git commit -m "test(postgres): PgFixture::shared — one cluster per test process

Booted from a dedicated parked thread (PDEATHSIG fires on spawning-THREAD
death, so the anchor thread must live for the process); reaped by an
atexit handler on normal exit (libtest exits via process::exit, which
skips static drops but runs atexit): SIGKILL + waitpid + dir cleanup."
```

### Task 4: Sweep `start()` → `shared()` across the tree

**Files:**
- Modify: ALL files calling `PgFixture::start()` — 344 call sites across 117 files (`control-plane/postgres` tests, `services/{query-api,engine,engine-serving,worker,transform,ingest,standalone}` tests, `query-api/tests/e2e_support.rs`). No exceptions: `tests/boot_throttle.rs` never boots a cluster (pure flock test), and no test in the tree kills/restarts the server, asserts process counts, or needs cluster-level isolation (plan-review verified). The sed pattern must not touch `MinioFixture::start()`.

**Interfaces:**
- Consumes: `PgFixture::shared()` (Task 3). `let fx = PgFixture::shared();` yields `&'static PgFixture`; every helper takes `&PgFixture` (none store by value — verified), so `&fx` borrows compile via `&&T → &T` deref coercion and method calls auto-deref.

- [ ] **Step 1: Mechanical sweep**

```bash
grep -rl 'PgFixture::start()' src --include='*.rs' \
  | xargs sed -i 's/PgFixture::start()/PgFixture::shared()/g'
git diff --stat | tail -3   # expect 117 files changed
grep -rn 'PgFixture::start()' src --include='*.rs' | wc -l  # expect 0
```

- [ ] **Step 2: Spot-compile the heaviest consumers**

Run: `buck2 build //src/control-plane/postgres: //src/services/query-api: > /tmp/b2.log 2>&1; tail -3 /tmp/b2.log`
Expected: BUILD SUCCEEDED. If a call site fails on `&&PgFixture`, bind through
a deref (`let fx: &PgFixture = PgFixture::shared();`) — do NOT revert to
`start()`.

- [ ] **Step 3: Full suite, both modes**

Run: `buck2 test //src/... -j 8 > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: all pass; record wall-clock.
Then: `buck2 clean 2>/dev/null; buck2 test //src/... > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|deadline" /tmp/t5.log`
Expected: all pass without `-j 8` (the claim this item exists to prove); record
wall-clock + any slot-deadline messages for the PR body. If the bare run
flakes on slot waits, keep `-j 8` as the documented mode and say so in the PR.

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log  # expect 0; commit anything it fixed
git add -A
git commit -m "test: sweep PgFixture::start() -> shared() (one cluster per test process)"
```

### Task 5: Close the register item

- [ ] **Step 1:** In `docs/ROADMAP.md`, flip `road-test-shared-pg-fixture` to `- [x]` / `status:done` / `pr:#N`, prepend a "Done (PR #N): …" resolution (retain the advisory-lock note; correct the entry's `boot_throttle.rs`/`s3_storage.rs` carve-out claim — neither needed an exception). Run `bash tools/docs.sh validate`.
- [ ] **Step 2:** Commit `docs(registers): close road-test-shared-pg-fixture (pr #N)`.
