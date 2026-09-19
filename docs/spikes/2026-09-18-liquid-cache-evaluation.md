# LiquidCache under the engine serving path: evaluation + implementation

> **Type:** evaluation spike, then implementation. **Date:** 2026-09-18.
> **Subject:** [`datafusion-contrib/liquid-cache`](https://github.com/datafusion-contrib/liquid-cache)
> (canonical repo: `XiangpengHao/liquid-cache`), Apache-2.0,
> "10x lower latency for cloud-native DataFusion".
> **Outcome:** **built, opt-in, off by default.** The integration shape turned out to be
> unusually clean; the cost is a DataFusion 54→55 + arrow 58→59 bump and three new git
> dependencies. It stays off unless an operator asks, for one resource-budgeting question
> and one hard `io_uring` prerequisite — see the two sections after *What was built*, which
> also correct a wrong claim in this document's first draft and the bug it concealed.

## TL;DR

| | |
|---|---|
| **Does it fit loom's architecture?** | Yes — no `TableProvider` changes at all |
| **What did it cost?** | arrow 58→59, DataFusion 54→55, iceberg pin moved, 3 git deps |
| **Is it on?** | **No.** `LOOM_LIQUID_CACHE_DIR` unset ⇒ serving is byte-identical to before |
| **Is ACL enforcement affected?** | No — and it is now asserted, not argued: `//src/services/query-api:liquid-cache-acl-e2e` |
| **What is still open?** | Resource budgeting on the arbitrary-SQL plane; `io_uring` availability wherever this would be turned on |

## What LiquidCache is

A pushdown cache layer for DataFusion. Rather than caching raw Parquet bytes or decoded
Arrow, it transcodes scanned data into a cache-specific columnar format (built on
[Vortex](https://github.com/vortex-data/vortex)), keeps only query-relevant columns
resident in memory, and spills the rest to local SSD via `t4` using direct I/O
(`O_DIRECT` on Linux) to bypass the OS page cache. It supports **selection pushdown**
(row filtering by boolean mask) and **predicate pushdown** (evaluating `col > 12` inside
the cache layer), so a filtered scan never hydrates columns it will discard.

It ships an in-process mode (`LiquidCacheLocalBuilder`) and a separate cache-server mode.
Only the in-process mode is relevant to loom: the engine service already owns the object
store and is the sole serving path, so a second network hop would be pure cost.

## Integration shape — why this was worth doing

The README's examples all use `ListingTable` / `ctx.register_parquet(...)`, which loom
does **not** use: `IcebergMirrorTableProvider` is a hand-written `TableProvider` that
prunes files against mirror stats and builds its own scan. That looks disqualifying, and
it is not.

`LiquidCacheLocalBuilder::build` installs its hook as a **`PhysicalOptimizerRule`**
(`LocalModeOptimizer`), not as a table provider or an object-store wrapper. That rule
walks the finished physical plan, downcasts each node to `DataSourceExec`, pulls out its
`FileScanConfig` + `ParquetSource`, and substitutes a `LiquidParquetSource`. Anything that
is not a `DataSourceExec` over a `ParquetSource` passes through untouched.

loom's `IcebergMirrorTableProvider::scan` (`engine-serving/src/serving.rs`) emits
**exactly** that node:

```rust
let source = Arc::new(ParquetSource::new(self.schema.clone()));
let mut builder = FileScanConfigBuilder::new(store_url, source).with_limit(limit);
// ... one PartitionedFile per surviving Iceberg data file ...
Ok(DataSourceExec::from_data_source(config))
```

So the cache applies to loom's custom provider with **no provider changes at all**, and
loom's existing file pruning composes rather than conflicts: the mirror prunes whole
files, the cache then serves the survivors.

## What was built

- **`engine-serving/src/liquid_cache.rs`** — a process-lifetime cache held as a template
  `SessionState`. This is the load-bearing part: `execute_query_stream` built a
  `SessionContext::new()` *per query*, so a per-context cache would never survive to a
  second query and would never hit. Each query now derives its own state from that
  template — inheriting the config and the optimizer rule that carries the `Arc`'d cache,
  but getting a **fresh catalog registry**, which the next section explains is not
  optional.
- **`serving.rs`** now calls `liquid_cache::new_session()` instead of
  `SessionContext::new()`. With the cache unconfigured this returns a plain
  `SessionContext::new()`, so the uncached path is unchanged.
- **`engine/src/run.rs`** — `LOOM_LIQUID_CACHE_DIR` (default empty ⇒ off) and
  `LOOM_LIQUID_CACHE_MEMORY_BYTES` (default 1 GiB), parsed in the binary and passed in as
  typed config, matching how `GovernedSqlLimits` is threaded. The cache is mounted before
  anything serves, so startup fails naming the directory rather than failing the first query.

### Correction: "the governed path is not cached" was wrong, and it hid a bug

The first draft of this document, of `liquid_cache.rs`'s module doc, and of the PR all
claimed the cache was confined to ungoverned queries, so ACL reasoning did not apply to it.
That framing was wrong, and it was load-bearing — it is why the integration shipped without
an ACL test, and why a real defect went unnoticed. The trigger for re-checking was a single
question: *ACLs have to be enforced, right?*

**What is actually cached.** Only `execute_governed_sql_stream` — the arbitrary-SQL plane
behind query-api's SQL console and `EXPLAIN` validation — builds its own session and stays
uncached. The primary governed read does not: `read_object` resolves the subject's policy,
compiles the row filter and column projection into SQL *text* in query-api, and ships it as
a plain `CommandStatementQuery`; the engine runs that through `execute_query_stream`, which
is exactly the cached context. The chain is
`handler.rs::read_object` → `fetch_rows` → `FlightSqlClient::execute` →
`EngineTicket::Sql` → `flight.rs::do_get_sql` → `execute_query_stream` → `new_session()`.
So ACL-bearing SQL was on the cached path from the first commit.

**The defect that hid behind it.** `SessionState` is `Clone`, but the clone is shallow:
`catalog_list` is an `Arc`. Handing every query `SHARED.clone()` therefore handed every
query the *same* table registry, and `execute_query_stream` registers its tables by name.
The second query to touch a table died on `Execution error: The table orders already
exists`. Every existing test passed anyway, because the cache is off by default and no test
turned it on — the integration was only ever exercised one query deep.

Had registration overwritten instead of erroring, the same sharing would have been quietly
worse: registration binds a snapshot (`execute_query_stream`'s `at`) and a concrete file
list, so one request's binding could have served another's scan, across subjects whose ACL
filters are already baked into the SQL.

**The fix.** `new_session` now derives a fresh `SessionState` per query via
`SessionStateBuilder::new_from_existing(...).with_catalog_list(MemoryCatalogProviderList::new())`,
so each query gets its own registry while inheriting the config, the physical-optimizer
rules (the `LocalModeOptimizer` whose `Arc` *is* the cache) and the UDFs.
`create_default_catalog_and_schema` is re-asserted because `new_from_existing` clears it
when the state being copied already has a default catalog.

**Why cross-subject sharing is nonetheless sound.** LiquidCache keys entries by file and
column and caches *scanned data*, not query results. Two subjects sharing an entry share no
more than they already share by reading the same Parquet file; each query's predicate is
still evaluated per query, and the forced `pushdown_filters = true` moves where that
evaluation happens, not whether it happens. That is now asserted rather than argued —
see `//src/services/query-api:liquid-cache-acl-e2e`.

**Still deliberately uncached:** the arbitrary-SQL governed plane, because
`governed.rs::build_session` pairs a `GreedyMemoryPool` with `DiskManagerMode::Disabled` so
a statement's memory budget binds and cannot be traded for an unbounded-`/tmp` disk DoS,
and LiquidCache's budget and on-disk tier sit outside both. That wants an explicit answer
and an STPA entry before it changes.

### `io_uring` is a hard prerequisite

LiquidCache's `t4` store enables its `io-uring` feature by default and `t4::mount` is
unconditional, so `configure` fails with `ENOSYS` ("Function not implemented", os error 38)
wherever the syscall is blocked. There is no memory-only mode to fall back to, and the
feature cannot be turned off from loom: Cargo feature resolution is a union, so nothing
loom declares can remove a default feature liquid-cache requests. Turning it off means
patching upstream.

Measured, not assumed: the BuildBuddy RE workers `buck2 test` routes fixture runs to from a
root host return `ENOSYS`; this session's own host (ext4, io_uring enabled) mounts fine.
The same block is common in hardened container runtimes, **including the Kubernetes
deployments loom's own Helm chart targets**. This does not affect anything today — the
cache is opt-in and off by default — but it is a hard gate on ever turning it on, and it
belongs in the operator docs before it is.

## Dependency cost (measured, not estimated)

| Dependency | before | after |
|---|---|---|
| `datafusion` | 54.0.0 | **55.1.0** |
| `arrow` / `parquet` | 58.3.0 | **59.3.0** |
| `iceberg` | git pin (arrow 58) | git pin `492234eb` (arrow 59.2) |
| `object_store` | 0.13.2 | 0.13.2 (unchanged) |
| git-sourced repos | 1 (iceberg) | **3** (iceberg, liquid-cache, vortex) |

**Why liquid-cache is a git pin, not a release.** The published `liquid-cache` 0.1.13
(May 2026) depends on **datafusion `^53.1.0` / arrow `^58.1.0`** — *older* than loom. Only
`main` carries the datafusion 55 + arrow 59 migration. Using the release would have meant
downgrading loom's DataFusion; `main` means loom moves forward instead. Exit to a released
version the day liquid-cache publishes against DataFusion 55.

**Why iceberg stays a git pin.** The published `iceberg` 0.10.1 is on **arrow 58**, so it
cannot satisfy the arrow-59 tree. `main` (`492234eb`) is on arrow 59.2. CLAUDE.md's stated
exit condition for that pin ("exit when iceberg publishes the post-0.9.1 release") is
therefore **not yet met in practice** — the release exists but is a major behind on arrow.

**Vortex** contributes 24 crates, all from one repo — reindeer emits one `git_fetch` per
*repo*, not per crate, so the rule count grows by 2, not by 24. `t4` (liquid-cache's
direct-I/O disk store) is Verus-verified, which pulls `vstd` and the `verus_*` crates into
the build graph; they build as ordinary Rust, loom runs no verification.

New reindeer fixups were needed for the build scripts this pulled in: `blake3`, `stacker`,
`psm`, `liblzma-sys` (native-lib shape, like `ring`/`zstd-sys`), and `io-uring`, `radium`,
`vstd`, `verus_prettyplease`, `object` (cfg-only). Note `blake3`/`stacker`/`psm` arrive via
`datafusion-functions`/`datafusion-sql` — they are the DataFusion 55 bump's cost, not
liquid-cache's.

## `serde_json/preserve_order` became graph-wide (and what it did NOT break)

`datafusion-physical-plan` 55.1.0 enables **`serde_json/preserve_order`**, which under
reindeer's feature unification applies graph-wide: `serde_json` gains an `indexmap`
dependency and `Value`'s map becomes insertion-ordered instead of key-sorted. This comes
from the DataFusion 54→55 bump, not from liquid-cache. Omitting it via a fixup is not an
option — `datafusion-physical-plan` genuinely requires it.

It surfaced as a failure in `//src/services/ingest:wire-violation`, and the first read of
that failure was wrong, so it is worth recording precisely:

- `WireViolation` declares its fields **alphabetically**, so the DTO — the actual 422 wire
  body — serializes in alphabetical key order. That did **not** change.
- The test's *oracle* is a hand-rolled `serde_json::json!` literal. It was written in
  declaration order and relied on `Value` sorting keys for it. The test said so in its own
  module doc: *"loom's serde_json has no `preserve_order`, so `Value` objects sort their
  keys — the reference below is therefore key-sorted."*
- With `preserve_order` on, `json!` stopped sorting, so the oracle drifted while the
  product stayed put.

**The wire is unchanged.** The fix was to write the reference's keys in alphabetical order
(only the `TypeMismatch` arm needed it) and correct the now-false module doc. That restores
the original byte-identity assertion in full rather than weakening it — this was never a
question about loom's wire contract.

The general lesson is the one CLAUDE.md already gives about `reindeer update`: a shared-dep
feature can be unioned into the graph by a crate your diff never mentions, and the failure
lands in a crate your diff never touched. Anywhere a `json!` literal is compared for byte
identity, key order is now load-bearing.

## Verification status

- `buck2 build -M none //src/...` — **clean**, zero errors, zero warnings.
- `./tools/clippy-all.sh` — **clean** across every first-party Rust target.
- `buck2 test //src/...` — **478 pass / 2 fail** on a healthy RE, both since resolved:
  `ingest:wire-violation` (fixed, above) and `query-api:expr-typecheck` (a BuildBuddy RE
  503, not a code fault). Later sweeps in the same session returned 175–229 failures that
  were **all** the identical `Remote Execution Error on get_digests_ttl` (503, then
  transient DNS), with zero test-body failures — BuildBuddy RE was degraded throughout,
  which accounts for the `dev-image` CI failures on this branch. It did **not** account for
  `affected`, which is a separate, concrete break — see below. (I asserted the RE-outage
  explanation for `affected` before reading its log; that was wrong, and the log shows every
  other action in the invocation succeeded.)
- **Fixed here (pre-existing, not caused by this branch):** `control-plane/postgres:minio-`
  `{amd64,arm64}.bin` could not build — MinIO decommissioned its community binary CDN in
  September 2026, and every `dl.min.io` path (versioned, unversioned, and the archive index)
  answers **410 Gone**. That hard-fails the `http_file` and therefore every S3-backed
  fixture, on this branch and on `main` alike. The two URLs are repointed at the GitHub
  release assets for the *same* tag; both committed `sha256`s are unchanged, which is the
  proof the bytes are identical and this is a source swap rather than a version move. It is
  strictly speaking outside this PR's scope, but it is what stands between `affected` and
  green, no fix for it existed to port, and it is two lines — so it rides here rather than
  leaving the branch red on someone else's break. Verified end-to-end, not just as a
  download: `buck2 test //src/control-plane/postgres:iceberg-s3-roundtrip :s3-storage` —
  **2 pass**, i.e. the repointed binary boots a real object store and the iceberg pin bump
  is now exercised against the S3 path too, which it could not be while the fetch was dead.

## Pointers

- Cache module: `src/services/engine-serving/src/liquid_cache.rs`.
- Scan construction the rule rewrites: `engine-serving/src/serving.rs`
  (`IcebergMirrorTableProvider::scan`), pruning at `prune_files` just above it.
- Governed session/resource budget (the reason the governed path is excluded):
  `engine-serving/src/governed.rs` (`build_session`).
- Config: `engine/src/run.rs` (`EngineTuning::liquid_cache_config`).
- LiquidCache hook: `src/datafusion-local/src/lib.rs` (`LiquidCacheLocalBuilder::build`),
  `src/datafusion/src/optimizers/mod.rs` (`LocalModeOptimizer`, `convert_parquet_scan`).
