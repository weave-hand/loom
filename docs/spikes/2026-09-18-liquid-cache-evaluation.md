# Spike: evaluating LiquidCache for the engine serving path

> **Type:** evaluation spike (read-only; no code changes). **Date:** 2026-09-18.
> **Subject:** [`datafusion-contrib/liquid-cache`](https://github.com/datafusion-contrib/liquid-cache)
> (canonical repo: `XiangpengHao/liquid-cache`), crate `liquid-cache` **0.1.13**,
> Apache-2.0, "10x lower latency for cloud-native DataFusion".
> **Question:** should loom put LiquidCache under the engine's DataFusion serving path?
> **Verdict:** **not now — but keep the door open.** The *integration shape* is close to
> ideal (loom needs zero `TableProvider` changes). The blockers are a two-major version
> skew, a large git-sourced dependency tree, and the fact that loom has no measured
> read-latency problem to solve. Revisit after the arrow-59 / DataFusion-55 bump.

## TL;DR

| | Verdict |
|---|---|
| **Does it fit loom's architecture?** | Yes, unusually well — see *Integration shape* |
| **Can we adopt it today?** | No — arrow 58 vs 59, DataFusion 54 vs 55 |
| **Is the blocker permanent?** | No — the arrow-59 prerequisite is already unblocked upstream |
| **Should we adopt it once unblocked?** | Only against a measured baseline; three open questions first |

## What LiquidCache is

A pushdown cache layer for DataFusion. Rather than caching raw Parquet bytes or decoded
Arrow, it transcodes scanned data into a cache-specific columnar format (built on
[Vortex](https://github.com/vortex-data/vortex)), keeps only query-relevant columns
resident in memory, and spills the rest to local SSD via `t4` using direct I/O
(`O_DIRECT` on Linux, `F_NOCACHE` on macOS) to bypass the OS page cache. It supports
**selection pushdown** (row filtering by boolean mask) and **predicate pushdown**
(evaluating expressions such as `col > 12` inside the cache layer), so a filtered scan
never has to hydrate columns it will discard.

It ships in two deployment modes: an in-process mode (`LiquidCacheLocalBuilder`) and a
separate cache-server mode (`LiquidCacheBuilder` + a Flight-based
`liquid-cache-datafusion-server`/`-client` pair). Only the in-process mode is relevant
to loom in the near term — the engine service already owns the object store and is the
sole serving path, so a second network hop would be pure cost.

## Integration shape — the good news

The README's examples all use `ListingTable` / `ctx.register_parquet(...)`, which loom
does **not** use: `IcebergMirrorTableProvider` is a hand-written `TableProvider` that
prunes files against mirror stats and builds its own scan. That looks disqualifying, and
it is not.

`LiquidCacheLocalBuilder::build` installs its hook as a **`PhysicalOptimizerRule`**
(`LocalModeOptimizer`), not as a table provider or an object-store wrapper:

```rust
let optimizer = LocalModeOptimizer::new(cache_ref.clone()).with_prefetch(self.prefetch);
let state = SessionStateBuilder::new()
    .with_config(config)
    .with_default_features()
    .with_physical_optimizer_rule(Arc::new(optimizer))
    .build();
```

That rule walks the finished physical plan, downcasts each node to `DataSourceExec`,
pulls out its `FileScanConfig` + `ParquetSource`, and substitutes a `LiquidParquetSource`
wrapped back into a fresh `FileScanConfig`/`DataSourceExec`. Anything that is not a
`DataSourceExec` over a `ParquetSource` passes through untouched.

loom's `IcebergMirrorTableProvider::scan` (`engine-serving/src/serving.rs:977-1002`)
emits **exactly** that node:

```rust
let source = Arc::new(ParquetSource::new(self.schema.clone()));
let mut builder = FileScanConfigBuilder::new(store_url, source).with_limit(limit);
// ... one PartitionedFile per surviving Iceberg data file ...
Ok(DataSourceExec::from_data_source(config))
```

So LiquidCache would apply to loom's custom provider with **no provider changes at all**,
and — because the rewrite is a whole-plan traversal — it would also reach scans nested
under `GovernedTableProvider` on the governed path, and the `feed`/`mv_delta`/`consolidate`
contexts. loom's existing file pruning stays where it is and composes: the mirror prunes
whole files, the cache then serves the survivors. That is a genuinely clean seam, and it
is the single strongest argument for revisiting this later.

## Blocker 1 — version skew (hard, today)

| Dependency | loom (`Cargo.lock`) | liquid-cache 0.1.13 |
|---|---|---|
| `datafusion` | **54.0.0** | **55.0.0** |
| `arrow` / `parquet` | **58.3.0** | **59.2.0** |
| `object_store` | 0.13.2 | 0.13.2 ✅ |
| `tonic` | 0.14 | 0.14.6 ✅ |

Two majors have to move, and arrow is the load-bearing one: CLAUDE.md's whole-tree
single-arrow-major invariant exists precisely so there are no `arrow-*-57`-style split
target families. Adopting LiquidCache means moving the *entire* tree to arrow 59.

**This prerequisite is already unblocked.** loom's arrow-58 floor comes from the pinned
`iceberg-rust` `main` commit (`148afc50`), the tree's only git dependency. `iceberg-rust`
`main` is now **0.10.1 on arrow 59.2** — which also satisfies CLAUDE.md's stated exit
condition for that pin ("exit when iceberg publishes the post-0.9.1 release"). So the
arrow-59 + DataFusion-55 bump is work loom wants on its own merits; it should be done
and judged separately, not smuggled in under a caching change.

## Blocker 2 — dependency weight

liquid-cache pulls **18 `vortex-*` crates from a git revision**
(`vortex-data/vortex` @ `265b7053ac80c3ee9ddccf1fb66f85effa902c62`), plus `t4` (0.1.9,
crates.io) for the direct-I/O disk store. Under reindeer's non-vendored mode every git
dependency becomes its own `git_fetch` rule. loom currently has exactly **one** git
dependency and CLAUDE.md flags it as notable; this would take the tree to ~19
git-sourced crates from a second upstream that is itself pinned to an unreleased commit
rather than a tagged version. That is a real, recurring maintenance tax — every vortex
bump is a manual rev bump plus a full `buck2 test //src/...` sweep.

## Blocker 3 — loom builds a `SessionContext` per query

`execute_query_stream` does `SessionContext::new()` on every call, and there are ~10 such
sites across `engine-serving` and `worker`. LiquidCache's cache is owned by the
`LiquidCacheParquetRef` the builder hands back, and the rule is installed on the
`SessionState` — so a per-query context means a per-query cache, i.e. no cache at all.

Making it useful requires a process-lifetime cache handle plus a shared `SessionState`
factory threaded through `execute_query_stream` / `build_serving_provider` /
`build_session`. That is a contained refactor rather than a redesign, but it is not free,
and it changes the currently-simple "every query starts from nothing" story.

## Open questions to answer before adopting

1. **Resource budgeting conflicts with the governed DoS posture.** `governed.rs`
   deliberately pairs a `GreedyMemoryPool` with `DiskManagerMode::Disabled` so that the
   memory budget actually binds and an oversized query cannot trade a memory DoS for an
   unbounded-`/tmp` disk DoS. LiquidCache introduces its own `max_memory_bytes` *and* its
   own on-disk cache — both outside DataFusion's `MemoryPool` and outside the disabled
   `DiskManager`. The reasoning in `build_session` would have to be redone, not just
   re-pointed.
2. **Forced session config.** `build` overrides `parquet.pushdown_filters = true`,
   `schema_force_view_types = false`, `skip_arrow_metadata = false`, `skip_metadata =
   false`, and a fixed `batch_size`. loom's provider currently reports
   `TableProviderFilterPushDown::Inexact`, so DataFusion re-applies every predicate above
   the scan; turning on row-level pushdown changes plan shape for every serving consumer.
   `schema_force_view_types = false` also changes string representation (`Utf8` vs
   `Utf8View`) across the serving path.
3. **ACL interaction (the one that needs a real answer).** With `pushdown_filters = true`,
   predicates from `GovernedTableProvider` — including ACL row filters — become candidates
   for evaluation *inside* the cache layer, and cached transcoded data is keyed per file,
   shared across subjects. Almost certainly sound (the same bytes, filtered per query),
   but "almost certainly" is not the standard for the governance boundary. This needs an
   explicit answer and an STPA entry before any adoption.

## Maturity

Three published versions (0.1.11 → 0.1.13), latest **2026-05-23**. The upstream FAQ
answers "is it production ready?" with **"Almost"**, describing the project as having
"grown out of research" and needing time to mature — it originates in Xiangpeng Hao's PhD
work, supported by SpiralDB, InfluxData, and Bauplan. It does pass ClickBench, TPC-H and
TPC-DS, with x86-64-specific optimizations and ARM fallbacks. Apache-2.0, so licensing is
a non-issue.

## Cost / benefit

**Benefit:** potentially large. loom serves every read by scanning Parquet on S3/MinIO
through a fresh context — there is no warm-data path at all today, so a working cache
would land on genuinely cold ground, and the pushdown story composes with the existing
mirror-stat pruning rather than duplicating it.

**Cost:** an arrow/DataFusion two-major bump, ~19 new git-sourced crates, a session-lifetime
refactor, and a re-derivation of the governed resource budget — against a **latency problem
loom has not yet measured**. There is no open issue in the tracker about serving read
latency, object-store read amplification, or caching. Adopting a "10x lower latency" cache
with no baseline is how you find out later that the 10x was 1.1x on your workload.

## Recommendation

**Do not adopt now.** Instead, in order:

1. **Do the arrow-59 / iceberg-rust-0.10.1 / DataFusion-55 bump on its own merits.** It is
   independently wanted, CLAUDE.md's exit condition for the git pin is met, and it is the
   hard prerequisite for ever revisiting this. Judge it as a dependency bump, not as
   caching work.
2. **Instrument the serving read path first.** Establish whether cold-scan latency against
   S3/MinIO is actually a problem worth a dependency of this weight, and what the shape of
   the miss is (metadata round-trips? column hydration? repeated scans of the same files?).
   Without that, there is no way to tell a win from noise.
3. **If and only if (2) shows a real problem, prototype behind `engine-serving`.** The
   `DataSourceExec` + `ParquetSource` shape means the prototype needs no `TableProvider`
   changes — the whole experiment is a shared `SessionState` plus a physical-optimizer
   rule, which is a small diff to put up and an easy one to revert. Answer the three open
   questions above *in* that prototype, not after it.
4. **Track it as `idea`, not `roadmap`.** This is a deliberately deferred option, and the
   thing that would promote it is a measurement, not an opinion.

## Pointers (for whoever picks this up later)

- loom scan construction: `src/services/engine-serving/src/serving.rs:977-1002`
  (`IcebergMirrorTableProvider::scan`), pruning at `prune_files` (`serving.rs:922`).
- Per-query contexts: `serving.rs:1055`, `feed.rs:506,573`, `mv_delta.rs:165`,
  `consolidate.rs:498,608`, `mv_enrich.rs:67`, `worker/src/transform.rs:249`,
  `worker/src/stream_mv.rs:124`.
- Governed session/resource budget: `engine-serving/src/governed.rs:458-470`
  (`build_session`).
- Object-store registration: `serving.rs:105-115`, `feed.rs:247-258`.
- LiquidCache hook: `src/datafusion-local/src/lib.rs` (`LiquidCacheLocalBuilder::build`),
  `src/datafusion/src/optimizers/mod.rs` (`LocalModeOptimizer`, `convert_parquet_scan`).
- Arrow-59 prerequisite: the `iceberg` rev in `src/control-plane/postgres/Cargo.toml`,
  `src/services/engine/Cargo.toml`, `src/services/worker/Cargo.toml`.
