# System capabilities

What loom can do **today**, per subsystem — behaviour, guarantees, and the key
design decisions behind each landed capability, with PR references inline.
These docs are the durable record of completed work; the registers
([`ROADMAP.md`](../ROADMAP.md), [`ISSUES.md`](../ISSUES.md),
[`FUTURE.md`](../FUTURE.md)) track only what is planned, broken, or deferred,
and each doc closes with a `## Known gaps` section linking the register ids
still open for its subsystem. Provenance: every doc carries an
`_As of <short-sha>._` line naming the main commit its claims were verified
against.

| Doc | Covers |
| --- | --- |
| [control-plane.md](control-plane.md) | The five-concern library (queue, catalog + Iceberg mirror, ontology, ACL, lineage) plus auth, the `Tx` seam, and the adapter hardening |
| [ingest.md](ingest.md) | Landing endpoint, materializer, snapshot commit, dataset→model binding, inference, constraints |
| [query-api.md](query-api.md) | Governed reads, typed JSON, link/graph traversal, the filter language, actions, object sets, error contract |
| [engine.md](engine.md) | The serving path, internal Flight SQL wire, Iceberg write paths (inline/flush/overwrite/compaction/COW), stats, GC |
| [transform.md](transform.md) | Queue-driven SQL transforms, typed transforms, output tuning, the worker execution model |
| [stream.md](stream.md) | Log tables and PK/CDC tables, framing/bucketing/offsets, CDC emission, dual base+changelog Iceberg tables, LastRow compaction, CDC-aware serving reads |
| [vector-search.md](vector-search.md) | `vector(N)` column type, index definitions, Flat/IVF/HNSW builds, `/search`, rebuild triggers |
| [build-and-test.md](build-and-test.md) | Hermetic buck2 builds + RE, test infrastructure, lint gates, CI, code-health and docs-register tooling |
| [python-sdk.md](python-sdk.md) | `loom_sdk` (`src/sdk/python/`): sync/async clients over a sans-IO core, the two-URL model, the pydantic ontology-declaration layer, the RE-pinned test estate incl. the real-composite e2e smoke |
| [ui.md](ui.md) | The Yew/WASM UI experiment and its browser e2e harness |
| [../deploy.md](../deploy.md) | Images, Helm chart, standalone binary, embedded Postgres (capabilities merged into the existing deploy doc) |
