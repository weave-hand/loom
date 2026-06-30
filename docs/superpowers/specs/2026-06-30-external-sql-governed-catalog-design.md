# External SQL wire — slice 1: governed catalog primitive + governed-SQL engine path

- **Date:** 2026-06-30
- **Area:** query
- **Register items:** carves slice 1 of [[fut-external-sql-wire]]; mints [[road-external-sql-governed-catalog]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

External clients will send **arbitrary SQL** to loom and get back results that obey the
caller's ACL — without loom trusting a SQL rewriter to be governance-complete. The arc
realizes this by **putting governance in the relations, not the query**: each ontology
type is exposed to DataFusion as a *governed* table (row filter applied, denied columns
absent, masked columns redacted), so any join / aggregate / subquery the client writes is
governed by construction.

This slice builds the **correctness core** of that model — the governed table provider
and an engine path that runs arbitrary SQL over a per-request governed catalog — entirely
**internal** (no TCP, no auth). The external Flight SQL/TCP listener that feeds it is
slice 2 ([[fut-external-sql-wire]]); catalog-metadata commands, prepared statements, TLS,
and write-back are slice 3+ ([[fut-flight-sql-surface]], [[fut-flight-export-tls]]).

## Background — why governance must move into the relations

loom's read governance today is **compiled into the SQL query-api emits**:
`handler::load_policy` resolves a subject's `(row_filters, denied, masked)` for a type and
`sql::compile_select_with` bakes them into the `SELECT` it sends the engine; the engine
(`engine_serving::execute_query_stream`) registers every live Iceberg table **ungoverned**
and runs that already-governed SQL blindly (`serving.rs:473`). This works precisely
because loom authors the SQL.

An external SQL wire inverts that: the **client** authors the SQL. ARCHITECTURE.md flags
the consequence as an open question — *"Some predicates can't be expressed as a simple
WHERE/projection rewrite… refuse, or generate a subquery?"* and *"governance over
arbitrary SQL"* (lines 120, 126). Parsing-and-rewriting arbitrary client SQL to inject ACL
is fragile (subqueries, CTEs, functions, aliasing — hard to prove complete). The
**governed-catalog** approach sidesteps it: the engine registers each type as a relation
that is *already* row-filtered / column-projected / masked, and DataFusion's own planner
carries that governance through whatever the client wrote.

## Design

### `GovernedTableProvider` (engine-serving)

A `TableProvider` decorator wrapping the existing `IcebergMirrorTableProvider`
(`engine-serving/src/serving.rs:413`) plus a per-type `TablePolicy { row_filters:
Vec<RowFilter>, denied: HashSet<String>, masked: HashSet<String> }`:

- **`schema()`** = the inner schema with **denied** columns removed and **masked** columns
  re-typed to `Utf8`. Deny ⇒ the column is *absent*, so client SQL naming it fails with a
  normal "column not found" (no value leak, same outcome as the column not existing). Mask
  ⇒ present but `Utf8` (matching the `'***'` rendering the governed HTTP read path and
  [[road-governed-flight-export]] already use).
- **`scan(projection, filters, limit)`** produces, in this order, so a row filter that
  references a *denied* column still works:
  1. scan the **inner** provider over the **full** schema;
  2. wrap it in a `FilterExec` that **unconditionally AND-s** the policy `row_filters`
     (combined with the *same* semantics `compile_select_with` uses, so the wire and the
     HTTP read path agree row-for-row) — translated `RowFilter → datafusion::Expr` by a new
     `row_filter_to_expr` helper covering the `CompareOp` surface;
  3. **project** to the allowed (non-denied) columns;
  4. **mask**: replace each masked column with the `'***'` `Utf8` literal.

  The client's own `filters`/`projection`/`limit` push down to the inner scan as usual
  (optimization); governance is applied regardless and cannot be pushed away.

The provider is **enforcing, not advisory**: there is no code path through it that returns
a denied column, an unmasked masked value, or a row the filter excludes.

### `GovernedCatalog` payload + governed-SQL engine path

The engine cannot resolve a subject's policy (it has no ACL/subject) — consistent with
loom's "authorize at the edge, execute blindly" stance. So the **caller** supplies a fully
resolved governed catalog and the engine just applies it:

```
GovernedCatalog { tables: Vec<GovernedTable> }
GovernedTable   { table: TableRef, row_filters: Vec<RowFilter>,
                  denied: Vec<String>, masked: Vec<String> }
```

A new `engine_serving::execute_governed_sql_stream(catalog, sql, governed: &GovernedCatalog,
serving_store)` mirrors `execute_query_stream` but, for each live table, registers a
`GovernedTableProvider` built from the matching `GovernedTable` (a table with **no**
`GovernedTable` entry is registered with an **empty-policy** governed provider — i.e. fully
visible — or omitted entirely; see Open-but-decided below). It then runs the **client's**
SQL over that `SessionContext` and returns the stream.

### Engine wire dispatch (internal)

The engine's Flight `do_get` already dispatches a `TicketStatementQuery` to the SQL path
(`engine/src/flight.rs:114`). Add a sibling **governed** ticket — a prost message
`GovernedStatementQuery { sql, catalog: GovernedCatalog }` packed as the Flight ticket —
dispatched to `execute_governed_sql_stream` and encoded through the same
`FlightDataEncoderBuilder` streaming path. The ungoverned `TicketStatementQuery` path is
untouched (still used by query-api's internal read until/unless it migrates). This keeps
the whole streaming/encoding stack shared; slice 2 just builds this ticket from a TCP
client's SQL + the subject's resolved policy.

### Decided (not open)

- **Masked column type is `Utf8 '***'`**, reusing the existing masking convention; a
  client doing arithmetic on a masked column gets a type error, which is acceptable (you
  cannot compute over data you may not see). Consistency with the HTTP/export masking wins
  over convenience.
- **A table with no `GovernedTable` entry** is registered **fully visible** (empty policy).
  Rationale: the caller (slice 2) always sends an entry for every type the subject may see
  and omits types the subject has *no* grant on, so an omitted table is unreachable anyway;
  making "absent ⇒ visible" keeps the engine dumb. Slice 2 owns deny-by-default at the edge
  (it never lists a type the subject can't read).
- **Row-filter combination semantics** are inherited verbatim from `compile_select_with`
  (shared helper or a shared spec), never re-derived — the governed provider and the HTTP
  read path MUST agree.

## Scope

In scope (slice 1):

- `GovernedTableProvider` (enforcing decorator) + `row_filter_to_expr` (`RowFilter →
  datafusion::Expr` over the `CompareOp` surface) in engine-serving.
- `GovernedCatalog`/`GovernedTable` payload types + `execute_governed_sql_stream`.
- The engine `do_get` `GovernedStatementQuery` ticket dispatch (internal wire only).

Out of scope (later slices):

- **Slice 2 ([[fut-external-sql-wire]]):** the external TCP `FlightServiceServer`,
  bearer-token auth, and query-api resolving `load_policy` for every visible type into a
  `GovernedCatalog` (deny-by-default at the edge). Service-account tokens
  ([[fut-auth-service-tokens]]) plug into that auth.
- **Slice 3+ ([[fut-flight-sql-surface]]):** governed `CommandGetTables`/`CommandGetDbSchemas`
  metadata, prepared statements, `do_put` write-back. TLS ([[fut-flight-export-tls]]).
- Cost-based pruning interactions, multi-statement/transaction semantics, and any change to
  the existing ungoverned internal read path.

## Testing

Engine-level tests (the `engine`/`engine-serving` fixture style; `rust_test` integration
targets), driving `execute_governed_sql_stream` / the governed ticket directly with
hand-built `GovernedCatalog`s — **no ACL/TCP needed**:

1. **Row filter holds under arbitrary SQL:** seed two types; a `GovernedTable` with a row
   filter on one; a client `SELECT … JOIN … WHERE …` that *tries to* see filtered-out rows
   → only filter-admitted rows return, regardless of the client predicate.
2. **Denied column is absent:** a query naming a denied column → planning error ("column not
   found"); a `SELECT *` over the type → the denied column is not in the result schema.
3. **Masked column redacted:** `SELECT masked_col` → every value is `'***'` `Utf8`, in both
   the result schema and the data (the export-style lockstep), even through an aggregate
   (`SELECT masked_col, count(*) … GROUP BY masked_col` groups on `'***'`).
4. **Aggregate/join governance:** a `GROUP BY` / `JOIN` that would expose filtered rows or
   denied columns via a different path returns only governed data — the property that
   justifies the whole approach.
5. **Parity with the HTTP read path:** for a single-type `SELECT` the governed provider
   returns exactly what `compile_select_with` + the HTTP read returns for the same subject
   policy (row-for-row, column-for-column) — pinning that the two governance routes agree.
6. **Empty policy = full visibility:** a `GovernedTable` with no filters/deny/mask returns
   the inner table unchanged.

## Risk

- **This is a governance-critical primitive** — a bypass is a data leak. Mitigated by: the
  provider being enforcing-by-construction (no un-governed code path through `scan`), the
  apply-order (filter on full schema *before* deny/project so filters can reference denied
  columns), tests 1–5 attacking each leak class through arbitrary SQL, and the parity test
  pinning agreement with the already-audited HTTP read path.
- `RowFilter → Expr` translation is new surface; bounded to the `CompareOp` set the HTTP
  path already supports, and pinned to the same combination semantics (test 5). A
  `RowFilter` shape that cannot be expressed as an `Expr` must **fail closed** (refuse the
  query), never silently drop the filter.
- The internal ungoverned read path and all existing serving behaviour are untouched (the
  governed ticket is additive), so the blast radius is the new path only.
