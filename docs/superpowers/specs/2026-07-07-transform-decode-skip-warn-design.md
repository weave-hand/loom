# Transform body decode: per-row skip-and-warn — design

**Item:** `#iss-transform-claim-poison-row` · **Area:** transform

## Problem

`de_body` (`src/control-plane/postgres/src/transforms.rs:24`) deserializes a
stored `transforms.transform.body` JSON column into a `TransformBody`. Three
call sites decode a *batch* of rows; two of them propagate a single row's decode
failure with `?`, so one corrupt or schema-stale body poisons the whole
operation. A body becomes undecodable only out-of-band today (a future
`TransformBody` variant change that strands old rows, or direct corruption) —
`define_transform` validates and re-serializes on write — so this is
defense-in-depth, but the two failure modes are severe.

**Surface 1 — the schedule claim batch starves every schedule.**
`claim_due_schedules` (`transforms.rs:514`) selects the due rows
(`... order by next_run_at, name ... for update skip locked`) and, in the loop,
builds each `TransformDef` with `body: de_body(r.body)?` (`transforms.rs:536`).
One undecodable row errors the closure, the transaction never reaches
`tx.commit()` (`transforms.rs:554`), and every `next_run_at` advance in the
batch rolls back. The poison row is unchanged, so it **stays due**, and because
its `next_run_at` never moves it **sorts earliest** and is re-selected on the
very next tick — where it fails again. The scheduler makes no forward progress:
one bad row **starves all schedules** indefinitely.

**Surface 2 — the trigger-cycle scan 500s every subsequent define.** When
`define_transform` (`transforms.rs:271`) defines an `on_input_commit` def, it
scans every *other* data-triggered def to build the edge set for
`validate_no_trigger_cycle`, decoding each with
`.map(|r| Ok((..., de_body(r.body)?))).collect::<Result<_>>()?`
(`transforms.rs:312-315`). One undecodable existing row makes that `?` fail, so
**every** subsequent `on_input_commit` define returns a 500 until the bad row is
repaired.

**The commit seam is already immune** — the pattern to mirror.
`pg_fire_data_triggers` (`transforms.rs:110`) decodes at two points, and both
**skip-and-warn** rather than `?`: the candidate scan
(`transforms.rs:129-136`) and the under-lock live re-read
(`transforms.rs:172-179`), each logging
`tracing::warn!(transform = %…, error = %e, "data trigger: undecodable body skipped")`
and continuing. So an ingest commit never fails on a stranded def; only the two
batch decoders above still hard-fail.

## Scope

Convert the two hard-failing batch decode points to **per-row skip-and-warn**,
matching the commit seam so all three decoders behave identically:

- `claim_due_schedules` loop (`transforms.rs:533-553`).
- `define_transform` trigger-cycle scan (`transforms.rs:312-315`).

**Non-goals (explicit):**

- **No dead-letter / quarantine table.** Skipped rows are logged, not moved to
  a poison-message store. A quarantine surface (operator-visible list of
  undecodable defs, requeue-on-repair) is a possible future item, not this
  change.
- **No change to `pg_fire_data_triggers`.** It already skip-and-warns at both
  decode points (`transforms.rs:129-136`, `172-179`); this change makes the
  other two consistent with it, nothing more.
- **No API or write-time validation change to `define_transform`.** Bodies are
  validated and re-serialized on write, so the API cannot *create* an
  undecodable row. This is purely read-path defense-in-depth for rows that went
  stale or corrupt out-of-band.

## Design

**Claim loop — advance-and-warn (not bare skip).** In the
`claim_due_schedules` loop, replace `body: de_body(r.body)?` with a `match`. On
`Ok`, behave exactly as today. On `Err(e)`, `tracing::warn!` (schedule name +
decode error, mirroring the seam's message) and **still advance `next_run_at`**
for that row using the row's own `schedule` column — which is a *separate*
column (`r.schedule`), not part of `body`, so `next_cron_occurrence` can be
computed without decoding. The row is *not* pushed to `claimed` (it cannot run
until fixed), but its `next_run_at` moves forward, so it leaves the due window
and no longer sorts earliest. A bare `continue` would leave it perpetually due
and re-consuming a claim slot every tick; advancing it is what actually lifts
the starvation. The transaction commits regardless, so every healthy schedule in
the batch is claimed and rescheduled. If `r.schedule` is somehow null (it is the
selection predicate, so it should not be) fall through to the existing
`else { continue }`.

**Trigger-cycle scan — omit undecodable defs from the edge set.** In
`define_transform`, replace the `collect::<Result<_>>()?` with a loop that, per
existing row, decodes and on `Err` warns and skips (does not push into
`bodies`). The candidate being defined (`def.body`, in-memory, always decodable)
is still appended, so the def under construction is always validated. The scan
then proceeds over the decodable subset.

**Is omitting a def from the cycle scan sound?** Yes — it is conservative in the
safe direction, and consistent with the graph's existing "contributes no edge"
semantics. `TriggerNode::resolve` (`core/src/transforms.rs:290`) already treats
a typed name with no live table binding as contributing no input/output edge
("a vanished binding cannot match a commit, so it forms no edge"); an
undecodable body is the same thing one step earlier — it likewise **cannot
fire**, because the commit seam skips it (`transforms.rs:129-136`). A firing
cycle requires every member to be live and decodable; a cycle routed through an
undecodable node cannot actually fire while that node is broken, so omitting it
can only fail to *reject* a would-be cycle that is already inert at runtime. When
the corrupt row is later repaired, defining or redefining any member re-runs the
scan with that node decodable and catches the cycle then. The scan therefore
never admits a cycle that could fire — the property `validate_no_trigger_cycle`
exists to guarantee.

**Trade-off.** A skipped schedule row never runs until its body is fixed, and a
skipped def is invisible to cycle detection until repaired — but in exchange the
*other* schedules are no longer starved and *other* `on_input_commit` defines no
longer 500. The failure is contained to the one bad row instead of taking the
whole subsystem down, and the warn log names the offending transform for repair.

## Testing

Control-plane/postgres fixture tests (new cases alongside the existing transform
fixture tests), each seeding an undecodable body by writing an
invalid/schema-stale JSON directly into `transforms.transform.body` (bypassing
`define_transform`, which would reject it):

- **Claim batch survives a poison row.** Seed one undecodable due schedule plus
  one or more healthy due schedules, all past `next_run_at`. Assert
  `claim_due_schedules` returns the healthy def(s) (the poison row is absent
  from the result), the transaction commits, and — the regression guard — the
  poison row's `next_run_at` has advanced so a second `claim_due_schedules` on
  the next tick does not re-surface it and the healthy schedules still progress.
- **Define survives a poison existing def.** Seed one undecodable
  `on_input_commit` def, then call `define_transform` with a fresh
  `on_input_commit` def. Assert it succeeds (no 500) and the cycle scan ran over
  the decodable subset.
- **Warn is emitted.** Capture the `tracing` output (as the seam's tests do) and
  assert a warn carrying the offending transform name and the decode error is
  logged at each converted site.
