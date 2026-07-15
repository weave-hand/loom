-- The per-incarnation reclaim watermark: the highest `end_snapshot` GC has physically
-- destroyed for this `table_id`.
--
-- Why it has to exist: reclaimed-ness was recorded NOWHERE. `gc_locked` returned a
-- `GcSummary` to its RPC caller and wrote no audit row, so any predicate computed from
-- SURVIVING mirror rows is blind to what GC already destroyed. That blindness is what
-- forced the read guard to use the `at < H` proxy (410 every below-horizon read, even
-- one whose rows were never end-capped).
--
-- What it proves: every reclaimed row `r` had `end(r) <= reclaimed_through`, and `r` was
-- visible at snapshot `S` iff `S < end(r)`. So `S >= reclaimed_through` proves no
-- reclaimed row of this incarnation was ever visible at `S` — the read is complete on
-- the already-destroyed axis. (The still-destroyable axis is clause 2 of the guard,
-- which needs no new state.)
--
-- Keyed on `table_id`, so it is per-INCARNATION for free: a dropped-and-recreated
-- `(schema, name)` does not inherit the dead incarnation's watermark.
alter table iceberg_mirror.table
    add column reclaimed_through bigint not null default 0;

-- Backfill EXISTING tables to the current snapshot tip, not to 0.
--
-- GC may already have run on this database, and what it destroyed is exactly what is not
-- recorded. Defaulting a pre-existing table to 0 would assert "nothing of mine has been
-- reclaimed" — a claim we cannot make — and the new precise guard would then SERVE a read
-- the old `at < H` guard refused. Assuming the worst (anything at or below the tip may
-- already be gone) keeps this migration monotone in safety: no read that 410s today can
-- start returning rows because of it.
--
-- Tables created after this migration start at 0 (nothing has been reclaimed, provably)
-- and get the full precision benefit immediately. Existing tables regain it as soon as
-- they take a snapshot above the tip recorded here.
update iceberg_mirror.table
   set reclaimed_through = coalesce((select max(snapshot_id) from iceberg_mirror.snapshot), 0);
