-- Per-table stream-consolidation trigger state, keyed by the STABLE table_id
-- (mirrors iceberg_mirror.inline_trigger, but counts accrued CDC delta rows
-- instead of live bytes). delta_count accumulates emitted -U/+U/-D rows since
-- the last consolidation; threshold is an optional per-table override of the
-- global LOOM_CONSOLIDATE_DELTA_THRESHOLD; enqueued debounces to one pending
-- stream_consolidate job per table until consolidation clears it.
create table iceberg_mirror.consolidate_trigger (
    table_id    bigint  primary key,
    delta_count bigint  not null default 0,
    threshold   bigint,
    enqueued    boolean not null default false
);
