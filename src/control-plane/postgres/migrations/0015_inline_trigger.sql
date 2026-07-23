-- Per-table inline-flush trigger state, keyed by the STABLE table_id (the same
-- id that names iceberg_mirror.inline_<table_id>), NOT the MVCC-versioned
-- iceberg_mirror.table rows. live_bytes accumulates the in-memory size of live
-- inline rows since the last flush; threshold is an optional per-table override
-- of the global LOOM_FLUSH_BYTE_THRESHOLD; enqueued debounces to one pending
-- flush job per table. See git history: 2026-06-19-inline-flush-trigger-design.
create table iceberg_mirror.inline_trigger (
    table_id   bigint  primary key,
    live_bytes bigint  not null default 0,
    threshold  bigint,
    enqueued   boolean not null default false
);
