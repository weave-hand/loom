-- Per-standing-query offset watermarks (road-stream-continuous): the next
-- unprocessed loom_offset per (mv, source_table_id, bucket). `mv` is the
-- OUTPUT's qualified name (core::mv_key) — the watermark follows the
-- materialization, not the def name. Advanced by CAS inside the micro-batch
-- output-commit transaction: the watermark moves iff the output lands.
create table stream.mv_watermark (
    mv              text   not null,
    source_table_id bigint not null,
    bucket          int    not null,
    next_offset     bigint not null check (next_offset >= 0),
    primary key (mv, source_table_id, bucket)
);
