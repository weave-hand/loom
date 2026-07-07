-- Marks a dataset as an append-only "log table" and fixes its bucket count.
-- Row presence == "this is a log table"; bucket_count is immutable after creation.
-- Referenced by the write path to decide whether to stamp per-bucket offsets.
create table stream.stream_table (
    table_id     bigint      primary key,
    bucket_count int         not null,
    created_at   timestamptz not null default now()
);
