-- PK/CDC stream tables (stream engine slice 2). `kind` discriminates an
-- append-only log table (slice 1) from a PK/CDC table; `bucket_key` names the
-- identity column a CDC table buckets on (hash(bucket_key) % bucket_count);
-- `changelog_table_id` points at the durable changelog Iceberg table (slice 2b;
-- NULL until then).
alter table stream.stream_table
    add column kind text not null default 'log'
        check (kind in ('log', 'cdc')),
    add column bucket_key text,
    add column changelog_table_id bigint;

-- A CDC table must name its bucket key; a log table must not.
alter table stream.stream_table
    add constraint cdc_requires_bucket_key
        check ((kind = 'cdc') = (bucket_key is not null));
