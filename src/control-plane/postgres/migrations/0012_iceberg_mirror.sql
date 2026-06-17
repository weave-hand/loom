create schema if not exists iceberg_mirror;

-- Catalog-global monotonic snapshot ids (loom's authority, independent of Iceberg's
-- per-table snapshot ids). One row per loom snapshot.
create table iceberg_mirror.snapshot (
    snapshot_id         bigint      primary key,
    snapshot_time       timestamptz not null default now(),
    schema_version      bigint      not null default 0,
    iceberg_snapshot_id bigint
);

-- Table existence, MVCC-versioned.
create table iceberg_mirror.table (
    table_id         bigserial primary key,
    table_namespace  text      not null,
    table_name       text      not null,
    begin_snapshot   bigint    not null,
    end_snapshot     bigint
);
create index iceberg_table_lookup_idx
    on iceberg_mirror.table (table_namespace, table_name, begin_snapshot);

-- Column schema, MVCC-versioned. `column_type` holds the Iceberg primitive type name
-- (e.g. "long","string"); the adapter maps it to a loom logical type on read.
create table iceberg_mirror.column (
    table_id        bigint  not null references iceberg_mirror.table(table_id),
    column_order    bigint  not null,
    column_name     text    not null,
    column_type     text    not null,
    nulls_allowed   boolean not null,
    begin_snapshot  bigint  not null,
    end_snapshot    bigint
);
create index iceberg_column_live_idx
    on iceberg_mirror.column (table_id, begin_snapshot);

-- Data files, MVCC-versioned.
create table iceberg_mirror.data_file (
    data_file_id    bigserial primary key,
    table_id        bigint  not null references iceberg_mirror.table(table_id),
    path            text    not null,
    file_format     text    not null,
    record_count    bigint  not null,
    file_size_bytes bigint  not null,
    begin_snapshot  bigint  not null,
    end_snapshot    bigint
);
create index iceberg_data_file_live_idx
    on iceberg_mirror.data_file (table_id, begin_snapshot);
