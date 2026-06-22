-- Per-file, per-column statistics for the Iceberg mirror, mirroring DuckLake's
-- ducklake_file_column_stats. NOT MVCC-versioned: stats are immutable for an
-- immutable data file and their lifecycle follows the data_file row. min/max are
-- stored as text and re-typed on read via the column's iceberg type (only the
-- StatValue primitives carry bounds; other types store NULL min/max).
create table iceberg_mirror.data_file_column_stat (
    data_file_id      bigint not null references iceberg_mirror.data_file(data_file_id),
    column_name       text   not null,
    null_count        bigint not null,
    column_size_bytes bigint not null,
    min_value         text,
    max_value         text,
    primary key (data_file_id, column_name)
);
