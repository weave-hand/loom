-- Slice-1 scalable COW: a table listed here has taken a mutation (carries inline
-- shadow deltas) so the byte-trigger flush is suppressed until slice-2
-- consolidation (flushing a version/tombstone would duplicate/resurrect a file row).
create table iceberg_mirror.shadow_flag (
    table_id bigint primary key references iceberg_mirror.table(table_id)
);
