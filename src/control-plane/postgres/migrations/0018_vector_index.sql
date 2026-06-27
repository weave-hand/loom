-- Binds a precomputed vector index (a Puffin sidecar) to a covered snapshot.
-- loom has no REST catalog; this mirror row is the binding pointer the paper
-- expresses via the REST catalog's statistics-file summary property.
create table iceberg_mirror.vector_index (
    table_id         bigint not null references iceberg_mirror.table(table_id),
    column_name      text   not null,
    covered_snapshot bigint not null,
    metric           text   not null,
    index_kind       text   not null,
    dim              integer not null,
    row_count        bigint not null,
    puffin_path      text   not null,
    created_at       timestamptz not null default now(),
    primary key (table_id, column_name, covered_snapshot)
);
create index iceberg_vector_index_lookup_idx
    on iceberg_mirror.vector_index (table_id, column_name, covered_snapshot);
