-- Catalog views: virtual datasets over one physical base table. A view shares
-- the (schema, name) namespace with physical tables (collision enforced in the
-- adapter against iceberg_mirror.table) and carries an optional RowFilter
-- predicate (jsonb, the control-plane serde) plus an optional column
-- projection. Metadata-only: no snapshots, no files.
-- (Naming: the spec sketches `catalog.dataset_view`; `catalog` is avoided as a
-- schema name for its SQL-keyword adjacency — the store is `dataset_view.view`.)
create schema if not exists dataset_view;

create table dataset_view.view (
    view_schema text not null,
    view_name   text not null,
    base_schema text not null,
    base_name   text not null,
    predicate   jsonb,
    columns     text[],
    primary key (view_schema, view_name)
);

create index dataset_view_by_base on dataset_view.view (base_schema, base_name);
