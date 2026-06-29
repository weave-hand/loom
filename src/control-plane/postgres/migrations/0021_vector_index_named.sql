-- Multiple named vector indexes per column: add index_name and fold it into the
-- mirror PK so N named indexes coexist for one (table_id, column_name, snapshot).
-- Existing rows backfill to 'default'.
alter table iceberg_mirror.vector_index
    add column index_name text not null default 'default';
alter table iceberg_mirror.vector_index
    drop constraint vector_index_pkey;
alter table iceberg_mirror.vector_index
    add primary key (table_id, column_name, index_name, covered_snapshot);
