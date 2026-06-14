-- Physical backing for ontology links: how `from` and `to` join.
-- backing_kind in ('fk','join_table'). from_column/to_column always set; the
-- join-table-only columns are null for an 'fk' link.
alter table ontology.link
    add column backing_kind      text not null default 'fk',
    add column from_column       text not null default '',
    add column to_column         text not null default '',
    add column from_key          text,
    add column to_key            text,
    add column join_table_schema text,
    add column join_table_name   text;
