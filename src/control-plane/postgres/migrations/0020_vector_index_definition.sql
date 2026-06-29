-- A named vector index declared on an object type's vector property. The
-- declaration is the authoritative source of an index's kind/metric/params;
-- the build primitive resolves it and copies it into the iceberg_mirror.vector_index
-- row. Multiple indexes may exist per property, distinguished by name.
create table ontology.vector_index_definition (
    type_name       text    not null references ontology.object_type (name) on delete cascade,
    name            text    not null,
    property_name   text    not null,
    metric          text    not null,
    index_kind      text    not null,
    nlist           integer,
    m               integer,
    ef_construction integer,
    primary key (type_name, name)
);
