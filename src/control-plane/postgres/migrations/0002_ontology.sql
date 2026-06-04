create schema if not exists ontology;

create table ontology.object_type (
    name         text primary key,
    table_schema text not null,
    table_name   text not null
);

create table ontology.property (
    type_name text    not null references ontology.object_type (name) on delete cascade,
    ordinal   int     not null,
    name      text    not null,
    ty        text    not null,
    required  boolean not null,
    primary key (type_name, ordinal)
);

create table ontology.link (
    name        text not null,
    from_type   text not null references ontology.object_type (name) on delete cascade,
    to_type     text not null references ontology.object_type (name),
    cardinality text not null,
    primary key (name, from_type)
);
