create table ontology.derived_property (
    type_name  text    not null references ontology.object_type (name) on delete cascade,
    ordinal    int     not null,
    name       text    not null,
    ty         text    not null,
    link_name  text    not null,
    agg_kind   text    not null,
    agg_column text,
    primary key (type_name, ordinal)
);
