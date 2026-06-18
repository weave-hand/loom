-- The property that is a type's primary key, if declared. Names a row in
-- ontology.property for the same type_name. Nullable: identity is opt-in.
alter table ontology.object_type add column identity text;
