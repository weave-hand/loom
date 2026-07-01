-- Per-value validation rules for an object-type property (model constraints).
-- Nullable: existing rows and unconstrained properties store NULL (the empty default).
alter table ontology.property add column constraints jsonb;
