-- Optional human-readable prose on every declarable ontology entity. Pure annotation:
-- nothing in the engine consumes it; it feeds the ontology read surface and generated
-- API docs. Nullable-add only — no rewrite, no backfill.
-- action_step gets no column: steps are positional, not named.
alter table ontology.object_type             add column description text;
alter table ontology.property                add column description text;
alter table ontology.link                    add column description text;
alter table ontology.derived_property        add column description text;
alter table ontology.action                  add column description text;
alter table ontology.action_param            add column description text;
alter table ontology.vector_index_definition add column description text;
