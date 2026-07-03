-- Custom-logic actions slice 2: computed (expression-valued) assignments.
--
-- An assignment's source is now either a fixed constant (`value`, the slice-1 shape) or a
-- bounded expression (`expr`, evaluated in query-api at invocation over the action's inputs).
-- Exactly one of (value, expr) is non-null per row. `value` becomes nullable (was NOT NULL);
-- existing rows are all constants (expr NULL) and are unaffected.
alter table ontology.action_assignment
    alter column value drop not null;

alter table ontology.action_assignment
    add column expr text;

alter table ontology.action_assignment
    add constraint action_assignment_source_ck
    check ((value is not null and expr is null) or (value is null and expr is not null));
