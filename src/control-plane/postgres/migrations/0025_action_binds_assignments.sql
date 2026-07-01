-- Custom-logic actions slice 1: declarative param->property mapping.

-- `binds` lets an action parameter write a differently-named property (rename).
-- NULL means the parameter binds the property of its own name (back-compatible).
alter table ontology.action_param
    add column binds text;

-- Constant assignments fill a property with a declared constant when no parameter
-- supplies it (the default/fixed-value case). `value` is the JSON wire form of the
-- scalar constant (the canonical representation the write path coerces to the
-- property's logical type). Ordered by `ordinal`.
create table ontology.action_assignment (
    action_name text  not null references ontology.action (name) on delete cascade,
    ordinal     int   not null,
    property    text  not null,
    value       jsonb not null,
    primary key (action_name, ordinal)
);
