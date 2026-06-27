alter table ontology.action
    add column kind text not null default 'insert';
