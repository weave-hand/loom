use async_trait::async_trait;
use control_plane_core::{
    ActionDef, ActionName, ActionStep, Aggregation, Assignment, AssignmentSource,
    ControlPlaneError, DerivedPropertyDef, IndexSpec, LinkBacking, LinkDef, ObjectType, Ontology,
    Page, PageReq, ParamDef, PropertyDef, Result, TableRef, TypeName, VectorIndexDef,
};
use sqlx::{AssertSqlSafe, PgPool};

use crate::{PgControlPlane, backend};

#[async_trait]
impl Ontology for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        // Reject malformed constraint declarations (type-inapplicable rule or invalid
        // regex) before any write — a define-time fault, never a silent write-time one.
        control_plane_core::validate_constraints(&ty.properties)?;
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // Source guard: read the type's PRIOR backing table (if any) before the upsert so
        // we emit the binding edge only when the type is new or its table changed. Reuses
        // `resolve`'s exact SQL string, so it shares that query's committed `.sqlx` cache
        // entry — no cache regeneration.
        let prior = sqlx::query!(
            "select table_schema, table_name from ontology.object_type where name = $1",
            ty.name.0,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        let binding_changed = match &prior {
            Some(r) => r.table_schema != ty.table.schema || r.table_name != ty.table.name,
            None => true,
        };
        sqlx::query!(
            "insert into ontology.object_type (name, table_schema, table_name, identity) \
             values ($1, $2, $3, $4) \
             on conflict (name) do update set table_schema = excluded.table_schema, \
                 table_name = excluded.table_name, identity = excluded.identity",
            ty.name.0,
            ty.table.schema,
            ty.table.name,
            ty.identity,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "delete from ontology.property where type_name = $1",
            ty.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        for (i, p) in ty.properties.iter().enumerate() {
            // Empty constraints (the common case) store NULL, not an empty JSON object,
            // keeping back-compat rows and unconstrained properties byte-identical.
            let constraints = if p.constraints.is_empty() {
                None
            } else {
                Some(
                    serde_json::to_value(&p.constraints)
                        .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
                )
            };
            sqlx::query!(
                "insert into ontology.property (type_name, ordinal, name, ty, required, constraints) \
                 values ($1, $2, $3, $4, $5, $6)",
                ty.name.0,
                i as i32,
                p.name,
                p.ty,
                p.required,
                constraints,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        sqlx::query!(
            "delete from ontology.derived_property where type_name = $1",
            ty.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        for (i, d) in ty.derived.iter().enumerate() {
            let (kind, column) = agg_parts(&d.agg);
            sqlx::query!(
                "insert into ontology.derived_property \
                 (type_name, ordinal, name, ty, link_name, agg_kind, agg_column) \
                 values ($1, $2, $3, $4, $5, $6, $7)",
                ty.name.0,
                i as i32,
                d.name,
                d.ty,
                d.link,
                kind,
                column,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        if binding_changed {
            crate::lineage::pg_emit(&mut *tx, &control_plane_core::type_table_binding_event(&ty))
                .await?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_link(&self, link: LinkDef) -> Result<()> {
        for endpoint in [&link.from, &link.to] {
            if !object_type_exists(&self.pool, &endpoint.0).await? {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        let bc = backing_cols(&link.backing);
        sqlx::query!(
            "insert into ontology.link \
               (name, from_type, to_type, cardinality, backing_kind, from_column, \
                to_column, from_key, to_key, join_table_schema, join_table_name) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             on conflict (name, from_type) do update set \
               to_type = excluded.to_type, cardinality = excluded.cardinality, \
               backing_kind = excluded.backing_kind, from_column = excluded.from_column, \
               to_column = excluded.to_column, from_key = excluded.from_key, \
               to_key = excluded.to_key, join_table_schema = excluded.join_table_schema, \
               join_table_name = excluded.join_table_name",
            link.name,
            link.from.0,
            link.to.0,
            link.cardinality.as_str(),
            bc.kind,
            bc.from_column,
            bc.to_column,
            bc.from_key,
            bc.to_key,
            bc.join_schema,
            bc.join_name,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        let row = sqlx::query!(
            "select table_schema, table_name, identity from ontology.object_type where name = $1",
            name.0,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        let prop_rows = sqlx::query!(
            "select name, ty, required, constraints from ontology.property \
             where type_name = $1 order by ordinal",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut props = Vec::with_capacity(prop_rows.len());
        for r in prop_rows {
            let constraints = match r.constraints {
                Some(v) => serde_json::from_value(v)
                    .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
                None => control_plane_core::PropertyConstraints::default(),
            };
            props.push(PropertyDef {
                name: r.name,
                ty: r.ty,
                required: r.required,
                constraints,
            });
        }
        let derived_rows = sqlx::query!(
            "select name, ty, link_name, agg_kind, agg_column from ontology.derived_property \
             where type_name = $1 order by ordinal",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut derived = Vec::with_capacity(derived_rows.len());
        for r in derived_rows {
            derived.push(DerivedPropertyDef {
                name: r.name,
                ty: r.ty,
                link: r.link_name,
                agg: rebuild_agg(&r.agg_kind, r.agg_column)?,
            });
        }
        Ok(ObjectType {
            name: name.clone(),
            table: TableRef {
                schema: row.table_schema,
                name: row.table_name,
            },
            properties: props,
            derived,
            identity: row.identity,
        })
    }

    async fn list_types(&self, _page: PageReq) -> Result<Page<ObjectType>> {
        let names = sqlx::query_scalar!("select name from ontology.object_type")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(self.get_type(&TypeName(n)).await?);
        }
        Ok(Page::from_full(out))
    }

    async fn links(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        if !object_type_exists(&self.pool, &name.0).await? {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query_as!(
            LinkRow,
            "select name, from_type, to_type, cardinality, backing_kind, from_column, \
                    to_column, from_key, to_key, join_table_schema, join_table_name \
             from ontology.link where from_type = $1",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        link_defs(rows)
    }

    async fn links_to(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        if !object_type_exists(&self.pool, &name.0).await? {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query_as!(
            LinkRow,
            "select name, from_type, to_type, cardinality, backing_kind, from_column, \
                    to_column, from_key, to_key, join_table_schema, join_table_name \
             from ontology.link where to_type = $1",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        link_defs(rows)
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        let row = sqlx::query!(
            "select table_schema, table_name from ontology.object_type where name = $1",
            name.0,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        Ok(TableRef {
            schema: row.table_schema,
            name: row.table_name,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_action(&self, action: ActionDef) -> Result<()> {
        if action.steps.is_empty() {
            return Err(ControlPlaneError::Validation(format!(
                "action `{}` has no steps",
                action.name.0
            )));
        }
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // Every step's target type must exist. The explicit check makes the error a clear
        // `Validation` (matching the memory fake) instead of a raw FK backend error; the FK
        // (0030_action_steps.sql) stays as the atomic backstop inside this tx.
        for step in &action.steps {
            if !object_type_exists(&mut *tx, &step.target.0).await? {
                return Err(ControlPlaneError::Validation(format!(
                    "action `{}` references unknown target type `{}`",
                    action.name.0, step.target.0
                )));
            }
        }
        // The `action` row now carries only `name`; target_type/kind live per-step.
        sqlx::query!(
            "insert into ontology.action (name) values ($1) on conflict (name) do nothing",
            action.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        // Clear-then-insert for idempotent redefine (steps cascade-clear their params/
        // assignments, but the explicit deletes keep the pattern self-contained and total).
        sqlx::query!(
            "delete from ontology.action_assignment where action_name = $1",
            action.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "delete from ontology.action_param where action_name = $1",
            action.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "delete from ontology.action_step where action_name = $1",
            action.name.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        for (si, step) in action.steps.iter().enumerate() {
            let step_ordinal = si as i32;
            sqlx::query!(
                "insert into ontology.action_step (action_name, ordinal, target_type, kind, bind) \
                 values ($1, $2, $3, $4, $5)",
                action.name.0,
                step_ordinal,
                step.target.0,
                step.kind.as_str(),
                step.bind,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            for (i, p) in step.parameters.iter().enumerate() {
                sqlx::query!(
                    "insert into ontology.action_param \
                     (action_name, step_ordinal, ordinal, name, ty, required, binds) \
                     values ($1, $2, $3, $4, $5, $6, $7)",
                    action.name.0,
                    step_ordinal,
                    i as i32,
                    p.name,
                    p.ty,
                    p.required,
                    p.binds,
                )
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
            }
            for (i, a) in step.assignments.iter().enumerate() {
                // Task 2 handles Const/Expr; the ref_bind/ref_prop columns exist for
                // Task 3's StepRef and stay NULL here.
                let (value, expr): (Option<serde_json::Value>, Option<String>) = match &a.source {
                    AssignmentSource::Const(v) => (Some(v.clone()), None),
                    AssignmentSource::Expr(s) => (None, Some(s.clone())),
                };
                let ref_bind: Option<String> = None;
                let ref_prop: Option<String> = None;
                sqlx::query!(
                    "insert into ontology.action_assignment \
                     (action_name, step_ordinal, ordinal, property, value, expr, ref_bind, ref_prop) \
                     values ($1, $2, $3, $4, $5, $6, $7, $8)",
                    action.name.0,
                    step_ordinal,
                    i as i32,
                    a.property,
                    value,
                    expr,
                    ref_bind,
                    ref_prop,
                )
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
            }
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        // The `action` row now carries only `name`; existence is the NotFound gate.
        if !action_exists(&self.pool, &name.0).await? {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let step_rows = sqlx::query!(
            "select ordinal, target_type, kind, bind from ontology.action_step \
             where action_name = $1 order by ordinal",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut steps = Vec::with_capacity(step_rows.len());
        for sr in step_rows {
            let params = sqlx::query!(
                "select name, ty, required, binds from ontology.action_param \
                 where action_name = $1 and step_ordinal = $2 order by ordinal",
                name.0,
                sr.ordinal,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
            let assignment_rows = sqlx::query!(
                "select property, value, expr, ref_bind, ref_prop from ontology.action_assignment \
                 where action_name = $1 and step_ordinal = $2 order by ordinal",
                name.0,
                sr.ordinal,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
            steps.push(ActionStep {
                target: TypeName(sr.target_type),
                kind: sr.kind.parse()?,
                parameters: params
                    .into_iter()
                    .map(|r| ParamDef {
                        name: r.name,
                        ty: r.ty,
                        required: r.required,
                        binds: r.binds,
                    })
                    .collect(),
                assignments: assignment_rows
                    .into_iter()
                    .map(|r| Assignment {
                        property: r.property,
                        // Task 2 reconstructs Const/Expr; the ref_bind/ref_prop pair (Task 3's
                        // StepRef) is always NULL here. The CHECK constraint (0030) guarantees
                        // exactly one source is set, so the fallthrough is unreachable in
                        // practice; map it to a Null constant to keep the mapping total.
                        source: match (r.value, r.expr) {
                            (_, Some(e)) => AssignmentSource::Expr(e),
                            (Some(v), None) => AssignmentSource::Const(v),
                            (None, None) => AssignmentSource::Const(serde_json::Value::Null),
                        },
                    })
                    .collect(),
                bind: sr.bind,
            });
        }
        Ok(ActionDef {
            name: name.clone(),
            steps,
        })
    }

    async fn define_vector_index(&self, def: VectorIndexDef) -> Result<()> {
        let prop_ty: Option<String> = sqlx::query_scalar!(
            "select ty from ontology.property where type_name = $1 and name = $2",
            def.type_name.0,
            def.property,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        match prop_ty {
            Some(t) if t.starts_with("vector(") => {}
            Some(_) => {
                return Err(ControlPlaneError::Validation(format!(
                    "property `{}` on type `{}` is not a vector type",
                    def.property, def.type_name.0
                )));
            }
            None => {
                return Err(ControlPlaneError::Validation(format!(
                    "type `{}` has no property `{}`",
                    def.type_name.0, def.property
                )));
            }
        }
        let (kind, nlist, m, ef) = def.spec.as_cols();
        sqlx::query!(
            "insert into ontology.vector_index_definition \
               (type_name, name, property_name, metric, index_kind, nlist, m, ef_construction) \
             values ($1, $2, $3, $4, $5, $6, $7, $8) \
             on conflict (type_name, name) do update set \
               property_name = excluded.property_name, metric = excluded.metric, \
               index_kind = excluded.index_kind, nlist = excluded.nlist, \
               m = excluded.m, ef_construction = excluded.ef_construction",
            def.type_name.0,
            def.name,
            def.property,
            def.metric.as_str(),
            kind,
            nlist.map(|v| v as i32),
            m.map(|v| v as i32),
            ef.map(|v| v as i32),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn get_vector_index(
        &self,
        type_name: &TypeName,
        name: &str,
    ) -> Result<Option<VectorIndexDef>> {
        vector_index_def_row(&self.pool, &type_name.0, name).await
    }

    async fn vector_indexes_for(&self, type_name: &TypeName) -> Result<Vec<VectorIndexDef>> {
        let rows = sqlx::query!(
            "select name, property_name, metric, index_kind, nlist, m, ef_construction \
             from ontology.vector_index_definition where type_name = $1",
            type_name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(VectorIndexDef {
                name: r.name,
                type_name: type_name.clone(),
                property: r.property_name,
                metric: r.metric.parse()?,
                spec: IndexSpec::from_label(
                    Some(r.index_kind.as_str()),
                    r.nlist.map(|v| v as u32),
                    r.m.map(|v| v as u32),
                    r.ef_construction.map(|v| v as u32),
                )?,
            });
        }
        Ok(out)
    }
}

/// Reverse-lookup: the identity column name for the object type stored at `table`,
/// or None if it has no declared identity / does not exist. Used by the engine
/// serving read to make merge-on-read identity-aware without a wire round-trip.
// AssertSqlSafe: static query against ontology.object_type; sqlx regen unavailable
// in this env (initdb-as-root). Convert to query! when regenerating locally.
pub async fn identity_for_table(pool: &PgPool, table: &TableRef) -> Result<Option<String>> {
    let row: Option<Option<String>> = sqlx::query_scalar(AssertSqlSafe(
        "select identity from ontology.object_type \
         where table_schema = $1 and table_name = $2",
    ))
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    // `identity` column is itself nullable → flatten Option<Option<String>>.
    Ok(row.flatten())
}

/// Read one named vector-index declaration as a `VectorIndexDef`. Shared by the
/// `Ontology::get_vector_index` impl and the build primitive (which has a raw pool).
pub async fn vector_index_def_row(
    pool: &sqlx::PgPool,
    type_name: &str,
    name: &str,
) -> Result<Option<VectorIndexDef>> {
    let row = sqlx::query!(
        "select property_name, metric, index_kind, nlist, m, ef_construction \
         from ontology.vector_index_definition where type_name = $1 and name = $2",
        type_name,
        name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    let Some(r) = row else { return Ok(None) };
    Ok(Some(VectorIndexDef {
        name: name.to_string(),
        type_name: TypeName(type_name.to_string()),
        property: r.property_name,
        metric: r.metric.parse()?,
        spec: IndexSpec::from_label(
            Some(r.index_kind.as_str()),
            r.nlist.map(|v| v as u32),
            r.m.map(|v| v as u32),
            r.ef_construction.map(|v| v as u32),
        )?,
    }))
}

/// True if an ontology object type named `name` exists. THE single existence
/// probe shared by the ontology reads/writes (`define_link`, `links`,
/// `links_to`, `define_action`) and the ACL write-time target checks
/// (`grant`, `set_policy`) — previously six verbatim copies of the same
/// `select exists` query.
pub(crate) async fn object_type_exists(ex: impl sqlx::PgExecutor<'_>, name: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        "select exists (select 1 from ontology.object_type where name = $1)",
        name,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?
    .unwrap_or(false))
}

/// True if an action named `name` exists. The `action` row now carries only its
/// name (target_type/kind moved to `action_step` in 0030), so existence is the
/// sole NotFound gate for `get_action`.
pub(crate) async fn action_exists(ex: impl sqlx::PgExecutor<'_>, name: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        "select exists (select 1 from ontology.action where name = $1)",
        name,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?
    .unwrap_or(false))
}

/// One `ontology.link` row. `links` and `links_to` run the same projection,
/// differing only in which endpoint column they filter on — `query_as!` into
/// this named row lets them share one mapping (`link_defs`).
struct LinkRow {
    name: String,
    from_type: String,
    to_type: String,
    cardinality: String,
    backing_kind: String,
    from_column: String,
    to_column: String,
    from_key: Option<String>,
    to_key: Option<String>,
    join_table_schema: Option<String>,
    join_table_name: Option<String>,
}

/// Map fetched link rows into a full (unpaginated) `Page<LinkDef>` — the
/// shared tail of `links`/`links_to`. Errors on a corrupt cardinality token.
fn link_defs(rows: Vec<LinkRow>) -> Result<Page<LinkDef>> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(LinkDef {
            name: r.name,
            from: TypeName(r.from_type),
            to: TypeName(r.to_type),
            cardinality: r.cardinality.parse()?,
            backing: backing_from_row(
                &r.backing_kind,
                r.from_column,
                r.to_column,
                r.from_key,
                r.to_key,
                r.join_table_schema,
                r.join_table_name,
            ),
        });
    }
    Ok(Page::from_full(out))
}

/// Split an aggregation into its persisted `(agg_kind, agg_column)` pair.
fn agg_parts(a: &Aggregation) -> (&'static str, Option<&str>) {
    match a {
        Aggregation::Count => ("count", None),
        Aggregation::Sum(c) => ("sum", Some(c.as_str())),
        Aggregation::Avg(c) => ("avg", Some(c.as_str())),
        Aggregation::Min(c) => ("min", Some(c.as_str())),
        Aggregation::Max(c) => ("max", Some(c.as_str())),
    }
}

/// Reconstruct an [`Aggregation`] from its persisted `(agg_kind, agg_column)` pair.
fn rebuild_agg(kind: &str, column: Option<String>) -> Result<Aggregation> {
    let col = || {
        column.clone().ok_or_else(|| {
            ControlPlaneError::Backend(format!("derived agg '{kind}' missing column").into())
        })
    };
    Ok(match kind {
        "count" => Aggregation::Count,
        "sum" => Aggregation::Sum(col()?),
        "avg" => Aggregation::Avg(col()?),
        "min" => Aggregation::Min(col()?),
        "max" => Aggregation::Max(col()?),
        other => {
            return Err(ControlPlaneError::Backend(
                format!("unknown derived agg kind '{other}'").into(),
            ));
        }
    })
}

/// The persisted column values for a link's physical backing.
struct BackingCols<'a> {
    kind: &'a str,
    from_column: &'a str,
    to_column: &'a str,
    from_key: Option<&'a str>,
    to_key: Option<&'a str>,
    join_schema: Option<&'a str>,
    join_name: Option<&'a str>,
}

fn backing_cols(b: &LinkBacking) -> BackingCols<'_> {
    match b {
        LinkBacking::ForeignKey {
            from_column,
            to_column,
        } => BackingCols {
            kind: "fk",
            from_column,
            to_column,
            from_key: None,
            to_key: None,
            join_schema: None,
            join_name: None,
        },
        LinkBacking::JoinTable {
            table,
            from_key,
            from_column,
            to_column,
            to_key,
        } => BackingCols {
            kind: "join_table",
            from_column,
            to_column,
            from_key: Some(from_key),
            to_key: Some(to_key),
            join_schema: Some(&table.schema),
            join_name: Some(&table.name),
        },
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "backing_from_row maps 7 independent row columns that cannot be meaningfully grouped"
)]
fn backing_from_row(
    kind: &str,
    from_column: String,
    to_column: String,
    from_key: Option<String>,
    to_key: Option<String>,
    join_schema: Option<String>,
    join_name: Option<String>,
) -> LinkBacking {
    match kind {
        "join_table" => LinkBacking::JoinTable {
            table: TableRef {
                schema: join_schema.unwrap_or_default(),
                name: join_name.unwrap_or_default(),
            },
            from_key: from_key.unwrap_or_default(),
            from_column,
            to_column,
            to_key: to_key.unwrap_or_default(),
        },
        _ => LinkBacking::ForeignKey {
            from_column,
            to_column,
        },
    }
}
