use async_trait::async_trait;
use control_plane_core::{
    ActionDef, ActionName, Aggregation, ControlPlaneError, DerivedPropertyDef, LinkBacking,
    LinkDef, ObjectType, Ontology, Page, PageReq, ParamDef, PropertyDef, Result, TableRef,
    TypeName,
};

use crate::{PgControlPlane, backend, cardinality_from_str, cardinality_to_str};

#[async_trait]
impl Ontology for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
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
            sqlx::query!(
                "insert into ontology.property (type_name, ordinal, name, ty, required) \
                 values ($1, $2, $3, $4, $5)",
                ty.name.0,
                i as i32,
                p.name,
                p.ty,
                p.required,
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
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_link(&self, link: LinkDef) -> Result<()> {
        for endpoint in [&link.from, &link.to] {
            let exists: bool = sqlx::query_scalar!(
                "select exists (select 1 from ontology.object_type where name = $1)",
                endpoint.0,
            )
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?
            .unwrap_or(false);
            if !exists {
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
            cardinality_to_str(link.cardinality),
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
        let props = sqlx::query!(
            "select name, ty, required from ontology.property \
             where type_name = $1 order by ordinal",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
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
            properties: props
                .into_iter()
                .map(|r| PropertyDef {
                    name: r.name,
                    ty: r.ty,
                    required: r.required,
                })
                .collect(),
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
        let exists: bool = sqlx::query_scalar!(
            "select exists (select 1 from ontology.object_type where name = $1)",
            name.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !exists {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query!(
            "select name, from_type, to_type, cardinality, backing_kind, from_column, \
                    to_column, from_key, to_key, join_table_schema, join_table_name \
             from ontology.link where from_type = $1",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(Page::from_full(
            rows.into_iter()
                .map(|r| LinkDef {
                    name: r.name,
                    from: TypeName(r.from_type),
                    to: TypeName(r.to_type),
                    cardinality: cardinality_from_str(r.cardinality.as_str()),
                    backing: backing_from_row(
                        &r.backing_kind,
                        r.from_column,
                        r.to_column,
                        r.from_key,
                        r.to_key,
                        r.join_table_schema,
                        r.join_table_name,
                    ),
                })
                .collect(),
        ))
    }

    async fn links_to(&self, name: &TypeName, _page: PageReq) -> Result<Page<LinkDef>> {
        let exists: bool = sqlx::query_scalar!(
            "select exists (select 1 from ontology.object_type where name = $1)",
            name.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !exists {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query!(
            "select name, from_type, to_type, cardinality, backing_kind, from_column, \
                    to_column, from_key, to_key, join_table_schema, join_table_name \
             from ontology.link where to_type = $1",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(Page::from_full(
            rows.into_iter()
                .map(|r| LinkDef {
                    name: r.name,
                    from: TypeName(r.from_type),
                    to: TypeName(r.to_type),
                    cardinality: cardinality_from_str(r.cardinality.as_str()),
                    backing: backing_from_row(
                        &r.backing_kind,
                        r.from_column,
                        r.to_column,
                        r.from_key,
                        r.to_key,
                        r.join_table_schema,
                        r.join_table_name,
                    ),
                })
                .collect(),
        ))
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
        let mut tx = self.pool.begin().await.map_err(backend)?;
        // The target type must exist. The explicit check makes the error a clear
        // `Validation` (matching the memory fake) instead of a raw FK backend error;
        // the FK (0009_actions.sql) stays as the atomic backstop inside this tx.
        let target_exists = sqlx::query_scalar!(
            "select exists (select 1 from ontology.object_type where name = $1)",
            action.target.0,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !target_exists {
            return Err(ControlPlaneError::Validation(format!(
                "action `{}` references unknown target type `{}`",
                action.name.0, action.target.0
            )));
        }
        sqlx::query!(
            "insert into ontology.action (name, target_type) values ($1, $2) \
             on conflict (name) do update set target_type = excluded.target_type",
            action.name.0,
            action.target.0,
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
        for (i, p) in action.parameters.iter().enumerate() {
            sqlx::query!(
                "insert into ontology.action_param (action_name, ordinal, name, ty, required) \
                 values ($1, $2, $3, $4, $5)",
                action.name.0,
                i as i32,
                p.name,
                p.ty,
                p.required,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn get_action(&self, name: &ActionName) -> Result<ActionDef> {
        let row = sqlx::query!(
            "select target_type from ontology.action where name = $1",
            name.0,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        let params = sqlx::query!(
            "select name, ty, required from ontology.action_param \
             where action_name = $1 order by ordinal",
            name.0,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(ActionDef {
            name: name.clone(),
            target: TypeName(row.target_type),
            parameters: params
                .into_iter()
                .map(|r| ParamDef {
                    name: r.name,
                    ty: r.ty,
                    required: r.required,
                })
                .collect(),
        })
    }
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

#[allow(clippy::too_many_arguments, reason = "backing_from_row maps 7 independent row columns that cannot be meaningfully grouped")]
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
