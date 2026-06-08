use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, LinkDef, ObjectType, Ontology, PropertyDef, Result, TableRef, TypeName,
};
use sqlx::Row as _;

use crate::{PgControlPlane, backend, cardinality_from_str, cardinality_to_str};

#[async_trait]
impl Ontology for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query(
            "insert into ontology.object_type (name, table_schema, table_name) \
             values ($1, $2, $3) \
             on conflict (name) do update set table_schema = excluded.table_schema, \
                 table_name = excluded.table_name",
        )
        .bind(&ty.name.0)
        .bind(&ty.table.schema)
        .bind(&ty.table.name)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query("delete from ontology.property where type_name = $1")
            .bind(&ty.name.0)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        for (i, p) in ty.properties.iter().enumerate() {
            sqlx::query(
                "insert into ontology.property (type_name, ordinal, name, ty, required) \
                 values ($1, $2, $3, $4, $5)",
            )
            .bind(&ty.name.0)
            .bind(i as i32)
            .bind(&p.name)
            .bind(&p.ty)
            .bind(p.required)
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
            let exists: bool = sqlx::query_scalar(
                "select exists (select 1 from ontology.object_type where name = $1)",
            )
            .bind(&endpoint.0)
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?;
            if !exists {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        sqlx::query(
            "insert into ontology.link (name, from_type, to_type, cardinality) \
             values ($1, $2, $3, $4) \
             on conflict (name, from_type) do update set to_type = excluded.to_type, \
                 cardinality = excluded.cardinality",
        )
        .bind(&link.name)
        .bind(&link.from.0)
        .bind(&link.to.0)
        .bind(cardinality_to_str(link.cardinality))
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        let row = sqlx::query(
            "select table_schema, table_name from ontology.object_type where name = $1",
        )
        .bind(&name.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        let props = sqlx::query(
            "select name, ty, required from ontology.property \
             where type_name = $1 order by ordinal",
        )
        .bind(&name.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(ObjectType {
            name: name.clone(),
            table: TableRef {
                schema: row.get("table_schema"),
                name: row.get("table_name"),
            },
            properties: props
                .iter()
                .map(|r| PropertyDef {
                    name: r.get("name"),
                    ty: r.get("ty"),
                    required: r.get("required"),
                })
                .collect(),
        })
    }

    async fn list_types(&self) -> Result<Vec<ObjectType>> {
        let names: Vec<String> = sqlx::query_scalar("select name from ontology.object_type")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(self.get_type(&TypeName(n)).await?);
        }
        Ok(out)
    }

    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>> {
        let exists: bool = sqlx::query_scalar(
            "select exists (select 1 from ontology.object_type where name = $1)",
        )
        .bind(&name.0)
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        if !exists {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        let rows = sqlx::query(
            "select name, from_type, to_type, cardinality from ontology.link where from_type = $1",
        )
        .bind(&name.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| LinkDef {
                name: r.get("name"),
                from: TypeName(r.get("from_type")),
                to: TypeName(r.get("to_type")),
                cardinality: cardinality_from_str(r.get::<String, _>("cardinality").as_str()),
            })
            .collect())
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        let row = sqlx::query(
            "select table_schema, table_name from ontology.object_type where name = $1",
        )
        .bind(&name.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))?;
        Ok(TableRef {
            schema: row.get("table_schema"),
            name: row.get("table_name"),
        })
    }
}
