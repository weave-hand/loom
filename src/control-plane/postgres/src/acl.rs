use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, Policy, PolicyTarget, Result, RoleId, SubjectId,
};
use sqlx::Row as _;

use crate::{PgControlPlane, action_to_str, backend, target_cols};

#[async_trait]
impl Acl for PgControlPlane {
    async fn define_subject(&self, id: &SubjectId) -> Result<()> {
        sqlx::query("insert into acl.subject (id) values ($1) on conflict (id) do nothing")
            .bind(&id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn define_role(&self, id: &RoleId) -> Result<()> {
        sqlx::query("insert into acl.role (id) values ($1) on conflict (id) do nothing")
            .bind(&id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        let s_exists: bool =
            sqlx::query_scalar("select exists (select 1 from acl.subject where id = $1)")
                .bind(&subject.0)
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        if !s_exists {
            return Err(ControlPlaneError::NotFound(format!(
                "subject {}",
                subject.0
            )));
        }
        let r_exists: bool =
            sqlx::query_scalar("select exists (select 1 from acl.role where id = $1)")
                .bind(&role.0)
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        sqlx::query(
            "insert into acl.role_member (subject_id, role_id) values ($1, $2) \
             on conflict do nothing",
        )
        .bind(&subject.0)
        .bind(&role.0)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        sqlx::query("delete from acl.role_member where subject_id = $1 and role_id = $2")
            .bind(&subject.0)
            .bind(&role.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget) -> Result<()> {
        let r_exists: bool =
            sqlx::query_scalar("select exists (select 1 from acl.role where id = $1)")
                .bind(&role.0)
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let (kind, a, b) = target_cols(&target);
        sqlx::query(
            "insert into acl.role_grant (role_id, action, target_kind, target_a, target_b) \
             values ($1, $2, $3, $4, $5) on conflict do nothing",
        )
        .bind(&role.0)
        .bind(action_to_str(action))
        .bind(kind)
        .bind(&a)
        .bind(&b)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        let (kind, a, b) = target_cols(target);
        sqlx::query(
            "delete from acl.role_grant where role_id = $1 and action = $2 \
             and target_kind = $3 and target_a = $4 and target_b = $5",
        )
        .bind(&role.0)
        .bind(action_to_str(action))
        .bind(kind)
        .bind(&a)
        .bind(&b)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn set_policy(&self, role: &RoleId, policy: Policy) -> Result<()> {
        let r_exists: bool =
            sqlx::query_scalar("select exists (select 1 from acl.role where id = $1)")
                .bind(&role.0)
                .fetch_one(&self.pool)
                .await
                .map_err(backend)?;
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let (kind, a, b) = target_cols(&policy.target);
        let row_filter = match &policy.row_filter {
            Some(f) => Some(
                serde_json::to_value(f)
                    .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
            ),
            None => None,
        };
        sqlx::query(
            "insert into acl.policy \
                 (role_id, target_kind, target_a, target_b, row_filter, deny_columns) \
             values ($1, $2, $3, $4, $5, $6) \
             on conflict (role_id, target_kind, target_a, target_b) do update set \
                 row_filter = excluded.row_filter, deny_columns = excluded.deny_columns",
        )
        .bind(&role.0)
        .bind(kind)
        .bind(&a)
        .bind(&b)
        .bind(row_filter)
        .bind(&policy.deny_columns)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn clear_policy(&self, role: &RoleId, target: &PolicyTarget) -> Result<()> {
        let (kind, a, b) = target_cols(target);
        sqlx::query(
            "delete from acl.policy where role_id = $1 and target_kind = $2 \
             and target_a = $3 and target_b = $4",
        )
        .bind(&role.0)
        .bind(kind)
        .bind(&a)
        .bind(&b)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let (kind, a, b) = target_cols(target);
        let allow: bool = sqlx::query_scalar(
            "select exists ( \
                 select 1 from acl.role_member m \
                 join acl.role_grant g on g.role_id = m.role_id \
                 where m.subject_id = $1 and g.action = $2 \
                   and g.target_kind = $3 and g.target_a = $4 and g.target_b = $5)",
        )
        .bind(&subject.0)
        .bind(action_to_str(action))
        .bind(kind)
        .bind(&a)
        .bind(&b)
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok(if allow {
            Decision::Allow
        } else {
            Decision::Deny
        })
    }

    async fn policies_for(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
    ) -> Result<Vec<Policy>> {
        let (kind, a, b) = target_cols(target);
        let rows = sqlx::query(
            "select p.row_filter, p.deny_columns from acl.role_member m \
             join acl.policy p on p.role_id = m.role_id \
             where m.subject_id = $1 and p.target_kind = $2 \
               and p.target_a = $3 and p.target_b = $4",
        )
        .bind(&subject.0)
        .bind(kind)
        .bind(&a)
        .bind(&b)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let raw: Option<serde_json::Value> = r.get("row_filter");
            let row_filter = match raw {
                Some(v) => Some(
                    serde_json::from_value(v)
                        .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
                ),
                None => None,
            };
            out.push(Policy {
                target: target.clone(),
                row_filter,
                deny_columns: r.get("deny_columns"),
            });
        }
        Ok(out)
    }
}
