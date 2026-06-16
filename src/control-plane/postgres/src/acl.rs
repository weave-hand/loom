use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, Effect, Page, PageReq, Policy, PolicyTarget, Result,
    RoleId, SubjectId, validate_row_filter,
};

use crate::{PgControlPlane, action_to_str, backend, effect_to_str, target_cols};

#[async_trait]
impl Acl for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_subject(&self, id: &SubjectId) -> Result<()> {
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &id.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_role(&self, id: &RoleId) -> Result<()> {
        sqlx::query!(
            "insert into acl.role (id) values ($1) on conflict (id) do nothing",
            &id.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        let s_exists = sqlx::query_scalar!(
            "select exists (select 1 from acl.subject where id = $1)",
            &subject.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !s_exists {
            return Err(ControlPlaneError::NotFound(format!(
                "subject {}",
                subject.0
            )));
        }
        let r_exists = sqlx::query_scalar!(
            "select exists (select 1 from acl.role where id = $1)",
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        sqlx::query!(
            "insert into acl.role_member (subject_id, role_id) values ($1, $2) \
             on conflict do nothing",
            &subject.0,
            &role.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        sqlx::query!(
            "delete from acl.role_member where subject_id = $1 and role_id = $2",
            &subject.0,
            &role.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        for id in [&role.0, &inherits.0] {
            let exists =
                sqlx::query_scalar!("select exists (select 1 from acl.role where id = $1)", id,)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(backend)?
                    .unwrap_or(false);
            if !exists {
                return Err(ControlPlaneError::NotFound(format!("role {id}")));
            }
        }
        // role -> inherits creates a cycle iff `role` is already reachable from
        // `inherits` (closure of `inherits` includes itself -> catches self-edge).
        // NOTE: this check + the insert below are separate round-trips, not one
        // transaction, so two concurrent add_role_inheritance calls inserting opposite
        // edges of a cycle could both pass. Safe under today's single-writer usage, and
        // harmless regardless: the check/policies_for closure walks dedup (SQL UNION /
        // the memory visited-set), so a cycle merely terminates rather than looping.
        // TODO: wrap in a SERIALIZABLE tx (or lock) if concurrent edge writes ever land.
        let creates_cycle = sqlx::query_scalar!(
            "with recursive clo(role_id) as ( \
                 select $1::text \
                 union \
                 select ri.inherits_id from acl.role_inherits ri \
                   join clo on ri.role_id = clo.role_id \
             ) \
             select exists (select 1 from clo where role_id = $2)",
            &inherits.0,
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if creates_cycle {
            return Err(ControlPlaneError::Conflict(format!(
                "role inheritance {} -> {} would create a cycle",
                role.0, inherits.0
            )));
        }
        sqlx::query!(
            "insert into acl.role_inherits (role_id, inherits_id) values ($1, $2) \
             on conflict do nothing",
            &role.0,
            &inherits.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        sqlx::query!(
            "delete from acl.role_inherits where role_id = $1 and inherits_id = $2",
            &role.0,
            &inherits.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> Result<()> {
        let r_exists = sqlx::query_scalar!(
            "select exists (select 1 from acl.role where id = $1)",
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let (kind, a, b) = target_cols(&target);
        sqlx::query!(
            "insert into acl.role_grant (role_id, action, target_kind, target_a, target_b, effect) \
             values ($1, $2, $3, $4, $5, $6) \
             on conflict (role_id, action, target_kind, target_a, target_b) \
             do update set effect = excluded.effect",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
            effect_to_str(effect),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        let (kind, a, b) = target_cols(target);
        sqlx::query!(
            "delete from acl.role_grant where role_id = $1 and action = $2 \
             and target_kind = $3 and target_a = $4 and target_b = $5",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, policy), level = "debug")]
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()> {
        let r_exists = sqlx::query_scalar!(
            "select exists (select 1 from acl.role where id = $1)",
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        // Best-effort, non-transactional validation (separate round-trips from the
        // insert below, like the role-exists check above): a concurrent type deletion
        // between this check and the insert is tolerated. Fine for current usage.
        if let Some(f) = &policy.row_filter {
            match &policy.target {
                PolicyTarget::Type(name) => {
                    let type_exists = sqlx::query_scalar!(
                        "select exists (select 1 from ontology.object_type where name = $1)",
                        &name.0,
                    )
                    .fetch_one(&self.pool)
                    .await
                    .map_err(backend)?
                    .unwrap_or(false);
                    if !type_exists {
                        return Err(ControlPlaneError::Validation(format!(
                            "policy references unknown type {}",
                            name.0
                        )));
                    }
                    let names = sqlx::query_scalar!(
                        "select name from ontology.property where type_name = $1",
                        &name.0,
                    )
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                    let set: std::collections::HashSet<String> = names.into_iter().collect();
                    validate_row_filter(f, Some(&set)).map_err(ControlPlaneError::Validation)?;
                }
                PolicyTarget::Table(_) => {
                    validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
                }
            }
        }
        let (kind, a, b) = target_cols(&policy.target);
        let row_filter = match &policy.row_filter {
            Some(f) => Some(
                serde_json::to_value(f)
                    .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
            ),
            None => None,
        };
        sqlx::query!(
            "insert into acl.policy \
                 (role_id, action, target_kind, target_a, target_b, row_filter, deny_columns, mask_columns) \
             values ($1, $2, $3, $4, $5, $6, $7, $8) \
             on conflict (role_id, action, target_kind, target_a, target_b) do update set \
                 row_filter = excluded.row_filter, \
                 deny_columns = excluded.deny_columns, \
                 mask_columns = excluded.mask_columns",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
            row_filter,
            &policy.deny_columns,
            &policy.mask_columns,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn clear_policy(
        &self,
        role: &RoleId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<()> {
        let (kind, a, b) = target_cols(target);
        sqlx::query!(
            "delete from acl.policy where role_id = $1 and action = $2 and target_kind = $3 \
             and target_a = $4 and target_b = $5",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
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
        let row = sqlx::query!(
            "with recursive eff(role_id) as ( \
                 select role_id from acl.role_member where subject_id = $1 \
                 union \
                 select ri.inherits_id from acl.role_inherits ri \
                   join eff on ri.role_id = eff.role_id \
             ) \
             select bool_or(g.effect = 'deny') as has_deny, \
                    bool_or(g.effect = 'allow') as has_allow \
             from eff join acl.role_grant g on g.role_id = eff.role_id \
             where g.action = $2 and g.target_kind = $3 \
               and g.target_a = $4 and g.target_b = $5",
            &subject.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok(if row.has_deny == Some(true) {
            Decision::Deny
        } else if row.has_allow == Some(true) {
            Decision::Allow
        } else {
            Decision::Deny
        })
    }

    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        _page: PageReq,
    ) -> Result<Page<Policy>> {
        let (kind, a, b) = target_cols(target);
        let rows = sqlx::query!(
            "with recursive eff(role_id) as ( \
                 select role_id from acl.role_member where subject_id = $1 \
                 union \
                 select ri.inherits_id from acl.role_inherits ri \
                   join eff on ri.role_id = eff.role_id \
             ) \
             select p.row_filter, p.deny_columns, p.mask_columns \
             from eff join acl.policy p on p.role_id = eff.role_id \
             where p.action = $2 and p.target_kind = $3 and p.target_a = $4 and p.target_b = $5",
            &subject.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let row_filter = match r.row_filter {
                Some(v) => Some(
                    serde_json::from_value(v)
                        .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
                ),
                None => None,
            };
            out.push(Policy {
                target: target.clone(),
                row_filter,
                deny_columns: r.deny_columns,
                mask_columns: r.mask_columns,
            });
        }
        Ok(Page::from_full(out))
    }
}
