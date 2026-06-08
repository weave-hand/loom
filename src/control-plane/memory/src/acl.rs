use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, Policy, PolicyTarget, Result, RoleId, SubjectId,
};

use crate::MemoryControlPlane;

/// Encoded `PolicyTarget` used as a map/set key: `(kind, a, b)`.
type TargetKey = (String, String, String);

fn target_key(t: &PolicyTarget) -> TargetKey {
    match t {
        PolicyTarget::Type(n) => ("type".into(), n.0.clone(), String::new()),
        PolicyTarget::Table(r) => ("table".into(), r.schema.clone(), r.name.clone()),
    }
}

#[derive(Default)]
pub(crate) struct AclState {
    subjects: HashSet<String>,
    roles: HashSet<String>,
    members: HashSet<(String, String)>, // (subject, role)
    grants: HashSet<(String, Action, TargetKey)>, // (role, action, target)
    policies: HashMap<(String, TargetKey), Policy>, // (role, target) -> policy
}

#[async_trait]
impl Acl for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_subject(&self, id: &SubjectId) -> Result<()> {
        self.acl.lock().unwrap().subjects.insert(id.0.clone());
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_role(&self, id: &RoleId) -> Result<()> {
        self.acl.lock().unwrap().roles.insert(id.0.clone());
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.subjects.contains(&subject.0) {
            return Err(ControlPlaneError::NotFound(format!(
                "subject {}",
                subject.0
            )));
        }
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        acl.members.insert((subject.0.clone(), role.0.clone()));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .members
            .remove(&(subject.0.clone(), role.0.clone()));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        acl.grants
            .insert((role.0.clone(), action, target_key(&target)));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .grants
            .remove(&(role.0.clone(), action, target_key(target)));
        Ok(())
    }

    #[tracing::instrument(skip(self, policy), level = "debug")]
    async fn set_policy(&self, role: &RoleId, policy: Policy) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let key = (role.0.clone(), target_key(&policy.target));
        acl.policies.insert(key, policy);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn clear_policy(&self, role: &RoleId, target: &PolicyTarget) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .policies
            .remove(&(role.0.clone(), target_key(target)));
        Ok(())
    }

    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        let allow = acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .any(|(_, role)| acl.grants.contains(&(role.clone(), action, tk.clone())));
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
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        Ok(acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .filter_map(|(_, role)| acl.policies.get(&(role.clone(), tk.clone())).cloned())
            .collect())
    }
}
