use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, Effect, Page, PageReq, Policy, PolicyTarget, Result,
    RoleId, SubjectId, validate_row_filter,
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
    grants: HashMap<(String, Action, TargetKey), Effect>, // (role, action, target) -> effect
    policies: HashMap<(String, Action, TargetKey), Policy>, // (role, action, target) -> policy
    inherits: HashSet<(String, String)>, // (role, inherits): role gains inherits's perms
}

impl AclState {
    /// Insert a subject id (idempotent). Used by `create_user` to make a new
    /// user a valid ACL principal.
    pub(crate) fn subjects_insert(&mut self, id: &str) {
        self.subjects.insert(id.to_string());
    }
}

/// True if `target` is reachable from `start` following role->inherits edges
/// (i.e. `start` transitively inherits `target`). Visited-set guards cycles.
fn reaches(edges: &HashSet<(String, String)>, start: &str, target: &str) -> bool {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut stack = vec![start];
    while let Some(r) = stack.pop() {
        if r == target {
            return true;
        }
        if seen.insert(r) {
            for (_, b) in edges.iter().filter(|(a, _)| a == r) {
                stack.push(b);
            }
        }
    }
    false
}

/// The transitive closure of `direct` over role->inherits edges (includes `direct`).
fn effective_roles(
    edges: &HashSet<(String, String)>,
    direct: impl IntoIterator<Item = String>,
) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = direct.into_iter().collect();
    while let Some(r) = stack.pop() {
        if seen.insert(r.clone()) {
            for (_, b) in edges.iter().filter(|(a, _)| a == &r) {
                if !seen.contains(b) {
                    stack.push(b.clone());
                }
            }
        }
    }
    seen
}

impl MemoryControlPlane {
    /// Property names of a defined ontology type, or `None` if undefined.
    fn type_properties(&self, name: &str) -> Option<Vec<String>> {
        let ont = self.ontology.lock();
        ont.types
            .get(name)
            .map(|t| t.properties.iter().map(|p| p.name.clone()).collect())
    }

    /// True if an ontology type with this name is defined.
    fn type_exists(&self, name: &str) -> bool {
        self.ontology.lock().types.contains_key(name)
    }
}

#[async_trait]
impl Acl for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_subject(&self, id: &SubjectId) -> Result<()> {
        self.acl.lock().subjects.insert(id.0.clone());
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_role(&self, id: &RoleId) -> Result<()> {
        self.acl.lock().roles.insert(id.0.clone());
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        let mut acl = self.acl.lock();
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
            .members
            .remove(&(subject.0.clone(), role.0.clone()));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        let mut acl = self.acl.lock();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        if !acl.roles.contains(&inherits.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", inherits.0)));
        }
        if role.0 == inherits.0 || reaches(&acl.inherits, &inherits.0, &role.0) {
            return Err(ControlPlaneError::Conflict(format!(
                "role inheritance {} -> {} would create a cycle",
                role.0, inherits.0
            )));
        }
        acl.inherits.insert((role.0.clone(), inherits.0.clone()));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        self.acl
            .lock()
            .inherits
            .remove(&(role.0.clone(), inherits.0.clone()));
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
        // role-exists check (short acl lock)
        {
            let acl = self.acl.lock();
            if !acl.roles.contains(&role.0) {
                return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
            }
        }
        // A Type target must reference an existing type. Read the ontology map with
        // no acl lock held (acl->ontology order, like set_policy) to avoid a
        // lock-ordering deadlock. Table targets stay unvalidated (deferred).
        if let PolicyTarget::Type(name) = &target
            && !self.type_exists(&name.0)
        {
            return Err(ControlPlaneError::Validation(format!(
                "grant references unknown type `{}`",
                name.0
            )));
        }
        self.acl
            .lock()
            .grants
            .insert((role.0.clone(), action, target_key(&target)), effect);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        self.acl
            .lock()
            .grants
            .remove(&(role.0.clone(), action, target_key(target)));
        Ok(())
    }

    #[tracing::instrument(skip(self, policy), level = "debug")]
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()> {
        // role-exists check (short acl lock)
        {
            let acl = self.acl.lock();
            if !acl.roles.contains(&role.0) {
                return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
            }
        }
        // validation (may lock ontology) — no acl lock held here, to avoid a
        // lock-ordering deadlock between the acl and ontology mutexes. A Type
        // target's existence is checked *always* (independent of row_filter); the
        // row-filter's columns are validated only when a row_filter is present.
        match &policy.target {
            PolicyTarget::Type(name) => {
                let props = self.type_properties(&name.0).ok_or_else(|| {
                    ControlPlaneError::Validation(format!(
                        "policy references unknown type {}",
                        name.0
                    ))
                })?;
                if let Some(f) = &policy.row_filter {
                    let set: HashSet<String> = props.into_iter().collect();
                    validate_row_filter(f, Some(&set)).map_err(ControlPlaneError::Validation)?;
                }
            }
            PolicyTarget::Table(_) => {
                if let Some(f) = &policy.row_filter {
                    validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
                }
            }
        }
        // insert (short acl lock)
        let key = (role.0.clone(), action, target_key(&policy.target));
        self.acl.lock().policies.insert(key, policy);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn clear_policy(
        &self,
        role: &RoleId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<()> {
        self.acl
            .lock()
            .policies
            .remove(&(role.0.clone(), action, target_key(target)));
        Ok(())
    }

    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let acl = self.acl.lock();
        let tk = target_key(target);
        let direct = acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .map(|(_, r)| r.clone());
        let effective = effective_roles(&acl.inherits, direct);
        let mut saw_allow = false;
        for role in &effective {
            match acl.grants.get(&(role.clone(), action, tk.clone())) {
                Some(Effect::Deny) => return Ok(Decision::Deny),
                Some(Effect::Allow) => saw_allow = true,
                None => {}
            }
        }
        Ok(if saw_allow {
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
        let acl = self.acl.lock();
        let tk = target_key(target);
        let direct = acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .map(|(_, r)| r.clone());
        let effective = effective_roles(&acl.inherits, direct);
        Ok(Page::from_full(
            effective
                .iter()
                .filter_map(|role| {
                    acl.policies
                        .get(&(role.clone(), action, tk.clone()))
                        .cloned()
                })
                .collect(),
        ))
    }
}
