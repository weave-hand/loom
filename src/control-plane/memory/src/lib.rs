//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev. NOT for production use. State is a `HashMap` behind a `Mutex`;
//! a `Tx` stages writes in its own buffer and applies them on commit (drops them
//! on rollback), so uncommitted writes are invisible to other transactions
//! (read-committed semantics).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use control_plane_core::{ControlPlane, Result, Tx};

#[derive(Clone, Default)]
pub struct MemoryControlPlane {
    state: Arc<Mutex<HashMap<String, i64>>>,
}

#[async_trait]
impl ControlPlane for MemoryControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx {
            shared: self.state.clone(),
            staged: HashMap::new(),
        }))
    }
}

struct MemoryTx {
    shared: Arc<Mutex<HashMap<String, i64>>>,
    staged: HashMap<String, i64>,
}

#[async_trait]
impl Tx for MemoryTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        let mut g = self
            .shared
            .lock()
            .expect("control-plane memory mutex poisoned");
        for (k, v) in self.staged {
            g.insert(k, v);
        }
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        // Dropping `self` discards the staged buffer.
        Ok(())
    }

    async fn probe_put(&mut self, key: &str, val: i64) -> Result<()> {
        self.staged.insert(key.to_string(), val);
        Ok(())
    }

    async fn probe_get(&mut self, key: &str) -> Result<Option<i64>> {
        if let Some(v) = self.staged.get(key) {
            return Ok(Some(*v));
        }
        let g = self
            .shared
            .lock()
            .expect("control-plane memory mutex poisoned");
        Ok(g.get(key).copied())
    }
}
