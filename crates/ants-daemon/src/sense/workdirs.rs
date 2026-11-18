//! Resolve sense's file access through the sessions already kept by the ant's MCP server.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ants_proto::UserId;
use ants_proto::provider::CallContext;
use ants_sense::Workdirs;

use crate::sync::MutexExt as _;

/// The workdirs of the ants a process serves, each resolved by its MCP server; clones share them.
#[derive(Clone, Default)]
pub(crate) struct Directories(Arc<Mutex<BTreeMap<UserId, Arc<dyn Workdirs>>>>);

impl Workdirs for Directories {
    fn workdir(&self, context: &CallContext) -> Option<PathBuf> {
        let resolver = self.0.locked().get(&context.ant).cloned()?;
        resolver.workdir(context)
    }
}

pub(crate) struct Serving {
    served: Directories,
    ant: UserId,
    resolver: Arc<dyn Workdirs>,
}

pub(crate) fn serve(served: &Directories, ant: &UserId, resolver: Arc<dyn Workdirs>) -> Serving {
    served.0.locked().insert(ant.clone(), resolver.clone());
    Serving { served: served.clone(), ant: ant.clone(), resolver }
}

impl Drop for Serving {
    fn drop(&mut self) {
        let mut served = self.served.0.locked();
        if served.get(&self.ant).is_some_and(|resolver| Arc::ptr_eq(resolver, &self.resolver)) {
            served.remove(&self.ant);
        }
    }
}

impl Workdirs for crate::mcp::Directories {
    fn workdir(&self, context: &CallContext) -> Option<PathBuf> {
        crate::mcp::Directories::workdir(self, context)
    }
}
