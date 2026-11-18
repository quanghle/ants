//! Resolve sense's file access through the sessions already kept by the ant's MCP server.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use ants_proto::UserId;
use ants_proto::provider::CallContext;
use ants_sense::Workdirs;

use crate::sync::MutexExt as _;

static SERVED: Mutex<BTreeMap<UserId, Arc<dyn Workdirs>>> = Mutex::new(BTreeMap::new());

pub(crate) struct Directories;

impl Workdirs for Directories {
    fn workdir(&self, context: &CallContext) -> Option<PathBuf> {
        let resolver = SERVED.locked().get(&context.ant).cloned()?;
        resolver.workdir(context)
    }
}

pub(crate) struct Serving {
    ant: UserId,
    resolver: Arc<dyn Workdirs>,
}

pub(crate) fn serve(ant: &UserId, resolver: Arc<dyn Workdirs>) -> Serving {
    SERVED.locked().insert(ant.clone(), resolver.clone());
    Serving { ant: ant.clone(), resolver }
}

impl Drop for Serving {
    fn drop(&mut self) {
        let mut served = SERVED.locked();
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
