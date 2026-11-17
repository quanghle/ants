//! The sign-ins to MCP servers of a thread's sessions, for a harness whose definition lists them (`mcp_sign_in`): the
//! translator reports each as it finds it, as a session opens and whenever a server later needs one, and the daemon
//! tells a human of those needing a browser or failing.

use std::fmt;
use std::sync::Arc;

use ants_proto::ThreadId;

/// How a sign-in to an MCP server came out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpSignIn {
    /// Signed in, with credentials the harness keeps.
    SignedIn { server: String },
    /// Waiting for a browser to open `url` on the ant's machine.
    Browser { server: String, url: String },
    /// Failed with `error`.
    Failed { server: String, error: String },
}

impl McpSignIn {
    pub fn server(&self) -> &str {
        match self {
            Self::SignedIn { server } | Self::Browser { server, .. } | Self::Failed { server, .. } => server,
        }
    }
}

/// Where the sign-ins of a thread's sessions go: the thread, where the harness keeps the credentials (two harnesses
/// keeping them in one place sign in alike), and the sign-ins.
pub type McpSignIns = Arc<dyn Fn(&ThreadId, &str, Vec<McpSignIn>) + Send + Sync>;

/// Where the sign-ins of the sessions of one thread go ([`McpSignIns`]).
#[derive(Clone)]
pub(crate) struct Report {
    thread: ThreadId,
    sink: McpSignIns,
}

impl Report {
    pub(crate) fn new(thread: ThreadId, sink: McpSignIns) -> Self {
        Self { thread, sink }
    }

    /// Hand over `sign_ins`, of a harness keeping its credentials in `store`.
    pub(crate) fn send(&self, store: &str, sign_ins: Vec<McpSignIn>) {
        if !sign_ins.is_empty() {
            (self.sink)(&self.thread, store, sign_ins);
        }
    }
}

impl PartialEq for Report {
    fn eq(&self, other: &Self) -> bool {
        self.thread == other.thread && Arc::ptr_eq(&self.sink, &other.sink)
    }
}

impl Eq for Report {}

impl fmt::Debug for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Report").field("thread", &self.thread).finish_non_exhaustive()
    }
}
