//! Telling a human of the sign-ins to MCP servers the ant's sessions could not make ([`crate::harness::McpSignIn`]):
//! one waiting for a browser on the ant's machine, or one that failed. A prompt on the thread of the session making it
//! tells the owner of the thread, else the default human, once for each server of a harness's credentials and outcome
//! until the server signs in again, and again for another browser sign-in's address. The prompt answers meanwhile,
//! without the server.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use ants_proto::{NoticeKind, ThreadId, UserId};
use tokio::sync::watch;

use crate::bus;
use crate::harness::{McpSignIn, McpSignIns};
use crate::sync::MutexExt as _;

use super::{Inbox, Incoming};

/// The most sign-ins kept for prompts yet to tell them.
const KEPT: usize = 64;

/// The most characters of an error a notice quotes.
const ERROR_CHARS: usize = 500;

/// The sign-ins the ant's sessions made, until told.
#[derive(Clone)]
pub(super) struct Untold {
    state: Arc<Mutex<State>>,
    /// Counts the sign-ins added, waking the prompts that tell them.
    added: Arc<watch::Sender<u64>>,
}

#[derive(Default)]
struct State {
    /// By thread and where the harness keeping the credentials keeps them.
    untold: VecDeque<(ThreadId, String, McpSignIn)>,
    /// What the last notice of each server told, by where the credentials are kept, since it last signed in.
    told: HashMap<(String, String), McpSignIn>,
}

impl Default for Untold {
    fn default() -> Self {
        Self { state: Arc::default(), added: Arc::new(watch::channel(0).0) }
    }
}

impl Untold {
    /// Where a launch's sessions hand their sign-ins.
    pub(super) fn sink(&self) -> McpSignIns {
        let untold = self.clone();
        Arc::new(move |thread: &ThreadId, store: &str, sign_ins: Vec<McpSignIn>| untold.add(thread, store, sign_ins))
    }

    /// Wakes as sign-ins are added.
    fn subscribe(&self) -> watch::Receiver<u64> {
        self.added.subscribe()
    }

    fn add(&self, thread: &ThreadId, store: &str, sign_ins: Vec<McpSignIn>) {
        {
            let mut state = self.state.locked();
            for sign_in in sign_ins {
                if let McpSignIn::SignedIn { server } = &sign_in {
                    state.told.remove(&(store.to_owned(), server.clone()));
                    state.untold.retain(|(_, of, untold)| of != store || untold.server() != server);
                    continue;
                }
                if state.untold.len() >= KEPT {
                    state.untold.pop_front();
                }
                state.untold.push_back((thread.clone(), store.to_owned(), sign_in));
            }
        }
        self.added.send_modify(|added| *added += 1);
    }

    /// The sign-ins of `thread` to tell, with where their credentials are kept, each noted as told: those telling
    /// something else of their server than its last notice did.
    fn take(&self, thread: &ThreadId) -> Vec<(String, McpSignIn)> {
        let mut state = self.state.locked();
        let mut tell = Vec::new();
        for (on, store, sign_in) in std::mem::take(&mut state.untold) {
            if on != *thread {
                state.untold.push_back((on, store, sign_in));
                continue;
            }
            let key = (store, sign_in.server().to_owned());
            let told = match (state.told.get(&key), &sign_in) {
                (Some(McpSignIn::Browser { url: told, .. }), McpSignIn::Browser { url, .. }) => told == url,
                (Some(McpSignIn::Failed { .. }), McpSignIn::Failed { .. }) => true,
                _ => false,
            };
            if !told {
                state.told.insert(key.clone(), sign_in.clone());
                tell.push((key.0, sign_in));
            }
        }
        tell
    }

    /// Forget having told `sign_in`, of credentials kept in `store`, whose notice was not sent.
    fn not_told(&self, store: &str, sign_in: &McpSignIn) {
        let mut state = self.state.locked();
        let key = (store.to_owned(), sign_in.server().to_owned());
        if state.told.get(&key) == Some(sign_in) {
            state.told.remove(&key);
        }
    }
}

/// What a notice of `sign_in`, a sign-in of `ant`'s session on `machine`, says; none for one signed in.
fn text(ant: &UserId, machine: &str, sign_in: &McpSignIn) -> Option<String> {
    match sign_in {
        McpSignIn::SignedIn { .. } => None,
        McpSignIn::Browser { server, url } => Some(format!(
            "[ants] {ant}'s MCP server {server} needs a browser sign-in on {machine}: open {url} in a browser there. \
             From another machine, run `ants ant mcp {ant} --sign-in {server}`, open the address it prints, and hand \
             the address the browser ends on to `--callback`. {ant} answers without {server} meanwhile."
        )),
        McpSignIn::Failed { server, error } => {
            let mut excerpt: String = error.trim().chars().take(ERROR_CHARS).collect();
            if error.trim().chars().nth(ERROR_CHARS).is_some() {
                excerpt.push('…');
            }
            Some(format!(
                "[ants] {ant} could not sign in to MCP server {server}: {excerpt}\n{ant} answers without {server}; \
                 `ants ant mcp {ant}` shows how its MCP servers stand."
            ))
        }
    }
}

impl Inbox<'_> {
    /// Run `work`, a prompt answering `incoming`, telling the owner of its thread, else the default human, of the
    /// sign-ins to MCP servers the thread's sessions could not make: as they come, and once it is done.
    pub(super) async fn telling_mcp_sign_ins<T>(&self, incoming: &Incoming, work: impl Future<Output = T>) -> T {
        let mut added = self.mcp_sign_ins.subscribe();
        tokio::pin!(work);
        let done = loop {
            tokio::select! {
                done = &mut work => break done,
                Ok(()) = added.changed() => Box::pin(self.tell_mcp_sign_ins(incoming)).await,
            }
        };
        Box::pin(self.tell_mcp_sign_ins(incoming)).await;
        done
    }

    async fn tell_mcp_sign_ins(&self, incoming: &Incoming) {
        let thread = &incoming.envelope.thread_id;
        for (store, sign_in) in self.mcp_sign_ins.take(thread) {
            let Some(text) = text(self.ant, self.machine, &sign_in) else { continue };
            let notice = self.outage_notice(incoming, &[]).with_text(text).as_notice(NoticeKind::McpSignIn);
            if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, None).await {
                let server = sign_in.server();
                tracing::warn!(%thread, server, error = format!("{e:#}"), "telling a human of an MCP server's sign-in");
                self.mcp_sign_ins.not_told(&store, &sign_in);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn browser(url: &str) -> McpSignIn {
        McpSignIn::Browser { server: "icm".to_owned(), url: url.to_owned() }
    }

    /// The sign-ins of `thread` to tell, without where their credentials are kept.
    fn take(untold: &Untold, thread: &ThreadId) -> Vec<McpSignIn> {
        untold.take(thread).into_iter().map(|(_, sign_in)| sign_in).collect()
    }

    #[test]
    fn a_sign_in_is_told_once_until_the_server_signs_in_again() {
        let untold = Untold::default();
        let sink = untold.sink();
        let (one, two) = (ThreadId::generate(), ThreadId::generate());
        let failed = McpSignIn::Failed { server: "ado".to_owned(), error: "no such server".to_owned() };
        sink(&one, "copilot", vec![browser("https://login/a"), failed.clone()]);
        assert!(take(&untold, &two).is_empty(), "another thread's");
        assert_eq!(take(&untold, &one), [browser("https://login/a"), failed.clone()]);
        sink(&two, "copilot", vec![browser("https://login/a"), failed.clone()]);
        assert!(take(&untold, &two).is_empty(), "told already");
        sink(&one, "copilot", vec![browser("https://login/b")]);
        assert_eq!(take(&untold, &one), [browser("https://login/b")], "another browser sign-in");
        sink(&two, "copilot", vec![McpSignIn::SignedIn { server: "icm".to_owned() }]);
        sink(&two, "copilot", vec![browser("https://login/b"), failed]);
        assert_eq!(take(&untold, &two), [browser("https://login/b")], "signed in since");
    }

    #[test]
    fn harnesses_keeping_credentials_apart_are_told_of_apart() {
        let untold = Untold::default();
        let sink = untold.sink();
        let thread = ThreadId::generate();
        sink(&thread, "copilot", vec![browser("https://login/a")]);
        sink(&thread, "fallback", vec![browser("https://login/a")]);
        assert_eq!(untold.take(&thread).len(), 2, "each harness's own sign-in");
        sink(&thread, "fallback", vec![browser("https://login/b"), McpSignIn::SignedIn { server: "icm".to_owned() }]);
        sink(&thread, "copilot", vec![browser("https://login/a")]);
        sink(&thread, "fallback", vec![browser("https://login/a")]);
        let told = untold.take(&thread);
        assert_eq!(told, [("fallback".to_owned(), browser("https://login/a"))], "only the harness signed in since");
    }

    #[test]
    fn a_notice_says_where_to_open_the_browser_or_why_the_sign_in_failed() {
        let ant: UserId = "dev-a".parse().unwrap();
        let browser = text(&ant, "box", &browser("https://login/a")).unwrap();
        assert!(
            browser.starts_with("[ants] dev-a's MCP server icm needs a browser sign-in on box: open https://login/a")
        );
        assert!(browser.contains("`ants ant mcp dev-a --sign-in icm`"), "{browser}");
        let failed = McpSignIn::Failed { server: "ado".to_owned(), error: "no such server".to_owned() };
        let failed = text(&ant, "box", &failed).unwrap();
        assert!(failed.starts_with("[ants] dev-a could not sign in to MCP server ado: no such server\n"), "{failed}");
        assert_eq!(text(&ant, "box", &McpSignIn::SignedIn { server: "icm".to_owned() }), None);
    }
}
