//! Answers to a harness's permission requests: the ant's allowlist first, then the humans and the ant's
//! chain, else reject.
//!
//! # Allowlist rules
//!
//! Each rule is `*`, `<kind>` or `<kind>:<pattern>`. `<kind>` is an ACP tool kind (`read`, `edit`, `delete`, `move`,
//! `search`, `execute`, `think`, `fetch`, `switch_mode`, `other`); a bare kind or `*` allows every tool of that kind or
//! every tool. In a pattern `*` stands for any run of characters, `/` and spaces included; the rest is literal. What
//! the pattern is matched against depends on the kind:
//!
//! - `execute`: the whole command line (`raw_input.command`). A command with shell control characters (`;`, `&`, `|`,
//!   `<`, `>`, a backtick, `$(` or a line break) never matches a pattern, so `execute:git status*` cannot run
//!   `git status; rm -rf ~`.
//! - `read`, `edit`, `delete`, `move`: every file the call touches (its locations, else path fields of the raw input),
//!   and all of them must match. A pattern starting with `/` matches the absolute path; any other pattern matches the
//!   path relative to the workdir, and never a file outside it. `edit:*` allows edits anywhere in the workdir.
//! - `fetch`: the URL (`raw_input.url`), else the title.
//! - anything else: the title the harness shows, such as an MCP tool's name. Claude Code titles MCP tools
//!   `mcp__<server>__<tool>`; the pattern may match that or just the tool.

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use agent_client_protocol::v1::schema::{
    PermissionOptionKind, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, ToolCallUpdateFields,
};
use ants_proto::{ThreadId, ToolCall, Verdict};
use anyhow::{Result, bail};
use futures::future::BoxFuture;
use serde_json::Value;

const KINDS: [&str; 10] =
    ["read", "edit", "delete", "move", "search", "execute", "think", "fetch", "switch_mode", "other"];
/// Longest raw tool input shown to the human.
const MAX_INPUT: usize = 2000;

/// One allowlist rule; see the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    /// None for every kind.
    kind: Option<String>,
    pattern: Option<String>,
}

impl FromStr for Rule {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        if s == "*" {
            return Ok(Self { kind: None, pattern: None });
        }
        let (kind, pattern) = match s.split_once(':') {
            Some((kind, pattern)) if !pattern.is_empty() => (kind, Some(pattern.to_owned())),
            Some(_) => bail!("allow rule {s:?} has an empty pattern"),
            None => (s, None),
        };
        if !KINDS.contains(&kind) {
            bail!("allow rule {s:?}: unknown tool kind {kind:?}, expected * or one of {}", KINDS.join(", "));
        }
        Ok(Self { kind: Some(kind.to_owned()), pattern })
    }
}

impl Rule {
    fn allows(&self, tool: &Tool<'_>, workdir: &Path) -> bool {
        if self.kind.as_deref().is_some_and(|k| k != tool.kind) {
            return false;
        }
        let Some(pattern) = &self.pattern else { return true };
        match tool.kind.as_str() {
            "execute" => command(tool.fields).is_some_and(|c| !has_shell_control(&c) && glob(pattern, &c)),
            "read" | "edit" | "delete" | "move" => {
                let paths = paths(tool.fields, workdir);
                !paths.is_empty() && paths.iter().all(|p| path_matches(pattern, p, workdir))
            }
            "fetch" => {
                let url = tool.fields.raw_input.as_ref().and_then(|i| i.get("url")).and_then(Value::as_str);
                url.or(tool.fields.title.as_deref()).is_some_and(|t| glob(pattern, t))
            }
            _ => tool
                .fields
                .title
                .as_deref()
                .is_some_and(|t| glob(pattern, t) || mcp_tool(t).is_some_and(|t| glob(pattern, t))),
        }
    }
}

/// The tool of a Claude Code MCP tool title, `mcp__<server>__<tool>`.
fn mcp_tool(title: &str) -> Option<&str> {
    let (server, tool) = title.strip_prefix("mcp__")?.split_once("__")?;
    (!server.is_empty() && !tool.is_empty()).then_some(tool)
}

/// Tool requests an ant may run without asking; see the [module docs](self) for the rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Allowlist(Vec<Rule>);

impl Allowlist {
    pub fn parse<S: AsRef<str>>(rules: &[S]) -> Result<Self> {
        rules.iter().map(|r| r.as_ref().parse()).collect::<Result<_>>().map(Self)
    }

    /// Allows every tool request.
    pub fn all() -> Self {
        Self(vec![Rule { kind: None, pattern: None }])
    }

    fn allows(&self, tool: &Tool<'_>, workdir: &Path) -> bool {
        self.0.iter().any(|r| r.allows(tool, workdir))
    }
}

/// The humans and the ant's chain, or whoever stands in: decide tool requests outside the allowlist.
pub trait Approver: Send + Sync {
    /// The decision on `tool`, requested on `thread`, among the `options` the harness offers. Never fails: anything
    /// that goes wrong, a timeout included, is [`Verdict::Reject`].
    fn decide(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> BoxFuture<'static, Verdict>;
}

/// What to answer when a harness asks permission to use a tool. The default rejects every request.
#[derive(Clone, Default)]
pub struct PermissionPolicy {
    allow: Allowlist,
    /// Who decides requests outside the allowlist. None rejects them.
    approver: Option<Arc<dyn Approver>>,
}

impl PermissionPolicy {
    pub fn new(allow: Allowlist, approver: Option<Arc<dyn Approver>>) -> Self {
        Self { allow, approver }
    }

    /// Reject every request, asking no one.
    pub fn reject_all() -> Self {
        Self::default()
    }

    /// Allow every request. Only for trusted, sandboxed workdirs.
    pub fn allow_all() -> Self {
        Self::new(Allowlist::all(), None)
    }

    /// Answer a request of the harness working in `workdir` on `thread`.
    pub async fn answer(
        &self,
        request: &RequestPermissionRequest,
        thread: Option<&ThreadId>,
        workdir: &Path,
    ) -> RequestPermissionResponse {
        let fields = &request.tool_call.fields;
        let tool = Tool { kind: kind_name(fields), fields };
        let verdict = if self.allow.allows(&tool, workdir) {
            tracing::info!(kind = %tool.kind, title = ?fields.title, "tool allowed by the allowlist");
            Verdict::AllowOnce
        } else {
            match (&self.approver, thread) {
                (Some(approver), Some(thread)) => {
                    let verdict = approver.decide(thread.clone(), summary(&tool), offered(request)).await;
                    tracing::info!(kind = %tool.kind, title = ?fields.title, ?verdict, "tool request decided");
                    verdict
                }
                _ => {
                    tracing::info!(kind = %tool.kind, title = ?fields.title, "tool request rejected");
                    Verdict::Reject
                }
            }
        };
        select(request, verdict)
    }
}

impl fmt::Debug for PermissionPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermissionPolicy")
            .field("allow", &self.allow)
            .field("escalates", &self.approver.is_some())
            .finish()
    }
}

struct Tool<'a> {
    kind: String,
    fields: &'a ToolCallUpdateFields,
}

fn kind_name(fields: &ToolCallUpdateFields) -> String {
    fields
        .kind
        .and_then(|k| serde_json::to_value(k).ok())
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| "other".to_owned())
}

/// The ACP option for `verdict`, picked by option kind, never by position or label.
fn select(request: &RequestPermissionRequest, verdict: Verdict) -> RequestPermissionResponse {
    let preferred: &[PermissionOptionKind] = match verdict {
        Verdict::AllowOnce => &[PermissionOptionKind::AllowOnce],
        Verdict::AllowAlways => &[PermissionOptionKind::AllowAlways, PermissionOptionKind::AllowOnce],
        Verdict::Reject => &[PermissionOptionKind::RejectOnce, PermissionOptionKind::RejectAlways],
    };
    let chosen =
        preferred.iter().find_map(|kind| request.options.iter().find(|o| o.kind == *kind).map(|o| o.option_id.clone()));
    RequestPermissionResponse::new(match chosen {
        Some(id) => RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id)),
        None => RequestPermissionOutcome::Cancelled,
    })
}

/// The verdicts a request's options make possible. Rejecting is always possible, by cancelling if nothing else.
fn offered(request: &RequestPermissionRequest) -> Vec<Verdict> {
    let mut verdicts = Vec::new();
    for (kind, verdict) in [
        (PermissionOptionKind::AllowOnce, Verdict::AllowOnce),
        (PermissionOptionKind::AllowAlways, Verdict::AllowAlways),
    ] {
        if request.options.iter().any(|o| o.kind == kind) {
            verdicts.push(verdict);
        }
    }
    verdicts.push(Verdict::Reject);
    verdicts
}

fn summary(tool: &Tool<'_>) -> ToolCall {
    let input = tool.fields.raw_input.as_ref().map(|v| {
        let mut s = v.to_string();
        if s.len() > MAX_INPUT {
            let end = (0..=MAX_INPUT).rev().find(|i| s.is_char_boundary(*i)).unwrap_or(0);
            s.truncate(end);
            s.push('…');
        }
        s
    });
    ToolCall {
        kind: tool.kind.clone(),
        title: tool.fields.title.clone().unwrap_or_default(),
        locations: tool.fields.locations.iter().flatten().map(|l| l.path.display().to_string()).collect(),
        input,
    }
}

fn command(fields: &ToolCallUpdateFields) -> Option<String> {
    match fields.raw_input.as_ref()?.get("command")? {
        Value::String(s) => Some(s.clone()),
        Value::Array(parts) => {
            parts.iter().map(|p| p.as_str().map(str::to_owned)).collect::<Option<Vec<_>>>().map(|p| p.join(" "))
        }
        _ => None,
    }
}

fn has_shell_control(command: &str) -> bool {
    command.contains([';', '&', '|', '<', '>', '`', '\n', '\r']) || command.contains("$(")
}

/// The files a call touches, absolute and lexically normalised.
fn paths(fields: &ToolCallUpdateFields, workdir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = fields.locations.iter().flatten().map(|l| l.path.clone()).collect();
    if found.is_empty()
        && let Some(input) = &fields.raw_input
    {
        for key in ["path", "fileName", "file_path", "filePath", "source", "destination"] {
            if let Some(p) = input.get(key).and_then(Value::as_str) {
                found.push(PathBuf::from(p));
            }
        }
    }
    found.iter().map(|p| normalize(&workdir.join(p))).collect()
}

fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            c => out.push(c),
        }
    }
    out
}

fn path_matches(pattern: &str, path: &Path, workdir: &Path) -> bool {
    if pattern.starts_with('/') {
        return path.to_str().is_some_and(|p| glob(pattern, p));
    }
    path.strip_prefix(normalize(workdir))
        .ok()
        .and_then(Path::to_str)
        .is_some_and(|rel| !rel.is_empty() && glob(pattern, rel))
}

/// Whether `text` matches `pattern`, where `*` stands for any run of characters.
fn glob(pattern: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pattern.chars().collect(), text.chars().collect());
    let (mut pi, mut ti) = (0, 0);
    // Where the last `*` was, and the text position it currently swallows up to.
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|c| *c == '*')
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agent_client_protocol::v1::schema::{PermissionOption, ToolCallLocation, ToolCallUpdate, ToolKind};
    use serde_json::json;

    use super::*;

    const WORKDIR: &str = "/work/repo";

    fn request(kind: ToolKind, title: &str, input: Value, locations: &[&str]) -> RequestPermissionRequest {
        let mut fields = ToolCallUpdateFields::new().kind(kind).title(title.to_owned()).raw_input(input);
        if !locations.is_empty() {
            fields = fields.locations(locations.iter().map(|p| ToolCallLocation::new(*p)).collect::<Vec<_>>());
        }
        RequestPermissionRequest::new(
            "s1",
            ToolCallUpdate::new("call-1", fields),
            vec![
                PermissionOption::new("yes", "Allow", PermissionOptionKind::AllowOnce),
                PermissionOption::new("always", "Always", PermissionOptionKind::AllowAlways),
                PermissionOption::new("no", "Reject", PermissionOptionKind::RejectOnce),
            ],
        )
    }

    fn shell(command: &str) -> RequestPermissionRequest {
        request(ToolKind::Execute, "Run shell command", json!({ "command": command }), &[])
    }

    fn edit(path: &str) -> RequestPermissionRequest {
        request(ToolKind::Edit, "Edit file", json!({ "fileName": path }), &[path])
    }

    fn allows(rules: &[&str], request: &RequestPermissionRequest) -> bool {
        let fields = &request.tool_call.fields;
        Allowlist::parse(rules).unwrap().allows(&Tool { kind: kind_name(fields), fields }, Path::new(WORKDIR))
    }

    fn chosen(response: &RequestPermissionResponse) -> Option<&str> {
        match &response.outcome {
            RequestPermissionOutcome::Selected(s) => Some(&s.option_id.0),
            _ => None,
        }
    }

    #[test]
    fn rules_parse_kinds_and_patterns() {
        assert!(Allowlist::parse(&["*", "read", "execute:git status*", "switch_mode"]).is_ok());
        for bad in ["shell", "execute:", "*:foo", ""] {
            assert!(Allowlist::parse(&[bad]).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn kinds_and_wildcards() {
        assert!(allows(&["*"], &shell("rm -rf /")));
        assert!(allows(&["execute"], &shell("cargo test")));
        assert!(!allows(&["read", "search"], &shell("cargo test")));
        assert!(!allows(&[], &shell("cargo test")));
    }

    #[test]
    fn command_patterns_match_the_whole_command_line() {
        let rules = ["execute:git status*", "execute:cargo test"];
        assert!(allows(&rules, &shell("git status --short")));
        assert!(allows(&rules, &shell("cargo test")));
        assert!(!allows(&rules, &shell("cargo test --release")));
        assert!(!allows(&rules, &shell("git push")));
        for chained in [
            "git status; rm -rf ~",
            "git status && curl x",
            "git status | sh",
            "git status $(rm x)",
            "git status `id`",
            "git status > f",
        ] {
            assert!(!allows(&rules, &shell(chained)), "{chained:?}");
        }
        assert!(allows(&["execute"], &shell("git status; rm -rf ~")), "a bare kind allows any command");
        let argv = request(ToolKind::Execute, "Run", json!({ "command": ["git", "status"] }), &[]);
        assert!(allows(&rules, &argv));
    }

    #[test]
    fn path_patterns_are_relative_to_the_workdir_unless_absolute() {
        assert!(allows(&["edit:*"], &edit("/work/repo/src/main.rs")));
        assert!(allows(&["edit:src/*.rs"], &edit("/work/repo/src/main.rs")));
        assert!(!allows(&["edit:*"], &edit("/home/me/.ssh/config")));
        assert!(!allows(&["edit:*"], &edit("/work/repo/../other/x")));
        assert!(!allows(&["edit:src/*"], &edit("/work/repo-evil/src/x")));
        assert!(allows(&["edit:/home/me/notes/*"], &edit("/home/me/notes/a.md")));
        assert!(!allows(&["read:*"], &edit("/work/repo/src/main.rs")), "the kind must match too");
        let relative = request(ToolKind::Read, "View", json!({ "path": "docs/a.md" }), &[]);
        assert!(allows(&["read:docs/*"], &relative));
        let nowhere = request(ToolKind::Edit, "Edit", json!({}), &[]);
        assert!(!allows(&["edit:*"], &nowhere), "a pattern needs a path to match");
    }

    #[test]
    fn other_kinds_match_the_title_or_url() {
        let mcp = request(ToolKind::Other, "bd-ready", json!({}), &[]);
        assert!(allows(&["other:bd-*"], &mcp));
        assert!(!allows(&["other:gh-*"], &mcp));
        let claude = request(ToolKind::Other, "mcp__ants__send_message", json!({}), &[]);
        assert!(allows(&["other:send_message"], &claude) && allows(&["other:mcp__ants__*"], &claude));
        assert!(!allows(&["other:ants"], &claude));
        let fetch = request(ToolKind::Fetch, "Fetch", json!({ "url": "https://docs.rs/tokio" }), &[]);
        assert!(allows(&["fetch:https://docs.rs/*"], &fetch));
        assert!(!allows(&["fetch:https://evil/*"], &fetch));
    }

    #[test]
    fn glob_stars_span_any_characters() {
        assert!(glob("a*c", "abbbc") && glob("*", "") && glob("a**", "a") && glob("*b*", "abc"));
        assert!(!glob("a*c", "abd") && !glob("abc", "ab") && !glob("", "a"));
    }

    /// Answers every request with a fixed verdict, recording what it was asked.
    struct Fake(Verdict, Mutex<Vec<(ThreadId, ToolCall, Vec<Verdict>)>>);

    impl Approver for Fake {
        fn decide(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> BoxFuture<'static, Verdict> {
            self.1.lock().unwrap().push((thread, tool, options));
            let verdict = self.0;
            Box::pin(async move { verdict })
        }
    }

    #[tokio::test]
    async fn requests_outside_the_allowlist_go_to_the_approver() {
        let thread: ThreadId = "t1".parse().unwrap();
        let workdir = Path::new(WORKDIR);
        let fake = Arc::new(Fake(Verdict::AllowAlways, Mutex::default()));
        let policy = PermissionPolicy::new(Allowlist::parse(&["read"]).unwrap(), Some(fake.clone()));

        let read = request(ToolKind::Read, "View", json!({ "path": "a" }), &[]);
        assert_eq!(chosen(&policy.answer(&read, Some(&thread), workdir).await), Some("yes"));
        assert!(fake.1.lock().unwrap().is_empty(), "allowed without asking");

        let push = shell("git push");
        assert_eq!(chosen(&policy.answer(&push, Some(&thread), workdir).await), Some("always"));
        let asked = fake.1.lock().unwrap().pop().unwrap();
        assert_eq!(asked.0, thread);
        assert_eq!((asked.1.kind.as_str(), asked.1.title.as_str()), ("execute", "Run shell command"));
        assert_eq!(asked.1.input.as_deref(), Some(r#"{"command":"git push"}"#));
        assert_eq!(asked.2, [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);

        let no = PermissionPolicy::new(Allowlist::default(), Some(Arc::new(Fake(Verdict::Reject, Mutex::default()))));
        assert_eq!(chosen(&no.answer(&push, Some(&thread), workdir).await), Some("no"));
        assert_eq!(chosen(&PermissionPolicy::reject_all().answer(&push, Some(&thread), workdir).await), Some("no"));
        assert_eq!(chosen(&policy.answer(&push, None, workdir).await), Some("no"), "no thread, no one to ask");
        assert_eq!(chosen(&PermissionPolicy::allow_all().answer(&push, None, workdir).await), Some("yes"));
    }
}
