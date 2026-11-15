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
//!   `mcp__<server>__<tool>` and Codex `mcp.<server>.<tool>`; the pattern may match that or just the tool. Codex
//!   reports MCP tool calls as `execute`; they count as `other`.

use std::collections::HashMap;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::{
    PermissionOptionKind, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, ToolCallUpdateFields,
};
use ants_proto::{ThreadId, ToolCall, Verdict};
use anyhow::{Result, bail};
use futures::future::BoxFuture;
use serde_json::Value;

use crate::redact::Redactor;
use crate::sync::MutexExt as _;

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
            _ => {
                tool.fields.title.as_deref().is_some_and(|t| glob(pattern, t))
                    || mcp_tool(tool.fields).is_some_and(|t| glob(pattern, t))
            }
        }
    }
}

/// The tool an MCP tool call runs, from Claude Code's title `mcp__<server>__<tool>` or Codex's call.
fn mcp_tool(fields: &ToolCallUpdateFields) -> Option<&str> {
    let title = fields.title.as_deref()?;
    if let Some((server, tool)) = title.strip_prefix("mcp__").and_then(|t| t.split_once("__")) {
        return (!server.is_empty() && !tool.is_empty()).then_some(tool);
    }
    codex_mcp_tool(fields)
}

/// The tool of a Codex MCP tool call: titled `mcp.<server>.<tool>`, with the server and tool, but no command, in its
/// raw input.
fn codex_mcp_tool(fields: &ToolCallUpdateFields) -> Option<&str> {
    let input = fields.raw_input.as_ref()?;
    if input.get("command").is_some() {
        return None;
    }
    let (server, tool) = (input.get("server")?.as_str()?, input.get("tool")?.as_str()?);
    let title = fields.title.as_deref()?.strip_prefix("mcp.")?.strip_prefix(server)?.strip_prefix('.')?;
    (!server.is_empty() && !tool.is_empty() && title == tool).then_some(tool)
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

/// The narrowest allowlist rule allowing `tool`, a call an ant escalated ([`Approver::decide`]): its exact command,
/// file, URL or title. None when no rule fits: a command with shell control characters, which no pattern matches, a
/// call touching several files or none, or a text holding `*`, which a pattern cannot write literally, or with
/// leading or trailing blanks, which a rule loses.
pub fn rule_for(tool: &ToolCall) -> Option<String> {
    let input = tool.input.as_deref().and_then(|i| serde_json::from_str::<Value>(i).ok());
    let pattern = match tool.kind.as_str() {
        "execute" => input.as_ref().and_then(command_in).filter(|c| !has_shell_control(c))?,
        "read" | "edit" | "delete" | "move" => match tool.locations.as_slice() {
            [path] if Path::new(path).is_absolute() && normalize(Path::new(path)) == Path::new(path) => path.clone(),
            _ => return None,
        },
        "fetch" => input
            .as_ref()
            .and_then(|i| i.get("url"))
            .and_then(Value::as_str)
            .map_or_else(|| tool.title.clone(), str::to_owned),
        _ => tool.title.clone(),
    };
    let fits = !pattern.is_empty() && pattern.trim() == pattern && !pattern.contains('*');
    (fits && KINDS.contains(&tool.kind.as_str())).then(|| format!("{}:{pattern}", tool.kind))
}

/// Whether the allowlist rule `rule` covers `tool`, a call as an approver is shown it ([`Approver::decide`]): how a
/// human's pre-allow rule for its assistant's decisions ([`ants_proto::PreAllow`]) is matched, by the requester's
/// daemon and the CLI alike. Stricter than an ant's allowlist, as the summary is all there is: an input shortened for
/// showing, or one that does not read as JSON, matches no `execute` pattern; a `read`, `edit`, `delete` or `move`
/// pattern must be absolute, and matches only calls whose every location is an absolute, normalised path; a malformed
/// rule covers nothing.
pub fn covers(rule: &str, tool: &ToolCall) -> bool {
    let Ok(rule) = rule.parse::<Rule>() else { return false };
    if rule.kind.as_deref().is_some_and(|k| k != tool.kind) {
        return false;
    }
    let Some(pattern) = &rule.pattern else { return true };
    let input = tool.input.as_deref().and_then(|i| serde_json::from_str::<Value>(i).ok());
    match tool.kind.as_str() {
        "execute" => input.as_ref().and_then(command_in).is_some_and(|c| !has_shell_control(&c) && glob(pattern, &c)),
        "read" | "edit" | "delete" | "move" => {
            pattern.starts_with('/')
                && !tool.locations.is_empty()
                && tool.locations.iter().all(|l| {
                    let path = Path::new(l);
                    path.is_absolute() && normalize(path) == path && glob(pattern, l)
                })
        }
        "fetch" => {
            let url = input.as_ref().and_then(|i| i.get("url")).and_then(Value::as_str);
            glob(pattern, url.unwrap_or(&tool.title))
        }
        _ => {
            let mcp = tool.title.strip_prefix("mcp__").and_then(|t| t.split_once("__")).map(|(_, tool)| tool);
            glob(pattern, &tool.title) || mcp.is_some_and(|t| !t.is_empty() && glob(pattern, t))
        }
    }
}

/// The humans and the ant's chain, or whoever stands in: decide tool requests outside the allowlist.
pub trait Approver: Send + Sync {
    /// The decision on `tool`, requested on `thread`, among the `options` the harness offers. Never fails: anything
    /// that goes wrong, a timeout included, is [`Verdict::Reject`].
    fn decide(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> BoxFuture<'static, Verdict>;
}

/// Threads being answered under narrowed tool rules ([`PermissionPolicy::narrow`]), with how many answers narrow each.
type Narrowed = Arc<Mutex<HashMap<ThreadId, (Allowlist, usize)>>>;

/// What to answer when a harness asks permission to use a tool. The default rejects every request.
#[derive(Clone, Default)]
pub struct PermissionPolicy {
    allow: Allowlist,
    /// Who decides requests outside the allowlist. None rejects them.
    approver: Option<Arc<dyn Approver>>,
    /// Shared by the policy's clones.
    narrowed: Narrowed,
    /// What the request the approver gets hides.
    redactor: Arc<Redactor>,
}

/// Keeps a thread's tool rules narrowed while held ([`PermissionPolicy::narrow`]).
#[must_use]
pub struct Narrowing {
    narrowed: Narrowed,
    thread: Option<ThreadId>,
}

impl Drop for Narrowing {
    fn drop(&mut self) {
        let Some(thread) = self.thread.take() else { return };
        let mut narrowed = self.narrowed.locked();
        if let Some((_, held)) = narrowed.get_mut(&thread) {
            *held -= 1;
            if *held == 0 {
                narrowed.remove(&thread);
            }
        }
    }
}

impl PermissionPolicy {
    pub fn new(allow: Allowlist, approver: Option<Arc<dyn Approver>>) -> Self {
        Self { allow, approver, narrowed: Narrowed::default(), redactor: Arc::default() }
    }

    /// Narrow the tool rules on `thread` to `rules`, if any, while the returned guard is held: a request is then
    /// allowed unasked only when both the allowlist and `rules` allow it. Rules that do not parse allow nothing.
    pub fn narrow(&self, thread: &ThreadId, rules: Option<&[String]>) -> Narrowing {
        let Some(rules) = rules else { return Narrowing { narrowed: self.narrowed.clone(), thread: None } };
        let rules = Allowlist::parse(rules).unwrap_or_else(|e| {
            tracing::warn!(%thread, error = %e, "the thread's tool rules do not parse, narrowing it to none");
            Allowlist::default()
        });
        let mut narrowed = self.narrowed.locked();
        let held = narrowed.get(thread).map_or(0, |(_, held)| *held);
        narrowed.insert(thread.clone(), (rules, held + 1));
        Narrowing { narrowed: self.narrowed.clone(), thread: Some(thread.clone()) }
    }

    /// Whether `tool` on `thread` falls outside the thread's narrowed tool rules.
    fn narrowed_out(&self, tool: &Tool<'_>, thread: Option<&ThreadId>, workdir: &Path) -> bool {
        let narrowed = self.narrowed.locked();
        thread.and_then(|t| narrowed.get(t)).is_some_and(|(rules, _)| !rules.allows(tool, workdir))
    }

    /// The same, redacting the tool requests the approver gets with `redactor`.
    #[must_use]
    pub fn with_redactor(self, redactor: Arc<Redactor>) -> Self {
        Self { redactor, ..self }
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
        let verdict = self.verdict(&request.tool_call.fields, || offered(request), thread, workdir).await;
        select(request, verdict)
    }

    /// Decide on `fields`, a tool call the daemon itself makes for the ant on `thread`, in `workdir`, as on a harness's
    /// request offering to allow it once or reject it.
    pub async fn decide(&self, fields: &ToolCallUpdateFields, thread: &ThreadId, workdir: &Path) -> Verdict {
        self.verdict(fields, || vec![Verdict::AllowOnce, Verdict::Reject], Some(thread), workdir).await
    }

    /// The allowlist's verdict on `fields`, unless the thread's narrowed rules leave it out, else the approver's among
    /// `options`, else reject.
    async fn verdict(
        &self,
        fields: &ToolCallUpdateFields,
        options: impl FnOnce() -> Vec<Verdict>,
        thread: Option<&ThreadId>,
        workdir: &Path,
    ) -> Verdict {
        let tool = Tool { kind: kind_name(fields), fields };
        let allowed = self.allow.allows(&tool, workdir);
        if allowed && !self.narrowed_out(&tool, thread, workdir) {
            tracing::info!(kind = %tool.kind, title = ?fields.title, "tool allowed by the allowlist");
            return Verdict::AllowOnce;
        }
        if allowed {
            tracing::info!(kind = %tool.kind, title = ?fields.title, "tool outside the thread's narrowed rules");
        }
        match (&self.approver, thread) {
            (Some(approver), Some(thread)) => {
                let verdict = approver.decide(thread.clone(), summary(&tool, &self.redactor), options()).await;
                tracing::info!(kind = %tool.kind, title = ?fields.title, ?verdict, "tool request decided");
                verdict
            }
            _ => {
                tracing::info!(kind = %tool.kind, title = ?fields.title, "tool request rejected");
                Verdict::Reject
            }
        }
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
    if codex_mcp_tool(fields).is_some() {
        return "other".to_owned();
    }
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

/// What the approver is shown of `tool`, redacted before the input is shortened, so no secret is cut in two.
fn summary(tool: &Tool<'_>, redactor: &Redactor) -> ToolCall {
    let input = tool.fields.raw_input.as_ref().map(|v| {
        let mut v = v.clone();
        redactor.redact_json(&mut v);
        let mut s = v.to_string();
        if s.len() > MAX_INPUT {
            let end = (0..=MAX_INPUT).rev().find(|i| s.is_char_boundary(*i)).unwrap_or(0);
            s.truncate(end);
            s.push('…');
        }
        s
    });
    let mut call = ToolCall {
        kind: tool.kind.clone(),
        title: tool.fields.title.clone().unwrap_or_default(),
        locations: tool.fields.locations.iter().flatten().map(|l| l.path.display().to_string()).collect(),
        input: None,
    };
    redactor.redact_tool(&mut call);
    ToolCall { input, ..call }
}

fn command(fields: &ToolCallUpdateFields) -> Option<String> {
    command_in(fields.raw_input.as_ref()?)
}

fn command_in(input: &Value) -> Option<String> {
    match input.get("command")? {
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
        let input = json!({ "server": "ants", "tool": "send_message", "arguments": { "to": "dev-b" } });
        let codex = request(ToolKind::Execute, "mcp.ants.send_message", input.clone(), &[]);
        assert!(allows(&["other:send_message"], &codex) && allows(&["other:mcp.ants.*"], &codex));
        assert!(!allows(&["other:ants"], &codex) && !allows(&["execute"], &codex), "it counts as other");
        let mut command = input;
        command["command"] = json!("mcp.ants.send_message");
        let lookalike = request(ToolKind::Execute, "mcp.ants.send_message", command, &[]);
        assert!(!allows(&["other:send_message"], &lookalike) && allows(&["execute"], &lookalike));
        let fetch = request(ToolKind::Fetch, "Fetch", json!({ "url": "https://docs.rs/tokio" }), &[]);
        assert!(allows(&["fetch:https://docs.rs/*"], &fetch));
        assert!(!allows(&["fetch:https://evil/*"], &fetch));
    }

    #[test]
    fn glob_stars_span_any_characters() {
        assert!(glob("a*c", "abbbc") && glob("*", "") && glob("a**", "a") && glob("*b*", "abc"));
        assert!(!glob("a*c", "abd") && !glob("abc", "ab") && !glob("", "a"));
    }

    #[test]
    fn the_suggested_rule_allows_the_escalated_call_exactly() {
        let rule = |request: &RequestPermissionRequest| {
            let fields = &request.tool_call.fields;
            rule_for(&summary(&Tool { kind: kind_name(fields), fields }, &Redactor::default()))
        };
        let fetch = request(ToolKind::Fetch, "Fetch", json!({ "url": "https://docs.rs/x" }), &[]);
        let mcp = request(ToolKind::Other, "mcp__bd__show", json!({}), &[]);
        for (request, expected) in [
            (shell("cargo test -p ants"), "execute:cargo test -p ants"),
            (edit("/work/repo/src/main.rs"), "edit:/work/repo/src/main.rs"),
            (fetch, "fetch:https://docs.rs/x"),
            (mcp, "other:mcp__bd__show"),
        ] {
            assert_eq!(rule(&request).as_deref(), Some(expected));
            assert!(allows(&[expected], &request), "{expected}");
        }
        assert!(!allows(&["execute:cargo test -p ants"], &shell("cargo test -p ants-daemon")));
        let two = request(ToolKind::Edit, "Edit", json!({}), &["/work/repo/a", "/work/repo/b"]);
        for none in [shell("git status; rm -rf ~"), shell("rm *.o"), shell(" ls"), edit("src/x"), two] {
            assert_eq!(rule(&none), None, "{:?}", none.tool_call.fields.raw_input);
        }
    }

    #[test]
    fn pre_allow_rules_cover_the_shown_call_conservatively() {
        let shown = |request: &RequestPermissionRequest| {
            let fields = &request.tool_call.fields;
            summary(&Tool { kind: kind_name(fields), fields }, &Redactor::default())
        };
        let test = shown(&shell("cargo test -p ants"));
        assert!(covers("execute:cargo test*", &test) && covers("execute", &test) && covers("*", &test));
        assert!(!covers("execute:cargo build*", &test) && !covers("read", &test) && !covers("nonsense", &test));
        assert!(!covers("execute:cargo test*", &shown(&shell("cargo test; rm -rf ~"))), "shell control");
        let long = shown(&shell(&format!("cargo test {}", "x".repeat(MAX_INPUT))));
        assert!(long.input.as_deref().is_some_and(|i| i.ends_with('…')));
        assert!(!covers("execute:cargo test*", &long), "a shortened input matches no command pattern");

        let edit = shown(&edit("/work/repo/src/main.rs"));
        assert!(covers("edit:/work/repo/*", &edit));
        assert!(!covers("edit:src/*", &edit), "no workdir to be relative to");
        let climbing = ToolCall { locations: vec!["/work/repo/../../etc/passwd".to_owned()], ..edit.clone() };
        assert!(!covers("edit:/work/repo/*", &climbing));
        let none = ToolCall { locations: Vec::new(), ..edit };
        assert!(!covers("edit:/work/repo/*", &none));

        let fetch = shown(&request(ToolKind::Fetch, "Fetch", json!({ "url": "https://docs.rs/x" }), &[]));
        assert!(covers("fetch:https://docs.rs/*", &fetch) && !covers("fetch:Fetch", &fetch));
        let mcp = shown(&request(ToolKind::Other, "mcp__bd__show", json!({}), &[]));
        assert!(covers("other:show", &mcp) && covers("other:mcp__bd__*", &mcp) && !covers("other:list", &mcp));
    }

    /// Answers every request with a fixed verdict, recording what it was asked.
    struct Fake(Verdict, Mutex<Vec<(ThreadId, ToolCall, Vec<Verdict>)>>);

    impl Approver for Fake {
        fn decide(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> BoxFuture<'static, Verdict> {
            self.1.locked().push((thread, tool, options));
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
        assert!(fake.1.locked().is_empty(), "allowed without asking");

        let push = shell("git push");
        assert_eq!(chosen(&policy.answer(&push, Some(&thread), workdir).await), Some("always"));
        let asked = fake.1.locked().pop().unwrap();
        assert_eq!(asked.0, thread);
        assert_eq!((asked.1.kind.as_str(), asked.1.title.as_str()), ("execute", "Run shell command"));
        assert_eq!(asked.1.input.as_deref(), Some(r#"{"command":"git push"}"#));
        assert_eq!(asked.2, [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);

        let no = PermissionPolicy::new(Allowlist::default(), Some(Arc::new(Fake(Verdict::Reject, Mutex::default()))));
        assert_eq!(chosen(&no.answer(&push, Some(&thread), workdir).await), Some("no"));
        assert_eq!(chosen(&PermissionPolicy::reject_all().answer(&push, Some(&thread), workdir).await), Some("no"));
        assert_eq!(chosen(&policy.answer(&push, None, workdir).await), Some("no"), "no thread, no one to ask");
        assert_eq!(chosen(&PermissionPolicy::allow_all().answer(&push, None, workdir).await), Some("yes"));

        let own = &shell("git push origin ants/dev-a/t1").tool_call.fields;
        assert_eq!(policy.decide(own, &thread, workdir).await, Verdict::AllowAlways);
        assert_eq!(fake.1.locked().pop().unwrap().2, [Verdict::AllowOnce, Verdict::Reject], "the daemon's own call");
        let allowed = PermissionPolicy::new(Allowlist::parse(&["execute:git push origin ants/*"]).unwrap(), None);
        assert_eq!(allowed.decide(own, &thread, workdir).await, Verdict::AllowOnce);
    }

    #[tokio::test]
    async fn a_narrowed_thread_asks_for_what_its_rules_leave_out() {
        let (thread, other): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());
        let workdir = Path::new(WORKDIR);
        let fake = Arc::new(Fake(Verdict::Reject, Mutex::default()));
        let policy = PermissionPolicy::new(Allowlist::all(), Some(fake.clone()));
        let (read, push) = (request(ToolKind::Read, "View", json!({ "path": "a" }), &[]), shell("git push"));

        let narrowed = policy.narrow(&thread, Some(&["read:*".to_owned()]));
        let again = policy.clone().narrow(&thread, Some(&["read:*".to_owned()]));
        assert_eq!(chosen(&policy.answer(&read, Some(&thread), workdir).await), Some("yes"));
        assert_eq!(chosen(&policy.answer(&push, Some(&thread), workdir).await), Some("no"));
        assert_eq!(fake.1.locked().len(), 1, "the approver decides what the narrowing leaves out");
        assert_eq!(chosen(&policy.answer(&push, Some(&other), workdir).await), Some("yes"), "only the thread");
        drop(narrowed);
        assert_eq!(chosen(&policy.answer(&push, Some(&thread), workdir).await), Some("no"), "still held");
        drop(again);
        assert_eq!(chosen(&policy.answer(&push, Some(&thread), workdir).await), Some("yes"));

        let _bad = policy.narrow(&thread, Some(&["nonsense".to_owned()]));
        assert_eq!(chosen(&policy.answer(&read, Some(&thread), workdir).await), Some("no"), "unparsed allows none");
        let _none = policy.narrow(&other, None);
        assert_eq!(chosen(&policy.answer(&push, Some(&other), workdir).await), Some("yes"));
    }

    #[tokio::test]
    async fn the_approver_gets_the_request_redacted_before_it_is_shortened() {
        let fake = Arc::new(Fake(Verdict::Reject, Mutex::default()));
        let redactor = Redactor::default().with_env([("DEPLOY_TOKEN", "d3pl0y-s3cret")]);
        let policy = PermissionPolicy::new(Allowlist::default(), Some(fake.clone())).with_redactor(Arc::new(redactor));
        let command = format!("{}deploy --key d3pl0y-s3cret", " ".repeat(MAX_INPUT - 37));
        let thread: ThreadId = "t1".parse().unwrap();
        let deploy = request(ToolKind::Execute, "deploy --key d3pl0y-s3cret", json!({ "command": command }), &[]);
        policy.answer(&deploy, Some(&thread), Path::new(WORKDIR)).await;
        let (_, tool, _) = fake.1.locked().pop().unwrap();
        assert_eq!(tool.title, "deploy --key [redacted]");
        let input = tool.input.unwrap();
        assert!(input.ends_with("deploy --key [redacted]\"}") && !input.contains("d3pl0y"), "{input}");
    }
}
