//! `[[ant]]` tables written as TOML text: an ant's settings that differ from the defaults ([`entry`]), and the entry
//! of an ant a manager adds from a template ([`template_entry`]).

use std::fmt::Write as _;
use std::path::Path;

use ants_proto::UserId;
use anyhow::{Context as _, Result};

use super::Ant;
use crate::profile::template::Request;

/// The `[[ant]]` table of `ant` for the config file: its name, workdir and the settings that differ from those of
/// [`Ant::new`], paths as they are.
pub fn entry(ant: &Ant) -> Result<String> {
    let mut out = String::from("[[ant]]\n");
    entry_settings(ant, &mut out)?;
    entry_limits(ant, &mut out);
    Ok(out)
}

/// Write to `out` the name, connection, paths, harness and permissions of `ant`'s `[[ant]]` table.
fn entry_settings(ant: &Ant, out: &mut String) -> Result<()> {
    let mut set = |key: &str, value: String| {
        let _ = writeln!(out, "{key} = {value}");
    };
    let path = |path: &Path| {
        path.to_str().map(toml_string).with_context(|| format!("ant {}: {} is no UTF-8", ant.name, path.display()))
    };
    let strings =
        |items: &mut dyn Iterator<Item = &str>| format!("[{}]", items.map(toml_string).collect::<Vec<_>>().join(", "));
    set("name", toml_string(ant.name.as_str()));
    let texts = [("nats", &ant.nats), ("model", &ant.model), ("reasoning_effort", &ant.reasoning_effort)];
    for (key, value) in texts.into_iter().chain([("context", &ant.context)]) {
        if let Some(value) = value {
            set(key, toml_string(value));
        }
    }
    if let Some(creds) = &ant.creds {
        set("creds", path(creds)?);
    }
    if let Some(domain) = &ant.js_domain {
        set("js_domain", toml_string(domain.as_str()));
    }
    if let Some(human) = &ant.human {
        set("human", toml_string(human.as_str()));
    }
    set("workdir", path(&ant.workdir)?);
    if let Some(home) = &ant.home {
        set("home", path(home)?);
    }
    for (key, value) in [("whoami", &ant.whoami), ("memory", &ant.memory), ("tasks", &ant.tasks)] {
        if let Some(value) = value {
            set(key, toml_string(value));
        }
    }
    if !ant.env.is_empty() {
        let pairs: Vec<String> =
            ant.env.iter().map(|(k, v)| format!("{} = {}", toml_string(k), toml_string(v))).collect();
        set("env", format!("{{ {} }}", pairs.join(", ")));
    }
    if let Some(harness) = &ant.harness {
        set("harness", toml_string(harness));
    }
    if let Some(tools) = ant.tools {
        set("tools", toml_string(tools.as_str()));
    }
    if !ant.fallback.is_empty() {
        set("fallback", strings(&mut ant.fallback.iter().map(String::as_str)));
    }
    if ant.allow_tools {
        set("allow_tools", "true".to_owned());
    }
    if let Some(worktrees) = ant.worktrees {
        set("worktrees", worktrees.to_string());
    }
    if !ant.allow.is_empty() {
        set("allow", strings(&mut ant.allow.iter().map(String::as_str)));
    }
    if !ant.capabilities.is_empty() {
        set("capabilities", strings(&mut ant.capabilities.iter().map(String::as_str)));
    }
    if !ant.escalate_to.is_empty() {
        set("escalate_to", strings(&mut ant.escalate_to.iter().map(UserId::as_str)));
    }
    if let Some(stand_in) = &ant.stand_in {
        set("stand_in", toml_string(stand_in.as_str()));
    }
    Ok(())
}

/// Write to `out` the limits and timeouts of `ant`'s `[[ant]]` table that differ from those of [`Ant::new`].
fn entry_limits(ant: &Ant, out: &mut String) {
    let default = Ant::new(ant.name.clone(), ant.workdir.clone());
    let mut set = |key: &str, value: String| {
        let _ = writeln!(out, "{key} = {value}");
    };
    if let Some(turns) = ant.turn_budget {
        set("turn_budget", turns.to_string());
    }
    let numbers = [
        ("max_harnesses", ant.max_harnesses as u64, default.max_harnesses as u64),
        ("max_tool_calls", ant.max_tool_calls as u64, default.max_tool_calls as u64),
        ("max_escalation_hops", ant.max_escalation_hops.into(), default.max_escalation_hops.into()),
        ("spend_warn_percent", ant.spend_warn_percent.into(), default.spend_warn_percent.into()),
    ];
    for (key, value, default) in numbers {
        if value != default {
            set(key, value.to_string());
        }
    }
    let durations = [
        ("approval_timeout", ant.approval_timeout, default.approval_timeout),
        ("idle_timeout", ant.idle_timeout, default.idle_timeout),
        ("prompt_timeout", ant.prompt_timeout, default.prompt_timeout),
        ("escalate_after", ant.escalate_after, default.escalate_after),
        ("escalation_reminder", ant.escalation_reminder, default.escalation_reminder),
    ];
    for (key, value, default) in durations {
        if value != default {
            set(key, toml_string(&ants_proto::duration::format(value)));
        }
    }
    let counts = [
        ("history_budget", ant.history_budget.map(|n| n as u64)),
        ("max_thread_tokens", ant.max_thread_tokens),
        ("max_daily_tokens", ant.max_daily_tokens),
    ];
    for (key, value) in counts {
        if let Some(value) = value {
            set(key, value.to_string());
        }
    }
    for (key, value) in [("max_thread_cost", ant.max_thread_cost), ("max_daily_cost", ant.max_daily_cost)] {
        if let Some(value) = value {
            set(key, toml_float(value));
        }
    }
    if ant.worktree_base != default.worktree_base {
        set("worktree_base", toml_string(ant.worktree_base.as_str()));
    }
    if ant.publish != default.publish {
        set("publish", toml_string(ant.publish.as_str()));
    }
}

/// The `[[ant]]` table of an ant named `name` that `manager` adds from `template` as `request` asks, with `creds` as
/// its credentials: those and what the request sets, paths as they are. Its other settings come from the template
/// whenever the file loads.
pub fn template_entry(
    name: &UserId,
    template: &str,
    manager: &UserId,
    creds: Option<&Path>,
    request: &Request,
) -> Result<String> {
    let mut out = String::from("[[ant]]\n");
    let mut set = |key: &str, value: String| {
        let _ = writeln!(out, "{key} = {value}");
    };
    let path = |path: &Path| {
        path.to_str().map(toml_string).with_context(|| format!("ant {name}: {} is no UTF-8", path.display()))
    };
    let strings =
        |items: &[String]| format!("[{}]", items.iter().map(|s| toml_string(s)).collect::<Vec<_>>().join(", "));
    set("name", toml_string(name.as_str()));
    set("template", toml_string(template));
    set("added_by", toml_string(manager.as_str()));
    if let Some(creds) = creds {
        set("creds", path(creds)?);
    }
    if let Some(workdir) = &request.workdir {
        set("workdir", path(workdir)?);
    }
    let texts = [
        ("harness", &request.harness),
        ("model", &request.model),
        ("reasoning_effort", &request.reasoning_effort),
        ("context", &request.context),
        ("whoami", &request.whoami),
        ("memory", &request.memory),
        ("tasks", &request.tasks),
    ];
    for (key, value) in texts {
        if let Some(value) = value {
            set(key, toml_string(value));
        }
    }
    if let Some(capabilities) = &request.capabilities {
        set("capabilities", strings(capabilities));
    }
    if !request.allow_sets.is_empty() {
        set("allow_sets", strings(&request.allow_sets));
    }
    let numbers = [
        ("max_thread_tokens", request.max_thread_tokens),
        ("max_daily_tokens", request.max_daily_tokens),
        ("max_harnesses", request.max_harnesses.map(|n| n as u64)),
        ("max_tool_calls", request.max_tool_calls.map(|n| n as u64)),
    ];
    for (key, value) in numbers {
        if let Some(value) = value {
            set(key, value.to_string());
        }
    }
    for (key, value) in [("max_thread_cost", request.max_thread_cost), ("max_daily_cost", request.max_daily_cost)] {
        if let Some(value) = value {
            set(key, toml_float(value));
        }
    }
    if let Some(timeout) = request.prompt_timeout {
        set("prompt_timeout", toml_string(&ants_proto::duration::format(timeout)));
    }
    Ok(out)
}

/// `s` as a TOML basic string.
pub fn toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push('\t'),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn toml_float(x: f64) -> String {
    if x.is_nan() {
        "nan".to_owned()
    } else if x.is_infinite() {
        if x > 0.0 { "inf" } else { "-inf" }.to_owned()
    } else {
        // Debug always shows a fraction or an exponent, as TOML floats need.
        format!("{x:?}")
    }
}
