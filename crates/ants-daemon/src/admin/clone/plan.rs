//! Checking a clone before anything changes ([`plan`]), and the `[[ant]]` table and shared settings it gives the clone.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{AdminOrder, UserId, is_shared_field};
use anyhow::{Context as _, Result, bail};
use serde_json::{Map, Value, json};

use crate::admin::add::{NOT_GIVEN, entry, entry_text};
use crate::config::{self, edit::Edit};
use crate::profile::shared;

/// Keys of the ant's table a clone does not copy: those naming it or set for it alone.
const NOT_CLONED: [&str; 8] = ["name", "creds", "paused", "added_by", "template", "builtin", "home", "allow_sets"];

/// The comment above the table of the clone `order` makes.
fn mark(order: &AdminOrder) -> String {
    format!("# cloned from {} by {}'s order {}", order.ant, order.admin, order.id)
}

/// A clone checked before anything changes.
pub(super) struct Plan {
    /// The `[[ant]]` table of the clone.
    pub(super) table: String,
    pub(super) workdir: PathBuf,
    pub(super) src_home: PathBuf,
    pub(super) new_home: PathBuf,
    /// The directory of the host credentials, where the clone's go.
    pub(super) dir: PathBuf,
    pub(super) creds_path: PathBuf,
    /// The clone's shared settings, where the hub keeps them.
    pub(super) shared: Option<Map<String, Value>>,
}

/// Check the clone of the order's ant as `new` with the config at `path`: none when the config shows it done already.
pub(super) fn plan(
    path: &Path,
    host_creds: &Path,
    order: &AdminOrder,
    new: &UserId,
    settings: &Map<String, Value>,
    src_shared: Option<Option<Map<String, Value>>>,
) -> Result<Option<Plan>> {
    let src = &order.ant;
    let mark = mark(order);
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    if text.lines().any(|line| line.trim() == mark) {
        return Ok(None);
    }
    if new.is_human() {
        bail!("{new} is a human's name, not an ant's");
    }
    if new == src {
        bail!("a clone needs another name than {src}");
    }
    if let Some(key) = NOT_GIVEN.iter().find(|key| settings.contains_key(**key)) {
        bail!("the daemon sets {key} itself: an order does not give it");
    }
    if settings.get("name").is_some_and(|name| name.as_str() != Some(new.as_str())) {
        bail!("the settings name another ant than {new}");
    }
    let loaded = config::load(path)?;
    if loaded.ants.iter().any(|a| a.name == *new) {
        bail!("config {} lists {new} already", path.display());
    }
    let ant = loaded
        .ants
        .iter()
        .find(|a| a.name == *src)
        .with_context(|| format!("config {} lists no ant {src}", path.display()))?;
    if ant.builtin {
        bail!("{src} is built in: it is not cloned");
    }
    let (base, user_home) = (path.parent().unwrap_or(Path::new(".")), config::home_dir()?);
    let src_home = ant.home.clone().unwrap_or_else(|| user_home.join(".ants").join(src.as_str()));
    let new_home = match settings.get("home") {
        Some(home) => config::resolve(Path::new(home.as_str().context("home is no string")?), base, &user_home),
        None => user_home.join(".ants").join(new.as_str()),
    };
    if new_home.starts_with(&src_home) || src_home.starts_with(&new_home) {
        bail!("the clone's home {} and {src}'s {} must not hold one another", new_home.display(), src_home.display());
    }
    if std::fs::read_dir(&new_home).is_ok_and(|mut dir| dir.next().is_some()) {
        bail!("{new}'s home {} holds files already", new_home.display());
    }
    let dir = host_creds.parent().context("the host credentials are in no directory")?;
    let creds_path = dir.join(format!("{new}.creds"));
    let shared_over = src_shared.as_ref().and_then(Option::as_ref);
    let source = Source { path, ant, home: &src_home, shared: shared_over };
    let target = Target { new, home: &new_home, creds: &creds_path, settings, mark: &mark };
    let (table, probe, workdir, shared) = table(source, target)?;
    config::parse(&entry_text(&text, &probe), &std::path::absolute(path)?, &user_home)
        .context("the config with the clone added")?;
    let shared = src_shared.map(|_| shared);
    Ok(Some(Plan { table, workdir, src_home, new_home, dir: dir.to_owned(), creds_path, shared }))
}

/// The ant cloned, as the clone's table is made from it.
#[derive(Clone, Copy)]
struct Source<'a> {
    /// The config file listing it.
    path: &'a Path,
    ant: &'a config::Ant,
    home: &'a Path,
    /// Its shared settings, when it has a record of them.
    shared: Option<&'a Map<String, Value>>,
}

/// The clone, as its table names it.
#[derive(Clone, Copy)]
struct Target<'a> {
    new: &'a UserId,
    home: &'a Path,
    creds: &'a Path,
    /// The order's settings, over those of the ant cloned.
    settings: &'a Map<String, Value>,
    /// The comment above the table ([`mark`]).
    mark: &'a str,
}

/// The `[[ant]]` table of the clone `new` of `ant`, one to check the file with before its workdir exists, and its
/// workdir: the table of `ant` in the file at `path` (or, for an ant added from a template, the settings the template
/// gave it) with `shared`, its shared settings when it has a record of them, and `settings` over it, its workdir moved
/// into `new_home` if it was in `src_home`; and the clone's shared settings, as the table gives them.
fn table(source: Source<'_>, target: Target<'_>) -> Result<(String, String, PathBuf, Map<String, Value>)> {
    let Source { path, ant, home: src_home, shared } = source;
    let Target { new, home: new_home, creds, settings, mark } = target;
    let raw = Edit::open(path)?.table(&ant.name)?;
    let mut table = if ant.template.is_some() {
        let mut plain = resolved(ant);
        plain.extend(raw.into_iter().filter(|(key, _)| key == "nats" || key == "js_domain"));
        plain
    } else {
        raw
    };
    table.retain(|key, _| !NOT_CLONED.contains(&key.as_str()));
    if let Some(shared) = shared {
        table.retain(|key, _| !shared::shares(ant, key));
        table.extend(shared.iter().filter(|(key, _)| shared::shares(ant, key)).map(|(k, v)| (k.clone(), v.clone())));
    }
    if let Ok(inside) = ant.workdir.strip_prefix(src_home) {
        let workdir = new_home.join(inside);
        table.insert("workdir".to_owned(), Value::from(workdir.to_str().context("the workdir is not UTF-8")?));
    }
    table.extend(settings.iter().filter(|(key, _)| *key != "name").map(|(k, v)| (k.clone(), v.clone())));
    let text = entry(new, creds, None, &table, mark)?;
    let (base, home) = (path.parent().unwrap_or(Path::new(".")), config::home_dir()?);
    let workdir = table.get("workdir").and_then(Value::as_str).context("the ant has no workdir")?;
    let workdir = config::resolve(Path::new(workdir), base, &home);
    if !workdir.exists() {
        table.insert("workdir".to_owned(), Value::from(base.to_str().context("the config's directory is not UTF-8")?));
    }
    let probe = entry(new, creds, None, &table, mark)?;
    let shared = table.iter().filter(|(key, _)| is_shared_field(key)).map(|(k, v)| (k.clone(), v.clone())).collect();
    Ok((text, probe, workdir, shared))
}

/// The settings of `ant`, added from a template, as a table of its own.
fn resolved(ant: &config::Ant) -> Map<String, Value> {
    let duration = |d: Duration| Value::from(ants_proto::duration::format(d));
    let escalate_to: Vec<&str> = ant.escalate_to.iter().map(UserId::as_str).collect();
    let table = json!({
        "workdir": ant.workdir,
        "whoami": ant.whoami,
        "memory": ant.memory,
        "tasks": ant.tasks,
        "env": ant.env,
        "harness": ant.harness,
        "model": ant.model,
        "reasoning_effort": ant.reasoning_effort,
        "context": ant.context,
        "allow": ant.allow,
        "approval_timeout": duration(ant.approval_timeout),
        "capabilities": ant.capabilities,
        "max_harnesses": ant.max_harnesses,
        "worktrees": ant.worktrees,
        "idle_timeout": duration(ant.idle_timeout),
        "prompt_timeout": duration(ant.prompt_timeout),
        "max_tool_calls": ant.max_tool_calls,
        "max_thread_tokens": ant.max_thread_tokens,
        "max_thread_cost": ant.max_thread_cost,
        "max_daily_tokens": ant.max_daily_tokens,
        "max_daily_cost": ant.max_daily_cost,
        "spend_warn_percent": ant.spend_warn_percent,
        "escalate_to": escalate_to,
        "worktree_base": ant.worktree_base.as_str(),
        "publish": ant.publish.as_str(),
    });
    let Value::Object(mut table) = table else { unreachable!("an object") };
    table.retain(|_, value| match value {
        Value::Null => false,
        Value::Array(items) => !items.is_empty(),
        Value::Object(items) => !items.is_empty(),
        _ => true,
    });
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clone_of_an_ant_added_from_a_template_keeps_what_the_template_fixed() {
        let dir = ants_test_support::scratch_dir("clone-templated").canonicalize().unwrap();
        let path = dir.join("config.toml");
        let text = r#"
            messenger = false
            sage = "none"

            [[ant_template]]
            name = "worker"
            managers = ["lead"]
            max_ants = 1
            workdirs = ["."]
            harnesses = ["codex"]
            models = { "gpt-5" = "high" }
            allow = ["read"]

            [[ant]]
            name = "lead"
            workdir = "."
            harness = "codex"

            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"
        "#;
        std::fs::write(&path, text).unwrap();
        let loaded = config::parse(text, &path, &dir).unwrap();
        let src = loaded.ants.iter().find(|a| a.name.as_str() == "lead-a").unwrap();
        let record = json!({ "model": "gpt-5", "whoami": "a tester" });
        let (src_home, new_home, creds) = (dir.join("lead-a"), dir.join("lead-b"), dir.join("lead-b.creds"));
        let source = Source { path: &path, ant: src, home: &src_home, shared: record.as_object() };
        let new = "lead-b".parse().unwrap();
        let settings = Map::new();
        let target = Target { new: &new, home: &new_home, creds: &creds, settings: &settings, mark: "# cloned" };
        let (.., shared) = table(source, target).unwrap();
        assert_eq!(shared.get("whoami"), Some(&json!("a tester")), "the shared settings of the ant cloned");
        assert_eq!(shared.get("allow"), Some(&json!(["read"])), "the template's");
        assert_eq!(shared.get("escalate_to"), Some(&json!(["lead"])));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
