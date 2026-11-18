//! The ant's confirmed sense skills, offered through each harness's own Agent Skills mechanism: Copilot CLI's skill
//! directories, a Claude Code plugin and Codex's extra skill roots. Harnesses without a clean way to add a directory
//! of skills see them only through the brief's index and `sense_skill`.

use std::path::{Path, PathBuf};

use agent_client_protocol::v1::schema::Meta;
use serde_json::{Value, json};

use super::Harness;

/// The variable Copilot CLI reads extra skill directories from, comma-separated.
const COPILOT_SKILLS_DIRS: &str = "COPILOT_SKILLS_DIRS";

/// The key of a session/new or session/load `_meta` naming the skills directory, which the native translators read.
const META_KEY: &str = "ants/skills";

/// The ant's confirmed skills: the directory holding them, one directory a skill, and the root of the Claude Code
/// plugin whose skills they are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skills {
    pub dir: PathBuf,
    pub plugin: PathBuf,
}

/// `skills` when its directory holds a skill: a directory with a `SKILL.md`.
pub(super) fn offered(skills: &Skills) -> Option<&Skills> {
    let mut entries = std::fs::read_dir(&skills.dir).ok()?.flatten();
    entries.any(|entry| entry.path().join("SKILL.md").is_file()).then_some(skills)
}

/// The variable adding the skills of `dir` to an ACP harness process with the ant's `env`, after the skill directories
/// it already names there, in the definition's or in the daemon's environment; None for a translated protocol.
pub(super) fn env(harness: &Harness, env: &[(String, String)], dir: &Path) -> Option<(String, String)> {
    if harness.protocol.is_native() {
        return None;
    }
    let ant = env.iter().rev().find(|(name, _)| name == COPILOT_SKILLS_DIRS).map(|(_, value)| value.clone());
    let named = ant.or_else(|| harness.env.get(COPILOT_SKILLS_DIRS).cloned());
    let named = named.or_else(|| std::env::var(COPILOT_SKILLS_DIRS).ok()).filter(|dirs| !dirs.is_empty());
    let dir = dir.display();
    let value = named.map_or_else(|| dir.to_string(), |dirs| format!("{dirs},{dir}"));
    Some((COPILOT_SKILLS_DIRS.to_owned(), value))
}

/// The `_meta` of a session/new or session/load offering `skills`: their directory for the native translators, and
/// for the Claude Agent SDK their plugin, loaded as a local one.
pub(super) fn meta(skills: &Skills) -> Meta {
    let plugins = json!([{ "type": "local", "path": skills.plugin, "skipMcpDiscovery": true }]);
    let mut meta = Meta::new();
    meta.insert(META_KEY.to_owned(), json!(skills.dir));
    meta.insert("claudeCode".to_owned(), json!({ "options": { "plugins": plugins } }));
    meta
}

/// The skills a session/new or session/load `meta` offers ([`meta`]).
pub(super) fn of_meta(meta: Option<&Meta>) -> Option<Skills> {
    let meta = meta?;
    let dir = meta.get(META_KEY).and_then(Value::as_str).map(PathBuf::from)?;
    let plugins = meta.get("claudeCode").and_then(|claude| claude.pointer("/options/plugins/0/path"));
    let plugin = plugins.and_then(Value::as_str).map(PathBuf::from)?;
    Some(Skills { dir, plugin })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{Harnesses, parse_definitions};

    fn harness(protocol: &str, env: &str) -> std::sync::Arc<Harness> {
        let text = format!("[harnesses.h]\ncommand = \"h\"\nprotocol = \"{protocol}\"\nenv = {{ {env} }}");
        Harnesses::builtin().define(parse_definitions(&text).unwrap()).unwrap().get("h").unwrap()
    }

    fn skills(dir: &Path) -> Skills {
        Skills { dir: dir.to_owned(), plugin: "/plugin".into() }
    }

    #[test]
    fn a_directory_offers_skills_once_one_has_its_skill_md() {
        let dir = ants_test_support::scratch_dir("skills-offered");
        assert_eq!(offered(&skills(&dir.join("missing"))), None);
        std::fs::create_dir_all(dir.join("draft")).unwrap();
        assert_eq!(offered(&skills(&dir)), None, "no SKILL.md yet");
        std::fs::write(dir.join("draft/SKILL.md"), "---\nname: draft\n---\n").unwrap();
        assert_eq!(offered(&skills(&dir)), Some(&skills(&dir)));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn acp_harnesses_get_the_skills_after_the_directories_already_named() {
        let dir = Path::new("/home/a/.sense/skills");
        let acp = harness("acp", "");
        let added = env(&acp, &[], dir);
        let daemon = std::env::var(COPILOT_SKILLS_DIRS).map_or(String::new(), |dirs| format!("{dirs},"));
        assert_eq!(added, Some((COPILOT_SKILLS_DIRS.to_owned(), format!("{daemon}/home/a/.sense/skills"))));
        let defined = harness("acp", "COPILOT_SKILLS_DIRS = \"/team\"");
        assert_eq!(env(&defined, &[], dir).unwrap().1, "/team,/home/a/.sense/skills");
        let ant = [(COPILOT_SKILLS_DIRS.to_owned(), "/mine".to_owned())];
        assert_eq!(
            env(&defined, &ant, dir).unwrap().1,
            "/mine,/home/a/.sense/skills",
            "the ant's over the definition's"
        );
        assert_eq!(env(&harness("codex", ""), &[], dir), None, "a translator offers them its own way");
    }

    #[test]
    fn the_meta_names_the_skills_and_their_plugin() {
        let offered = Skills { dir: "/home/a/.sense/skills".into(), plugin: "/home/a/plugin".into() };
        let meta = meta(&offered);
        assert_eq!(of_meta(Some(&meta)), Some(offered));
        let plugins = &meta["claudeCode"]["options"]["plugins"];
        assert_eq!(plugins, &json!([{ "type": "local", "path": "/home/a/plugin", "skipMcpDiscovery": true }]));
        assert_eq!(of_meta(None), None);
        assert_eq!(of_meta(Some(&Meta::new())), None);
    }
}
