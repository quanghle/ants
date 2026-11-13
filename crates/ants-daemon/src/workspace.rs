//! The persona's own bd workspace: its to-dos and memories, local to its machine.
//!
//! Harness sessions reach it through `BD_DB`, so the bd hooks they run (`bd prime`) and every bd command they issue
//! use the persona's workspace instead of the workdir's. bd's SQLite transactions keep concurrent sessions safe.

use std::path::{Path, PathBuf};

use ants_proto::PersonaId;
use anyhow::{Context as _, Result, bail};

/// Longest issue id prefix bd accepts.
const MAX_PREFIX: usize = 32;

/// `~/.ants/<persona>`: the persona's own files.
pub fn default_home(persona: &PersonaId) -> Result<PathBuf> {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")).context("no home directory")?;
    Ok(Path::new(&home).join(".ants").join(persona.as_str()))
}

/// The workspace database inside a persona's `home`.
pub fn database(home: &Path) -> PathBuf {
    home.join(".bd").join("bd.db")
}

/// Create the persona's workspace in `home` unless it exists, and return its database.
pub async fn ensure(home: &Path, persona: &PersonaId) -> Result<PathBuf> {
    let db = database(home);
    if db.exists() {
        return Ok(db);
    }
    tokio::fs::create_dir_all(home).await.with_context(|| format!("creating {}", home.display()))?;
    let out = tokio::process::Command::new("bd")
        .arg("init")
        .arg("-C")
        .arg(home)
        .args(["--prefix", prefix(persona), "--quiet"])
        // Inherited, these would point bd init at another workspace.
        .env_remove("BD_DB")
        .env_remove("BD_REMOTE")
        .output()
        .await
        .context("running bd init (is bd on PATH?)")?;
    if !out.status.success() {
        bail!("bd init in {}: {}", home.display(), String::from_utf8_lossy(&out.stderr).trim());
    }
    tracing::info!(db = %db.display(), "created persona workspace");
    Ok(db)
}

/// Environment of a harness session, pointing bd at the workspace database `db`.
pub fn harness_env(db: &Path) -> Result<Vec<(String, String)>> {
    let db = db.to_str().with_context(|| format!("workspace path is not UTF-8: {}", db.display()))?;
    // An inherited BD_REMOTE alongside BD_DB makes every bd command fail; empty counts as unset.
    Ok(vec![("BD_DB".to_owned(), db.to_owned()), ("BD_REMOTE".to_owned(), String::new())])
}

fn prefix(persona: &PersonaId) -> &str {
    let s = persona.as_str();
    s[..s.len().min(MAX_PREFIX)].trim_end_matches('-')
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    fn bd(env: &[(String, String)], args: &[&str]) -> String {
        let out = Command::new("bd")
            .args(args)
            .envs(env.iter().map(|(k, v)| (k, v)))
            .env("BD_ACTOR", "test")
            .output()
            .expect("running bd");
        assert!(out.status.success(), "bd {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).expect("bd output")
    }

    #[test]
    fn long_persona_names_fit_the_prefix_limit() {
        let persona: PersonaId = format!("{}-b", "a".repeat(MAX_PREFIX - 1)).parse().unwrap();
        assert_eq!(prefix(&persona), "a".repeat(MAX_PREFIX - 1));
    }

    #[tokio::test]
    async fn each_persona_remembers_in_its_own_workspace() {
        if !ants_test_support::have("bd") {
            return;
        }
        let root = std::env::temp_dir().join(format!("ants-workspace-{}", std::process::id()));
        let (a, b): (PersonaId, PersonaId) = ("dev-a".parse().unwrap(), "dev-b".parse().unwrap());

        let db_a = ensure(&root.join("dev-a"), &a).await.unwrap();
        assert!(db_a.exists());
        assert_eq!(ensure(&root.join("dev-a"), &a).await.unwrap(), db_a, "second start reuses the workspace");
        let db_b = ensure(&root.join("dev-b"), &b).await.unwrap();

        let (env_a, env_b) = (harness_env(&db_a).unwrap(), harness_env(&db_b).unwrap());
        bd(&env_a, &["remember", "the build needs nightly"]);
        assert!(bd(&env_a, &["prime", "--hook", "copilot"]).contains("the build needs nightly"));
        assert!(!bd(&env_b, &["prime", "--hook", "copilot"]).contains("the build needs nightly"));

        std::fs::remove_dir_all(&root).unwrap();
    }
}
