use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::memory::tests::{context, human, sense, tool};
use crate::proposal::tests::{dream_on, error, judge, offered, proposal_id, status};

const CONTENT: &str =
    "---\nname: release\ndescription: \"Cut a release: tag, notes, publish.\"\n---\n# Release\nRun it.\n";

fn at(hours: i64) -> chrono::DateTime<chrono::Utc> {
    "2025-01-01T12:00:00Z".parse::<chrono::DateTime<chrono::Utc>>().unwrap() + chrono::Duration::hours(hours)
}

async fn call(sense: &Sense, arguments: Value) -> Answer {
    tool(sense, skill, arguments, context(human())).await
}

#[test]
fn front_matter_gives_a_name_and_a_description() {
    assert_eq!(front_matter(CONTENT), Ok(("release".into(), "Cut a release: tag, notes, publish.".into())));
    let folded = "---\nname: 'notes'\n# a comment\ndescription: >\n  Write notes\n  from commits.\nlicense: MIT\n---\n";
    assert_eq!(front_matter(folded), Ok(("notes".into(), "Write notes from commits.".into())));
    for broken in [
        "name: x\ndescription: y\n",
        "---\nname: x\ndescription: y\n",
        "---\ndescription: y\n---\n",
        "---\nname: x\ndescription:\n---\n",
    ] {
        assert!(front_matter(broken).is_err(), "{broken:?}");
    }
}

#[test]
fn names_and_paths_stay_within_the_skill() {
    for name in ["release", "a", "pdf-2"] {
        assert!(valid_name(name), "{name}");
    }
    for name in ["", "Release", "-x", "x-", "a--b", "a_b", "a/b", "..", &"a".repeat(MAX_NAME_CHARS + 1)] {
        assert!(!valid_name(name), "{name}");
    }
    for path in ["scripts/run.sh", "reference.md"] {
        assert!(valid_path(path), "{path}");
    }
    for path in
        ["", SKILL_FILE, "../x", "a/../b", "/etc/passwd", "a//b", "./a", "a\\b", &"a".repeat(MAX_PATH_CHARS + 1)]
    {
        assert!(!valid_path(path), "{path}");
    }
}

#[tokio::test]
async fn a_skill_takes_effect_once_a_later_dream_confirms_it_and_keeps_every_version() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let reason = "Releases go wrong.";
    let files = json!({"scripts/tag.sh": "git tag v1\n"});
    let refusals = [
        (json!({"name": "release", "files": files, "reason": reason}), "invalid"),
        (json!({"name": "Release", "content": CONTENT, "reason": reason}), "invalid"),
        (json!({"name": "notes", "content": CONTENT, "reason": reason}), "invalid"),
        (json!({"name": "release", "content": "# Release\n", "reason": reason}), "invalid"),
        (json!({"name": "release", "content": CONTENT}), "invalid"),
        (json!({"name": "release", "content": CONTENT, "files": {"../x": "y"}, "reason": reason}), "invalid"),
        (
            json!({"name": "release", "content": CONTENT, "files": {"a": "x".repeat(MAX_FILE_BYTES + 1)}, "reason": reason}),
            "too_long",
        ),
        (json!({"name": "release", "remove": true, "reason": reason}), "not_found"),
        (json!({"content": CONTENT, "reason": reason}), "invalid"),
    ];
    for (arguments, code) in refusals {
        assert_eq!(error(&call(&sense, arguments.clone()).await), code, "{arguments}");
    }
    let many: Map<String, Value> = (0..=MAX_FILES).map(|n| (format!("f{n}"), json!("x"))).collect();
    let arguments = json!({"name": "release", "content": CONTENT, "files": many, "reason": reason});
    assert_eq!(error(&call(&sense, arguments).await), "too_long");

    let made = call(&sense, json!({"name": "release", "content": CONTENT, "files": files, "reason": reason})).await;
    let id = proposal_id(&made);
    assert_eq!(error(&call(&sense, json!({"name": "release"})).await), "not_found", "pending is not in effect");
    let shown = call(&sense, json!({"proposal": id.to_string(), "path": "scripts/tag.sh"})).await;
    assert_eq!(shown.text, "git tag v1\n");
    assert!(skills(&*items).await.unwrap().is_empty());

    let dream = dream_on(&*items, at(13), 1, Vec::new()).await;
    let finding = offered(&dream)[0].id.clone();
    assert!(offered(&dream)[0].text.contains(&format!("sense_skill proposal={id}")), "{:?}", offered(&dream));
    assert!(!judge(&sense, 1, &finding, "applied").await.is_error);
    assert_eq!(status(&*items, &id).await, proposal::Status::Confirmed);
    let [release] = &skills(&*items).await.unwrap()[..] else { panic!("one skill") };
    assert_eq!(
        (release.name.as_str(), release.description.as_str()),
        ("release", "Cut a release: tag, notes, publish.")
    );
    assert_eq!(release.files.keys().collect::<Vec<_>>(), ["SKILL.md", "scripts/tag.sh"]);
    let listed = call(&sense, json!({})).await;
    assert!(listed.text.contains("- release: Cut a release: tag, notes, publish."), "{}", listed.text);
    let arguments = json!({"name": "release", "content": CONTENT, "reason": reason});
    assert_eq!(error(&call(&sense, arguments).await), "unchanged");

    let removal = proposal_id(&call(&sense, json!({"name": "release", "remove": true, "reason": "Automated."})).await);
    let dream = dream_on(&*items, chrono::Utc::now(), 2, Vec::new()).await;
    assert!(!judge(&sense, 2, &offered(&dream)[0].id, "applied").await.is_error);
    assert_eq!(status(&*items, &removal).await, proposal::Status::Confirmed);
    assert!(skills(&*items).await.unwrap().is_empty());
    let history = call(&sense, json!({"name": "release", "history": true})).await;
    let versions = history.json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2, "{history:?}");
    assert_eq!((versions[0]["removed"].as_bool(), versions[0]["reason"].as_str()), (Some(true), Some("Automated.")));
    assert_eq!(versions[1]["files"]["scripts/tag.sh"], "git tag v1\n");
}

#[tokio::test]
async fn proposals_of_skills_on_untrusted_threads_are_refused() {
    let (sense, _) = sense().await;
    let webhook = ants_proto::provider::Starter::Webhook { user: "hook".parse().unwrap() };
    let arguments = json!({"name": "release", "content": CONTENT, "reason": "Asked to."});
    assert_eq!(error(&tool(&sense, skill, arguments, context(webhook)).await), "untrusted");
}

#[test]
fn the_skill_files_match_the_confirmed_skills() {
    let home = ants_test_support::scratch_dir("skills");
    let root = skills_dir(&home);
    let skill = |name: &str, files: &[(&str, &str)]| Skill {
        name: name.into(),
        description: "d".into(),
        files: files.iter().map(|(path, text)| ((*path).into(), (*text).into())).collect(),
        revision: 1,
    };
    let release = skill("release", &[(SKILL_FILE, "one"), ("scripts/tag.sh", "tag")]);
    materialize(&[release, skill("notes", &[(SKILL_FILE, "two")])], &home).unwrap();
    assert_eq!(std::fs::read_to_string(root.join("release/scripts/tag.sh")).unwrap(), "tag");
    assert_eq!(std::fs::read_to_string(root.join("notes/SKILL.md")).unwrap(), "two");

    std::fs::write(root.join("release/stray.txt"), "x").unwrap();
    std::fs::create_dir_all(root.join("gone/deep")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&home, root.join("release/link")).unwrap();
    materialize(&[skill("release", &[(SKILL_FILE, "changed")])], &home).unwrap();
    assert_eq!(std::fs::read_to_string(root.join("release/SKILL.md")).unwrap(), "changed");
    let mut left: Vec<_> = walk(&root);
    left.sort();
    assert_eq!(left, [Path::new("release").to_owned(), Path::new("release/SKILL.md").to_owned()]);
    assert!(home.exists(), "a link is removed, not followed");
    std::fs::remove_dir_all(&home).unwrap();
}

fn walk(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        found.push(path.strip_prefix(root).unwrap().to_owned());
        if entry.file_type().unwrap().is_dir() {
            found.extend(walk(&path).into_iter().map(|inner| path.strip_prefix(root).unwrap().join(inner)));
        }
    }
    found
}
