//! Tests of who reviews each ant: the built-in sage by default and served only while named, an ant named as a sage
//! having none, and the `sage` setting naming an ant or none.

use std::path::Path;

use super::*;
use crate::config::{Loaded, builtin_sage, parse};
use crate::harness::Harnesses;

/// `text` with `harness = "copilot"` on top, parsed as if it sat in this crate's directory.
fn load(text: &str) -> Result<Loaded> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    parse(&format!("harness = \"copilot\"\n{text}"), &dir.join("config.toml"), dir.parent().unwrap())
}

/// Each ant's name, with the ant reviewing it.
fn reviewers(text: &str) -> Vec<(String, Option<String>)> {
    let loaded = load(text).unwrap();
    loaded.ants.iter().map(|ant| (ant.name.to_string(), ant.reviewed_by().map(|s| s.to_string()))).collect()
}

fn err(text: &str) -> String {
    format!("{:#}", load(text).unwrap_err())
}

fn ant(name: &str, more: &str) -> String {
    format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\n{more}\n")
}

fn pairs(list: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
    list.iter().map(|(ant, sage)| ((*ant).to_owned(), sage.map(str::to_owned))).collect()
}

#[test]
fn every_ant_names_the_builtin_sage_by_default_which_the_file_then_serves() {
    let text = format!("{}{}", ant("dev-a", ""), ant("dev-b", ""));
    assert_eq!(
        reviewers(&text),
        pairs(&[
            ("dev-a", Some("system-sage")),
            ("dev-b", Some("system-sage")),
            ("system-messenger", None),
            ("system-sage", None)
        ]),
        "the built-in ants have no sage"
    );
    let ants = load(&text).unwrap().ants;
    let sage = ants.iter().find(|ant| ant.name == UserId::sage()).unwrap();
    assert_eq!(sage.builtin, Some(ants_proto::BuiltinAnt::Sage));
    assert!(sage.is_sage);
    assert_eq!(sage.harness.as_deref(), Some("copilot"), "the file's top-level harness");

    let home = ants_test_support::scratch_dir("sage-home").canonicalize().unwrap();
    let sage = Ant { human: Some(ants_test_support::human()), ..builtin_sage(Some("copilot"), &home) };
    let config = sage.into_config("nats://unused", &Harnesses::builtin()).unwrap();
    assert_eq!(config.presence.builtin, Some(ants_proto::BuiltinAnt::Sage));
    assert_eq!(config.harness.workdir, home.join(".ants/agents/system-sage").canonicalize().unwrap());
    assert_eq!(config.policy.sage, None);
    std::fs::remove_dir_all(&home).unwrap();

    let names: Vec<String> = load("").unwrap().ants.iter().map(|ant| ant.name.to_string()).collect();
    assert_eq!(names, ["system-messenger"], "the built-in messenger names no sage");
}

#[test]
fn the_file_serves_the_builtin_sage_only_while_an_ant_names_it() {
    let off = format!("sage = \"none\"\n{}{}", ant("dev-a", ""), ant("dev-b", "sage = \"none\""));
    assert_eq!(reviewers(&off), pairs(&[("dev-a", None), ("dev-b", None), ("system-messenger", None)]));

    let each_off = format!("{}{}", ant("dev-a", "sage = \"none\""), ant("dev-b", "sage = \"none\""));
    assert_eq!(reviewers(&each_off), pairs(&[("dev-a", None), ("dev-b", None), ("system-messenger", None)]));

    let one_on = format!("sage = \"none\"\n{}{}", ant("dev-a", ""), ant("dev-b", "sage = \"system-sage\""));
    assert_eq!(
        reviewers(&one_on),
        pairs(&[("dev-a", None), ("dev-b", Some("system-sage")), ("system-messenger", None), ("system-sage", None)])
    );

    let listed = format!("sage = \"sage\"\n{}{}", ant("dev-a", ""), ant("sage", "harness = \"claude\""));
    assert_eq!(reviewers(&listed), pairs(&[("dev-a", Some("sage")), ("sage", None), ("system-messenger", None)]));
    let ants = load(&listed).unwrap().ants;
    let sage = ants.iter().find(|ant| ant.name.as_str() == "sage").unwrap();
    assert_eq!((sage.builtin, sage.harness.as_deref()), (None, Some("claude")), "an ordinary ant can be assigned");
}

#[test]
fn an_ant_named_as_a_sage_has_none_of_its_own() {
    let other = format!("{}{}{}", ant("dev-a", "sage = \"lead\""), ant("dev-b", ""), ant("lead", ""));
    assert_eq!(
        reviewers(&other),
        pairs(&[
            ("dev-a", Some("lead")),
            ("dev-b", Some("system-sage")),
            ("lead", None),
            ("system-messenger", None),
            ("system-sage", None)
        ])
    );

    let top = format!("sage = \"lead\"\n{}{}", ant("dev-a", ""), ant("lead", ""));
    assert_eq!(reviewers(&top), pairs(&[("dev-a", Some("lead")), ("lead", None), ("system-messenger", None)]));

    let elsewhere = ant("dev-a", "sage = \"reviewer\"");
    assert_eq!(
        reviewers(&elsewhere),
        pairs(&[("dev-a", Some("reviewer")), ("system-messenger", None)]),
        "a sage the file does not list may be served on another machine"
    );

    let chained = format!("{}{}", ant("dev-a", "sage = \"lead\""), ant("lead", "sage = \"sage\""));
    let e = err(&chained);
    assert!(e.contains("ant lead: sage names sage, but lead reviews other ants and a sage has no sage"), "{e}");
    let crossed = format!("sage = \"lead\"\n{}{}", ant("dev-a", ""), ant("lead", "sage = \"dev-a\""));
    assert_eq!(
        reviewers(&crossed),
        pairs(&[("dev-a", None), ("lead", Some("dev-a")), ("system-messenger", None)]),
        "lead's own setting makes dev-a a sage, over the file's naming lead"
    );
    let off = format!("{}{}", ant("dev-a", "sage = \"lead\""), ant("lead", "sage = \"none\""));
    assert_eq!(reviewers(&off), pairs(&[("dev-a", Some("lead")), ("lead", None), ("system-messenger", None)]));
}

#[test]
fn a_sage_setting_names_an_ant_or_none() {
    assert!(err(&ant("dev-a", "sage = \"dev-a\"")).contains("ant dev-a: sage names the ant itself"));
    let e = err(&ant("dev-a", "sage = \"Lead\""));
    assert!(e.contains("sage \"Lead\" is neither an ant's name nor none"), "{e}");
    assert!(err("sage = \"\"").contains("sage \"\" is neither"));
    assert_eq!("none".parse::<Sage>().unwrap().to_string(), "none");
    assert_eq!("lead".parse::<Sage>().unwrap().to_string(), "lead");
}
