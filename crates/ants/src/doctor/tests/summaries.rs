//! Tests of the doctor's check of the `[cli]` summary keys: TL;DRs asked ahead of time from a summary ant whose harness
//! or a fallback reports no tokens, which `summary_daily_tokens` then never stops.

use super::*;

#[test]
fn summaries_ahead_from_an_ant_whose_harness_reports_no_tokens_are_warned_of() {
    let dir = ants_test_support::scratch_dir("doctor-summary-tokens");
    let config = dir.join("ants.toml");
    let checks = |cli: &str| {
        let text = format!(
            "messenger = false\nsage = \"none\"\nharness = \"claude\"\n[cli]\n{cli}\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nfallback = [\"codex\", \"copilot\"]\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n"
        );
        std::fs::write(&config, text).unwrap();
        let summaries = config::CliDefaults::load(&config).unwrap().summaries;
        check_summary_tokens(&config::load(&config).unwrap(), &summaries)
    };
    let ahead = |ant: &str| format!("summaries = \"ahead\"\nsummary_daily_tokens = 1000\nsummary_ant = \"{ant}\"");
    let warned = checks(&ahead("dev-a"));
    let found: Vec<_> = warned.iter().map(|c| (c.check.as_str(), c.status, c.detail.as_str())).collect();
    assert_eq!(
        found,
        [(
            "cli summaries",
            Status::Warn,
            "summaries = \"ahead\" asks dev-a for TL;DRs and harness copilot reports neither tokens nor cost, so \
             summary_daily_tokens never trips on it: ants chat asks ahead of time without bound"
        )],
        "claude and codex report tokens"
    );
    assert!(warned[0].fix[0].starts_with("set [cli] summary_ant to an ant whose harness"), "{}", warned[0]);
    assert!(checks(&ahead("dev-b")).is_empty(), "claude reports tokens");
    assert!(checks(&ahead("dev-c")).is_empty(), "an ant not served here goes unchecked");
    assert!(checks("summary_ant = \"dev-a\"").is_empty(), "asked on demand only");
    std::fs::remove_dir_all(&dir).unwrap();
}
