//! The check of the `[cli]` summary keys: TL;DRs asked ahead of time stop at `summary_daily_tokens`, counted from the
//! tokens the summary ant's replies carry, so its harness and fallbacks must report tokens.

use ants_daemon::config::{self, Summaries, SummaryConfig};
use ants_daemon::harness::Reported;

use super::Check;

/// A warning for each harness, of the summary ant served by this config file or of a fallback of it, reporting no
/// tokens while `summaries` asks ahead of time: `summary_daily_tokens` never stops it.
pub(super) fn check_summary_tokens(loaded: &config::Loaded, summaries: &SummaryConfig) -> Vec<Check> {
    if summaries.mode != Summaries::Ahead {
        return Vec::new();
    }
    let Some(ant) = loaded.ants.iter().find(|ant| ant.name == summaries.ant) else { return Vec::new() };
    let fix = "set [cli] summary_ant to an ant whose harness and fallbacks report tokens, or summaries = \"on-demand\" \
               (docs/config.md#catch-up-summaries)";
    ant.unreported(&loaded.harnesses, &[("summary_daily_tokens", Reported::Tokens)])
        .into_iter()
        .map(|(why, _)| {
            let detail = format!(
                "summaries = \"ahead\" asks {} for TL;DRs and {why}: ants chat asks ahead of time without bound",
                ant.name
            );
            Check::warn("cli summaries", detail, vec![fix.to_owned()])
        })
        .collect()
}
