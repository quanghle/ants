//! The warning that spend passed a share of a cap, short of it, and the notice telling a human of it.

use ants_proto::{ThreadId, UserId};

/// Spend that passed the warning share of a cap, short of the cap.
#[derive(Debug, Clone, PartialEq)]
pub struct Warning {
    /// The cap's config key, such as `max_daily_cost`.
    pub key: &'static str,
    /// The template whose pool the cap is, for `pool_daily_tokens` and `pool_daily_cost`.
    pub pool: Option<String>,
    /// What was spent against the cap, as `ants spend` shows it: `850/1000 tokens` or `4.10/5.00`.
    pub figure: String,
    /// The share of the cap spent, in percent, rounded down.
    pub percent: u64,
}

impl Warning {
    /// Whether it is about the ant's daily allowance rather than the thread's.
    pub fn daily(&self) -> bool {
        self.key.starts_with("max_daily_") || self.key.starts_with("pool_daily_")
    }

    /// The notice telling a human of it, for `ant` spending on `thread`.
    pub fn text(&self, ant: &UserId, thread: &ThreadId) -> String {
        let Self { key, pool, figure, percent } = self;
        if let Some(template) = pool {
            return format!(
                "[ants] The ants added from template {template} spent {figure} ({percent}%) of its {key} today (UTC), \
                 {ant} last, on thread {thread}. At the cap, the prompts of all of them are cancelled and escalated \
                 until 00:00 UTC. To raise the cap, set {key} of [[ant_template]] {template} in the config files of \
                 the daemons serving them and reload them (SIGHUP)."
            );
        }
        let raise = format!(
            "To raise the cap, set {key} of {ant} in the config file of the daemon serving it and reload the daemon \
             (SIGHUP)."
        );
        if self.daily() {
            format!(
                "[ants] {ant} spent {figure} ({percent}%) of {key} today (UTC), last on thread {thread}. At the cap, \
                 its prompts are cancelled and escalated until 00:00 UTC. {raise} `ants spend {ant}` shows its spend."
            )
        } else {
            format!(
                "[ants] {ant} spent {figure} ({percent}%) of {key} on thread {thread} since a human's last message \
                 there. At the cap, its prompt there is cancelled and the thread escalated. A human's message on the \
                 thread gives it a fresh allowance. {raise} `ants spend {ant} --thread {thread}` shows its spend."
            )
        }
    }
}
