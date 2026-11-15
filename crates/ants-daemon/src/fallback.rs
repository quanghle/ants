//! Fallbacks: the harnesses and models an ant fails over to while its own harness cannot serve, being signed out
//! ([`crate::signin`]), out of quota ([`crate::ratelimit`]) or failing to start ([`crate::breaker`]).
//!
//! An ant's `fallback` list names each one as `<harness>`, a harness with its default model, `<harness>:<model>`, a
//! harness with that model, or `:<model>`, the ant's own harness with another model. They are tried in order: each
//! message goes to the first one not turned away lately, in a new session seeded from the thread's history unless it
//! runs the ant's own harness. A fallback that is signed out, limited or fails to start is passed over for a while. The
//! ant goes back to its own harness once a check finds it signed in or starting, or, after a quota, once the backoff
//! passes and a message gets through.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use tokio::time::Instant;

use crate::harness::{Harness, Harnesses, ModelSettings};
use crate::sync::MutexExt as _;

/// How long a fallback that was signed out or failed to start is passed over, and a limited one that gave no hint.
pub const RETRY_AFTER: Duration = Duration::from_mins(5);

/// A harness and model an ant fails over to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fallback {
    pub harness: Arc<Harness>,
    /// Only the model, if one is named, and the ant's own history budget: the ant's reasoning effort and context are
    /// its own harness's settings.
    pub settings: ModelSettings,
}

impl Fallback {
    /// The fallback named by `entry`, of the ant whose own harness is `own` with `settings`, from `harnesses`.
    pub fn parse(entry: &str, own: &Arc<Harness>, settings: &ModelSettings, harnesses: &Harnesses) -> Result<Self> {
        let (harness, model) = match entry.trim().split_once(':') {
            Some(("", model)) => (own.clone(), Some(model)),
            Some((name, model)) => (harnesses.get(name.trim())?, Some(model)),
            None if entry.trim().is_empty() => bail!("fallback entries must not be blank"),
            None => (harnesses.get(entry.trim())?, None),
        };
        let model = match model.map(str::trim) {
            Some("") => bail!("fallback {entry:?} names no model after the colon"),
            model => model.map(str::to_owned),
        };
        let settings = ModelSettings { model, history_budget: settings.history_budget, ..ModelSettings::default() };
        settings.validate(&harness)?;
        Ok(Self { harness, settings })
    }

    /// As the ant's roster entry and replies name it: `<harness>` or `<harness>:<model>`.
    pub fn name(&self) -> String {
        match &self.settings.model {
            Some(model) => format!("{}:{model}", self.harness),
            None => self.harness.to_string(),
        }
    }
}

impl fmt::Display for Fallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name())
    }
}

/// The fallbacks `entries` name, in order, for an ant whose own harness is `own` with `settings`: each names a known
/// harness, once, and differs from the ant's own harness and model.
pub fn resolve(
    entries: &[String],
    own: &Arc<Harness>,
    settings: &ModelSettings,
    harnesses: &Harnesses,
) -> Result<Vec<Fallback>> {
    let mut seen = BTreeSet::new();
    let mut fallbacks = Vec::with_capacity(entries.len());
    for entry in entries {
        let fallback =
            Fallback::parse(entry, own, settings, harnesses).map_err(|e| e.context(format!("fallback {entry:?}")))?;
        if fallback.harness == *own && fallback.settings.model == settings.model {
            bail!("fallback {entry:?} is the ant's own harness and model");
        }
        if !seen.insert(fallback.name()) {
            bail!("fallback {} is listed twice", fallback.name());
        }
        fallbacks.push(fallback);
    }
    Ok(fallbacks)
}

/// A prompt on fallback `index` failed: the context of its error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnFallback {
    pub index: usize,
    pub name: String,
}

impl fmt::Display for OnFallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "on fallback {}", self.name)
    }
}

/// Which fallbacks are passed over for now, and since when one serves.
#[derive(Default)]
pub struct Failover(Mutex<State>);

#[derive(Default)]
struct State {
    /// Fallbacks turned away lately, and when they may be tried again.
    down: HashMap<usize, Instant>,
    /// When a fallback first answered since the ant's own harness last could.
    since: Option<DateTime<Utc>>,
}

impl Failover {
    /// The first of `count` fallbacks not passed over at `now`.
    pub fn pick(&self, count: usize, now: Instant) -> Option<usize> {
        let state = self.0.locked();
        (0..count).find(|i| state.down.get(i).is_none_or(|until| *until <= now))
    }

    /// Pass fallback `index` over until `until`.
    pub fn down(&self, index: usize, until: Instant) {
        self.0.locked().down.insert(index, until);
    }

    /// When the first fallback passed over at `now` may be tried again.
    pub fn next_up(&self, now: Instant) -> Option<Instant> {
        self.0.locked().down.values().copied().filter(|until| *until > now).min()
    }

    /// A fallback answered: since when one serves.
    pub fn served(&self) -> DateTime<Utc> {
        *self.0.locked().since.get_or_insert_with(Utc::now)
    }

    /// The ant's own harness serves again: every fallback may be tried at the next outage. Returns whether one
    /// served.
    pub fn clear(&self) -> bool {
        std::mem::take(&mut *self.0.locked()).since.is_some()
    }
}

/// The sentence of a notice telling where messages to `ant` go while its own harness cannot serve, until `back`
/// happens; `None` without `fallbacks`, when they wait.
pub fn meanwhile(ant: &impl fmt::Display, fallbacks: &[String], back: &str) -> Option<String> {
    let (first, rest) = fallbacks.split_first()?;
    let then = if rest.is_empty() { String::new() } else { format!(", then {}", rest.join(", then ")) };
    Some(format!("Messages to {ant} go to its fallback {first}{then} meanwhile, and back to the harness {back}."))
}

/// What a notice of an outage of the ant's own harness says the ant cannot answer with: everything, without
/// `fallbacks`.
pub fn own(fallbacks: &[String]) -> &'static str {
    if fallbacks.is_empty() { "" } else { " with its own harness" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(entries: &[&str], model: Option<&str>) -> Result<Vec<String>> {
        let harnesses = Harnesses::builtin();
        let own = harnesses.get("copilot").unwrap();
        let settings = ModelSettings { model: model.map(str::to_owned), ..ModelSettings::default() };
        let entries: Vec<String> = entries.iter().map(|e| (*e).to_owned()).collect();
        Ok(resolve(&entries, &own, &settings, &harnesses)?.iter().map(Fallback::name).collect())
    }

    #[test]
    fn entries_name_a_harness_a_harness_and_model_or_another_model() {
        let names = resolved(&["claude", "claude:opus", ":gpt-5", " antigravity : pro "], None).unwrap();
        assert_eq!(names, ["claude", "claude:opus", "copilot:gpt-5", "antigravity:pro"]);
        assert_eq!(resolved(&["copilot"], Some("gpt-5")).unwrap(), ["copilot"], "the harness's default model");

        let error = |entries: &[&str], model| format!("{:#}", resolved(entries, model).unwrap_err());
        assert!(error(&["nope"], None).contains("unknown harness \"nope\""), "{}", error(&["nope"], None));
        assert!(error(&[" "], None).contains("must not be blank"));
        assert!(error(&["claude:"], None).contains("names no model"));
        assert!(error(&["copilot"], None).contains("own harness and model"));
        assert!(error(&[":gpt-5"], Some("gpt-5")).contains("own harness and model"));
        assert!(error(&["claude", " claude"], None).contains("listed twice"));
    }

    #[test]
    fn a_fallback_turned_away_is_passed_over_until_its_time() {
        let failover = Failover::default();
        let now = Instant::now();
        assert_eq!(failover.pick(2, now), Some(0));
        failover.down(0, now + Duration::from_secs(60));
        failover.down(1, now + Duration::from_secs(30));
        assert_eq!(failover.pick(2, now), None);
        assert_eq!(failover.next_up(now), Some(now + Duration::from_secs(30)));
        let later = now + Duration::from_secs(30);
        assert_eq!(failover.pick(2, later), Some(1));
        assert_eq!(failover.next_up(later), Some(now + Duration::from_secs(60)));

        let since = failover.served();
        assert_eq!(failover.served(), since, "the same outage");
        assert!(failover.clear());
        assert!(!failover.clear());
        assert_eq!((failover.pick(2, now), failover.next_up(now)), (Some(0), None), "a new outage tries all again");
    }

    #[test]
    fn notices_name_the_fallbacks_in_order() {
        assert_eq!(meanwhile(&"dev-a", &[], "once signed in"), None);
        assert_eq!(
            meanwhile(&"dev-a", &["claude".to_owned(), "copilot:gpt-5".to_owned()], "once a check starts it").unwrap(),
            "Messages to dev-a go to its fallback claude, then copilot:gpt-5 meanwhile, and back to the harness once \
             a check starts it."
        );
    }
}
