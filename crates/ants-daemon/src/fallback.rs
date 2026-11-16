//! Fallbacks: the harnesses and models an ant fails over to while its own harness cannot serve, being signed out
//! ([`crate::signin`]), out of quota ([`crate::ratelimit`]) or failing to start ([`crate::breaker`]).
//!
//! An ant's `fallback` list names each one as `<harness>`, a harness with its default model, `<harness>:<model>`, a
//! harness with that model, or `:<model>`, the ant's own harness with another model. They are tried in order: each
//! message goes to the first one not turned away lately, in a new session seeded from the thread's history unless it
//! runs the ant's own harness. A fallback that is signed out, limited or fails to start is passed over for a while;
//! with every one passed over, messages wait, and a human is told once until a fallback answers again. The ant goes
//! back to its own harness once a check finds it signed in or starting, or, after a quota, once the backoff passes and
//! a message gets through.
//!
//! An ant added from a template takes the template's `fallback` list, which no request chooses, less any entry that
//! names the harness and model the ant runs ([`inherit`]).

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use tokio::time::Instant;

use ants_proto::{Envelope, UserId};

use crate::harness::{Harness, Harnesses, ModelSettings};
use crate::humans::Registry;
use crate::sync::MutexExt as _;

/// How long a fallback that was signed out or failed to start is passed over, and a limited one that gave no hint.
pub const RETRY_AFTER: Duration = Duration::from_mins(5);

/// The most of what the last fallback said that a notice of every fallback passed over quotes.
const DETAIL_CHARS: usize = 500;

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
        let (harness, model) = split(entry)?;
        let harness = match harness {
            Some(name) => harnesses.get(name)?,
            None => own.clone(),
        };
        let settings = ModelSettings {
            model: model.map(str::to_owned),
            history_budget: settings.history_budget,
            ..ModelSettings::default()
        };
        settings.validate(&harness)?;
        Ok(Self { harness, settings })
    }

    /// As the ant's colony entry and replies name it: `<harness>` or `<harness>:<model>`.
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

/// The harness `entry` names, `None` for the ant's own, and the model it names, if any.
fn split(entry: &str) -> Result<(Option<&str>, Option<&str>)> {
    let (harness, model) = match entry.trim().split_once(':') {
        Some((name, model)) => (Some(name.trim()).filter(|name| !name.is_empty()), Some(model.trim())),
        None if entry.trim().is_empty() => bail!("fallback entries must not be blank"),
        None => (Some(entry.trim()), None),
    };
    if model == Some("") {
        bail!("fallback {entry:?} names no model after the colon");
    }
    Ok((harness, model))
}

/// The fallbacks `entries` name, in order, for an ant whose own harness is `own` with `settings`: each names a known
/// harness, once, and differs from the ant's own harness and model.
pub fn resolve(
    entries: &[String],
    own: &Arc<Harness>,
    settings: &ModelSettings,
    harnesses: &Harnesses,
) -> Result<Vec<Fallback>> {
    resolve_as(entries, own, settings, harnesses, false)
}

/// The fallbacks an ant added from a template takes from the template's `entries` ([`check`]): those [`resolve`]
/// gives, leaving out an entry that names the ant's own harness and model, as its manager may have picked them, or a
/// fallback named before it, as a `:<model>` entry may name one another entry names for the ant's harness.
pub fn inherit(
    entries: &[String],
    own: &Arc<Harness>,
    settings: &ModelSettings,
    harnesses: &Harnesses,
) -> Result<Vec<Fallback>> {
    resolve_as(entries, own, settings, harnesses, true)
}

fn resolve_as(
    entries: &[String],
    own: &Arc<Harness>,
    settings: &ModelSettings,
    harnesses: &Harnesses,
    inherited: bool,
) -> Result<Vec<Fallback>> {
    let mut seen = BTreeSet::new();
    let mut fallbacks = Vec::with_capacity(entries.len());
    for entry in entries {
        let fallback =
            Fallback::parse(entry, own, settings, harnesses).map_err(|e| e.context(format!("fallback {entry:?}")))?;
        let is_own = fallback.harness == *own && fallback.settings.model == settings.model;
        let new = !is_own && seen.insert(fallback.name());
        if new {
            fallbacks.push(fallback);
        } else if !inherited && is_own {
            bail!("fallback {entry:?} is the ant's own harness and model");
        } else if !inherited {
            bail!("fallback {} is listed twice", fallback.name());
        }
    }
    Ok(fallbacks)
}

/// Check the fallback list `entries` of an ant template: each entry names a harness `known` accepts and a model
/// after any colon, and appears once. Whether one names an ant's own harness and model depends on the ant
/// ([`inherit`]).
pub fn check(entries: &[String], known: impl Fn(&str) -> Result<()>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for entry in entries {
        let (harness, model) = split(entry)
            .and_then(|(harness, model)| harness.map_or(Ok(()), &known).map(|()| (harness, model)))
            .map_err(|e| e.context(format!("fallback {entry:?}")))?;
        if !seen.insert((harness, model)) {
            bail!("fallback {entry:?} is listed twice");
        }
    }
    Ok(())
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
    /// A human was told every fallback is passed over, and none has answered since.
    told: bool,
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
        let mut state = self.0.locked();
        state.told = false;
        *state.since.get_or_insert_with(Utc::now)
    }

    /// Every fallback is passed over: whether to tell a human, once until a fallback answers again or the ant's own
    /// harness does.
    pub fn exhausted(&self) -> bool {
        !std::mem::replace(&mut self.0.locked().told, true)
    }

    /// Telling a human every fallback is passed over failed: tell the next time.
    pub fn untold(&self) {
        self.0.locked().told = false;
    }

    /// The ant's own harness serves again: every fallback may be tried at the next outage. Returns whether one
    /// served.
    pub fn clear(&self) -> bool {
        std::mem::take(&mut *self.0.locked()).since.is_some()
    }
}

/// The sentence of a notice telling where messages to `ant` go while its own harness cannot serve, until `back`
/// happens; `None` without `fallbacks`, when they wait.
fn meanwhile(ant: &impl fmt::Display, fallbacks: &[String], back: &str) -> Option<String> {
    let (first, rest) = fallbacks.split_first()?;
    let then = if rest.is_empty() { String::new() } else { format!(", then {}", rest.join(", then ")) };
    Some(format!("Messages to {ant} go to its fallback {first}{then} meanwhile, and back to the harness {back}."))
}

/// Where a notice of an outage of the ant's harness goes, and what it names: `ant`'s notice on the thread of
/// `envelope` (a channel post if `post`), to the thread's human owner, else the default human of `humans`, naming the
/// `machine` the ant runs on and the `fallbacks` messages go to meanwhile.
pub struct OutageNotice<'a> {
    pub ant: &'a UserId,
    pub envelope: &'a Envelope,
    pub post: bool,
    pub machine: &'a str,
    pub fallbacks: &'a [String],
    pub humans: &'a Registry,
}

impl OutageNotice<'_> {
    /// The notice `text`: a reply to the envelope unless it stands for a channel post.
    pub fn with_text(&self, text: String) -> Envelope {
        let Self { ant, envelope, post, humans, .. } = *self;
        let owner = humans.human_owner(envelope.owner.as_ref());
        let mut notice = Envelope::on_thread(
            envelope.thread_id.clone(),
            ant.clone(),
            owner.clone().unwrap_or_else(|| humans.default_human().clone()),
            text,
        )
        .with_owner(owner)
        .with_creator(humans.ant_creator(envelope.creator.as_ref()))
        .with_origin_thread(envelope.origin_thread.clone());
        if !post {
            notice.in_reply_to = Some(envelope.id);
        }
        notice
    }

    /// The sentence telling where messages go while the ant's own harness cannot serve, until `back` happens; `None`
    /// without fallbacks, when they wait.
    pub fn meanwhile(&self, back: &str) -> Option<String> {
        meanwhile(self.ant, self.fallbacks, back)
    }

    /// What the notice says the ant cannot answer with: everything, without fallbacks.
    pub fn own(&self) -> &'static str {
        own(self.fallbacks)
    }
}

/// The notice `to` its thread's owner that the ant cannot answer with a fallback either while its own harness `own`
/// is `outage`: each fallback is passed over, `last` having turned a message away just now, being `why` (`detail`),
/// until `next`.
pub fn exhausted_notice(
    to: &OutageNotice<'_>,
    (own, outage): (&str, &str),
    (last, why, detail): (&str, &str, &str),
    next: Option<DateTime<Utc>>,
) -> Envelope {
    let (ant, fallbacks) = (to.ant, to.fallbacks);
    let mut excerpt: String = detail.trim().chars().take(DETAIL_CHARS).collect();
    if detail.trim().chars().nth(DETAIL_CHARS).is_some() {
        excerpt.push('…');
    }
    let turned = if fallbacks.len() > 1 {
        let names = fallbacks.join(", ");
        format!("fallbacks {names} turned messages away lately, the last, {last}, being {why} ({excerpt})")
    } else {
        format!("fallback {last} is {why} ({excerpt})")
    };
    let next = next.map_or_else(String::new, |at| {
        format!(", the next one at {}", at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    });
    let text = format!(
        "[ants] {ant} cannot answer with a fallback either: {turned}.\nMessages to {ant} wait meanwhile, spending no \
         attempts, until harness {own}, {outage}, answers again or a fallback answers when tried again{next}."
    );
    to.with_text(text)
}

/// What a notice of an outage of the ant's own harness says the ant cannot answer with: everything, without
/// `fallbacks`.
fn own(fallbacks: &[String]) -> &'static str {
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
    fn a_templated_ant_leaves_out_its_own_harness_and_model_and_repeats() {
        let harnesses = Harnesses::builtin();
        let entries: Vec<String> =
            ["copilot", ":gpt-5", "claude", "copilot:gpt-5", " claude"].iter().map(|e| (*e).to_owned()).collect();
        let known = |name: &str| harnesses.get(name).map(drop);
        let err = format!("{:#}", check(&entries, known).unwrap_err());
        assert!(err.contains("fallback \" claude\" is listed twice"), "{err}");
        let entries = &entries[..4];
        check(entries, known).unwrap();
        let inherited = |own: &str, model: Option<&str>| {
            let own = harnesses.get(own).unwrap();
            let settings = ModelSettings { model: model.map(str::to_owned), ..ModelSettings::default() };
            inherit(entries, &own, &settings, &harnesses).unwrap().iter().map(Fallback::name).collect::<Vec<_>>()
        };
        assert_eq!(inherited("copilot", None), ["copilot:gpt-5", "claude"], "the later copilot:gpt-5 is a repeat");
        assert_eq!(inherited("copilot", Some("gpt-5")), ["copilot", "claude"]);
        assert_eq!(inherited("claude", None), ["copilot", "claude:gpt-5", "copilot:gpt-5"]);

        for (entries, error) in [
            (&["vim"][..], "fallback \"vim\": unknown harness \"vim\""),
            (&["claude:"], "names no model"),
            (&[" "], "must not be blank"),
        ] {
            let entries: Vec<String> = entries.iter().map(|e| (*e).to_owned()).collect();
            let err = format!("{:#}", check(&entries, known).unwrap_err());
            assert!(err.contains(error), "{err}");
        }
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

        assert!(failover.exhausted());
        assert!(!failover.exhausted(), "told once");
        failover.untold();
        assert!(failover.exhausted(), "telling failed");
        let since = failover.served();
        assert!(failover.exhausted(), "a fallback answered since");
        assert_eq!(failover.served(), since, "the same outage");
        assert!(failover.exhausted());
        assert!(failover.clear());
        assert!(failover.exhausted(), "a new outage");
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
