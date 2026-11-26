//! Reading the colony: its entries, which of them are live, and the ant a request for a capability goes to.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ants_proto::{Activity, ColonyEntry, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream::kv;
use chrono::{DateTime, Utc};
use futures::TryStreamExt as _;

use crate::bus::relayed::GuardedKv as _;

/// The entries in the colony, by ant. An entry removed while listing is left out, and a malformed one is skipped
/// with a warning. So is one naming another ant than its key: credentials vouch only for the key.
pub async fn list_entries(kv: &kv::Store) -> Result<Vec<ColonyEntry>> {
    let mut keys: Vec<String> = kv.keys().await.context("listing the colony")?.try_collect().await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(value) = kv.guarded_get(&key).await.with_context(|| format!("reading {key} in the colony"))? else {
            continue;
        };
        match serde_json::from_slice::<ColonyEntry>(&value) {
            Ok(entry) => match entry.check_key(&key) {
                Ok(()) => entries.push(entry),
                Err(e) => tracing::warn!(key, error = %e, "skipping a colony entry for another ant"),
            },
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed colony entry"),
        }
    }
    Ok(entries)
}

/// Whether `entry` advertises `capability`. Tags compare whole, ignoring case and surrounding space.
pub fn has_capability(entry: &ColonyEntry, capability: &str) -> bool {
    let capability = capability.trim();
    entry.capabilities.iter().any(|c| c.trim().eq_ignore_ascii_case(capability))
}

/// An entry not refreshed for this long has missed a heartbeat: its daemon may be gone, and the entry expires once
/// [`ants_proto::COLONY_TTL`] has passed. Human's clients judge entries alike ([`ants_client::teams::is_live`]).
pub const STALE_AFTER: Duration = ants_client::teams::STALE_AFTER;

/// Whether `entry` was refreshed within [`STALE_AFTER`] of `now`. A heartbeat ahead of `now` counts as fresh.
pub fn is_live(entry: &ColonyEntry, now: DateTime<Utc>) -> bool {
    ants_client::teams::is_live(entry, now)
}

/// The live entry, of the colony's `entries` at `now`, of another ant naming `ant` as its sage: `ant` is a sage while
/// there is one, whichever machine's config or shared settings made it one, and a sage is reviewed by no one.
pub fn naming_as_sage<'a>(ant: &UserId, entries: &'a [ColonyEntry], now: DateTime<Utc>) -> Option<&'a ColonyEntry> {
    entries.iter().find(|entry| entry.ant != *ant && entry.sage.as_ref() == Some(ant) && is_live(entry, now))
}

/// The ant `request`, needing `capability`, goes to, of the entries other than `asking` that advertise it: ants served
/// before those their daemon cannot serve, then ants not paused before paused ones, then ants whose harness is signed in and starting before those signed out or failing to
/// start, then those held back by no provider limit before limited ones, unless a fallback answers for them, then
/// those answered by their own harness before those answered by a fallback, then live entries before stale ones, then
/// idle ants before those of an activity unknown to this daemon (newer daemons) and those working, then the fewest
/// messages in hand, then the ant whose profile best matches `request` ([`match_scores`]), then the first by id. A
/// profile, the ant's own untrusted words, thus only breaks a tie of all the rest. The same colony at the same time
/// always gives the same answer.
pub(crate) fn pick<'a>(
    entries: &'a [ColonyEntry],
    capability: &str,
    request: &str,
    asking: &UserId,
    now: DateTime<Utc>,
) -> Option<&'a ColonyEntry> {
    let candidates: Vec<&ColonyEntry> =
        entries.iter().filter(|e| &e.ant != asking && has_capability(e, capability)).collect();
    let scores = match_scores(&words(request), &candidates);
    candidates
        .into_iter()
        .zip(scores)
        .min_by_key(|(e, score)| {
            let busyness = match e.activity {
                Activity::Idle => 0,
                Activity::Unknown => 1,
                Activity::Working => 2,
            };
            (
                e.unserved.is_some(),
                e.paused.is_some(),
                (e.signed_out.is_some() || e.failing.is_some()) && e.fallback.is_none(),
                e.limited.is_some() && e.fallback.is_none(),
                e.fallback.is_some(),
                !is_live(e, now),
                busyness,
                e.handling,
                std::cmp::Reverse(*score),
                &e.ant,
            )
        })
        .map(|(e, _)| e)
}

/// The weight of a word held by one candidate profile alone; one held by `n` weighs `SCALE / n`. A multiple of every
/// count up to 16, so that small colonies divide it exactly.
const SCALE: u64 = 720_720;

/// The words of `text` [`pick`] compares: runs of letters and digits, lowercased.
fn words(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric()).filter(|word| !word.is_empty()).map(str::to_lowercase).collect()
}

/// How well the profile of each of `candidates` matches the `wanted` words ([`words`]): the sum, over the wanted
/// words it holds, of [`SCALE`] divided by the number of candidate profiles holding that word. A word rare among the
/// candidates thus outweighs one most of them hold, in any language, without a list of common words.
fn match_scores(wanted: &BTreeSet<String>, candidates: &[&ColonyEntry]) -> Vec<u64> {
    let shared: Vec<BTreeSet<String>> = candidates
        .iter()
        .map(|e| e.profile.as_deref().map_or_else(BTreeSet::new, |p| words(p).intersection(wanted).cloned().collect()))
        .collect();
    let mut holders: BTreeMap<&str, u64> = BTreeMap::new();
    for word in shared.iter().flatten() {
        *holders.entry(word).or_default() += 1;
    }
    shared.iter().map(|words| words.iter().map(|word| SCALE / holders[word.as_str()]).sum()).collect()
}

#[cfg(test)]
mod tests {
    use ants_proto::{HarnessFailing, HarnessLimit, ServingFallback, SignInNeeded};

    use super::*;
    use crate::colony::tests::entry;

    #[test]
    fn pick_is_the_first_other_ant_with_the_whole_tag() {
        let entries = [
            entry("ops", &["rust-ops"]),
            entry("dev-c", &[" Rust "]),
            entry("dev-b", &["rust"]),
            entry("dev-a", &["rust"]),
        ];
        let me: UserId = "dev-a".parse().unwrap();
        let now = Utc::now();
        let picked = |capability| pick(&entries, capability, "", &me, now).map(|e| e.ant.as_str());
        assert_eq!(picked("rust"), Some("dev-b"));
        assert_eq!(picked("RUST "), Some("dev-b"));
        assert_eq!(picked("rust-ops"), Some("ops"));
        assert_eq!(picked("ops"), None, "tags match whole, not in part");
        assert_eq!(pick(&entries, "rust", "", &"dev-b".parse().unwrap(), now).map(|e| e.ant.as_str()), Some("dev-a"));
    }

    #[test]
    fn pick_prefers_live_then_idle_then_least_loaded_ants() {
        let now = Utc::now();
        let with = |ant, activity, handling, beat_secs_ago| ColonyEntry {
            activity,
            handling,
            heartbeat_at: now - chrono::Duration::seconds(beat_secs_ago),
            ..entry(ant, &["rust"])
        };
        let me: UserId = "me".parse().unwrap();
        let picked = |entries: &[ColonyEntry]| pick(entries, "rust", "", &me, now).map(|e| e.ant.to_string());
        let (idle, working, unknown) = (Activity::Idle, Activity::Working, Activity::Unknown);

        let mut entries = vec![
            with("dev-a", working, 1, 0),
            with("dev-b", unknown, 0, 0),
            with("dev-c", working, 3, 0),
            with("dev-d", idle, 0, 0),
            with("dev-e", idle, 0, 0),
            with("dev-0", idle, 0, 25),
            with("me", idle, 0, 0),
        ];
        assert_eq!(picked(&entries).as_deref(), Some("dev-d"), "idle and live, ties by name, never the caller");
        entries.retain(|e| !["dev-d", "dev-e"].contains(&e.ant.as_str()));
        assert_eq!(picked(&entries).as_deref(), Some("dev-b"), "a newer daemon's unknown activity before working");
        entries.remove(1);
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "fewest messages in hand");
        entries[0].handling = 5;
        assert_eq!(picked(&entries).as_deref(), Some("dev-c"));
        entries.retain(|e| e.ant.as_str() != "dev-c");
        entries[0].heartbeat_at = now - chrono::Duration::seconds(21);
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a stale entry still takes requests when all are stale");
        entries[0].heartbeat_at = now + chrono::Duration::seconds(5);
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "a heartbeat ahead of this clock counts as live");
        entries[0].signed_out = Some(SignInNeeded { since: now, sign_in: None });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a signed-in ant, even stale, before a signed-out one");
        let until = now + chrono::Duration::seconds(60);
        entries[1].limited = Some(HarnessLimit { kind: ants_proto::LimitKind::RateLimit, since: now, until });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a limited ant before a signed-out one");
        entries[0].signed_out = None;
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "an ant held back by no limit before a limited one");
        entries[0].failing = Some(HarnessFailing { since: now, error: "not found".to_owned() });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a limited ant before one failing to start");
        entries[0].fallback = Some(ServingFallback { name: "claude".to_owned(), since: now });
        assert_eq!(picked(&entries).as_deref(), Some("dev-a"), "an ant a fallback answers for before a limited one");
        entries[1].limited = None;
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "its own harness before a fallback");
        entries[0].fallback = None;
        entries[0].failing = None;
        entries[0].paused = Some(ants_proto::Paused { since: now, by: None });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "an ant not paused, even stale, before a paused one");
        entries[1].signed_out = Some(SignInNeeded { since: now, sign_in: None });
        assert_eq!(picked(&entries).as_deref(), Some("dev-0"), "a signed-out ant before a paused one");
    }

    #[test]
    fn a_profile_matching_the_request_better_only_breaks_a_tie() {
        let now = Utc::now();
        let with =
            |ant, profile: Option<&str>| ColonyEntry { profile: profile.map(str::to_owned), ..entry(ant, &["rust"]) };
        let me: UserId = "me".parse().unwrap();
        let mut entries = vec![
            with("dev-a", None),
            with("dev-b", Some("Good for NATS reconnects and Rust daemons.")),
            with("dev-c", Some("NATS, NATS and NATS again.")),
            ColonyEntry { profile: Some("Rust daemons reconnecting to NATS.".to_owned()), ..entry("dev-0", &["go"]) },
        ];
        let picked =
            |entries: &[ColonyEntry], request| pick(entries, "rust", request, &me, now).unwrap().ant.to_string();
        let slow = "The nats client reconnects slowly";
        assert_eq!(picked(&entries, slow), "dev-b", "a word only dev-b holds outweighs one both hold");
        assert_eq!(picked(&entries, "Fix the build"), "dev-a", "no word shared: the first by name");
        assert_eq!(picked(&entries, "NATS NATS"), "dev-b", "a word both hold, counted once: the first by name");
        assert_eq!(picked(&entries, "Reconnecting daemons"), "dev-b", "never an ant without the capability");
        (entries[1].activity, entries[1].handling) = (Activity::Working, 1);
        assert_eq!(picked(&entries, slow), "dev-c", "an idle ant before a busy one whatever its profile");
        entries[2].handling = 1;
        assert_eq!(picked(&entries, slow), "dev-a", "the fewest messages in hand before the profile");
    }

    #[test]
    fn a_word_rare_among_the_profiles_outweighs_words_most_hold_in_any_language() {
        let now = Utc::now();
        let with = |ant, profile: &str| ColonyEntry { profile: Some(profile.to_owned()), ..entry(ant, &["dienst"]) };
        let me: UserId = "me".parse().unwrap();
        let entries = vec![
            with("dev-a", "Die Datenbank und der Server."),
            with("dev-b", "Die Mails und der Cache."),
            with("dev-c", "Die Cronjobs und der Speicher."),
            with("dev-d", "Die Logs und der Export."),
            with("dev-z", "Warteschlange."),
        ];
        let picked = |request| pick(&entries, "dienst", request, &me, now).unwrap().ant.to_string();
        assert_eq!(
            picked("Die Warteschlange und der Rest hängen."),
            "dev-z",
            "one word no other profile holds outweighs three that four hold"
        );
        assert_eq!(picked("Der Speicher läuft voll."), "dev-c", "a rare word decides among those sharing a common one");
        assert_eq!(picked("Die und der"), "dev-a", "words all of them hold leave the tie: the first by name");
        let tokyo = [with("dev-a", "請求書 と 支払い"), with("dev-b", "ログ と 監視")];
        let picked = pick(&tokyo, "dienst", "監視 と アラート", &me, now).unwrap();
        assert_eq!(picked.ant.to_string(), "dev-b", "words of any script count");
    }

    #[test]
    fn match_scores_weigh_each_shared_word_by_the_profiles_holding_it() {
        let profiles = [Some("alpha beta"), Some("beta gamma"), Some("beta"), None];
        let entries: Vec<ColonyEntry> = profiles
            .iter()
            .enumerate()
            .map(|(i, p)| ColonyEntry { profile: p.map(str::to_owned), ..entry(&format!("dev-{i}"), &[]) })
            .collect();
        let candidates: Vec<&ColonyEntry> = entries.iter().collect();
        let scores = match_scores(&words("Alpha, BETA and gamma; beta again"), &candidates);
        assert_eq!(scores, vec![SCALE + SCALE / 3, SCALE / 3 + SCALE, SCALE / 3, 0]);
    }
}
