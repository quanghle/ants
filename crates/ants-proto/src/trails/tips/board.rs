//! How a trail's tips rank ([`TipBoard`]): the words telling tips apart, each tip's votes and status, and the tips a
//! step carries, a search finds and a new tip resembles.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::fmt;

use chrono::{DateTime, Utc};

#[cfg(doc)]
use super::{MAX_TIPS_BLOCK, MAX_TIPS_CARRIED, script_sha256};
use super::{
    MAX_TIPS_FOUND, TIP_HALF_LIFE, TIP_RELEVANCE, TIP_RELEVANT_WORDS, TIP_SIMILARITY, TIP_UNUSED, TipRetired,
    TipVerdict, TipVote, TrailTip,
};
#[cfg(doc)]
use crate::TRAIL_TIPS_BUCKET;
use crate::{TIP_RETIRED, TRAIL_CLOCK_SKEW, TipId, TrailName, UserId};

/// Words [`tip_words`] and a search's words leave out.
const STOPWORDS: &[&str] = &[
    "a", "about", "after", "all", "also", "an", "and", "any", "are", "as", "at", "be", "been", "before", "but", "by",
    "can", "do", "does", "for", "from", "has", "have", "if", "in", "into", "is", "it", "its", "not", "of", "on",
    "only", "or", "should", "so", "than", "that", "the", "their", "then", "there", "these", "they", "this", "those",
    "to", "was", "were", "what", "when", "which", "while", "will", "with", "would",
];

/// The distinct words of `text` that tell tips apart: its lowercase alphanumeric words of 3 characters or more, but
/// for common English ones.
pub fn tip_words(text: &str) -> BTreeSet<String> {
    words(text).filter(|word| word.chars().count() >= 3 && !STOPWORDS.contains(&word.as_str())).collect()
}

/// The distinct words a tip search for `query` looks for: its lowercase alphanumeric words but for common English
/// ones. A query without any matches no tip.
pub fn search_words(query: &str) -> BTreeSet<String> {
    words(query).filter(|word| !STOPWORDS.contains(&word.as_str())).collect()
}

/// The lowercase alphanumeric words of `text`, in order.
fn words(text: &str) -> impl Iterator<Item = String> {
    text.split(|c: char| !c.is_alphanumeric()).filter(|word| !word.is_empty()).map(str::to_lowercase)
}

/// A tip's votes by verdict.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    pub helpful: u32,
    pub stale: u32,
    pub wrong: u32,
}

impl Tally {
    /// Helpful votes less stale and wrong ones.
    pub fn net(&self) -> i64 {
        i64::from(self.helpful) - i64::from(self.stale) - i64::from(self.wrong)
    }
}

/// Whether a tip is offered to steps and searches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TipStatus {
    Live,
    Retired(Retirement),
}

impl TipStatus {
    pub fn is_live(&self) -> bool {
        *self == Self::Live
    }
}

impl fmt::Display for TipStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => f.write_str("live"),
            Self::Retired(Retirement::ByHuman { by, .. }) => write!(f, "retired by {by}"),
            Self::Retired(Retirement::VotedWrong) => f.write_str("voted wrong"),
            Self::Retired(Retirement::Unused) => write!(f, "unused for {} days", TIP_UNUSED.as_secs() / 86_400),
        }
    }
}

/// Why a tip is retired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retirement {
    /// A human retired it ([`TrailTip::retired`]).
    ByHuman { by: UserId, at: DateTime<Utc> },
    /// More ants voted it wrong than helpful.
    VotedWrong,
    /// Neither written nor voted helpful for [`TIP_UNUSED`].
    Unused,
}

/// A tip as a [`TipBoard`] ranks it at a time, for a script.
#[derive(Debug, Clone, PartialEq)]
pub struct Ranked<'a> {
    pub tip: &'a TrailTip,
    /// Its votes, oldest first, its writer's left out.
    pub votes: &'a [TipVote],
    pub tally: Tally,
    /// When it was written or last voted helpful.
    pub last_used: DateTime<Utc>,
    /// Its net votes, halved every [`TIP_HALF_LIFE`] since last used, and halved again for an older script.
    pub score: f64,
    pub status: TipStatus,
    /// Whether it was left on another script than the current one, and no helpful vote was cast on the current one.
    pub older_script: bool,
}

/// How a new tip compares with a trail's live tips of the same tip key ([`TipBoard::similar`]).
#[derive(Debug, Clone, PartialEq)]
pub enum Similarity<'a> {
    /// A tip of the same words.
    Exact(&'a TrailTip),
    /// Tips sharing at least [`TIP_SIMILARITY`] of the new tip's words ([`tip_words`]), most shared first.
    Similar(Vec<&'a TrailTip>),
    Distinct,
}

/// The tips of one trail, with their votes, ranked for a reader. Pure: the time and the current script's
/// [`script_sha256`] are given. A tip or a vote dated more than [`TRAIL_CLOCK_SKEW`] ahead of that time counts for
/// nothing at it. Tips of different writers may share an id: a tip is its writer's and id's.
#[derive(Debug, Clone)]
pub struct TipBoard {
    trail: TrailName,
    tips: Vec<(TrailTip, Vec<TipVote>)>,
}

impl TipBoard {
    /// The board of `trail` from the last value of each key of [`TRAIL_TIPS_BUCKET`] under `<trail>.>`, keys without
    /// the bucket's subject prefix. An entry that does not read as a tip, a vote or a retirement under its key, or is
    /// of another trail, is skipped, as are a writer's vote on its own tip, the votes and retirements of no tip
    /// found, and the votes that judged another text than the tip's ([`TipVote::text`]). A tip is retired by a human only as its retirement's key says, whatever its record says; an entry under
    /// that key that does not read as a retirement retires it all the same, as by the user `human` when it was written.
    pub fn new<K: AsRef<str>, V: AsRef<[u8]>>(trail: TrailName, entries: impl IntoIterator<Item = (K, V)>) -> Self {
        let mut tips = Vec::new();
        let mut votes: HashMap<String, Vec<(String, TipVote)>> = HashMap::new();
        let mut retirements: HashMap<String, Option<TipRetired>> = HashMap::new();
        for (key, value) in entries {
            let (key, value) = (key.as_ref(), value.as_ref());
            if let [trail, TIP_RETIRED, writer, id] = key.split('.').collect::<Vec<_>>()[..] {
                let retired = serde_json::from_slice::<TipRetired>(value).ok();
                retirements.insert(format!("{trail}.{writer}.{id}"), retired);
            } else if key.split('.').count() == 3 {
                if let Ok(tip) = serde_json::from_slice::<TrailTip>(value)
                    && tip.trail == trail
                    && tip.check_key(key).is_ok()
                {
                    tips.push(tip);
                }
            } else if let [trail, _voter, writer, id] = key.split('.').collect::<Vec<_>>()[..]
                && let Ok(vote) = serde_json::from_slice::<TipVote>(value)
            {
                votes.entry(format!("{trail}.{writer}.{id}")).or_default().push((key.to_owned(), vote));
            }
        }
        let tips = tips
            .into_iter()
            .map(|mut tip| {
                let unread = || TipRetired { by: UserId::human(), at: tip.written_at };
                tip.retired = retirements.remove(&tip.key()).map(|retired| retired.unwrap_or_else(unread));
                let mut votes: Vec<TipVote> = votes
                    .remove(&tip.key())
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(key, vote)| vote.voter != tip.writer && vote.check_key(&tip, key).is_ok())
                    .map(|(_, vote)| vote)
                    .collect();
                votes.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.voter.cmp(&b.voter)));
                (tip, votes)
            })
            .collect();
        Self { trail, tips }
    }

    pub fn trail(&self) -> &TrailName {
        &self.trail
    }

    /// How many tips it has, retired ones included.
    pub fn len(&self) -> usize {
        self.tips.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tips.is_empty()
    }

    /// The tip `id` of `writer`, or of the one writer having a tip of that id when none is given, ranked at `now` for
    /// the current script `script`. Without such a tip, the writers of the tips of that id, none when the trail has
    /// none: more than one when no writer is given and tips of different writers share the id.
    pub fn tip(
        &self,
        id: &TipId,
        writer: Option<&UserId>,
        now: DateTime<Utc>,
        script: Option<&str>,
    ) -> Result<Ranked<'_>, Vec<&UserId>> {
        let of_id: Vec<_> = self.dated(now).filter(|(tip, _)| tip.id == *id).collect();
        let found = match writer {
            Some(writer) => of_id.iter().find(|(tip, _)| tip.writer == *writer),
            None if of_id.len() == 1 => of_id.first(),
            None => None,
        };
        match found {
            Some((tip, votes)) => Ok(rank(tip, votes, now, script)),
            None => Err(of_id.iter().map(|(tip, _)| &tip.writer).collect()),
        }
    }

    /// The tips dated no more than [`TRAIL_CLOCK_SKEW`] ahead of `now`.
    fn dated(&self, now: DateTime<Utc>) -> impl Iterator<Item = &(TrailTip, Vec<TipVote>)> {
        self.tips.iter().filter(move |(tip, _)| tip.written_at <= now + TRAIL_CLOCK_SKEW)
    }

    /// Every tip ranked at `now` for the current script `script`: the live ones first, in rank order (score, then
    /// last used, then written, latest first, then id), then the retired ones, in rank order too.
    pub fn ranked(&self, now: DateTime<Utc>, script: Option<&str>) -> Vec<Ranked<'_>> {
        let mut ranked: Vec<Ranked<'_>> = self.dated(now).map(|(tip, votes)| rank(tip, votes, now, script)).collect();
        ranked.sort_by(|a, b| b.status.is_live().cmp(&a.status.is_live()).then_with(|| rank_order(a, b)));
        ranked
    }

    /// How many tips are live at `now`.
    pub fn live(&self, now: DateTime<Utc>) -> usize {
        self.live_ranked(now, None).count()
    }

    fn live_ranked(&self, now: DateTime<Utc>, script: Option<&str>) -> impl Iterator<Item = Ranked<'_>> {
        self.dated(now).map(move |(tip, votes)| rank(tip, votes, now, script)).filter(|r| r.status.is_live())
    }

    /// The tips a step under `tip_key` with `prompt` may carry, in the order to carry them, each a live tip of a
    /// score of 0 or more: those under the step's tip key, then the trail-wide ones relevant to the prompt (sharing at
    /// least [`TIP_RELEVANCE`] and [`TIP_RELEVANT_WORDS`] of their words, [`tip_words`]), each in rank order; never
    /// tips under another key. The caller takes at most [`MAX_TIPS_CARRIED`] within [`MAX_TIPS_BLOCK`].
    pub fn carried(
        &self,
        tip_key: Option<&str>,
        prompt: &str,
        now: DateTime<Utc>,
        script: Option<&str>,
    ) -> Vec<Ranked<'_>> {
        let prompt: BTreeSet<String> = words(prompt).collect();
        let relevant = |tip: &TrailTip| {
            let words = tip_words(&tip.text);
            let shared = words.intersection(&prompt).count();
            shared >= TIP_RELEVANT_WORDS && shared as f64 >= TIP_RELEVANCE * words.len() as f64
        };
        let mut carried: Vec<Ranked<'_>> = self
            .live_ranked(now, script)
            .filter(|r| r.score >= 0.0)
            .filter(|r| match r.tip.tip_key.as_deref() {
                None => relevant(r.tip),
                key => tip_key.is_some() && key == tip_key,
            })
            .collect();
        carried.sort_by(|a, b| a.tip.tip_key.is_none().cmp(&b.tip.tip_key.is_none()).then_with(|| rank_order(a, b)));
        carried
    }

    /// Up to `limit` (1 to [`MAX_TIPS_FOUND`]) live tips matching a search: with `tip_key`, those under that key,
    /// first, and the trail-wide ones; with `query`, those sharing at least one of its [`search_words`] in their text
    /// or tip key, most shared first, and none when it has no such word; then in rank order.
    pub fn search(
        &self,
        query: Option<&str>,
        tip_key: Option<&str>,
        limit: usize,
        now: DateTime<Utc>,
        script: Option<&str>,
    ) -> Vec<Ranked<'_>> {
        let searched = query.is_some();
        let query = search_words(query.unwrap_or_default());
        let shared = |tip: &TrailTip| {
            let text = format!("{} {}", tip.text, tip.tip_key.as_deref().unwrap_or_default());
            words(&text).collect::<BTreeSet<String>>().intersection(&query).count()
        };
        let mut found: Vec<(bool, usize, Ranked<'_>)> = self
            .live_ranked(now, script)
            .filter(|r| tip_key.is_none() || r.tip.tip_key.is_none() || r.tip.tip_key.as_deref() == tip_key)
            .map(|r| (tip_key.is_some() && r.tip.tip_key.as_deref() == tip_key, shared(r.tip), r))
            .filter(|(_, shared, _)| !searched || *shared > 0)
            .collect();
        found.sort_by(|(a_keyed, a_shared, a), (b_keyed, b_shared, b)| {
            b_keyed.cmp(a_keyed).then(b_shared.cmp(a_shared)).then_with(|| rank_order(a, b))
        });
        found.into_iter().take(limit.clamp(1, MAX_TIPS_FOUND)).map(|(_, _, r)| r).collect()
    }

    /// How a new tip of `text` under `tip_key` compares with the live tips under the same key at `now`, trail-wide
    /// ones with trail-wide ones: one of the same words, in the same order, tips sharing at least [`TIP_SIMILARITY`]
    /// of its words ([`tip_words`]), or none.
    pub fn similar(&self, text: &str, tip_key: Option<&str>, now: DateTime<Utc>) -> Similarity<'_> {
        let same_key: Vec<Ranked<'_>> =
            self.live_ranked(now, None).filter(|r| r.tip.tip_key.as_deref() == tip_key).collect();
        let sequence: Vec<String> = words(text).collect();
        if let Some(exact) = same_key.iter().find(|r| words(&r.tip.text).eq(sequence.iter().cloned())) {
            return Similarity::Exact(exact.tip);
        }
        let new = tip_words(text);
        if new.is_empty() {
            return Similarity::Distinct;
        }
        let mut similar: Vec<(usize, Ranked<'_>)> = same_key
            .into_iter()
            .map(|r| (tip_words(&r.tip.text).intersection(&new).count(), r))
            .filter(|(shared, _)| *shared as f64 >= TIP_SIMILARITY * new.len() as f64)
            .collect();
        if similar.is_empty() {
            return Similarity::Distinct;
        }
        similar.sort_by(|(a_shared, a), (b_shared, b)| b_shared.cmp(a_shared).then_with(|| rank_order(a, b)));
        Similarity::Similar(similar.into_iter().map(|(_, r)| r.tip).collect())
    }
}

/// `tip` with its `votes`, oldest first, ranked at `now` for the current script `script`, those dated more than
/// [`TRAIL_CLOCK_SKEW`] ahead of `now` left out.
fn rank<'a>(tip: &'a TrailTip, votes: &'a [TipVote], now: DateTime<Utc>, script: Option<&str>) -> Ranked<'a> {
    let votes = &votes[..votes.partition_point(|vote| vote.at <= now + TRAIL_CLOCK_SKEW)];
    let mut tally = Tally::default();
    for vote in votes {
        match vote.verdict {
            TipVerdict::Helpful => tally.helpful += 1,
            TipVerdict::Stale => tally.stale += 1,
            TipVerdict::Wrong => tally.wrong += 1,
        }
    }
    let helpful = || votes.iter().filter(|vote| vote.verdict == TipVerdict::Helpful);
    let last_used = helpful().map(|vote| vote.at).fold(tip.written_at, DateTime::max);
    let unused = (now - last_used).to_std().unwrap_or_default();
    let older_script = script
        .is_some_and(|current| current != tip.script && !helpful().any(|vote| vote.script.as_deref() == Some(current)));
    let halvings = unused.as_secs_f64() / TIP_HALF_LIFE.as_secs_f64();
    let weight = 0.5f64.powf(halvings) * if older_script { 0.5 } else { 1.0 };
    // Age and an older script weaken a tip: they shrink a positive net and deepen a negative one.
    let net = tally.net() as f64;
    let score = if net < 0.0 { net / weight } else { net * weight };
    let status = match &tip.retired {
        Some(TipRetired { by, at }) => TipStatus::Retired(Retirement::ByHuman { by: by.clone(), at: *at }),
        None if tally.wrong > tally.helpful => TipStatus::Retired(Retirement::VotedWrong),
        None if unused >= TIP_UNUSED => TipStatus::Retired(Retirement::Unused),
        None => TipStatus::Live,
    };
    Ranked { tip, votes, tally, last_used, score, status, older_script }
}

/// Rank order: score, then last used, then written, latest first, then id.
fn rank_order(a: &Ranked<'_>, b: &Ranked<'_>) -> Ordering {
    b.score
        .total_cmp(&a.score)
        .then(b.last_used.cmp(&a.last_used))
        .then(b.tip.written_at.cmp(&a.tip.written_at))
        .then_with(|| a.tip.id.cmp(&b.tip.id))
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::trails::tips::{MAX_TIP_REASON, MAX_TIP_TEXT, check_tip_key, script_sha256};
    use crate::{Error, MAX_TITLE, RunId, p};

    fn day(n: i64) -> DateTime<Utc> {
        "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::days(n)
    }

    fn tip(writer: &str, id: &str, tip_key: Option<&str>, text: &str, written: i64) -> TrailTip {
        TrailTip {
            trail: "deploy".parse().unwrap(),
            id: id.parse().unwrap(),
            writer: p(writer),
            text: text.to_owned(),
            tip_key: tip_key.map(str::to_owned),
            run: RunId::generate(),
            script: "old".to_owned(),
            written_at: day(written),
            retired: None,
        }
    }

    fn vote(voter: &str, verdict: TipVerdict, at: i64) -> TipVote {
        TipVote { voter: p(voter), verdict, reason: None, text: String::new(), script: None, at: day(at) }
    }

    /// `vote` judging `tip`'s text.
    fn on(tip: &TrailTip, vote: TipVote) -> TipVote {
        TipVote { text: tip.text_sha256(), ..vote }
    }

    fn board(tips: &[&TrailTip], votes: &[(&TrailTip, TipVote)]) -> TipBoard {
        board_retired(tips, votes, &[])
    }

    /// The board of `tips` with `votes`, each judging its tip's text unless it names one, each of `retired` retired
    /// by a human on a day.
    fn board_retired(
        tips: &[&TrailTip],
        votes: &[(&TrailTip, TipVote)],
        retired: &[(&TrailTip, &str, i64)],
    ) -> TipBoard {
        let tips = tips.iter().map(|tip| (tip.key(), serde_json::to_vec(tip).unwrap()));
        let votes = votes.iter().map(|(tip, vote)| {
            let vote = if vote.text.is_empty() { on(tip, vote.clone()) } else { vote.clone() };
            (tip.vote_key(&vote.voter), serde_json::to_vec(&vote).unwrap())
        });
        let retired = retired.iter().map(|(tip, by, at)| {
            (tip.retired_key(), serde_json::to_vec(&TipRetired { by: p(by), at: day(*at) }).unwrap())
        });
        TipBoard::new("deploy".parse().unwrap(), tips.chain(votes).chain(retired))
    }

    fn ids<'a>(ranked: impl IntoIterator<Item = Ranked<'a>>) -> Vec<&'a str> {
        ranked.into_iter().map(|r| r.tip.id.as_str()).collect()
    }

    #[test]
    fn a_tip_retires_when_a_human_says_more_vote_it_wrong_or_it_goes_unused() {
        let by_human = tip("dev-a", "aaaaaaaa", None, "pin the base image", 0);
        let wronged = tip("dev-a", "bbbbbbbb", None, "use port 8080", 0);
        let unused = tip("dev-a", "cccccccc", None, "run the linter first", 0);
        let renewed = tip("dev-a", "dddddddd", None, "tag before pushing", 0);
        let votes = [
            (&wronged, vote("dev-b", TipVerdict::Wrong, 1)),
            (&wronged, vote("dev-c", TipVerdict::Wrong, 1)),
            (&wronged, vote("dev-d", TipVerdict::Helpful, 1)),
            (&renewed, vote("dev-b", TipVerdict::Helpful, 50)),
            (&renewed, vote("dev-c", TipVerdict::Stale, 60)),
        ];
        let board = board_retired(&[&by_human, &wronged, &unused, &renewed], &votes, &[(&by_human, "alice", 1)]);
        let status = |id: &str| board.tip(&id.parse().unwrap(), None, day(100), None).unwrap().status;
        assert_eq!(status("aaaaaaaa"), TipStatus::Retired(Retirement::ByHuman { by: p("alice"), at: day(1) }));
        assert_eq!(status("bbbbbbbb"), TipStatus::Retired(Retirement::VotedWrong));
        assert_eq!(status("cccccccc"), TipStatus::Retired(Retirement::Unused));
        let renewed = board.tip(&"dddddddd".parse().unwrap(), None, day(100), None).unwrap();
        assert_eq!((renewed.status, renewed.last_used), (TipStatus::Live, day(50)), "only a helpful vote renews it");
        assert_eq!(board.live(day(100)), 1);
        assert_eq!(board.live(day(89)), 2, "unused for less than 90 days, a tip lives");
        assert_eq!(ids(board.ranked(day(100), None))[0], "dddddddd", "live tips rank first");
    }

    #[test]
    fn a_human_retires_a_tip_under_a_key_no_ant_writes_whatever_its_record_says() {
        let left = tip("dev-a", "aaaaaaaa", None, "pin the base image", 0);
        assert_eq!(left.retired_key(), "deploy._retired.dev-a.aaaaaaaa");
        assert!(left.retired_key().split('.').nth(1).unwrap().parse::<UserId>().is_err(), "no ant writes it");
        let mut claimed = tip("dev-a", "bbbbbbbb", None, "use port 8080", 0);
        claimed.retired = Some(TipRetired { by: p("alice"), at: day(1) });
        let board = board_retired(&[&left, &claimed], &[], &[(&left, "alice", 1)]);
        let ranked = board.tip(&left.id, None, day(2), None).unwrap();
        assert_eq!(ranked.status, TipStatus::Retired(Retirement::ByHuman { by: p("alice"), at: day(1) }));
        assert_eq!(ranked.tip.retired, Some(TipRetired { by: p("alice"), at: day(1) }));
        let ranked = board.tip(&claimed.id, None, day(2), None).unwrap();
        assert_eq!(
            (ranked.status, &ranked.tip.retired),
            (TipStatus::Live, &None),
            "a record's own claim counts for nothing"
        );
        let elsewhere = tip("dev-b", "cccccccc", None, "no such tip", 0);
        let entries = [
            (left.key(), serde_json::to_vec(&left).unwrap()),
            (elsewhere.retired_key(), serde_json::to_vec(&TipRetired { by: p("alice"), at: day(1) }).unwrap()),
            (left.retired_key(), b"not json".to_vec()),
        ];
        let board = TipBoard::new("deploy".parse().unwrap(), entries);
        assert_eq!(board.len(), 1, "a retirement is no tip");
        assert_eq!(
            board.tip(&left.id, None, day(2), None).unwrap().status,
            TipStatus::Retired(Retirement::ByHuman { by: UserId::human(), at: day(0) }),
            "a retirement that does not read retires it all the same"
        );
    }

    #[test]
    fn a_score_halves_every_half_life_and_again_for_an_older_script() {
        let left = tip("dev-a", "aaaaaaaa", None, "pin the base image", 0);
        let mut votes =
            vec![(&left, vote("dev-b", TipVerdict::Helpful, 0)), (&left, vote("dev-c", TipVerdict::Helpful, 0))];
        let score = |votes: &[(&TrailTip, TipVote)], script| board(&[&left], votes).ranked(day(30), script)[0].score;
        assert!((score(&votes, None) - 1.0).abs() < 1e-9);
        assert!((score(&votes, Some("old")) - 1.0).abs() < 1e-9, "the tip's own script is no older one");
        assert!((score(&votes, Some("new")) - 0.5).abs() < 1e-9);
        let mut on_new = vote("dev-d", TipVerdict::Helpful, 30);
        on_new.script = Some("new".to_owned());
        votes.push((&left, on_new));
        let board = board(&[&left], &votes);
        let ranked = &board.ranked(day(30), Some("new"))[0];
        assert!(!ranked.older_script, "a helpful vote on the current script clears the mark");
        assert!((ranked.score - 3.0).abs() < 1e-9);
    }

    #[test]
    fn age_and_an_older_script_lower_a_tip_voted_down_as_they_lower_one_voted_up() {
        let on_new =
            |id, written| TrailTip { script: "new".to_owned(), ..tip("dev-a", id, None, "pin the image", written) };
        let older = tip("dev-a", "aaaaaaaa", None, "pin the base image", 0);
        let unused = on_new("bbbbbbbb", 0);
        let recent = on_new("cccccccc", 60);
        let helpful = on_new("dddddddd", 0);
        let older_helpful = tip("dev-a", "eeeeeeee", None, "pin the base image", 0);
        let votes = [
            (&older, vote("dev-b", TipVerdict::Stale, 60)),
            (&unused, vote("dev-b", TipVerdict::Stale, 60)),
            (&recent, vote("dev-b", TipVerdict::Stale, 60)),
            (&helpful, vote("dev-b", TipVerdict::Helpful, 0)),
            (&older_helpful, vote("dev-b", TipVerdict::Helpful, 0)),
        ];
        let board = board(&[&older, &unused, &recent, &helpful, &older_helpful], &votes);
        let score = |tip: &TrailTip| board.tip(&tip.id, None, day(60), Some("new")).unwrap().score;
        assert!((score(&recent) + 1.0).abs() < 1e-9);
        assert!((score(&unused) + 4.0).abs() < 1e-9, "unused for two half-lives, a negative net weighs four times");
        assert!((score(&older) + 8.0).abs() < 1e-9, "and twice that on an older script");
        assert!((score(&helpful) - 0.25).abs() < 1e-9, "a positive net still shrinks");
        assert!((score(&older_helpful) - 0.125).abs() < 1e-9);
        let ranked = ids(board.ranked(day(60), Some("new")));
        assert_eq!(ranked, ["dddddddd", "eeeeeeee", "cccccccc", "bbbbbbbb", "aaaaaaaa"]);
    }

    #[test]
    fn only_tips_and_votes_under_their_own_keys_count_and_never_the_writers_vote() {
        let left = tip("dev-a", "aaaaaaaa", None, "pin the base image", 0);
        let missing = tip("dev-a", "bbbbbbbb", None, "gone", 0);
        let mut elsewhere = tip("dev-a", "cccccccc", None, "another trail", 0);
        elsewhere.trail = "build".parse().unwrap();
        let forged = on(&left, vote("dev-c", TipVerdict::Wrong, 1));
        let own = on(&left, vote("dev-a", TipVerdict::Helpful, 1));
        let stale = on(&left, vote("dev-b", TipVerdict::Stale, 1));
        let entries = [
            (left.key(), serde_json::to_vec(&left).unwrap()),
            (left.vote_key(&p("dev-a")), serde_json::to_vec(&own).unwrap()),
            (left.vote_key(&p("dev-b")), serde_json::to_vec(&stale).unwrap()),
            (left.vote_key(&p("dev-b")).replace("dev-b", "dev-d"), serde_json::to_vec(&forged).unwrap()),
            (
                missing.vote_key(&p("dev-b")),
                serde_json::to_vec(&on(&missing, vote("dev-b", TipVerdict::Helpful, 1))).unwrap(),
            ),
            (format!("deploy.dev-b.{}", elsewhere.id), serde_json::to_vec(&elsewhere).unwrap()),
            ("deploy.dev-a.dddddddd".to_owned(), b"not json".to_vec()),
        ];
        let board = TipBoard::new("deploy".parse().unwrap(), entries);
        assert_eq!(board.len(), 1);
        let ranked = board.tip(&left.id, None, day(1), None).unwrap();
        assert_eq!(ranked.tally, Tally { helpful: 0, stale: 1, wrong: 0 });
        assert_eq!(ranked.votes.len(), 1);
    }

    #[test]
    fn a_tip_its_writer_puts_again_with_other_text_keeps_none_of_the_votes_on_the_first() {
        let left = tip("dev-a", "aaaaaaaa", Some("build"), "cache the deps", 0);
        let other = tip("dev-a", "bbbbbbbb", Some("build"), "pin the toolchain", 0);
        let votes = [
            (&left, vote("dev-b", TipVerdict::Helpful, 1)),
            (&left, vote("dev-c", TipVerdict::Helpful, 1)),
            (&other, vote("dev-b", TipVerdict::Helpful, 1)),
        ];
        let first = board(&[&left, &other], &votes);
        assert_eq!(ids(first.carried(Some("build"), "", day(2), None)), ["aaaaaaaa", "bbbbbbbb"]);
        let rewritten = TrailTip { text: "skip the tests".to_owned(), ..left.clone() };
        let entries = votes.iter().map(|(tip, vote)| (tip.vote_key(&vote.voter), on(tip, vote.clone())));
        let entries = entries
            .map(|(key, vote)| (key, serde_json::to_vec(&vote).unwrap()))
            .chain([&rewritten, &other].map(|tip| (tip.key(), serde_json::to_vec(tip).unwrap())));
        let board = TipBoard::new("deploy".parse().unwrap(), entries);
        let ranked = board.tip(&left.id, None, day(2), None).unwrap();
        assert_eq!(ranked.tip.text, "skip the tests");
        assert_eq!((ranked.tally, ranked.votes.len(), ranked.score), (Tally::default(), 0, 0.0));
        assert_eq!(ids(board.carried(Some("build"), "", day(2), None)), ["bbbbbbbb", "aaaaaaaa"], "nor ranks on them");
        let revoted = (&rewritten, vote("dev-b", TipVerdict::Wrong, 2));
        let board = self::board(&[&rewritten], &[revoted]);
        assert_eq!(board.tip(&left.id, None, day(2), None).unwrap().tally.wrong, 1, "a vote on the new text counts");
    }

    #[test]
    fn a_step_carries_its_keys_tips_then_the_trail_wide_ones_its_prompt_matches() {
        let keyed = tip("dev-a", "aaaaaaaa", Some("build"), "cache the deps", 0);
        let keyed_better = tip("dev-a", "bbbbbbbb", Some("build"), "pin the toolchain", 0);
        let relevant = tip("dev-a", "cccccccc", None, "the release notes list breaking changes", 0);
        let one_shared = tip("dev-a", "dddddddd", None, "release builds need signing keys", 0);
        let diluted =
            tip("dev-a", "hhhhhhhh", None, "release notes need signing keys from the vault every audit cycle", 0);
        let irrelevant = tip("dev-a", "eeeeeeee", None, "read the changelog", 0);
        let other_key = tip("dev-a", "ffffffff", Some("test"), "run the release notes check", 0);
        let down = tip("dev-a", "gggggggg", Some("build"), "skip the tests", 0);
        let votes = [
            (&relevant, vote("dev-b", TipVerdict::Helpful, 0)),
            (&relevant, vote("dev-c", TipVerdict::Helpful, 0)),
            (&keyed_better, vote("dev-b", TipVerdict::Helpful, 0)),
            (&down, vote("dev-b", TipVerdict::Stale, 0)),
        ];
        let tips = [&keyed, &keyed_better, &relevant, &one_shared, &diluted, &irrelevant, &other_key, &down];
        let board = board(&tips, &votes);
        let prompt = "Build version 1.2 and summarize its release notes";
        assert_eq!(ids(board.carried(Some("build"), prompt, day(1), None)), ["bbbbbbbb", "aaaaaaaa", "cccccccc"]);
        assert_eq!(ids(board.carried(None, prompt, day(1), None)), ["cccccccc"]);
        assert_eq!(ids(board.carried(None, "write the release", day(1), None)), Vec::<&str>::new());
    }

    #[test]
    fn a_search_keeps_the_tip_key_and_the_query_words_then_ranks() {
        let keyed = tip("dev-a", "aaaaaaaa", Some("build"), "cache the cargo registry", 0);
        let both_words = tip("dev-a", "bbbbbbbb", None, "the cargo registry mirror is slow", 0);
        let one_word = tip("dev-a", "cccccccc", None, "a registry token expires", 0);
        let other_key = tip("dev-a", "dddddddd", Some("test"), "the cargo registry is flaky", 0);
        let tips = [&keyed, &both_words, &one_word, &other_key];
        let board = board(&tips, &[(&one_word, vote("dev-b", TipVerdict::Helpful, 0))]);
        let search = |query, tip_key, limit| ids(board.search(query, tip_key, limit, day(1), None));
        assert_eq!(search(None, None, 10), ["cccccccc", "aaaaaaaa", "bbbbbbbb", "dddddddd"]);
        assert_eq!(search(Some("Cargo registry"), None, 10), ["aaaaaaaa", "bbbbbbbb", "dddddddd", "cccccccc"]);
        assert_eq!(search(Some("cargo"), Some("build"), 10), ["aaaaaaaa", "bbbbbbbb"]);
        assert_eq!(search(Some("nothing"), None, 10), Vec::<&str>::new());
        assert_eq!(search(Some("what should it do"), None, 10), Vec::<&str>::new(), "common words alone match none");
        assert_eq!(search(Some("??"), Some("build"), 10), Vec::<&str>::new(), "nor does a query of no word");
        assert!(search_words("what should it do?").is_empty());
        assert_eq!(search(None, None, 0).len(), 1, "a limit is at least 1");
        assert_eq!(search(None, None, usize::MAX).len(), 4);
    }

    #[test]
    fn a_new_tip_is_exact_similar_or_distinct_among_its_keys_tips() {
        let left = tip("dev-a", "aaaaaaaa", Some("build"), "Pin the Rust toolchain before building.", 0);
        let board = board(&[&left], &[]);
        let similar = |text, tip_key| board.similar(text, tip_key, day(1));
        assert_eq!(similar("pin the rust toolchain before building", Some("build")), Similarity::Exact(&left));
        assert_eq!(similar("pin the rust toolchain", Some("build")), Similarity::Similar(vec![&left]));
        assert_eq!(similar("pin the node version in the dockerfile", Some("build")), Similarity::Distinct);
        assert_eq!(similar("pin the rust toolchain before building", None), Similarity::Distinct);
    }

    #[test]
    fn a_tip_and_a_vote_hold_their_bounds_and_keys() {
        let left = tip("dev-a", "aaaaaaaa", Some("build"), "cache the deps", 0);
        assert_eq!(left.key(), "deploy.dev-a.aaaaaaaa");
        assert_eq!(left.vote_key(&p("dev-b")), "deploy.dev-b.dev-a.aaaaaaaa");
        assert!(left.check_key("deploy.dev-a.aaaaaaaa").is_ok());
        assert!(matches!(left.check_key("deploy.dev-b.aaaaaaaa"), Err(Error::ForeignTip { kind: "tip", .. })));
        for (text, key) in [(" ", None), (&*"x".repeat(MAX_TIP_TEXT + 1), None), ("ok", Some("two\nlines"))] {
            let bad = TrailTip { text: text.to_owned(), tip_key: key.map(str::to_owned), ..left.clone() };
            assert!(matches!(bad.check(), Err(Error::InvalidTip(_))), "{text:?} {key:?}");
        }
        assert!(check_tip_key(&"k".repeat(MAX_TITLE)).is_ok());
        assert!(check_tip_key(&"k".repeat(MAX_TITLE + 1)).is_err());
        let mut voted = on(&left, vote("dev-b", TipVerdict::Wrong, 0));
        assert!(voted.check_key(&left, "deploy.dev-b.dev-a.aaaaaaaa").is_ok());
        assert!(matches!(voted.check_key(&left, "deploy.dev-c.dev-a.aaaaaaaa"), Err(Error::ForeignTip { .. })));
        let rewritten = TrailTip { text: "cache nothing".to_owned(), ..left };
        assert!(matches!(voted.check_key(&rewritten, "deploy.dev-b.dev-a.aaaaaaaa"), Err(Error::InvalidTipVote(_))));
        voted.reason = Some("x".repeat(MAX_TIP_REASON + 1));
        assert!(matches!(voted.check(), Err(Error::InvalidTipVote(_))));
        assert_eq!("stale".parse::<TipVerdict>().unwrap(), TipVerdict::Stale);
        assert_eq!(serde_json::to_value(TipVerdict::Helpful).unwrap(), "helpful");
        assert_eq!(script_sha256("").len(), 64);
        assert_eq!(&script_sha256("")[..8], "e3b0c442");
    }

    #[test]
    fn a_tip_or_vote_dated_ahead_of_now_counts_for_nothing_until_then() {
        let ahead = tip("dev-a", "aaaaaaaa", None, "pin the base image", 10);
        let left = tip("dev-a", "bbbbbbbb", None, "use port 8080", 0);
        let votes = [
            (&left, vote("dev-b", TipVerdict::Helpful, 1)),
            (&left, vote("dev-c", TipVerdict::Helpful, 500)),
            (&ahead, vote("dev-b", TipVerdict::Helpful, 10)),
        ];
        let board = board(&[&ahead, &left], &votes);
        assert_eq!(ids(board.ranked(day(5), None)), ["bbbbbbbb"], "a tip dated ahead is no tip yet");
        assert!(board.tip(&ahead.id, None, day(5), None).unwrap_err().is_empty());
        let ranked = board.tip(&left.id, None, day(5), None).unwrap();
        assert_eq!((ranked.tally.helpful, ranked.votes.len(), ranked.last_used), (1, 1, day(1)));
        let retired = board.tip(&left.id, None, day(200), None).unwrap();
        assert_eq!(retired.status, TipStatus::Retired(Retirement::Unused), "no vote dated ahead keeps it live");
        let within = day(10) - TRAIL_CLOCK_SKEW;
        assert_eq!(board.live(within), 2, "a tip dated within the clock skew counts");
        assert_eq!(board.live(day(10) - TRAIL_CLOCK_SKEW - TimeDelta::seconds(1)), 1);
    }

    #[test]
    fn tips_of_different_writers_sharing_an_id_are_told_apart_by_writer() {
        let first = tip("dev-a", "aaaaaaaa", None, "pin the base image", 0);
        let copy = tip("dev-b", "aaaaaaaa", None, "use port 8080", 1);
        let board = board(&[&first, &copy], &[(&first, vote("dev-c", TipVerdict::Wrong, 1))]);
        assert_eq!(board.len(), 2);
        let writers = board.tip(&first.id, None, day(2), None).unwrap_err();
        assert_eq!(writers, [&p("dev-a"), &p("dev-b")], "an id alone names neither");
        let ranked = board.tip(&first.id, Some(&p("dev-a")), day(2), None).unwrap();
        assert_eq!((ranked.tip, ranked.tally.wrong), (&first, 1));
        let ranked = board.tip(&copy.id, Some(&p("dev-b")), day(2), None).unwrap();
        assert_eq!((ranked.tip, ranked.tally.wrong), (&copy, 0), "a vote reaches the tip of its key's writer alone");
        let missing = board.tip(&first.id, Some(&p("dev-c")), day(2), None).unwrap_err();
        assert_eq!(missing.len(), 2);
        assert_eq!(board.tip(&first.id, Some(&p("dev-b")), day(0), None).unwrap_err(), [&p("dev-a")]);
    }
}
