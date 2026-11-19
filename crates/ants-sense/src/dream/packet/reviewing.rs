//! The reviews section of a sage's own dream packet: the reviews it made of other ants since its last dream, by their
//! records alone ([`ReviewRecord`]), never the text of their threads, which the packet leaves out ([`Covered::review`]):
//! those asked, answered or told of within the dream's period, the latest asked first, each with the ant and dream
//! reviewed, when asked and answered, what an answered one spent ([`ReviewRecord::spent`]) or that its spend is
//! unknown, each finding's kind, for a follow-up the earlier finding it follows, and what came of it, and how many
//! findings of the answer were left out past its caps or as unreadable; then the
//! tally of all its reviews ([`tally_lines`]). It takes at most a share of the packet's budget ([`REVIEWING_SHARE`]),
//! the earliest reviews left out first, and shrinks to its heading and tally for room before the identity is cut to
//! titles.
//!
//! [`Covered::review`]: crate::dream::Covered::review

use std::collections::BTreeMap;
use std::fmt::Write as _;

use ants_proto::UserId;
use chrono::{DateTime, Utc};

use super::Section;
use crate::brief::TOP_EXPERIENCE;
use crate::dream::review::left_out;
use crate::dream::vitals::cost;
use crate::dream::{Dream, Judgement, ReviewRecord, ReviewedFinding, review_records, review_tally, tally_lines};
use crate::store::{Items, StoreError};

/// The share of a packet's budget a sage's reviews of other ants take at most: one part in this many.
pub const REVIEWING_SHARE: usize = 8;

/// The reviews section of the packet of `dream` of a sage, whose items `items` holds: the reviews asked, answered or
/// with a verdict told within the dream's period; empty when there are none.
pub async fn reviewing(items: &dyn Items, dream: &Dream) -> Result<Section, StoreError> {
    let within = |at: Option<DateTime<Utc>>| at.is_some_and(|at| dream.since <= at && at < dream.at);
    let reviews = review_records(items, usize::MAX).await?;
    let mut listed: Vec<_> = reviews
        .iter()
        .filter(|review| {
            within(Some(review.received))
                || within(review.answered)
                || review.findings.iter().any(|finding| within(finding.told))
        })
        .collect();
    if listed.is_empty() {
        return Ok(Section::default());
    }
    listed.sort_by(|a, b| b.received.cmp(&a.received).then_with(|| a.ant.cmp(&b.ant)));
    let head = format!(
        "\n## Reviews of other ants\n\nThe reviews made of other ants since {} UTC, by their records, the latest asked \
         first: the ant and dream reviewed, when asked and answered, what the review thread spent by the answer, and \
         what came of each finding, as the ant told it. An ant tells its verdicts with its next review, so the \
         findings of its latest one show unanswered until then. The text of the review threads, the work of other \
         ants, is left out.\n",
        dream.since.format("%Y-%m-%d %H:%M")
    );
    let tally =
        review_tally(items, TOP_EXPERIENCE).await?.map_or_default(|tally| format!("\n{}\n", tally_lines(&tally)));
    let mut followed = Followed::new();
    for (ant, finding) in reviews
        .iter()
        .filter(|review| review.answered.is_some_and(|at| at < dream.at))
        .flat_map(|review| review.findings.iter().map(|finding| (&review.ant, finding)))
    {
        if let Some(target) = &finding.follows {
            followed.entry((ant, target)).or_default().push(&finding.id);
        }
    }
    let entries = listed.into_iter().map(|record| review(record, &followed)).collect();
    Ok(Section { head, entries, tally, noun: "earlier review" })
}

type Followed<'a> = BTreeMap<(&'a UserId, &'a str), Vec<&'a str>>;

/// `review` as the section shows it.
fn review(review: &ReviewRecord, followed: &Followed<'_>) -> String {
    let minute = |at: &DateTime<Utc>| at.format("%Y-%m-%d %H:%M").to_string();
    let mut text = format!("\n### Dream {} of {}, asked {} UTC", review.dream, review.ant, minute(&review.received));
    match &review.answered {
        Some(at) => {
            let _ = write!(text, ", answered {} UTC", minute(at));
            match review.spent {
                Some(spent) => {
                    let _ = write!(text, ", spent {} tokens and cost {}", spent.tokens, cost(spent.cost_micros));
                }
                None => text.push_str(", spend unknown"),
            }
        }
        None => text.push_str(", not answered"),
    }
    text.push_str("\n\n");
    if review.answered.is_some() && review.findings.is_empty() {
        text.push_str("No findings.\n");
    }
    for finding in &review.findings {
        let _ = write!(text, "- {} {}", finding.id, finding.kind);
        if let Some(follows) = &finding.follows {
            let _ = write!(text, " of {follows}");
        } else if finding.unlinked {
            text.push_str(", unlinked");
        }
        let _ = writeln!(text, ": {}", came_of(finding));
        if let Some(follow_ups) = followed.get(&(&review.ant, finding.id.as_str())) {
            let _ = writeln!(text, "  Followed up by {}.", follow_ups.join(", "));
        }
    }
    text.push_str(&left_out(review.over, review.unreadable));
    text
}

/// What came of `finding`, as its ant told.
fn came_of(finding: &ReviewedFinding) -> &'static str {
    match (finding.verdict, finding.holds) {
        (Some(Judgement::Applied), true) => "applied, still holding",
        (Some(Judgement::Applied), false) => "applied, since undone",
        (Some(Judgement::Rejected), _) => "rejected",
        (Some(Judgement::Deferred), _) => "deferred",
        (None, _) => "unanswered",
    }
}
