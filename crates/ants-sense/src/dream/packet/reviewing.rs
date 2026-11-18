//! The reviews section of a sage's own dream packet: the reviews it made of other ants since its last dream, by their
//! records alone ([`ReviewRecord`]), never the text of their threads, which the packet leaves out ([`Covered::review`]):
//! those asked, answered or told of within the dream's period, the latest asked first, each with the ant and dream
//! reviewed, when asked and answered, and each finding's kind and what came of it; then the tally of all its reviews
//! ([`tally_lines`]). It takes at most a share of the packet's budget ([`REVIEWING_SHARE`]), the earliest reviews left
//! out first, and shrinks to its heading and tally for room before the identity is cut to titles.
//!
//! [`Covered::review`]: crate::dream::Covered::review

use std::fmt::Write as _;

use chrono::{DateTime, Utc};

use crate::brief::TOP_EXPERIENCE;
use crate::dream::{Dream, Judgement, ReviewRecord, ReviewedFinding, review_records, review_tally, tally_lines};
use crate::store::{Items, StoreError};

/// The share of a packet's budget a sage's reviews of other ants take at most: one part in this many.
pub const REVIEWING_SHARE: usize = 8;

/// The reviews section of a sage's own packet ([`reviewing`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reviewing {
    /// Its heading and what it holds.
    pub head: String,
    /// Each review, the latest asked first, with what came of its findings.
    pub reviews: Vec<String>,
    /// The tally of all the sage's reviews.
    pub tally: String,
}

impl Reviewing {
    /// Whether it lists no review: an ant that reviewed none in the period.
    pub fn is_empty(&self) -> bool {
        self.reviews.is_empty()
    }

    /// Its text, keeping the first `kept` reviews.
    pub fn text(&self, kept: usize) -> String {
        let mut text = self.head.clone();
        for review in self.reviews.iter().take(kept) {
            text.push_str(review);
        }
        text.push_str(&self.left_out(kept));
        text.push_str(&self.tally);
        text
    }

    /// The most reviews it keeps within `bytes`; none when its heading and tally alone take more.
    pub(super) fn within(&self, bytes: usize) -> usize {
        let mut kept = 0;
        let mut taken = self.head.len() + self.tally.len();
        for review in &self.reviews {
            if taken + review.len() + self.left_out(kept + 1).len() > bytes {
                break;
            }
            taken += review.len();
            kept += 1;
        }
        kept
    }

    fn left_out(&self, kept: usize) -> String {
        match self.reviews.len().saturating_sub(kept) {
            0 => String::new(),
            1 => "\n1 earlier review left out for room.\n".into(),
            left => format!("\n{left} earlier reviews left out for room.\n"),
        }
    }
}

/// The reviews section of the packet of `dream` of a sage, whose items `items` holds: the reviews asked, answered or
/// with a verdict told within the dream's period; empty when there are none.
pub async fn reviewing(items: &dyn Items, dream: &Dream) -> Result<Reviewing, StoreError> {
    let within = |at: Option<DateTime<Utc>>| at.is_some_and(|at| dream.since <= at && at < dream.at);
    let mut reviews: Vec<_> = review_records(items, usize::MAX)
        .await?
        .into_iter()
        .filter(|review| {
            within(Some(review.received))
                || within(review.answered)
                || review.findings.iter().any(|finding| within(finding.told))
        })
        .collect();
    if reviews.is_empty() {
        return Ok(Reviewing::default());
    }
    reviews.sort_by(|a, b| b.received.cmp(&a.received).then_with(|| a.ant.cmp(&b.ant)));
    let head = format!(
        "\n## Reviews of other ants\n\nThe reviews made of other ants since {} UTC, by their records, the latest asked \
         first: the ant and dream reviewed, when asked and answered, and what came of each finding, as the ant told \
         it. An ant tells its verdicts with its next review, so the findings of its latest one show unanswered until \
         then. The text of the review threads, the work of other ants, is left out.\n",
        dream.since.format("%Y-%m-%d %H:%M")
    );
    let tally =
        review_tally(items, TOP_EXPERIENCE).await?.map_or_default(|tally| format!("\n{}\n", tally_lines(&tally)));
    Ok(Reviewing { head, reviews: reviews.iter().map(review).collect(), tally })
}

/// `review` as the section shows it.
fn review(review: &ReviewRecord) -> String {
    let minute = |at: &DateTime<Utc>| at.format("%Y-%m-%d %H:%M").to_string();
    let mut text = format!("\n### Dream {} of {}, asked {} UTC", review.dream, review.ant, minute(&review.received));
    match &review.answered {
        Some(at) => {
            let _ = write!(text, ", answered {} UTC", minute(at));
        }
        None => text.push_str(", not answered"),
    }
    text.push_str("\n\n");
    if review.answered.is_some() && review.findings.is_empty() {
        text.push_str("No findings.\n");
    }
    for finding in &review.findings {
        let _ = writeln!(text, "- {} {}: {}", finding.id, finding.kind, came_of(finding));
    }
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
