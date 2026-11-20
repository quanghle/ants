//! The beginning of a dream: the findings its packet offers, gathered from sense's data ([`begin`]).

use super::*;

/// What a dream begins with, gathered by the daemon.
#[derive(Debug, Clone)]
pub struct Start<'a> {
    pub at: DateTime<Utc>,
    pub recipient: Recipient,
    pub threads: Vec<Covered>,
    pub commits: Vec<Commit>,
    /// The context the diary of the last dream is written in when the ant left it unfinished.
    pub writer: &'a CallContext,
    /// The vital signs of the work on each of `threads`.
    pub vitals: Vec<(ThreadId, Signs)>,
    /// The colony's saved trails, by which the review a sage is asked tells it whether its applied findings naming
    /// trails hold.
    pub trails: &'a ColonyTrails,
}

/// Begin the next dream as `start` says, unless one began within [`DREAM_EVERY_HOURS`]: its findings gathered, the
/// last dream closed with its diary if the ant left it unfinished, and the findings rejected in it rested, in one
/// write. A thread covered that a review record names is a review ([`Covered::review`]), whatever its origin. Returns
/// the new dream; none when one began meanwhile.
pub async fn begin(items: &dyn Items, start: Start<'_>) -> Result<Option<Dream>, StoreError> {
    let card = identity::card(items).await?;
    let ages = &memory::Ages::new(start.at, card.iter().flat_map(|card| card.dormant(start.at)));
    let start = &start;
    read_modify_write(|| async move {
        let record = record(items).await?;
        let previous = match &record {
            Some((record, _)) => get(items, record.number).await?,
            None => None,
        };
        if previous.as_ref().is_some_and(|(previous, _)| previous.next() > start.at) {
            return Ok(None);
        }
        let number = record.as_ref().map_or(1, |(record, _)| record.number + 1);
        let last = previous.as_ref().map(|(previous, _)| previous);
        let since = since(last, start.at);
        let mut rested = record.as_ref().map_or_else(Vec::new, |(record, _)| record.rested.clone());
        for resting in last.into_iter().flat_map(Dream::resting) {
            rested.retain(|rested| rested.key != resting.key);
            rested.push(resting);
        }
        let reviews = review_records(items, usize::MAX).await?;
        let findings = findings(items, last, number, start, &reviews, &mut rested, ages).await?;
        let mut writes = Vec::new();
        if let Some((previous, revision)) = previous
            && previous.state != State::Done
        {
            writes.extend(close(&previous, previous.clone(), revision, start.writer, start.at));
        }
        let (state, sage, review) = match &start.recipient {
            Recipient::Sage(sage) => {
                let (outcomes, earlier) = packet::review_history(items, number, ages, sage, start.trails).await?;
                (State::Sent, Some(sage.clone()), Some(Review::new(outcomes, earlier)))
            }
            Recipient::Ant => (State::Fallback, None, None),
        };
        let dream = Dream {
            number,
            at: start.at,
            since,
            state,
            sage,
            review,
            thread: None,
            threads: start
                .threads
                .iter()
                .map(|covered| Covered {
                    review: covered.review || reviews.iter().any(|review| review.thread == covered.thread),
                    ..covered.clone()
                })
                .collect(),
            commits: start.commits.clone(),
            findings,
            verdicts: Vec::new(),
            writes: Vec::new(),
            trails: Vec::new(),
            held: Vec::new(),
            diary: None,
            vitals: Some(Signs::sum(start.vitals.iter().map(|(_, signs)| signs))),
        };
        writes.push(Write::Put {
            id: Dream::id(number),
            kind: DREAM.into(),
            values: dream.values(),
            expected: Some(0),
        });
        let expected = Some(record.map_or(0, |(_, revision)| revision));
        let values = Record { number, rested }.values();
        writes.push(Write::Put { id: record_id(), kind: DREAMS.into(), values, expected });
        items.write(&writes, BY).await?;
        Ok(Some(dream))
    })
    .await
}

/// A persona or skill proposal awaiting the ant's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub id: ItemId,
    /// What it proposes, as a finding states it.
    pub text: String,
    pub untrusted: bool,
}

/// The pending persona and skill proposals a dream beginning at `at` offers: those made [`proposal::CONFIRM_AFTER_HOURS`] or
/// more before, each offered as a finding until the ant answers it.
pub(super) async fn proposals(items: &dyn Items, at: DateTime<Utc>) -> Result<Vec<Proposal>, StoreError> {
    Ok(proposal::pending(items)
        .await?
        .into_iter()
        .filter(|found| found.proposal.ripe() <= at)
        .map(|found| Proposal { text: found.finding(), untrusted: !found.proposal.writer.trusted(), id: found.item.id })
        .collect())
}

/// Whether a pending proposal ripe at `now` awaits a dream: one the `last` dream did not offer, so a dream is due for
/// it even when no thread was active.
pub async fn proposal_due(items: &dyn Items, last: Option<&Dream>, now: DateTime<Utc>) -> Result<bool, StoreError> {
    let offered = |id: &ItemId| last.is_some_and(|last| last.findings.iter().any(|finding| finding.items.contains(id)));
    Ok(proposal::pending(items).await?.iter().any(|found| found.proposal.ripe() <= now && !offered(found.id())))
}

/// A finding gathered for a dream, before it is numbered.
///
/// A finding is about its items, or about a subject without items, and may rest on a count: [`Gathered::key`] names
/// it the same in every dream, so a rejected one rests ([`Rested`]) until an item changes or the count differs.
pub(super) struct Gathered {
    pub(super) kind: FindingKind,
    /// What it is about: its items' ids joined by commas, or a subject naming it without items.
    pub(super) about: String,
    pub(super) items: Vec<ItemId>,
    pub(super) count: Option<u64>,
    pub(super) text: String,
    pub(super) untrusted: bool,
}

impl Gathered {
    /// A finding of `kind` about `items`.
    fn on(kind: FindingKind, items: Vec<ItemId>, text: String, untrusted: bool) -> Self {
        let about = items.iter().map(ItemId::as_str).collect::<Vec<_>>().join(",");
        Self { kind, about, items, count: None, text, untrusted }
    }

    pub(super) fn key(&self) -> String {
        format!("{}:{}", self.kind, self.about)
    }

    /// Whether what it is about changed since `rested` was rejected: the count it rests on, or one of its items other
    /// than by a read ([`memory::Access`]).
    async fn changed(&self, items: &dyn Items, rested: &Rested) -> Result<bool, StoreError> {
        if self.count != rested.count {
            return Ok(true);
        }
        for id in &self.items {
            let history = items.history(id).await?;
            let changed = |revision: &Revision| {
                revision.at > rested.at && revision.change.after.keys().any(|value| value != "access")
            };
            if history.iter().any(changed) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// The findings of dream `number`, beginning as `start` says after `previous`: the findings of the sage's answer to the
/// review of `previous` come after the ant was woken with the packet alone ([`Review::late`]), at most
/// [`MAX_LATE_FINDINGS`], the findings of `previous` held unshown under its sage's answer and its open findings, those
/// that still hold ([`Dream::again`]), then the packet's own on what they do not cover yet, those on the identity of
/// the ant first ([`identity_findings`]), which stay whatever a sage answers as the open ones do
/// ([`review::answered`]), then those on memories and proposals, then those on its practice as a sage from the records
/// of the `reviews` it made ([`practice_findings`]), then those on the vital signs of the work ([`vitals`]), up to
/// [`MAX_FINDINGS`]. An ant both worked with and reviewed has one finding on its relationship record, the identity's,
/// telling of both. An open finding on the identity, the practice or the vital signs holds while its ground does,
/// taking the text, count and trust gathered now. One of the `rested` stays out while what it is about is unchanged and
/// the ant was active less than [`REST_DAYS`] since the verdict, as `ages` counts; `rested` keeps those resting on.
async fn findings(
    items: &dyn Items,
    previous: Option<&Dream>,
    number: u64,
    start: &Start<'_>,
    reviews: &[ReviewRecord],
    rested: &mut Vec<Rested>,
    ages: &memory::Ages,
) -> Result<Vec<Finding>, StoreError> {
    let proposals = proposals(items, start.at).await?;
    let memories: Vec<_> = items.list(MEMORY, usize::MAX).await?.into_iter().filter_map(MemoryItem::of).collect();
    let live = |id: &ItemId| memories.iter().find(|found| found.id() == id).filter(|f| f.memory.status.live());
    let identity_found = identity_findings::gather(items, &start.writer.ant, reviews).await?;
    let mut practice = practice_findings::gather(items, reviews, ages).await?;
    let folded = |reviewed: &Gathered| {
        reviewed.kind == FindingKind::NoReviewedRelationship
            && identity_found
                .iter()
                .any(|worked| worked.kind == FindingKind::NoRelationship && worked.about == reviewed.about)
    };
    practice.retain(|reviewed| !folded(reviewed));
    let vital = vitals::gather(&start.vitals, &vitals::trend(items, number).await?);
    let late = previous.and_then(|previous| previous.review.as_ref()).map(|review| review.late.iter());
    let mut found: Vec<Finding> = late.into_iter().flatten().cloned().collect();
    for open in previous.into_iter().flat_map(Dream::again) {
        let mut open = open.clone();
        let holds = match open.kind {
            FindingKind::Sage => true,
            FindingKind::Proposal => open.items.iter().all(|id| proposals.iter().any(|proposal| &proposal.id == id)),
            FindingKind::NeedsReview => {
                open.items.iter().all(|id| live(id).is_some_and(|f| f.memory.status == Status::NeedsReview))
            }
            kind if kind.identity() || kind.practice() || kind == FindingKind::Vitals => {
                let mut regathered = identity_found.iter().chain(&practice).chain(&vital);
                let now = regathered.find(|gathered| open.key.as_ref() == Some(&gathered.key()));
                if let Some(now) = now {
                    (open.text, open.items, open.count) = (now.text.clone(), now.items.clone(), now.count);
                    open.untrusted = now.untrusted;
                }
                now.is_some()
            }
            _ => open.items.iter().all(|id| live(id).is_some()),
        };
        if holds {
            found.push(open);
        }
    }
    let diary = previous.and_then(|previous| previous.diary.as_ref());
    let mut fresh = identity_found;
    fresh.extend(gather(items, &memories, since(previous, start.at), diary, proposals).await?);
    fresh.extend(practice);
    fresh.extend(vital);
    let covered: BTreeSet<_> = found.iter().filter_map(|finding| finding.key.clone()).collect();
    let resting = |at| ages.since(at) < Duration::days(REST_DAYS);
    let mut n = 0;
    for gathered in fresh {
        if found.len() >= MAX_FINDINGS {
            break;
        }
        let key = gathered.key();
        if covered.contains(&key) {
            continue;
        }
        if let Some(index) = rested.iter().position(|rested| rested.key == key) {
            if resting(rested[index].at) && !gathered.changed(items, &rested[index]).await? {
                continue;
            }
            rested.remove(index);
        }
        n += 1;
        found.push(Finding {
            id: format!("{number}.{n}"),
            kind: gathered.kind,
            text: gathered.text,
            items: gathered.items,
            untrusted: gathered.untrusted,
            key: Some(key),
            count: gathered.count,
            advice: None,
            late: false,
        });
    }
    found.truncate(MAX_FINDINGS);
    rested.retain(|rested| resting(rested.at));
    Ok(found)
}

/// The packet's own findings on `memories` and `proposals`, for a dream reviewing from `since`, the last dream's
/// `diary` left out of those to distill.
async fn gather(
    items: &dyn Items,
    memories: &[MemoryItem],
    since: DateTime<Utc>,
    diary: Option<&ItemId>,
    proposals: Vec<Proposal>,
) -> Result<Vec<Gathered>, StoreError> {
    let mut fresh = Vec::new();
    let mine = |found: &MemoryItem| found.memory.trust == memory::Trust::Untrusted;
    for found in memories.iter().filter(|found| found.memory.status.live()) {
        let (memory, votes) = (&found.memory, &found.memory.votes);
        if votes.helpful > 0 && votes.stale > 0 && found.item.updated.at >= since {
            let text = format!(
                "Memory {} \"{}\" is voted helpful {} and stale {} times: check it, then vote, supersede or keep it.",
                found.id(),
                memory.title,
                votes.helpful,
                votes.stale
            );
            fresh.push(Gathered::on(FindingKind::Contradiction, vec![found.id().clone()], text, mine(found)));
        }
    }
    for found in memory::needs_review(items).await? {
        let why = if found.memory.citation_warnings.is_empty() { "" } else { " (a citation changed)" };
        let text = format!(
            "Memory {} \"{}\" needs review{why}: check it, then vote it helpful, supersede it or vote it wrong.",
            found.id(),
            found.memory.title
        );
        fresh.push(Gathered::on(FindingKind::NeedsReview, vec![found.id().clone()], text, mine(&found)));
    }
    for cluster in memory::duplicates(items).await? {
        let named: Vec<_> = cluster.iter().map(|found| format!("{} \"{}\"", found.id(), found.memory.title)).collect();
        let text = format!(
            "Memories {} are near duplicates: keep one, superseding it with what all hold, and vote the others wrong.",
            named.join(", ")
        );
        let ids = cluster.iter().map(|found| found.id().clone()).collect();
        fresh.push(Gathered::on(FindingKind::Duplicates, ids, text, cluster.iter().any(mine)));
    }
    let mut episodes: Vec<_> = memories
        .iter()
        .filter(|found| found.memory.kind == MemoryKind::Episode && found.memory.status.live())
        .filter(|found| found.item.created.at >= since && Some(found.id()) != diary)
        .collect();
    episodes.sort_by_key(|found| found.item.created.at);
    for found in episodes {
        let text = format!(
            "Episode {} \"{}\": distill what it teaches into lessons, facts or decisions with sense_remember.",
            found.id(),
            found.memory.title
        );
        fresh.push(Gathered::on(FindingKind::Distill, vec![found.id().clone()], text, mine(found)));
    }
    for proposal in proposals {
        fresh.push(Gathered::on(FindingKind::Proposal, vec![proposal.id], proposal.text, proposal.untrusted));
    }
    Ok(fresh)
}
