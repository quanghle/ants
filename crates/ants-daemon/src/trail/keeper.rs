//! The saves and removals of ants' trails ([`SaveTrailRequest`]): ant credentials write nothing of [`TRAILS_BUCKET`],
//! a trail's key naming no saver, so an ant asks ([`request`]) and `ants signer` answers ([`serve`]), writing for the
//! ant only a trail it saves as its own, under a new name or over one it saved, and removing only one it saved, and
//! recording a run as the trail's source only from the ant's own record of that run in [`TRAIL_RUNS_BUCKET`]. A daemon
//! on a server without credentials answers its ant itself ([`answer`]).

use std::time::Duration;

use ants_proto::{
    JsDomain, RunId, SAVE_TRAIL_REPEAT, SAVE_TRAIL_WAIT, SaveTrailOutcome, SaveTrailReply, SaveTrailRequest,
    SavedTrail, Saver, TRAIL_RUNS_BUCKET, TRAILS_BUCKET, TrailChange, TrailName, TrailRun, UserId, subject,
    trail_run_key,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use futures::StreamExt as _;

use crate::bus::relayed;
use crate::records::direct_kv::{DirectKv, Lookup};

/// Queue group of the processes answering trail saves: one of them takes each request.
const QUEUE: &str = "savetrail";
/// Trail saves one process answers at once.
const CONCURRENT: usize = 4;

/// What the bucket holds under a trail's name, as the keeper reads it.
enum Held {
    Absent,
    Saved(SavedTrail),
    Unreadable,
}

async fn held(kv: &DirectKv, name: &TrailName) -> Result<Option<(Held, u64)>> {
    let (found, last) = kv.lookup_last(name.as_str()).await.with_context(|| format!("reading saved trail {name}"))?;
    let held = match found {
        Lookup::NoBucket => return Ok(None),
        Lookup::NoValue => Held::Absent,
        Lookup::Value(value) => match serde_json::from_slice::<SavedTrail>(&value) {
            Ok(trail) if trail.check_key(name.as_str()).is_ok() => Held::Saved(trail),
            _ => Held::Unreadable,
        },
    };
    Ok(Some((held, last)))
}

/// Why a save recording `run` as the source of `trail`, by `ant`, is refused, read from the ant's record of the run in
/// `runs`, [`TRAIL_RUNS_BUCKET`] read past relayed messages: None when the record is the ant's own of a finished run of
/// an inline script, `trail`'s. Fails only when the bucket is unreachable.
async fn unverified_source(
    runs: Option<&DirectKv>,
    ant: &UserId,
    run: &RunId,
    trail: &SavedTrail,
) -> Result<Option<String>> {
    let unverified = |why: &str| Ok(Some(format!("trail {} names run {run} as its source, but {why}", trail.name)));
    let Some(runs) = runs else { return unverified("the runs of the colony are unread here") };
    let key = trail_run_key(ant, run);
    let header = match runs.lookup(&key).await.with_context(|| format!("reading the record of run {run}"))? {
        Lookup::NoBucket => return unverified(&format!("the bus keeps no {TRAIL_RUNS_BUCKET}")),
        Lookup::NoValue => return unverified(&format!("{ant} keeps no record of such a run")),
        Lookup::Value(value) => match serde_json::from_slice::<TrailRun>(&value) {
            Ok(header) if header.conductor == *ant && header.run == *run => header,
            _ => return unverified(&format!("the record {ant} keeps of it does not read as that run's")),
        },
    };
    if !header.finished() {
        return unverified("the run has not finished");
    }
    if header.first_trail().is_some() {
        return unverified("the run ran a saved trail, not an inline script");
    }
    match header.route.first() {
        Some(leg) if leg.script == trail.script => Ok(None),
        _ => unverified("the run's script is another"),
    }
}

/// The answer to `request`, made on `kv`, [`TRAILS_BUCKET`] as credentials writing any trail reach it, a run named as
/// a trail's source checked in `runs` ([`TRAIL_RUNS_BUCKET`], read past relayed messages; with none, refused): the
/// change made when the ant may make it and the entry is still the one the ant checked. A repeat of a change made
/// already is [`SaveTrailOutcome::Done`]. Fails only when a bucket is unreachable, for the ant to ask again.
pub async fn answer(kv: &DirectKv, runs: Option<&DirectKv>, request: &SaveTrailRequest) -> Result<SaveTrailOutcome> {
    let refused = |why: String| Ok(SaveTrailOutcome::Refused { why });
    let mine = Saver::Ant { user: request.ant.clone() };
    let name = request.change.name();
    if let TrailChange::Save { trail, .. } = &request.change {
        if trail.saved_by != mine {
            return refused(format!("an ant saves a trail as its own, not as {}", trail.saved_by));
        }
        if let Err(e) = trail.check() {
            return refused(e.to_string());
        }
        if let Some(run) = &trail.saved_from
            && let Some(why) = unverified_source(runs, &request.ant, run, trail).await?
        {
            return refused(why);
        }
    }
    let Some((held, seq)) = held(kv, name).await? else {
        return refused(format!("the bus keeps no {TRAILS_BUCKET} (`ants init` creates it)"));
    };
    let others = |trail: &SavedTrail| format!("trail {name} was saved by {}", trail.saved_by);
    let unreadable = format!("trail {name} is saved but unreadable; only a human replaces or removes it");
    let done = |made: bool| if made { SaveTrailOutcome::Done } else { SaveTrailOutcome::Changed };
    match &request.change {
        TrailChange::Save { trail, .. } if matches!(&held, Held::Saved(now) if now == trail) => {
            Ok(SaveTrailOutcome::Done)
        }
        TrailChange::Save { trail, last: None } => match held {
            Held::Absent => Ok(done(kv.create(trail.key(), serde_json::to_vec(trail)?).await?)),
            Held::Saved(now) if now.saved_by != mine => refused(others(&now)),
            Held::Unreadable => refused(unreadable),
            Held::Saved(_) => Ok(SaveTrailOutcome::Changed),
        },
        TrailChange::Save { trail, last: Some(last) } => match held {
            _ if seq != *last => Ok(SaveTrailOutcome::Changed),
            Held::Saved(now) if now.saved_by != mine => refused(others(&now)),
            Held::Unreadable => refused(unreadable),
            Held::Absent => Ok(SaveTrailOutcome::Changed),
            Held::Saved(_) => Ok(done(kv.update(trail.key(), serde_json::to_vec(trail)?, *last).await?)),
        },
        TrailChange::Remove { last, .. } => match held {
            Held::Absent => Ok(SaveTrailOutcome::Done),
            _ if seq != *last => Ok(SaveTrailOutcome::Changed),
            Held::Saved(now) if now.saved_by != mine => refused(others(&now)),
            Held::Unreadable => refused(unreadable),
            Held::Saved(_) => Ok(done(kv.delete_expecting(name.as_str(), *last, None).await?)),
        },
    }
}

/// Answer the trail saves of ants on `client`, in the queue group of every process that does, until the subscription
/// ends: `ants signer`, with the credentials of [`Permissions::for_trail_keeper`], reading the runs named as trails'
/// sources by direct gets of [`TRAIL_RUNS_BUCKET`] naming no subject. Each answer goes to the asking ant's
/// [`subject::save_trail_reply`] only, and a request on another ant's subject goes unanswered.
///
/// [`Permissions::for_trail_keeper`]: ants_proto::Permissions::for_trail_keeper
pub async fn serve(client: async_nats::Client, js: jetstream::Context, js_domain: Option<JsDomain>) -> Result<()> {
    let kv = DirectKv::whole(&js, js_domain.as_ref(), TRAILS_BUCKET).await?.past_relays();
    let runs = DirectKv::whole(&js, js_domain.as_ref(), TRAIL_RUNS_BUCKET).await?.past_relays();
    let requests = relayed::queue_subscribe(&client, subject::ALL_SAVE_TRAIL_REQUESTS, QUEUE.to_owned())
        .await
        .context("subscribing to trail saves")?;
    requests.for_each_concurrent(CONCURRENT, |msg| respond(&client, &kv, &runs, msg)).await;
    bail!("the subscription to trail saves ended")
}

async fn respond(client: &async_nats::Client, kv: &DirectKv, runs: &DirectKv, msg: async_nats::Message) {
    if relayed::heard_relayed(&msg) {
        tracing::warn!(subject = %msg.subject, "dropping a trail save a stream relayed");
        return;
    }
    let request = serde_json::from_slice::<SaveTrailRequest>(&msg.payload)
        .map_err(|e| e.to_string())
        .and_then(|r| r.check_subject(&msg.subject).map(|()| r).map_err(|e| e.to_string()));
    let request = match request {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(subject = %msg.subject, error, "dropping a trail save");
            return;
        }
    };
    let outcome = match answer(kv, Some(runs), &request).await {
        Ok(outcome) => outcome,
        Err(e) => {
            // No answer: the ant repeats the request, and a later attempt may get through.
            let trail = request.change.name();
            tracing::warn!(ant = %request.ant, %trail, error = format!("{e:#}"), "answering a trail save");
            return;
        }
    };
    if let SaveTrailOutcome::Refused { why } = &outcome {
        tracing::info!(ant = %request.ant, trail = %request.change.name(), why, "refusing a trail save");
    }
    let reply = SaveTrailReply { request_id: request.id, ant: request.ant, outcome };
    let payload = serde_json::to_vec(&reply).expect("an answer");
    if let Err(e) = client.publish(reply.subject(), payload.into()).await {
        tracing::warn!(error = %e, "answering a trail save");
    }
}

/// Make `request` on `client`, as its ant, repeating it every [`SAVE_TRAIL_REPEAT`]: the answer, None when nothing
/// answered within [`SAVE_TRAIL_WAIT`].
pub async fn request(client: &async_nats::Client, request: &SaveTrailRequest) -> Result<Option<SaveTrailOutcome>> {
    request_within(client, request, SAVE_TRAIL_WAIT).await
}

/// Make `request` as [`request`] does, waiting at most `wait`.
pub async fn request_within(
    client: &async_nats::Client,
    request: &SaveTrailRequest,
    wait: Duration,
) -> Result<Option<SaveTrailOutcome>> {
    let mut replies =
        relayed::subscribe(client, request.reply_subject()).await.context("subscribing to the answers")?;
    let payload = serde_json::to_vec(request)?;
    let deadline = tokio::time::Instant::now() + wait;
    while tokio::time::Instant::now() < deadline {
        client.publish(request.subject(), payload.clone().into()).await.context("asking to save the trail")?;
        let next = (tokio::time::Instant::now() + SAVE_TRAIL_REPEAT).min(deadline);
        while let Ok(msg) = tokio::time::timeout_at(next, replies.next()).await {
            let msg = msg.context("the answers ended")?;
            match serde_json::from_slice::<SaveTrailReply>(&msg.payload) {
                Ok(reply) if reply.request_id == request.id && reply.check_subject(&msg.subject).is_ok() => {
                    return Ok(Some(reply.outcome));
                }
                Ok(_) => tracing::debug!("ignoring an answer to another trail save"),
                Err(e) => tracing::warn!(error = %e, "ignoring a malformed trail save answer"),
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
