//! The team changes of ants ([`TeamAdminRequest`]): the directory is one entry for every team, so ant credentials
//! write nothing of [`TEAMS_BUCKET`]; an ant asks ([`request`]) and `ants signer` answers ([`serve`]), writing for the
//! ant only what a lead may change of the teams it manages ([`TeamDirectory::apply_for_ant`]), on the directory as the
//! ant read it. A daemon on a server without credentials answers its ant itself ([`answer`]).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::{
    HUMANS_BUCKET, IDENTITIES_BUCKET, JsDomain, TEAMADM_REPEAT, TEAMADM_WAIT, TeamAdminOutcome, TeamAdminReply,
    TeamAdminRequest, TeamRefusal, UserId, Uuid, subject,
};
#[cfg(doc)]
use ants_proto::{TEAMS_BUCKET, TeamDirectory};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use futures::StreamExt as _;
use tokio::sync::Mutex;

use crate::bus::relayed;
use crate::records::direct_kv::{DirectKv, Lookup};

/// Queue group of the processes answering team changes: one of them takes each request.
const QUEUE: &str = "teamadm";
/// Answers remembered for requests repeated with the same id; the oldest are forgotten first.
const MAX_ANSWERED: usize = 1024;

/// The ants of the bus, by their identity records in [`IDENTITIES_BUCKET`].
async fn known_ants(identities: Option<&DirectKv>, names: &[&UserId]) -> Result<Vec<UserId>> {
    let mut known = Vec::new();
    let Some(identities) = identities else { return Ok(known) };
    for name in names {
        if matches!(identities.lookup(name.as_str()).await?, Lookup::Value(_)) {
            known.push((*name).clone());
        }
    }
    Ok(known)
}

/// Refuse `name` for a new team when an ant (of `identities`), a human (of `humans`, [`HUMANS_BUCKET`]) or a reserved
/// name holds it: teams share the names of ants and humans. Fails when a bucket is unreachable, or not given.
async fn check_team_name(
    identities: Option<&DirectKv>,
    humans: Option<&DirectKv>,
    name: &UserId,
) -> Result<Result<(), TeamRefusal>> {
    let (Some(identities), Some(humans)) = (identities, humans) else {
        bail!("no ants and humans to check the name of team {name} against");
    };
    let taken = name.is_reserved()
        || !known_ants(Some(identities), &[name]).await?.is_empty()
        || matches!(humans.lookup(name.as_str()).await?, Lookup::Value(_));
    Ok(if taken { Err(TeamRefusal::NameTaken { name: name.clone() }) } else { Ok(()) })
}

/// The answer to `request`, made on `kv`, [`TEAMS_BUCKET`] as credentials writing the directory reach it, an ant
/// added checked in `identities` ([`IDENTITIES_BUCKET`], read by direct gets naming no subject; with none, every ant
/// added is refused), and a team created refused under the name of an ant, a human of `humans` ([`HUMANS_BUCKET`]) or
/// a reserved name ([`TeamRefusal::NameTaken`]): the change made when the ant may make it and the directory is still
/// the one the ant read, else [`TeamAdminOutcome::Changed`]. Fails only when a bucket is unreachable, for the ant to
/// ask again.
pub async fn answer(
    kv: &DirectKv,
    identities: Option<&DirectKv>,
    humans: Option<&DirectKv>,
    request: &TeamAdminRequest,
) -> Result<TeamAdminOutcome> {
    let refused = |refusal: TeamRefusal| Ok(TeamAdminOutcome::Refused { refusal });
    if request.is_expired(Utc::now()) {
        return refused(TeamRefusal::Expired);
    }
    if let Err(refusal) = request.check_ops() {
        return refused(refusal);
    }
    let Some(read) = super::read(kv).await? else { return refused(TeamRefusal::NoDirectory) };
    if read.last != request.last {
        return Ok(TeamAdminOutcome::Changed);
    }
    let mut directory = match read.directory {
        Ok(directory) => directory,
        Err(refusal) => return refused(refusal),
    };
    for created in request.ops.iter().filter_map(ants_proto::TeamOp::team_created) {
        if let Err(refusal) = check_team_name(identities, humans, created).await? {
            return refused(refusal);
        }
    }
    let named: Vec<&UserId> = request.ops.iter().filter_map(ants_proto::TeamOp::ant_added).collect();
    let known = known_ants(identities, &named).await?;
    let changed = match directory.apply_for_ant(&request.ant, &request.team, &request.ops, |a| known.contains(a)) {
        Ok(changed) => changed,
        Err(refusal) => return refused(refusal),
    };
    if changed.is_empty() {
        return Ok(TeamAdminOutcome::Applied { last: read.last, changed });
    }
    let payload = serde_json::to_vec(&directory)?;
    if !kv.update(ants_proto::TEAM_DIRECTORY_KEY, payload, read.last).await? {
        return Ok(TeamAdminOutcome::Changed);
    }
    let (_, last) = kv.lookup_last(ants_proto::TEAM_DIRECTORY_KEY).await?;
    Ok(TeamAdminOutcome::Applied { last, changed })
}

/// Answer the team changes of ants on `client`, in the queue group of every process that does, until the subscription
/// ends: `ants signer`, with the credentials of [`Permissions::for_team_keeper`]. Each answer goes to the asking ant's
/// [`subject::teamadm_reply`] only, a request on another ant's subject goes unanswered, and a request repeated with the
/// same id gets the first answer.
///
/// [`Permissions::for_team_keeper`]: ants_proto::Permissions::for_team_keeper
pub async fn serve(client: async_nats::Client, js: jetstream::Context, js_domain: Option<JsDomain>) -> Result<()> {
    let kv = super::open(&js, js_domain.as_ref()).await?;
    let identities = DirectKv::whole(&js, js_domain.as_ref(), IDENTITIES_BUCKET).await?;
    let humans = DirectKv::new(&js, js_domain.as_ref(), HUMANS_BUCKET).await?;
    let requests = relayed::queue_subscribe(&client, subject::ALL_TEAMADM_REQUESTS, QUEUE.to_owned())
        .await
        .context("subscribing to team changes")?;
    let answered = Arc::new(Mutex::new(VecDeque::new()));
    // One at a time: each change is checked against the directory the one before it wrote.
    requests.for_each(|msg| respond(&client, &kv, &identities, &humans, &answered, msg)).await;
    bail!("the subscription to team changes ended")
}

async fn respond(
    client: &async_nats::Client,
    kv: &DirectKv,
    identities: &DirectKv,
    humans: &DirectKv,
    answered: &Mutex<VecDeque<(Uuid, UserId, TeamAdminOutcome)>>,
    msg: async_nats::Message,
) {
    if relayed::heard_relayed(&msg) {
        tracing::warn!(subject = %msg.subject, "dropping a team change a stream relayed");
        return;
    }
    let request = serde_json::from_slice::<TeamAdminRequest>(&msg.payload)
        .map_err(|e| e.to_string())
        .and_then(|r| r.check_subject(&msg.subject).map(|()| r).map_err(|e| e.to_string()));
    let request = match request {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(subject = %msg.subject, error, "dropping a team change");
            return;
        }
    };
    let first = answered.lock().await.iter().find(|(id, ant, _)| *id == request.id && *ant == request.ant).cloned();
    let outcome = match first {
        Some((.., outcome)) => outcome,
        None => match answer(kv, Some(identities), Some(humans), &request).await {
            Ok(outcome) => {
                let mut answered = answered.lock().await;
                if answered.len() == MAX_ANSWERED {
                    answered.pop_front();
                }
                answered.push_back((request.id, request.ant.clone(), outcome.clone()));
                outcome
            }
            Err(e) => {
                // No answer: the ant repeats the request, and a later attempt may get through.
                tracing::warn!(ant = %request.ant, team = %request.team, error = format!("{e:#}"), "changing a team");
                return;
            }
        },
    };
    match &outcome {
        TeamAdminOutcome::Applied { changed, .. } if !changed.is_empty() => {
            tracing::info!(ant = %request.ant, team = %request.team, ?changed, "changed a team");
        }
        TeamAdminOutcome::Refused { refusal } => {
            tracing::info!(ant = %request.ant, team = %request.team, %refusal, "refusing a team change");
        }
        _ => {}
    }
    let reply = TeamAdminReply { request_id: request.id, ant: request.ant, outcome };
    let payload = serde_json::to_vec(&reply).expect("an answer");
    if let Err(e) = client.publish(reply.subject(), payload.into()).await {
        tracing::warn!(error = %e, "answering a team change");
    }
}

/// Make `request` on `client`, as its ant, repeating it every [`TEAMADM_REPEAT`]: the answer, None when nothing
/// answered within [`TEAMADM_WAIT`].
pub async fn request(client: &async_nats::Client, request: &TeamAdminRequest) -> Result<Option<TeamAdminOutcome>> {
    request_within(client, request, TEAMADM_WAIT).await
}

/// Make `request` as [`request`] does, waiting at most `wait`.
pub async fn request_within(
    client: &async_nats::Client,
    request: &TeamAdminRequest,
    wait: Duration,
) -> Result<Option<TeamAdminOutcome>> {
    let mut replies =
        relayed::subscribe(client, request.reply_subject()).await.context("subscribing to the answers")?;
    let payload = serde_json::to_vec(request)?;
    let deadline = tokio::time::Instant::now() + wait;
    while tokio::time::Instant::now() < deadline {
        client.publish(request.subject(), payload.clone().into()).await.context("asking to change the team")?;
        let next = (tokio::time::Instant::now() + TEAMADM_REPEAT).min(deadline);
        while let Ok(msg) = tokio::time::timeout_at(next, replies.next()).await {
            let msg = msg.context("the answers ended")?;
            match serde_json::from_slice::<TeamAdminReply>(&msg.payload) {
                Ok(reply) if reply.request_id == request.id && reply.check_subject(&msg.subject).is_ok() => {
                    return Ok(Some(reply.outcome));
                }
                Ok(_) => tracing::debug!("ignoring an answer to another team change"),
                Err(e) => tracing::warn!(error = %e, "ignoring a malformed team change answer"),
            }
        }
    }
    Ok(None)
}

/// Where an ant's daemon changes the teams its ant manages: through `ants signer` on `client`, or on the directory
/// itself, checking as the signer does, as a daemon without credentials does.
#[derive(Clone)]
pub struct Changer {
    pub client: async_nats::Client,
    pub kv: DirectKv,
    pub identities: Option<DirectKv>,
    /// The humans' entries ([`HUMANS_BUCKET`]), which a team created must not be named after.
    pub humans: Option<DirectKv>,
    /// Whether the signer writes: the daemon connected with credentials, which write nothing of [`TEAMS_BUCKET`].
    pub signer: bool,
}

impl Changer {
    /// Change the team `team` for `ant` with `ops`, read again and asked anew while the directory changes meanwhile.
    /// The outcome, None when no signer answered.
    pub async fn change(
        &self,
        ant: &UserId,
        team: &UserId,
        ops: Vec<ants_proto::TeamOp>,
        thread_id: Option<ants_proto::ThreadId>,
    ) -> Result<Option<TeamAdminOutcome>> {
        const TRIES: usize = 5;
        for _ in 0..TRIES {
            let last = super::read(&self.kv).await?.map_or(0, |read| read.last);
            let mut ask = TeamAdminRequest::new(ant.clone(), team.clone(), ops.clone(), last);
            ask.thread_id.clone_from(&thread_id);
            let outcome = if self.signer {
                request(&self.client, &ask).await?
            } else {
                Some(answer(&self.kv, self.identities.as_ref(), self.humans.as_ref(), &ask).await?)
            };
            if !matches!(outcome, Some(TeamAdminOutcome::Changed)) {
                return Ok(outcome);
            }
        }
        Ok(Some(TeamAdminOutcome::Refused { refusal: TeamRefusal::Busy }))
    }
}

#[cfg(test)]
mod tests;
