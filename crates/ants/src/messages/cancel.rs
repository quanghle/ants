//! Stopping the prompt an ant runs on a thread, as `ants cancel` and `ants chat` ask its daemons to.

use ants_daemon::bus::relayed;
use std::time::Duration;

use ants_proto::{CancelReply, CancelRequest, ThreadId, UserId};
use anyhow::{Result, bail};
use futures::StreamExt as _;

/// How long `ants cancel` waits for a daemon of the ant to answer.
pub(crate) const CANCEL_WAIT: Duration = Duration::from_secs(5);

/// How long `ants cancel` waits on, after a daemon of the ant runs no prompt on the thread, for another daemon of the
/// same ant that may run it.
pub(crate) const CANCEL_GRACE: Duration = Duration::from_millis(500);

/// Cancel the prompt `ant` runs on `thread` as human `me`, through `client`; what `ants cancel` prints.
pub(crate) async fn cancelled(
    client: &async_nats::Client,
    me: &UserId,
    ant: UserId,
    thread: ThreadId,
) -> Result<String> {
    let request = CancelRequest::new(ant, thread, me.clone());
    let answers = relayed::subscribe(client, request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    let (ant, thread) = (&request.ant, &request.thread_id);
    match cancel_outcome(&request, answers, CANCEL_WAIT, CANCEL_GRACE).await {
        Some(true) => {
            Ok(format!("cancelled the prompt of {ant} on thread {thread}; the partial reply follows on the thread"))
        }
        Some(false) => bail!("{ant} has no prompt running on thread {thread}"),
        None => bail!("no daemon serving {ant} answered within {}s", CANCEL_WAIT.as_secs()),
    }
}

/// What the daemons of the ant answer to `request`: whether one cancelled the prompt, or `None` when none answers
/// within `wait`. That no prompt runs is the answer once `grace` passes with no daemon cancelling it.
pub(crate) async fn cancel_outcome(
    request: &CancelRequest,
    mut answers: impl futures::Stream<Item = async_nats::Message> + Unpin,
    wait: Duration,
    grace: Duration,
) -> Option<bool> {
    let mut deadline = tokio::time::Instant::now() + wait;
    let mut outcome = None;
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, answers.next()).await {
        let Ok(answer) = serde_json::from_slice::<CancelReply>(&msg.payload) else { continue };
        if answer.request_id != request.id || answer.check_subject(&msg.subject).is_err() {
            continue;
        }
        if answer.cancelled {
            return Some(true);
        }
        if outcome.is_none() {
            outcome = Some(false);
            deadline = deadline.min(tokio::time::Instant::now() + grace);
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancel_waits_for_any_daemon_of_the_ant_to_cancel_the_prompt() {
        let request = CancelRequest::new("dev-a".parse().unwrap(), "t1".parse().unwrap(), UserId::human());
        let msg = |answer: &CancelReply| async_nats::Message {
            subject: answer.subject().into(),
            reply: None,
            payload: serde_json::to_vec(answer).unwrap().into(),
            headers: None,
            status: None,
            description: None,
            length: 0,
        };
        let other = CancelRequest::new(request.ant.clone(), request.thread_id.clone(), UserId::human());
        let (idle, busy) = (msg(&request.answer(false)), msg(&request.answer(true)));
        let (long, grace) = (Duration::from_secs(60), Duration::from_millis(50));
        let answers = |msgs: Vec<async_nats::Message>| futures::stream::iter(msgs).chain(futures::stream::pending());

        let outcome = cancel_outcome(&request, answers(vec![idle.clone(), busy]), long, grace).await;
        assert_eq!(outcome, Some(true), "another daemon of the ant runs the prompt");
        let alone = vec![msg(&other.answer(true)), idle];
        let outcome =
            tokio::time::timeout(Duration::from_secs(5), cancel_outcome(&request, answers(alone), long, grace)).await;
        assert_eq!(outcome, Ok(Some(false)), "no prompt, known once the grace passes");
        let outcome = cancel_outcome(&request, answers(vec![msg(&other.answer(true))]), grace, grace).await;
        assert_eq!(outcome, None, "no daemon answered this request");
    }
}
