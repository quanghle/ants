//! Asking `ants signer`, with the machine's host credentials, to sign an added or cloned ant's key or revoke a retired ant's, on
//! the admin order carried out: an enrollment request carrying the order ([`EnrollRequest::order`]), sent again every
//! [`ENROLL_REPEAT`] until the signer answers for the key or the wait ends.

use std::path::Path;
use std::time::Duration;

use ants_proto::{AdminOrder, ENROLL_REPEAT, EnrollReply, EnrollRequest, UserId, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use chrono::Utc;
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;

use crate::{creds, keystore};

/// How long the daemon waits for `ants signer` to answer.
pub(super) const SIGNER_WAIT: Duration = Duration::from_mins(2);

/// Ask the signer about `public_key` for `ant`, the ant of `order` or the clone it makes, with the host credentials at
/// `host_creds`, waiting up to `wait`: its answer, approving, rejecting or retiring the key. Fails when none comes.
pub(super) async fn ask(
    js: &jetstream::Context,
    host_creds: &Path,
    order: &AdminOrder,
    ant: &UserId,
    public_key: &str,
    wait: Duration,
) -> Result<EnrollReply> {
    let (jwt, key) = creds::load(host_creds, keystore::os())?;
    let client = js.client();
    let mut replies = client.subscribe(subject::enroll_reply(ant)).await?;
    let mut request = EnrollRequest::new(ant.clone(), public_key, order.machine.as_str());
    request.enrollment = jwt;
    request.order = Some(Box::new(order.clone()));
    let mut repeats = tokio::time::interval(ENROLL_REPEAT);
    let end = tokio::time::sleep(wait);
    tokio::pin!(end);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                request.sent_at = Utc::now();
                let signature = key.sign(&request.signed()).map_err(|e| anyhow!("signing the request: {e}"))?;
                request.signature = BASE64URL_NOPAD.encode(&signature);
                client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
                client.flush().await?;
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                match serde_json::from_slice::<EnrollReply>(&msg.payload) {
                    // A rejection of every key of the name is a human's of another request.
                    Ok(EnrollReply::Rejected { public_key: None, .. }) => {}
                    Ok(reply) if reply.ant() == ant && reply.answers(public_key) => return Ok(reply),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "ignoring a malformed enrollment answer"),
                }
            }
            () = &mut end => bail!(
                "no answer from `ants signer` in {}s: is it running where the account key is?",
                wait.as_secs()
            ),
        }
    }
}
