//! Asking `ants signer`, with the machine's host credentials, to sign an added ant's key or revoke a retired ant's, on
//! the admin order carried out: an enrollment request carrying the order ([`EnrollRequest::order`]), sent again every
//! [`ENROLL_REPEAT`] until the signer answers for the key or the wait ends. Behind a leaf node, the request also
//! carries the uplink credentials, signed with their key too ([`EnrollRequest::leaf`]), for the signer to renew them.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{AdminOrder, ENROLL_REPEAT, EnrollLeaf, EnrollReply, EnrollRequest, Machine, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use chrono::Utc;
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;

use crate::{creds, keystore};

/// How long the daemon waits for `ants signer` to answer.
pub(super) const SIGNER_WAIT: Duration = Duration::from_mins(2);

/// The uplink of the machine's leaf node: its machine name and the path of its credentials.
pub(super) struct Uplink {
    pub machine: Machine,
    pub creds: PathBuf,
}

/// Ask the signer about `public_key` for the ant of `order` with the host credentials at `host_creds`, and the
/// credentials of `uplink` if given, waiting up to `wait`: its answer, approving, rejecting or retiring the key. Fails
/// when none comes.
pub(super) async fn ask(
    js: &jetstream::Context,
    host_creds: &Path,
    uplink: Option<&Uplink>,
    order: &AdminOrder,
    public_key: &str,
    wait: Duration,
) -> Result<EnrollReply> {
    let (jwt, key) = creds::load(host_creds, keystore::os())?;
    let uplink = uplink
        .map(|u| {
            let (jwt, key) = creds::load(&u.creds, keystore::os())
                .with_context(|| format!("the uplink credentials of the leaf node {}", u.machine))?;
            Ok::<_, anyhow::Error>((EnrollLeaf { machine: u.machine.clone(), jwt, signature: String::new() }, key))
        })
        .transpose()?;
    let client = js.client();
    let ant = &order.ant;
    let mut replies = client.subscribe(subject::enroll_reply(ant)).await?;
    let mut request = EnrollRequest::new(ant.clone(), public_key, order.machine.as_str());
    request.enrollment = jwt;
    request.order = Some(Box::new(order.clone()));
    let leaf_key = uplink.map(|(leaf, key)| {
        request.leaf = Some(leaf);
        key
    });
    let mut repeats = tokio::time::interval(ENROLL_REPEAT);
    let end = tokio::time::sleep(wait);
    tokio::pin!(end);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                request.sent_at = Utc::now();
                let signed = request.signed();
                let signature = key.sign(&signed).map_err(|e| anyhow!("signing the request: {e}"))?;
                request.signature = BASE64URL_NOPAD.encode(&signature);
                if let (Some(leaf), Some(key)) = (request.leaf.as_mut(), &leaf_key) {
                    let signature = key.sign(&signed).map_err(|e| anyhow!("signing the request: {e}"))?;
                    leaf.signature = BASE64URL_NOPAD.encode(&signature);
                }
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
