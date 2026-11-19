//! The tips ants leave on saved trails, and their votes, in [`TRAIL_TIPS_BUCKET`]: each ant writes its own tips and its
//! own votes, and reads every trail's by direct gets naming no subject.

use std::time::Duration;

use ants_proto::{JsDomain, TRAIL_TIPS_BUCKET, TipBoard, TipVote, TrailName, TrailTip};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::StatusCode;
use async_nats::jetstream::{self, message::StreamMessage};
use futures::StreamExt as _;

use crate::records::direct_kv::DirectKv;

/// Messages asked for at once by a direct get, and the bytes they come to at most (a larger message comes alone).
const BATCH: usize = 256;
const BATCH_BYTES: usize = 4 << 20;
/// The header marking a KV delete or purge.
const KV_OPERATION: &str = "KV-Operation";
const NUM_PENDING: &str = "Nats-Num-Pending";

/// The tips of the colony's trails. Cheap to clone.
#[derive(Clone)]
pub struct Tips {
    js: jetstream::Context,
    kv: DirectKv,
}

impl Tips {
    /// The tips in [`TRAIL_TIPS_BUCKET`], without a request: whether the bucket exists shows on the first read or
    /// write. `js_domain` is the domain `js` was made for ([`crate::bus::jetstream`]).
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<Self> {
        Ok(Self { js: js.clone(), kv: DirectKv::whole(js, js_domain, TRAIL_TIPS_BUCKET).await? })
    }

    /// Every tip left on `trail` and every vote on them, as last put: deleted entries are left out.
    pub async fn board(&self, trail: &TrailName) -> Result<TipBoard> {
        let filter = format!("$KV.{TRAIL_TIPS_BUCKET}.{trail}.>");
        let prefix = format!("$KV.{TRAIL_TIPS_BUCKET}.");
        let client = self.js.client();
        let wait = client.timeout().unwrap_or(Duration::from_secs(10));
        let inbox = client.new_inbox();
        let mut replies = client.subscribe(inbox.clone()).await?;
        let (mut entries, mut seq) = (Vec::new(), 1);
        loop {
            let request = serde_json::json!({
                "seq": seq,
                "next_by_subj": filter,
                "batch": BATCH,
                "max_bytes": BATCH_BYTES,
            });
            let request = async_nats::Request::new().inbox(inbox.clone()).payload(request.to_string().into());
            self.js
                .send_request(format!("DIRECT.GET.KV_{TRAIL_TIPS_BUCKET}"), request)
                .await
                .with_context(|| format!("reading the tips of {trail}"))?;
            let mut got = 0;
            loop {
                let Ok(Some(msg)) = tokio::time::timeout(wait, replies.next()).await else {
                    bail!("no answer reading the tips of {trail} from {TRAIL_TIPS_BUCKET} (`ants init` creates it)");
                };
                match msg.status {
                    None => {}
                    Some(StatusCode::NOT_FOUND) => return Ok(TipBoard::new(trail.clone(), entries)),
                    Some(StatusCode::NO_RESPONDERS) => {
                        bail!("there is no bucket {TRAIL_TIPS_BUCKET}: `ants init` creates it")
                    }
                    Some(status) if status.as_u16() == 204 => {
                        let pending = msg.headers.as_ref().and_then(|h| h.get(NUM_PENDING));
                        if got == 0 || pending.is_some_and(|n| n.as_str() == "0") {
                            return Ok(TipBoard::new(trail.clone(), entries));
                        }
                        break;
                    }
                    Some(status) => {
                        bail!(
                            "reading the tips of {trail}: {TRAIL_TIPS_BUCKET} answered {status} {:?}",
                            msg.description
                        )
                    }
                }
                got += 1;
                let msg = StreamMessage::try_from(msg).map_err(|e| anyhow!(e))?;
                seq = msg.sequence + 1;
                let deleted = msg.headers.get(KV_OPERATION).is_some();
                if let Some(key) = msg.subject.strip_prefix(prefix.as_str()).filter(|_| !deleted) {
                    entries.push((key.to_owned(), msg.payload.to_vec()));
                }
            }
        }
    }

    /// Store `tip` under its key, which must hold nothing yet.
    pub async fn leave(&self, tip: &TrailTip) -> Result<()> {
        let key = tip.key();
        let value = serde_json::to_vec(tip)?;
        if !self.kv.create(&key, value).await.with_context(|| format!("leaving tip {key}"))? {
            bail!("tip {key} exists");
        }
        Ok(())
    }

    /// Store `vote` on `tip`, under the key of its voter's vote, replacing the voter's earlier vote on it.
    pub async fn vote(&self, tip: &TrailTip, vote: &TipVote) -> Result<()> {
        let key = tip.vote_key(&vote.voter);
        let value = serde_json::to_vec(vote)?;
        self.kv.put(&key, value).await.with_context(|| format!("voting on tip {}", tip.key()))
    }
}
