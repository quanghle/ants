//! The tips ants leave on saved trails, and their votes, in [`TRAIL_TIPS_BUCKET`]: each ant writes its own tips and its
//! own votes, and reads every trail's by direct gets naming no subject.

use ants_proto::{JsDomain, TRAIL_TIPS_BUCKET, TipBoard, TipVote, TrailName, TrailTip};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;

use crate::records::direct_kv::DirectKv;

/// The tips of the colony's trails. Cheap to clone.
#[derive(Clone)]
pub struct Tips {
    kv: DirectKv,
}

impl Tips {
    /// The tips in [`TRAIL_TIPS_BUCKET`], without a request: whether the bucket exists shows on the first read or
    /// write. `js_domain` is the domain `js` was made for ([`crate::bus::jetstream`]).
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<Self> {
        Ok(Self { kv: DirectKv::whole(js, js_domain, TRAIL_TIPS_BUCKET).await?.past_relays() })
    }

    /// Every tip left on `trail` and every vote on them, as last put: deleted entries are left out.
    pub async fn board(&self, trail: &TrailName) -> Result<TipBoard> {
        let mut entries = Vec::new();
        self.kv
            .scan(&format!("{trail}.>"), |key, value| entries.push((key.to_owned(), value.to_vec())))
            .await
            .with_context(|| format!("reading the tips of {trail}"))?;
        Ok(TipBoard::new(trail.clone(), entries))
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

impl super::Trails {
    /// The script of the trail saved as `name`, which a run of it would take: an error when none is saved so.
    pub async fn saved_script(&self, name: &TrailName) -> Result<String, String> {
        self.inner.saved.script(name).await
    }
}
