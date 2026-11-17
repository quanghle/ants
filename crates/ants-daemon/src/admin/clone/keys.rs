//! The clone's key, signed by `ants signer`, and its credentials.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use nkeys::KeyPair;

use super::Cloning;
use super::plan::Plan;
use super::undo::Done;
use crate::admin::LeafNode;
use crate::admin::add::{Renewal, key_for, sign, write_secret};
use crate::admin::ask::Uplink;
use crate::secrets::creds;

/// The clone's key, signed by `ants signer`.
pub(super) struct Signed<'a> {
    /// Where the key is kept until the clone is listed.
    pub(super) seed_path: PathBuf,
    pub(super) key: KeyPair,
    pub(super) public_key: String,
    pub(super) jwt: String,
    pub(super) renewal: Option<Renewal<'a>>,
}

impl Cloning<'_> {
    /// Have `ants signer` sign the clone's key, kept beside the host credentials, and write its credentials there,
    /// renewing the uplink of `leaf`, if given, to carry it.
    pub(super) async fn sign<'l>(
        &self,
        leaf: Option<(&'l LeafNode, Uplink)>,
        plan: &Plan,
        done: &mut Done,
    ) -> Result<Signed<'l>> {
        let new = self.new;
        let seed_path = plan.dir.join(format!("{new}.nk"));
        done.files.push(seed_path.clone());
        let key = key_for(&plan.creds_path, &seed_path, new)?;
        let public_key = key.public_key();
        let (jwt, renewal) =
            sign(self.js, self.host_creds, leaf, &self.orders.accounts, self.order, new, &public_key).await?;
        let seed = key.seed().map_err(|e| anyhow!("the seed of {public_key}: {e}"))?;
        done.files.push(plan.creds_path.clone());
        write_secret(&plan.creds_path, &creds::text(&jwt, &seed))?;
        Ok(Signed { seed_path, key, public_key, jwt, renewal })
    }
}
