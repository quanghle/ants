//! `ants init`: create what the hub lacks, as [`crate::init_hub`] does, and say what was done.

use anyhow::Result;

use crate::{Nats, init_hub};

pub(crate) async fn run(nats: &Nats<'_>, identities_restored: bool) -> Result<()> {
    for line in init_hub(nats, identities_restored).await? {
        println!("{line}");
    }
    Ok(())
}
