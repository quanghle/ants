//! The team directory as daemons, `ants signer` and the CLI read and write it: the one entry of [`TEAMS_BUCKET`],
//! read by direct gets naming no subject past a message a stream relayed onto its key, and written by compare-and-set
//! on the stream sequence of the key's last message.

use ants_proto::{JsDomain, TEAM_DIRECTORY_KEY, TEAMS_BUCKET, TeamDirectory, TeamRefusal};
use anyhow::{Context as _, Result};
use async_nats::jetstream;

use crate::records::direct_kv::{DirectKv, Lookup};

pub mod keeper;

/// How often [`modify`] reads the directory again when another writer changed it meanwhile.
const TRIES: usize = 10;

/// The directory as read: what it holds, a directory breaking the rules read as [`TeamRefusal::Unreadable`], and the
/// stream sequence of its key's last message, which [`write`] expects (0 for none).
#[derive(Debug, Clone)]
pub struct Read {
    pub directory: Result<TeamDirectory, TeamRefusal>,
    pub last: u64,
}

impl Read {
    /// The directory, or why it cannot be read.
    pub fn directory(&self) -> Result<&TeamDirectory, TeamRefusal> {
        self.directory.as_ref().map_err(Clone::clone)
    }
}

/// The team directory of `js`, as credentials reading it by direct gets naming no subject reach it.
pub async fn open(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<DirectKv> {
    Ok(DirectKv::whole(js, js_domain, TEAMS_BUCKET).await?.past_relays())
}

/// Read the directory from `kv` ([`open`]): None when the bus keeps no [`TEAMS_BUCKET`]; an absent entry reads as an
/// empty directory. Fails only when the bucket is unreachable.
pub async fn read(kv: &DirectKv) -> Result<Option<Read>> {
    let (found, last) = kv.lookup_last(TEAM_DIRECTORY_KEY).await.context("reading the team directory")?;
    let directory = match found {
        Lookup::NoBucket => return Ok(None),
        Lookup::NoValue => Ok(TeamDirectory::default()),
        Lookup::Value(bytes) => TeamDirectory::read(&bytes),
    };
    Ok(Some(Read { directory, last }))
}

/// Read the directory from `kv`, refusing a bus without [`TEAMS_BUCKET`] or with a directory breaking the rules.
pub async fn read_checked(kv: &DirectKv) -> Result<(TeamDirectory, u64)> {
    let read = read(kv).await?.ok_or(TeamRefusal::NoDirectory)?;
    Ok((read.directory?, read.last))
}

/// Store `directory`, checked first, if the key's last message is still the one at stream sequence `last`: whether it
/// stored it, false when the directory changed meanwhile.
pub async fn write(kv: &DirectKv, directory: &TeamDirectory, last: u64) -> Result<bool> {
    directory.check()?;
    kv.update(TEAM_DIRECTORY_KEY, serde_json::to_vec(directory)?, last).await
}

/// Change the directory of `kv` with `change` and store it by compare-and-set, reading it again and changing it anew
/// while another writer changed it meanwhile: what `change` returned on the change stored, and nothing stored when
/// `change` changed nothing. A refusal of `change` stores nothing.
pub async fn modify<T>(
    kv: &DirectKv,
    mut change: impl FnMut(&mut TeamDirectory) -> Result<T, TeamRefusal>,
) -> Result<T> {
    for _ in 0..TRIES {
        let (mut directory, last) = read_checked(kv).await?;
        let before = directory.clone();
        let made = change(&mut directory)?;
        if directory == before || write(kv, &directory, last).await? {
            return Ok(made);
        }
    }
    Err(TeamRefusal::Busy.into())
}
