//! The teams as a human's client reads and writes them ([`crate::teams`]): the directory read with its revision by a
//! direct get naming no subject, past a message a stream relayed onto its key ([`Client::teams`]); a team created,
//! changed or deleted ([`Client::create_team`], [`Client::change_team`], [`Client::delete_team`]), each checked
//! against the humans and the ants of the bus, applied to the directory as read and stored by compare-and-set, read
//! and applied anew while other writers changed it meanwhile.

use std::collections::BTreeSet;

use ants_proto::{
    HUMANS_BUCKET, HumanEntry, Humans, IDENTITIES_BUCKET, TEAM_DIRECTORY_KEY, TEAMS_BUCKET, TeamDirectory, TeamRefusal,
    UserId, Uuid, subject,
};
use serde::{Deserialize, Serialize};

use super::Client;
use crate::error::Error;
use crate::requests::{self, Got, Position, REQUEST_WAIT};
use crate::teams::change::{self, Change, Create};
use crate::teams::{Directory, TeamError};
use crate::transport::TransportError;

/// How often a change reads the directory again when another writer changed it meanwhile.
const TRIES: usize = 10;

/// What a change of the teams did: a line each, the members it added and removed, and the directory as stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edited {
    pub lines: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<UserId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<UserId>,
    pub directory: Directory,
}

impl Client {
    /// The team directory as stored now, with its revision; an absent entry reads as an empty directory. Refused as
    /// [`TeamRefusal::NoDirectory`] on a bus keeping no [`TEAMS_BUCKET`], and as [`TeamRefusal::Unreadable`] when the
    /// directory breaks the rules.
    pub async fn teams(&self) -> Result<Directory, TeamError> {
        const WHAT: &str = "reading the team directory";
        let get = requests::kv_last(self.domain(), TEAMS_BUCKET, TEAM_DIRECTORY_KEY);
        let answer = match self.transport.request(get, REQUEST_WAIT).await {
            Ok(answer) => answer,
            Err(TransportError::NoResponders) => return Err(TeamRefusal::NoDirectory.into()),
            Err(e) => return Err(Error::transport(WHAT, e).into()),
        };
        let (value, revision) = match requests::kv_entry_or_relayed(WHAT, answer)? {
            Ok(entry) => (entry.value, entry.revision),
            // The revision stays the key's last message's, which a write expects.
            Err(revision) => (self.last_put(WHAT, TEAMS_BUCKET, TEAM_DIRECTORY_KEY).await?, revision),
        };
        let directory = match value {
            Some(bytes) => TeamDirectory::read(&bytes)?,
            None => TeamDirectory::default(),
        };
        Ok(Directory { directory, revision })
    }

    /// The value its writer last put under `key` of `bucket`, past the messages a stream relayed onto it, read by
    /// batched direct gets naming no subject; none when it has none or was deleted.
    async fn last_put(&self, what: &str, bucket: &str, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let reply = format!("{}.{}", ants_proto::inbox_prefix(&self.session.human), Uuid::now_v7().simple());
        let mut answers = self.subscribe(what, reply.clone()).await?;
        let (stream, filter) = (subject::kv_stream(bucket), subject::kv(bucket, key));
        let (mut at, mut last) = (Position::all(), None);
        loop {
            let get = requests::batch_get_of(self.domain(), &stream, &filter, &at, reply.clone());
            self.transport.publish(get).await.map_err(|e| Error::transport(what, e))?;
            let mut got = 0;
            let more = loop {
                let answer = answers.next(Some(REQUEST_WAIT)).await.map_err(|e| Error::transport(what, e))?;
                match requests::batch_answer(answer)? {
                    Got::Stored(stored) => {
                        got += 1;
                        at.after(stored.seq);
                        last = (!requests::kv_deleted(&stored)).then_some(stored.payload);
                    }
                    Got::Relayed(seq) => {
                        got += 1;
                        at.after(seq);
                    }
                    Got::End { pending } => break got > 0 && pending != Some(0),
                    Got::Nothing => break false,
                }
            };
            if !more {
                return Ok(last);
            }
        }
    }

    /// Which of `names` are humans of the bus, each read by a direct get of its entry: the client's own human always.
    async fn humans_among(&self, names: &[UserId]) -> Result<Humans, Error> {
        const WHAT: &str = "reading the humans";
        let me = &self.session.human;
        let mut humans = Humans::of([me.clone()]);
        for name in names.iter().filter(|name| *name != me).collect::<BTreeSet<_>>() {
            let answer = match self
                .transport
                .request(requests::kv_get(self.domain(), HUMANS_BUCKET, name.as_str()), REQUEST_WAIT)
                .await
            {
                Ok(answer) => answer,
                Err(TransportError::NoResponders) => continue,
                Err(e) => return Err(Error::transport(WHAT, e)),
            };
            let Some(value) = requests::kv_entry(WHAT, answer)?.value else { continue };
            if let Ok(entry) = serde_json::from_slice::<HumanEntry>(&value) {
                let _ = humans.add(name.as_str(), entry);
            }
        }
        Ok(humans)
    }

    /// Which of `names` are ants of the bus: those with an identity record in [`IDENTITIES_BUCKET`], each read by a
    /// direct get naming no subject. None is on a bus keeping no such bucket.
    pub async fn known_ants(&self, names: Vec<UserId>) -> Result<BTreeSet<UserId>, Error> {
        const WHAT: &str = "reading the ants' identities";
        let mut known = BTreeSet::new();
        for name in names.into_iter().collect::<BTreeSet<_>>() {
            let get = requests::kv_last(self.domain(), IDENTITIES_BUCKET, name.as_str());
            let answer = match self.transport.request(get, REQUEST_WAIT).await {
                Ok(answer) => answer,
                Err(TransportError::NoResponders) => return Ok(known),
                Err(e) => return Err(Error::transport(WHAT, e)),
            };
            if requests::kv_entry(WHAT, answer)?.value.is_some() {
                known.insert(name);
            }
        }
        Ok(known)
    }

    /// Create the team of `create` as the client's human, refused as `ants team create` refuses it: a human as its
    /// lead or a member, an owner who is no human, a name a team, an ant or a human holds or one reserved, a member
    /// neither an ant of the bus nor a team, or one on a team already.
    pub async fn create_team(&self, create: Create) -> Result<Edited, TeamError> {
        let humans = self.humans_among(&create.names()).await?;
        let created = create.check(&self.session.human, &humans)?;
        let known = self.known_ants(create.names()).await?;
        change::check_name(&created.team, &humans, &known)?;
        let (lines, directory) = self.modify(|directory| change::create(directory, &created, &known)).await?;
        Ok(Edited { lines, added: created.members.clone(), removed: Vec::new(), directory })
    }

    /// Write `change` to `team`, refused as `ants team set`, `add` and `remove` refuse it: nothing to change, a human
    /// to add, an owner who is no human, a member to add neither an ant of the bus nor a team, or on a team already,
    /// a lead that is no direct ant member of the team.
    pub async fn change_team(&self, team: UserId, change: Change) -> Result<Edited, TeamError> {
        let humans = self.humans_among(&change.names()).await?;
        change.check(&team, &humans)?;
        let known = self.known_ants(change.add.clone()).await?;
        let (changed, directory) = self.modify(|directory| change.apply(directory, &team, &known)).await?;
        let lines = vec![change.line(&team, &changed)];
        Ok(Edited { lines, added: changed.added, removed: changed.removed, directory })
    }

    /// Delete `team`: it leaves the team holding it, and its members are on no team.
    pub async fn delete_team(&self, team: UserId) -> Result<Edited, TeamError> {
        let (lines, directory) = self.modify(|directory| directory.delete(&team)).await?;
        Ok(Edited { lines, directory, ..Edited::default() })
    }

    /// Change the directory with `change` and store it by compare-and-set, reading it again and changing it anew while
    /// another writer changed it meanwhile: what `change` returned, with the directory as stored, nothing stored when
    /// `change` changed nothing; [`TeamError::Conflict`] once it kept changing. A refusal of `change` stores nothing.
    async fn modify<T>(
        &self,
        mut change: impl FnMut(&mut TeamDirectory) -> Result<T, TeamRefusal>,
    ) -> Result<(T, Directory), TeamError> {
        const WHAT: &str = "writing the team directory";
        for _ in 0..TRIES {
            let Directory { mut directory, revision } = self.teams().await?;
            let before = directory.clone();
            let made = change(&mut directory)?;
            if directory == before {
                return Ok((made, Directory { directory, revision }));
            }
            directory.check()?;
            let value = serde_json::to_vec(&directory).map_err(|e| Error::invalid(WHAT, e))?;
            let put = requests::kv_update(self.domain(), TEAMS_BUCKET, TEAM_DIRECTORY_KEY, value, revision);
            if let Some(revision) = requests::kv_updated(WHAT, &self.request(WHAT, put).await?)? {
                return Ok((made, Directory { directory, revision }));
            }
        }
        Err(TeamError::Conflict)
    }
}
