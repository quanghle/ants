//! An ant's sense log, read and written by the daemon serving the ant ([`AntLog`]).

use std::path::PathBuf;

use ants_proto::{Machine, SenseId, UserId};
use chrono::Utc;
use serde_json::{Value, json};

use super::{
    Anchor, AppendError, Chain, ChainStatus, Event, EventLog, IMPORTED, LogError, Position, RENAMED, Signer, Stored,
    Verdict, Verifier, VerifyError,
};

/// Messages read at once.
const READ_BATCH: usize = 256;
/// How often [`AntLog::append`] tries while other writers keep appending.
pub const APPEND_TRIES: usize = 10;

/// A message of the log and the event it holds, verified.
#[derive(Debug, Clone, PartialEq)]
pub struct Checked {
    pub stored: Stored,
    pub event: Event,
}

/// The log of sense identity `sense` of an ant, in `log`: read in order and verified with `verifier` ([`Chain`]), and
/// appended to with events signed by `signer` that chain to the last read. With a home, what it verified is kept there
/// ([`ChainStatus`]), and the furthest event verified before is expected to be found unchanged.
pub struct AntLog<L, S, V> {
    log: L,
    signer: S,
    verifier: V,
    machine: Machine,
    chain: Chain,
    home: Option<PathBuf>,
}

impl<L: EventLog, S: Signer, V: Verifier> AntLog<L, S, V> {
    /// The log of `sense`, the identity of `ant`, served on `machine`, its status kept in `home`, the ant's home, if
    /// any. Reads nothing yet: [`AntLog::catch_up`] does.
    pub fn new(
        log: L,
        signer: S,
        verifier: V,
        sense: SenseId,
        ant: UserId,
        machine: Machine,
        home: Option<PathBuf>,
    ) -> Result<Self, LogError> {
        let kept = match &home {
            Some(home) => super::read_status(home).map_err(|e| LogError(format!("reading the chain status: {e}")))?,
            None => None,
        };
        let seen = kept.filter(|status| status.sense == sense).and_then(|status| status.seen);
        let chain = Chain::new(sense, ant, seen);
        Ok(Self { log, signer, verifier, machine, chain, home })
    }

    /// The log of an ant once named `earlier` too, whose events under those names it reads as its own
    /// ([`Chain::with_earlier_names`]).
    #[must_use]
    pub fn with_earlier_names(self, earlier: Vec<UserId>) -> Self {
        Self { chain: self.chain.with_earlier_names(earlier), ..self }
    }

    pub fn log(&self) -> &L {
        &self.log
    }

    pub fn chain(&self) -> &Chain {
        &self.chain
    }

    pub fn status(&self) -> ChainStatus {
        ChainStatus::of(&self.chain, self.log.incarnation())
    }

    /// Read and verify the messages appended since the last read, up to the log's end, and keep the status: the events
    /// that verified, in order, those breaking the chain left out ([`Chain::broken`]).
    pub async fn catch_up(&mut self) -> Result<Vec<Checked>, LogError> {
        let mut checked = Vec::new();
        while let Some(read) = self.read_next().await? {
            checked.extend(read);
        }
        Ok(checked)
    }

    /// Read and verify the next messages since the last read, a batch at a time: the events that verified, in order,
    /// or none at the log's end, where the status is kept. On failing, the events of the batch verified before are
    /// lost: [`AntLog::resume`] reads them again.
    pub async fn read_next(&mut self) -> Result<Option<Vec<Checked>>, LogError> {
        let read = self.log.read_after(self.chain.sense(), self.chain.last_sequence(), READ_BATCH).await?;
        if read.is_empty() {
            self.chain.end();
            self.keep_status()?;
            return Ok(None);
        }
        let mut checked = Vec::new();
        for stored in read {
            loop {
                match self.chain.check(&stored, &self.verifier).await.map_err(verifying)? {
                    Verdict::Verified(event) => checked.push(Checked { stored, event }),
                    Verdict::Broken => {}
                    Verdict::Unanchored { index } => {
                        let anchor = self.find_anchor(index).await?;
                        self.chain.anchored(anchor);
                        continue;
                    }
                }
                break;
            }
        }
        Ok(Some(checked))
    }

    /// The first [`Anchor`] past the last read, signed for the bus by the ant, that vouches for the event at `index`.
    async fn find_anchor(&self, index: u64) -> Result<Option<Anchor>, LogError> {
        let sense = self.chain.sense();
        let mut after = self.chain.last_sequence();
        loop {
            let read = self.log.read_after(sense, after, READ_BATCH).await?;
            let Some(last) = read.last() else { return Ok(None) };
            after = last.sequence;
            for stored in &read {
                let Ok(event) = stored.event() else { continue };
                if event.kind != IMPORTED || event.sense != *sense || !self.chain.knows(&event.ant) {
                    continue;
                }
                let Ok(anchor) = serde_json::from_value::<Anchor>(event.data.clone()) else { continue };
                if anchor.through.index < index || anchor.through.index >= event.index {
                    continue;
                }
                match self.verifier.verify(&event, stored.stored_at).await {
                    Ok(()) => return Ok(Some(anchor)),
                    Err(VerifyError::Invalid(_)) => {}
                    Err(e @ VerifyError::Unavailable(_)) => return Err(verifying(e)),
                }
            }
        }
    }

    /// The messages of the log up to the last read, in order, as stored.
    pub async fn messages(&self) -> Result<Vec<Stored>, LogError> {
        let (sense, last) = (self.chain.sense(), self.chain.last_sequence());
        let (mut messages, mut after) = (Vec::new(), 0);
        while after < last {
            let read = self.log.read_after(sense, after, READ_BATCH).await?;
            let Some(read_last) = read.last() else {
                return Err(LogError(format!("the log ends before message {last}, read before")));
            };
            after = read_last.sequence;
            messages.extend(read.into_iter().filter(|stored| stored.sequence <= last));
        }
        Ok(messages)
    }

    /// Go on reading from `position`, as read before, or with none from the log's start ([`Chain::resume`]).
    pub fn resume(&mut self, position: Option<Position>) {
        self.chain.resume(position);
    }

    /// Append an event of `kind` holding `data`, after the last event of the log: on finding another writer appended
    /// since the last read, read what it appended and try again. Returns the event as stored, with the time it was
    /// stored as this writer saw it, and those read on the way.
    pub async fn append(&mut self, kind: &str, data: Value) -> Result<(Checked, Vec<Checked>), LogError> {
        let mut read = Vec::new();
        for _ in 0..APPEND_TRIES {
            match self.try_append(kind, data.clone()).await? {
                Ok(appended) => return Ok((appended, read)),
                Err(missed) => read.extend(missed),
            }
        }
        Err(self.kept_appending())
    }

    /// Append an event of `kind` holding `data`, after the last event of the log, once: on finding another writer
    /// appended since the last read, read what it appended and return it instead, for the caller to decide again. When
    /// the log's events are of an earlier name of the ant, it appends the ant's rename ([`RENAMED`]) instead and returns
    /// that as read, for the caller to decide again.
    pub async fn try_append(&mut self, kind: &str, data: Value) -> Result<Result<Checked, Vec<Checked>>, LogError> {
        if let Some(from) = self.chain.named().filter(|named| *named != self.chain.ant()) {
            let renamed = json!({ "from": from });
            return Ok(Err(match self.append_once(RENAMED, renamed).await? {
                Ok(renamed) => vec![renamed],
                Err(missed) => missed,
            }));
        }
        self.append_once(kind, data).await
    }

    async fn append_once(&mut self, kind: &str, data: Value) -> Result<Result<Checked, Vec<Checked>>, LogError> {
        let tip = self.chain.tip();
        let mut event = Event {
            sense: self.chain.sense().clone(),
            index: tip.map_or(1, |tip| tip.index + 1),
            prev: tip.map(|tip| tip.sha256.clone()),
            at: Utc::now(),
            ant: self.chain.ant().clone(),
            machine: self.machine.clone(),
            kind: kind.to_owned(),
            data,
            credentials: None,
            signature: None,
        };
        if let Some(signature) = self.signer.sign(&event.signed()).map_err(|e| LogError(format!("signing: {e}")))? {
            (event.credentials, event.signature) = (Some(signature.credentials), Some(signature.signature));
        }
        let payload = serde_json::to_vec(&event).map_err(|e| LogError(e.to_string()))?;
        match self.log.append(self.chain.sense(), payload.clone(), self.chain.last_sequence()).await {
            Ok(sequence) => {
                let stored = Stored { sequence, stored_at: Utc::now(), payload };
                self.chain.appended(&stored, &event);
                self.keep_status()?;
                Ok(Ok(Checked { stored, event }))
            }
            Err(AppendError::Conflict { .. }) => Ok(Err(self.catch_up().await?)),
            Err(AppendError::Failed(e)) => Err(e),
        }
    }

    fn kept_appending(&self) -> LogError {
        LogError(format!("other writers kept appending to sense identity {}", self.chain.sense()))
    }

    fn keep_status(&self) -> Result<(), LogError> {
        let Some(home) = &self.home else { return Ok(()) };
        super::write_status(home, &self.status()).map_err(|e| LogError(format!("keeping the chain status: {e}")))
    }
}

fn verifying(e: VerifyError) -> LogError {
    match e {
        VerifyError::Invalid(e) | VerifyError::Unavailable(e) => LogError(format!("verifying: {e}")),
    }
}
