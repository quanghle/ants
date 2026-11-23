//! An ant's sense data taken to another bus ([`SenseExport`]): its event log, as stored, and its skill files, under a
//! manifest of their SHA-256 hashes. An import ([`import`]) takes it in under the same sense id once the hashes and the
//! log's chain verify: each event at its position, chained to the one before and signed by credentials of the ant
//! that the old bus's account ([`Manifest::accounts`]) signed, valid when the event was made. Revocations on the old
//! bus are not seen from the new one. The events are appended as they are, so their hashes and signatures stay
//! whole, then an [`Anchor`] ([`IMPORTED`]) signed on the new bus vouches for them there ([`crate::log::Chain`]).
//!
//! The export itself is not signed: its checks find a file damaged or edited since the export, not a log made anew
//! with other credentials of the account.

use std::collections::HashSet;
use std::io;
use std::path::{Component, Path, PathBuf};

use ants_proto::{SenseId, UserId};
use data_encoding::BASE64;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::home::Home;
use crate::log::{
    AccountVerifier, Anchor, AntLog, Chain, Event, EventLog, IMPORTED, LogError, Mark, NoRevocations, Position, Signer,
    Stored, Verdict, Verifier, sha256,
};

/// The version of [`Manifest`] written and read.
pub const VERSION: u32 = 1;
/// The export's file holding the log: each message as stored, one a line.
pub const LOG_FILE: &str = "log.jsonl";
/// The directory of the export holding the ant's skill files.
pub const SKILLS_DIR: &str = "skills";

/// sense's part of an ant's export: what [`ants_proto::provider::Export::data`] holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SenseExport {
    pub manifest: Manifest,
    pub files: Vec<File>,
}

/// What a [`SenseExport`] holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub sense: SenseId,
    /// The ant exported, by its name then.
    pub ant: UserId,
    /// The events of the log.
    pub events: u64,
    /// The log's last event; none for an empty log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip: Option<Mark>,
    /// The keys of the team accounts whose ants' credentials signed the log's events (`A...`): the old bus's, and those
    /// of the buses before it, as the log's anchors tell; none for a server without authentication.
    pub accounts: Vec<String>,
    /// Every file of the export: [`LOG_FILE`], then the skill files.
    pub files: Vec<Entry>,
}

/// A file of a [`SenseExport`], by its path, as the manifest lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

/// A file of a [`SenseExport`]: [`LOG_FILE`], or a skill file under [`SKILLS_DIR`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File {
    pub path: String,
    /// Its bytes, base64.
    pub data: String,
}

impl File {
    pub fn new(path: String, bytes: &[u8]) -> Self {
        Self { path, data: BASE64.encode(bytes) }
    }
}

/// Why an import took nothing in.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// The export does not verify.
    #[error("the export does not verify: {0}")]
    Invalid(String),
    /// The log here holds other events.
    #[error("{0}")]
    Refused(String),
    #[error("the log: {0}")]
    Log(#[from] LogError),
}

/// What an import took in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Imported {
    /// The events appended; none when the log held them all already.
    pub appended: u64,
    /// The skill files, by their path under the skills directory, for [`Imported::write_skills`].
    pub skills: Vec<(PathBuf, Vec<u8>)>,
}

impl Imported {
    /// Write the skill files into `home`'s skills directory ([`Home::skills`]).
    pub fn write_skills(&self, home: &Path) -> io::Result<()> {
        let dir = Home::new(home).skills();
        for (path, bytes) in &self.skills {
            let file = dir.join(path);
            std::fs::create_dir_all(file.parent().expect("a file in a directory"))?;
            std::fs::write(file, bytes)?;
        }
        Ok(())
    }
}

/// Mark `home`, restored from an export, as awaiting the import of the ant's sense data ([`Home::awaits_import`]):
/// until it lands ([`import_done`]), the daemon opens none of them, so nothing is written to the ant's log on the new
/// bus first.
pub fn expect_import(home: &Path) -> io::Result<()> {
    let file = Home::new(home).importing();
    std::fs::create_dir_all(file.parent().expect("a file in a directory"))?;
    std::fs::write(file, b"")
}

/// The import of the ant's sense data into `home` landed: its mark goes.
pub fn import_done(home: &Path) -> io::Result<()> {
    match std::fs::remove_file(Home::new(home).importing()) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// The skill files kept in `home`, by their path under its skills directory ([`Home::skills`]), `/`-separated: its
/// regular files, symbolic links not followed.
pub fn skill_files(home: &Path) -> io::Result<Vec<(String, Vec<u8>)>> {
    let root = Home::new(home).skills();
    let mut files = Vec::new();
    let mut dirs = vec![root.clone()];
    while let Some(dir) = dirs.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                dirs.push(entry.path());
            } else if kind.is_file() {
                let path = entry.path();
                let relative = path.strip_prefix(&root).expect("under the skills directory");
                let name =
                    relative.to_str().ok_or_else(|| io::Error::other(format!("{} is not UTF-8", path.display())))?;
                files.push((name.replace(std::path::MAIN_SEPARATOR, "/"), std::fs::read(&path)?));
            }
        }
    }
    files.sort();
    Ok(files)
}

/// The export of the log of `sense`, whose `messages` the store holds, all of them in order and verified, of `ant` on
/// a bus of `accounts`, with the ant's skill files, by their path under [`SKILLS_DIR`].
pub fn pack(
    sense: SenseId,
    ant: UserId,
    messages: &[Stored],
    accounts: &[String],
    skills: Vec<(String, Vec<u8>)>,
) -> Result<SenseExport, LogError> {
    let mut log = Vec::new();
    let mut signers: Vec<String> = accounts.to_vec();
    for stored in messages {
        if stored.payload.contains(&b'\n') {
            return Err(LogError::integrity(format!("message {} holds a line break", stored.sequence)));
        }
        log.extend_from_slice(&stored.payload);
        log.push(b'\n');
        if let Some(anchor) = stored.event().ok().filter(|event| event.kind == IMPORTED).and_then(anchor_of) {
            signers.extend(anchor.accounts);
        }
    }
    // Over a server without authentication events are unsigned: none can be checked against any account.
    if accounts.is_empty() {
        signers.clear();
    }
    signers.sort();
    signers.dedup();
    let tip = messages.last().map(|stored| Mark { index: messages.len() as u64, sha256: stored.sha256() });
    let mut files = vec![File::new(LOG_FILE.to_owned(), &log)];
    let mut entries = vec![entry(LOG_FILE, &log)];
    for (path, bytes) in skills {
        let path = format!("{SKILLS_DIR}/{path}");
        entries.push(entry(&path, &bytes));
        files.push(File::new(path, &bytes));
    }
    let manifest = Manifest {
        version: VERSION,
        sense,
        ant,
        events: messages.len() as u64,
        tip,
        accounts: signers,
        files: entries,
    };
    Ok(SenseExport { manifest, files })
}

fn entry(path: &str, bytes: &[u8]) -> Entry {
    Entry { path: path.to_owned(), sha256: sha256(bytes), size: bytes.len() as u64 }
}

fn anchor_of(event: Event) -> Option<Anchor> {
    serde_json::from_value(event.data).ok()
}

/// Take `export` in as the log `log` reads, not read yet, once it verifies for the log's ant (`verify`): the log
/// must be empty or hold the export's first events as they are, as after an import cut short or on a move back to a
/// bus the ant left. The events it does not hold are appended as they are, then an [`Anchor`] signed by the log's
/// signer, after a rename when the ant is imported under another name. Nothing is merged: a log holding other events
/// refuses, a log holding all of the export's takes nothing more.
pub async fn import<L: EventLog, S: Signer, V: Verifier>(
    log: &mut AntLog<L, S, V>,
    export: &SenseExport,
) -> Result<Imported, ImportError> {
    let verified = verify(export, log.chain()).await?;
    let sense = log.chain().sense().clone();
    let mut held: Vec<Stored> = Vec::new();
    loop {
        let after = held.last().map_or(0, |stored| stored.sequence);
        let read = log.log().read_after(&sense, after, 256).await?;
        if read.is_empty() {
            break;
        }
        held.extend(read);
    }
    let lines = &verified.lines;
    if let Some(index) = held.iter().zip(lines).position(|(stored, line)| stored.payload != *line) {
        return Err(ImportError::Refused(format!(
            "the log of sense identity {sense} here holds other events, from event {}: nothing imported",
            index + 1
        )));
    }
    let skills = verified.skills;
    if held.len() > lines.len() || lines.is_empty() {
        return Ok(Imported { appended: 0, skills });
    }
    let mut last = held.last().map_or(0, |stored| stored.sequence);
    for line in &lines[held.len()..] {
        last = log.log().append(&sense, line.clone(), last).await.map_err(|e| {
            ImportError::Refused(format!("appending to the log of sense identity {sense}: {e}; import again to go on"))
        })?;
    }
    let tip = export.manifest.tip.clone().expect("a tip, as the log is not empty");
    let bytes = lines.iter().map(|line| line.len() as u64).sum();
    let events = lines.len() as u64;
    let position = Position { sequence: last, tip: tip.clone(), events, bytes, broken: None, ant: verified.ant };
    log.resume(Some(position));
    let anchor = Anchor { accounts: export.manifest.accounts.clone(), through: tip };
    log.append(IMPORTED, json!(anchor)).await?;
    Ok(Imported { appended: (lines.len() - held.len()) as u64, skills })
}

/// An export that verified.
struct Verified {
    /// The log's messages.
    lines: Vec<Vec<u8>>,
    /// The name of the ant the log's last event is of.
    ant: Option<UserId>,
    skills: Vec<(PathBuf, Vec<u8>)>,
}

/// Check `export` for the log `chain` reads: its files and their hashes against its manifest, and its log against the
/// manifest's accounts, as a log of `chain`'s sense identity and ant from its first event.
async fn verify(export: &SenseExport, chain: &Chain) -> Result<Verified, ImportError> {
    let invalid = |reason: String| ImportError::Invalid(reason);
    let manifest = &export.manifest;
    if manifest.version != VERSION {
        return Err(invalid(format!("version {}, where {VERSION} is read", manifest.version)));
    }
    if manifest.sense != *chain.sense() {
        return Err(invalid(format!("of sense identity {}, not {}", manifest.sense, chain.sense())));
    }
    let mut paths = HashSet::new();
    for file in &export.files {
        if !paths.insert(file.path.as_str()) {
            return Err(invalid(format!("{} twice", file.path)));
        }
        if !manifest.files.iter().any(|entry| entry.path == file.path) {
            return Err(invalid(format!("{}, which the manifest does not list", file.path)));
        }
    }
    let (mut log, mut skills, mut listed) = (None, Vec::new(), HashSet::new());
    for entry in &manifest.files {
        if !listed.insert(entry.path.as_str()) {
            return Err(invalid(format!("{} listed twice", entry.path)));
        }
        let file = export.files.iter().find(|file| file.path == entry.path);
        let file = file.ok_or_else(|| invalid(format!("no {}, which the manifest lists", entry.path)))?;
        let bytes = BASE64.decode(file.data.as_bytes()).map_err(|e| invalid(format!("{}: {e}", entry.path)))?;
        if bytes.len() as u64 != entry.size || sha256(&bytes) != entry.sha256 {
            return Err(invalid(format!("{} is not the file the manifest lists: hash or size differs", entry.path)));
        }
        if entry.path == LOG_FILE {
            log = Some(bytes);
        } else {
            skills.push((skill_path(&entry.path).ok_or_else(|| invalid(format!("a file at {}", entry.path)))?, bytes));
        }
    }
    let log = log.ok_or_else(|| invalid(format!("no {LOG_FILE}")))?;
    if !log.is_empty() && log.last() != Some(&b'\n') {
        return Err(invalid(format!("{LOG_FILE} ends within a line")));
    }
    let lines: Vec<Vec<u8>> = log.split(|byte| *byte == b'\n').map(<[u8]>::to_vec).collect();
    let lines = lines[..lines.len() - 1].to_vec();
    if lines.len() as u64 != manifest.events {
        return Err(invalid(format!("{} events, where the manifest tells {}", lines.len(), manifest.events)));
    }
    let mut checking =
        Chain::new(chain.sense().clone(), chain.ant().clone(), None).with_earlier_names(chain.earlier().to_vec());
    checking = checking.without_anchors();
    let verifier = AccountVerifier::new(manifest.accounts.clone(), NoRevocations);
    for (sequence, line) in (1..).zip(&lines) {
        let made = serde_json::from_slice::<Event>(line).map_or_else(|_| chrono::Utc::now(), |event| event.at);
        let stored = Stored { sequence, stored_at: made, payload: line.clone() };
        let verdict = checking.check(&stored, &verifier).await.map_err(|e| invalid(e.to_string()))?;
        if verdict == Verdict::Broken
            && let Some(broken) = checking.broken()
        {
            return Err(invalid(format!("event {}: {}", broken.index, broken.reason)));
        }
    }
    if checking.tip() != manifest.tip.as_ref() {
        return Err(invalid("its log does not end at the event the manifest tells".into()));
    }
    Ok(Verified { lines, ant: checking.named().cloned(), skills })
}

/// The path under [`SKILLS_DIR`] of the export's file at `path`; none when it lies elsewhere.
fn skill_path(path: &str) -> Option<PathBuf> {
    let relative = Path::new(path).strip_prefix(SKILLS_DIR).ok()?;
    let normal = relative.components().all(|component| matches!(component, Component::Normal(_)));
    (normal && relative.components().next().is_some()).then(|| relative.to_owned())
}

#[cfg(test)]
mod tests;
