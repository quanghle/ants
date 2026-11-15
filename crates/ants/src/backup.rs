//! `ants auth backup` and `ants auth restore`: the team keys in one file encrypted with a passphrase ([`age`]), and
//! the record of the last backup, which `ants doctor` compares the team directory with ([`state`]).
//!
//! The team keys (the operator, account and system account seeds) live in files of the team directory, never in the
//! OS credential store, which holds only user seeds ([`crate::keys`]): a backup takes them from there, with
//! `server.conf`, `issued.jsonl` and `revoked.json`, and a restore writes them back there. Credentials files
//! (`.creds`) stay out: `ants auth` issues them again.

use std::collections::BTreeMap;
use std::fs;
use std::io::{IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow, bail};
use nkeys::KeyPair;
use serde::{Deserialize, Serialize};

use crate::age;
use crate::auth::{self, ACCOUNT_SEED, OPERATOR_SEED, SERVER_CONF, SYSTEM_SEED};
use crate::revoke::{ISSUED, REVOKED};

/// The record of the last backup, in the team directory.
pub const RECORD: &str = "backup.json";
/// The environment variable a passphrase may come from, for scripts.
pub const PASSPHRASE_ENV: &str = "ANTS_BACKUP_PASSPHRASE";
const FORMAT: u32 = 1;
/// The seeds of the team, by file, with the first letter of their public keys.
const SEEDS: [(&str, char); 3] = [(OPERATOR_SEED, 'O'), (ACCOUNT_SEED, 'A'), (SYSTEM_SEED, 'A')];
/// The other files of the team directory a backup holds; the last two only once there.
const FILES: [&str; 3] = [SERVER_CONF, ISSUED, REVOKED];

/// What a backup file holds, encrypted.
#[derive(Serialize, Deserialize)]
struct Contents {
    ants_backup: u32,
    /// Seconds since the epoch.
    created_at: u64,
    /// The public key of the team's account.
    account: String,
    /// The text of each file, by name.
    files: BTreeMap<String, String>,
}

/// The last backup of a team directory, or the backup it was restored from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// When the backup was made, seconds since the epoch.
    pub at: u64,
    pub file: PathBuf,
    /// The public key of the team's account.
    pub account: String,
    /// The entries of `issued.jsonl` it holds.
    pub issued: usize,
    /// The SHA-256 of the `revoked.json` it holds, in hex; empty without one.
    pub revoked: String,
}

/// How the team directory stands against its last backup.
#[derive(Debug, PartialEq, Eq)]
pub enum State {
    /// No backup of these team keys was made, or none recorded.
    None,
    /// The last backup holds the team directory as it is.
    Current(Record),
    /// Keys were issued or revoked since the last backup.
    Behind {
        record: Record,
        /// Entries of `issued.jsonl` since; negative when it lost some.
        issued: isize,
        revoked: bool,
    },
}

/// `ants auth backup FILE`, with the passphrase from `passphrase_file`, [`PASSPHRASE_ENV`] or the terminal.
pub fn run_backup(dir: &Path, file: &Path, passphrase_file: Option<&Path>) -> Result<()> {
    if !auth::holds_team(dir) {
        bail!("no team keys in {}: back them up on the machine holding them, or give --dir", dir.display());
    }
    let passphrase = passphrase(passphrase_file, true)?;
    let record = backup(dir, file, &passphrase, age::WORK_FACTOR)?;
    println!(
        "team keys of {} ({} issued.jsonl entries) backed up to {}: keep it off this machine, and the passphrase \
         apart from it; `ants auth restore FILE` brings them back",
        dir.display(),
        record.issued,
        record.file.display()
    );
    Ok(())
}

/// `ants auth restore FILE`, with the passphrase as for [`run_backup`].
pub fn run_restore(dir: &Path, file: &Path, passphrase_file: Option<&Path>, force: bool) -> Result<()> {
    if !force {
        refuse_overwrite(dir)?;
    }
    let passphrase = passphrase(passphrase_file, false)?;
    let record = restore(dir, file, &passphrase, force)?;
    println!(
        "team keys of account {} restored into {}, as backed up {} ({} issued.jsonl entries); keys issued and \
         revocations made since are not in it: revoke any such key again, and push the account with `ants auth push`",
        record.account,
        dir.display(),
        ants_daemon::creds::time(record.at),
        record.issued
    );
    Ok(())
}

/// Back up the team keys of `dir` into `out`, encrypted with `passphrase` at scrypt's `work_factor` and readable by
/// its owner only, and record the backup in `dir`.
pub fn backup(dir: &Path, out: &Path, passphrase: &str, work_factor: u8) -> Result<Record> {
    check_passphrase(passphrase)?;
    let account = auth::account_key(dir)?;
    let mut files = BTreeMap::new();
    for (i, name) in SEEDS.iter().map(|(name, _)| *name).chain(FILES).enumerate() {
        let path = dir.join(name);
        match fs::read_to_string(&path) {
            Ok(text) => {
                files.insert(name.to_owned(), text);
            }
            // Only issued.jsonl and revoked.json may be missing: no keys issued or revoked yet.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && i > SEEDS.len() => {}
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }
    let contents = Contents { ants_backup: FORMAT, created_at: auth::now()?, account, files };
    let encrypted = age::encrypt(&serde_json::to_vec(&contents)?, passphrase.as_bytes(), work_factor)?;
    auth::replace_secret(out, &encrypted)?;
    let out = std::path::absolute(out).unwrap_or_else(|_| out.to_owned());
    let record = Record::of(&contents, out);
    write_record(dir, &record)?;
    Ok(record)
}

/// Restore the team keys of the backup `file`, encrypted with `passphrase`, into `dir`, over the team files there
/// with `force` (removing those the backup has none of), else only into a directory without any; the backup then
/// counts as the last one of `dir`.
pub fn restore(dir: &Path, file: &Path, passphrase: &str, force: bool) -> Result<Record> {
    let encrypted = fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    let plaintext =
        age::decrypt(&encrypted, passphrase.as_bytes()).with_context(|| format!("decrypting {}", file.display()))?;
    let contents: Contents = serde_json::from_slice(&plaintext)
        .with_context(|| format!("{} is not a backup of `ants auth backup`", file.display()))?;
    if contents.ants_backup > FORMAT {
        bail!("{} is a backup of a newer ants (format {}): upgrade ants", file.display(), contents.ants_backup);
    }
    check_contents(&contents).with_context(|| format!("{} holds no team keys to restore", file.display()))?;
    if !force {
        refuse_overwrite(dir)?;
    }
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    for name in FILES.into_iter().chain(SEEDS.iter().map(|(name, _)| *name)) {
        let path = dir.join(name);
        match contents.files.get(name) {
            Some(text) if name.ends_with(".nk") => auth::replace_secret(&path, text)?,
            Some(text) => replace(&path, text)?,
            None => match fs::remove_file(&path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    return Err(e).with_context(|| format!("removing {}", path.display()));
                }
                _ => {}
            },
        }
    }
    let file = std::path::absolute(file).unwrap_or_else(|_| file.to_owned());
    let record = Record::of(&contents, file);
    write_record(dir, &record)?;
    Ok(record)
}

/// How the team directory `dir` stands against its last backup.
pub fn state(dir: &Path) -> Result<State> {
    let path = dir.join(RECORD);
    let record: Record = match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("reading {}", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(State::None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    if record.account != auth::account_key(dir)? {
        return Ok(State::None);
    }
    let issued = entries(read_optional(&dir.join(ISSUED))?.as_deref());
    let revoked = digest(read_optional(&dir.join(REVOKED))?.as_deref());
    if issued == record.issued && revoked == record.revoked {
        return Ok(State::Current(record));
    }
    let since = issued as isize - record.issued as isize;
    let revoked = revoked != record.revoked;
    Ok(State::Behind { record, issued: since, revoked })
}

impl Record {
    fn of(contents: &Contents, file: PathBuf) -> Self {
        let text = |name: &str| contents.files.get(name).map(String::as_str);
        Self {
            at: contents.created_at,
            file,
            account: contents.account.clone(),
            issued: entries(text(ISSUED)),
            revoked: digest(text(REVOKED)),
        }
    }
}

fn write_record(dir: &Path, record: &Record) -> Result<()> {
    let mut text = serde_json::to_string_pretty(record)?;
    text.push('\n');
    replace(&dir.join(RECORD), &text)
}

/// Replace the file at `path` with `text` at once: written beside it, then renamed over it.
fn replace(path: &Path, text: &str) -> Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(format!(".{}.tmp", std::process::id()));
    let temporary = PathBuf::from(temporary);
    let written = fs::write(&temporary, text)
        .with_context(|| format!("writing {}", temporary.display()))
        .and_then(|()| fs::rename(&temporary, path).with_context(|| format!("replacing {}", path.display())));
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    written
}

fn read_optional(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn entries(issued: Option<&str>) -> usize {
    issued.map_or(0, |text| text.lines().filter(|line| !line.trim().is_empty()).count())
}

fn digest(text: Option<&str>) -> String {
    text.map_or_else(String::new, |text| {
        data_encoding::HEXLOWER.encode(ring::digest::digest(&ring::digest::SHA256, text.as_bytes()).as_ref())
    })
}

/// Fail unless `contents` holds the seeds of an operator, the account it names and a system account, and the
/// server.conf, and only files of a team directory.
fn check_contents(contents: &Contents) -> Result<()> {
    for name in contents.files.keys() {
        if !SEEDS.iter().any(|(seed, _)| seed == name) && !FILES.contains(&name.as_str()) {
            bail!("an unknown file {name:?}");
        }
    }
    for (name, prefix) in SEEDS {
        let seed = contents.files.get(name).with_context(|| format!("no {name}"))?;
        let key = KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{name}: {e}"))?.public_key();
        if !key.starts_with(prefix) || (name == ACCOUNT_SEED && key != contents.account) {
            bail!("{name} holds the key {key}, not that of the team's {}", name.trim_end_matches(".nk"));
        }
    }
    contents.files.get(SERVER_CONF).with_context(|| format!("no {SERVER_CONF}"))?;
    Ok(())
}

/// Fail when `dir` holds any team file a restore would replace.
fn refuse_overwrite(dir: &Path) -> Result<()> {
    let there: Vec<&str> =
        SEEDS.iter().map(|(name, _)| *name).chain(FILES).filter(|name| dir.join(name).exists()).collect();
    if !there.is_empty() {
        bail!(
            "{} already holds team files ({}): restore over them with --force, after backing them up if they may be \
             needed",
            dir.display(),
            there.join(", ")
        );
    }
    Ok(())
}

fn check_passphrase(passphrase: &str) -> Result<()> {
    if passphrase.trim().is_empty() {
        bail!("an empty passphrase protects nothing: give one");
    }
    Ok(())
}

/// The passphrase: the first line of `file`, else [`PASSPHRASE_ENV`], else read from the terminal without echo,
/// twice to `confirm` it.
fn passphrase(file: Option<&Path>, confirm: bool) -> Result<String> {
    let passphrase = if let Some(file) = file {
        let text = fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
        text.lines().next().unwrap_or_default().to_owned()
    } else if let Some(passphrase) = std::env::var_os(PASSPHRASE_ENV) {
        passphrase.into_string().map_err(|_| anyhow!("{PASSPHRASE_ENV} is not valid UTF-8"))?
    } else {
        let passphrase = prompt("passphrase")?;
        check_passphrase(&passphrase)?;
        if confirm && prompt("passphrase again")? != passphrase {
            bail!("the passphrases differ");
        }
        passphrase
    };
    check_passphrase(&passphrase)?;
    Ok(passphrase)
}

/// A line read from the terminal without echo, after `label` on stderr.
fn prompt(label: &str) -> Result<String> {
    use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
    use ratatui::crossterm::terminal;

    if !std::io::stdin().is_terminal() {
        bail!("no terminal to read the passphrase from: give --passphrase-file FILE or set {PASSPHRASE_ENV}");
    }
    eprint!("{label}: ");
    std::io::stderr().flush()?;
    terminal::enable_raw_mode().context("reading the passphrase from the terminal")?;
    let read = (|| -> std::io::Result<Option<String>> {
        let mut line = String::new();
        loop {
            match event::read()? {
                Event::Key(key) if key.kind != KeyEventKind::Release => match key.code {
                    KeyCode::Enter => return Ok(Some(line)),
                    KeyCode::Char('c' | 'd') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(None),
                    KeyCode::Esc => return Ok(None),
                    KeyCode::Backspace => {
                        line.pop();
                    }
                    KeyCode::Char(c) => line.push(c),
                    _ => {}
                },
                Event::Paste(text) => line.push_str(&text),
                _ => {}
            }
        }
    })();
    let _ = terminal::disable_raw_mode();
    eprintln!();
    match read {
        Ok(Some(line)) => Ok(line),
        Ok(None) => bail!("no passphrase given"),
        Err(e) => Err(anyhow::Error::from(e).context("reading the passphrase from the terminal")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::age::Refused;
    use crate::revoke::{self, Issued, Kind};

    const PASS: &str = "correct horse battery staple";
    /// A low work factor, for fast tests.
    const FAST: u8 = 10;

    fn team(root: &Path) -> PathBuf {
        let dir = root.join("auth");
        auth::init(&dir).unwrap();
        revoke::record(&dir, &Issued::new(&KeyPair::new_user().public_key(), "dev-a", Kind::Ant, None).unwrap())
            .unwrap();
        dir
    }

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        std::os::unix::fs::PermissionsExt::mode(&fs::metadata(path).unwrap().permissions()) & 0o777
    }

    #[cfg(not(unix))]
    fn mode(_: &Path) -> u32 {
        0o600
    }

    #[test]
    fn a_backup_restores_the_team_keys_elsewhere_only_with_its_passphrase() {
        let root = ants_test_support::scratch_dir("backup");
        let dir = team(&root);
        let file = root.join("team.age");
        let record = backup(&dir, &file, PASS, FAST).unwrap();
        assert_eq!((record.issued, mode(&file)), (1, 0o600));

        let elsewhere = root.join("restored");
        let e = restore(&elsewhere, &file, "wrong", false).unwrap_err();
        assert_eq!(e.downcast_ref::<Refused>(), Some(&Refused::WrongPassphrase), "{e:#}");
        assert!(!elsewhere.exists());
        restore(&elsewhere, &file, PASS, false).unwrap();
        for name in [OPERATOR_SEED, ACCOUNT_SEED, SYSTEM_SEED, SERVER_CONF, ISSUED] {
            assert_eq!(fs::read(elsewhere.join(name)).unwrap(), fs::read(dir.join(name)).unwrap(), "{name}");
        }
        assert_eq!(mode(&elsewhere.join(ACCOUNT_SEED)), 0o600);
        assert!(matches!(state(&elsewhere).unwrap(), State::Current(_)));

        let e = restore(&dir, &file, PASS, false).unwrap_err();
        assert!(format!("{e:#}").contains("--force"), "{e:#}");
        auth::init(&root.join("other")).unwrap();
        fs::copy(root.join("other").join(ACCOUNT_SEED), dir.join(ACCOUNT_SEED)).unwrap();
        restore(&dir, &file, PASS, true).unwrap();
        assert_eq!(fs::read(dir.join(ACCOUNT_SEED)).unwrap(), fs::read(elsewhere.join(ACCOUNT_SEED)).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_altered_backup_restores_nothing() {
        let root = ants_test_support::scratch_dir("backup-altered");
        let dir = team(&root);
        let file = root.join("team.age");
        backup(&dir, &file, PASS, FAST).unwrap();
        let mut lines: Vec<String> = fs::read_to_string(&file).unwrap().lines().map(str::to_owned).collect();
        let payload = lines.len() - 2;
        let line = &mut lines[payload];
        let flipped = if line.starts_with('A') { "B" } else { "A" };
        line.replace_range(..1, flipped);
        fs::write(&file, lines.join("\n")).unwrap();
        let elsewhere = root.join("restored");
        let e = restore(&elsewhere, &file, PASS, false).unwrap_err();
        assert_eq!(e.downcast_ref::<Refused>(), Some(&Refused::Damaged), "{e:#}");
        assert!(!elsewhere.exists());
        assert!(backup(&dir, &file, " ", FAST).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn the_state_tells_keys_issued_or_revoked_since_the_last_backup() {
        let root = ants_test_support::scratch_dir("backup-state");
        let dir = team(&root);
        assert_eq!(state(&dir).unwrap(), State::None);
        let record = backup(&dir, &root.join("team.age"), PASS, FAST).unwrap();
        assert_eq!(state(&dir).unwrap(), State::Current(record.clone()));
        revoke::record(&dir, &Issued::new(&KeyPair::new_user().public_key(), "dev-b", Kind::Ant, None).unwrap())
            .unwrap();
        assert_eq!(state(&dir).unwrap(), State::Behind { record: record.clone(), issued: 1, revoked: false });
        revoke::revoke(&dir, Some("dev-a"), &[]).unwrap();
        assert_eq!(state(&dir).unwrap(), State::Behind { record, issued: 1, revoked: true });
        fs::remove_file(dir.join(ACCOUNT_SEED)).unwrap();
        fs::remove_file(dir.join(SYSTEM_SEED)).unwrap();
        auth::init(&dir).unwrap();
        assert_eq!(state(&dir).unwrap(), State::None);
        fs::remove_dir_all(root).unwrap();
    }
}
