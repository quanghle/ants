//! `ants keys`: move the NKey seeds of credentials files into the OS credential store and back, or remove
//! credentials from the machine ([`crate::keys`]).

use std::path::PathBuf;

use ants_daemon::keystore;
use anyhow::{Result, bail};

use crate::connection::Connection;
use crate::keys::{self, Moved};

#[derive(clap::Subcommand, Debug)]
pub(crate) enum KeysCommand {
    /// Move the seed of each credentials file (default: --creds), and of a human device's encryption key, into the
    /// OS credential store: the macOS keychain, the Linux Secret Service or the Windows Credential Manager. The seed
    /// leaves the file only once stored, read back and used to authenticate at --nats; the file then keeps the user
    /// JWT and the stored keys' references. Leaf node uplink credentials, which nats-server reads, are refused.
    Store { files: Vec<PathBuf> },
    /// Move the seeds of each credentials file (default: --creds) back from the OS credential store into the file,
    /// readable by its owner only, then delete it from the store: for a machine without an unlocked store, or to
    /// back the seed up.
    Unstore { files: Vec<PathBuf> },
    /// Delete each credentials file and the seeds it keeps in the OS credential store. The credentials, and the live
    /// ones they were renewed from, stay valid until they expire: revoke them with `ants auth revoke --key KEY
    /// --earlier` on the machine holding the team keys.
    Remove { files: Vec<PathBuf> },
}

/// Run `ants keys` with the global `--nats` and `--creds`.
pub(crate) async fn run(conn: &Connection<'_>, command: KeysCommand) -> Result<()> {
    let (nats, cli_creds) = (conn.nats, conn.creds);
    let store = keystore::os();
    let files = |files: Vec<PathBuf>| match (files.is_empty(), cli_creds) {
        (false, _) => Ok(files),
        (true, Some(creds)) => Ok(vec![creds.to_owned()]),
        (true, None) => bail!("name the credentials files, or set --creds"),
    };
    match command {
        KeysCommand::Store { files: named } => {
            for path in files(named)? {
                match keys::store_seed(&path, store, nats).await? {
                    Moved::Now(key) => println!("{}: seed moved to the OS credential store, key {key}", path.display()),
                    Moved::Before(key) => {
                        println!("{}: seed already in the OS credential store, key {key}", path.display());
                    }
                }
            }
        }
        KeysCommand::Unstore { files: named } => {
            for path in files(named)? {
                match keys::unstore_seed(&path, store)? {
                    Moved::Now(key) => println!("{}: seed moved back into the file, key {key}", path.display()),
                    Moved::Before(key) => println!("{}: seed already in the file, key {key}", path.display()),
                }
            }
        }
        KeysCommand::Remove { files: named } => {
            for path in files(named)? {
                let finding = keys::remove(&path, store)?;
                println!(
                    "{} removed; the credentials of {} stay valid until they expire: revoke them with \
                     `ants auth revoke --key {} --earlier` on the machine holding the team keys",
                    path.display(),
                    finding.holder(),
                    finding.key
                );
            }
        }
    }
    Ok(())
}
