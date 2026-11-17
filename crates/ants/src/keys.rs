//! `ants keys`: moving the NKey seed of a `.creds` file into the OS credential store ([`keystore`]) and back, and
//! removing credentials from a machine. The seed of a human device's encryption key, when the file holds one
//! ([`creds::xkey_of`]), goes along with the NKey seed, stored under its own public key.
//!
//! A file whose seed is stored keeps its user JWT and, in place of the seed, the public key it is stored under
//! ([`creds::stored_text`]); every ants process reads the seed from the store when it connects with the file
//! ([`creds::load`]). A seed leaves a file only once stored, read back, and proven by authenticating at the server
//! with the file as it is to be; the file is then replaced at once, and a failure on the way deletes what was stored
//! and leaves the file as it was. Renewal ([`replace`]) keeps the new seed where the earlier one was. New credentials
//! of `ants add --store-key` and `ants auth user|channel-manager --store-key` put their seed in the store at once
//! ([`write_new_stored`]), never in a file.
//!
//! Leaf node uplink credentials stay in their file, which nats-server reads; so do the team keys of `ants auth`.

use std::fs;
use std::path::{Path, PathBuf};

use ants_daemon::bus;
use ants_daemon::secrets::creds::{self, Finding, Kind, Seed};
use ants_daemon::secrets::keystore::{self, Store};
use anyhow::{Context as _, Result, bail};
use nkeys::{KeyPair, XKey};

use crate::auth;

/// Where a seed went: moved now, or there before; with its public key.
#[derive(Debug, PartialEq, Eq)]
pub enum Moved {
    Now(String),
    Before(String),
}

/// Move the seed of the credentials at `path` into `store`, once stored, read back and used to authenticate at `url`
/// with the file as it is to be. A failure leaves the file as it was and deletes what it stored.
pub async fn store_seed(path: &Path, store: &dyn Store, url: &str) -> Result<Moved> {
    let text = fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let jwt = auth::creds_jwt(&text).with_context(|| format!("no user JWT in {}", path.display()))?;
    let seed = match creds::seed_of(&text) {
        Some(Seed::File(seed)) => seed,
        Some(Seed::Stored(key)) => {
            creds::load(path, store)?;
            return Ok(Moved::Before(key.to_owned()));
        }
        None => bail!("no seed in {}", path.display()),
    };
    let finding = Finding::from_jwt(jwt).with_context(|| format!("credentials {}", path.display()))?;
    if matches!(finding.kind, Kind::Leaf { .. } | Kind::LeafSystem { .. }) {
        bail!(
            "{} are the uplink credentials of a leaf node, which nats-server reads from the file: their seed stays there",
            path.display()
        );
    }
    let key =
        KeyPair::from_seed(seed).map_err(|e| anyhow::anyhow!("the seed in {}: {e}", path.display()))?.public_key();
    if key != finding.key {
        bail!("the seed in {} is not that of the key {} of their user JWT", path.display(), finding.key);
    }
    let xkey = held_xkey(&text, path, store)?;
    let new = NewCreds { jwt, key: &key, seed, xkey: xkey.as_ref() };
    write_stored(path, new, store, url, Proof::Required)
        .await
        .with_context(|| format!("the seed stays in {}", path.display()))?;
    Ok(Moved::Now(key))
}

/// Move the seed of the credentials at `path` from `store` back into the file, then delete it from the store.
pub fn unstore_seed(path: &Path, store: &dyn Store) -> Result<Moved> {
    let text = fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let key = match creds::seed_of(&text) {
        Some(Seed::Stored(key)) => key.to_owned(),
        Some(Seed::File(seed)) => {
            let key = KeyPair::from_seed(seed).map_err(|e| anyhow::anyhow!("the seed in {}: {e}", path.display()))?;
            return Ok(Moved::Before(key.public_key()));
        }
        None => bail!("no seed in {}", path.display()),
    };
    let (jwt, user) = creds::load(path, store)?;
    let xkey = held_xkey(&text, path, store)?;
    auth::replace_creds(path, &jwt, &user, xkey.as_ref())?;
    let (_, back) = creds::load(path, store)?;
    if back.public_key() != key {
        bail!("{} hold another key than {key} once written", path.display());
    }
    let mut stored = vec![key.clone()];
    if let Some(Seed::Stored(xkey)) = creds::xkey_of(&text) {
        stored.push(xkey.to_owned());
    }
    for key in stored {
        if let Err(e) = store.delete(&key) {
            eprintln!("warning: the seed of {key} stays in the OS credential store too: {e:#}");
        }
    }
    Ok(Moved::Now(key))
}

/// The encryption key whose seed the `.creds` file at `path`, of text `text`, holds or names in `store`, if any
/// ([`creds::load_xkey`]). A user JWT binding a key whose seed the file does not hold is no error here: the NKey seed
/// moves without it.
pub(crate) fn held_xkey(text: &str, path: &Path, store: &dyn Store) -> Result<Option<XKey>> {
    if creds::xkey_of(text).is_none() {
        return Ok(None);
    }
    creds::load_xkey(path, store)
}

/// Delete the credentials at `path` and the seed they keep in `store`, if they do; the file stays when the store
/// cannot delete it. Returns what the credentials were.
pub fn remove(path: &Path, store: &dyn Store) -> Result<Finding> {
    let text = fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let finding = Finding::from_creds(&text).with_context(|| format!("credentials {}", path.display()))?;
    for seed in [creds::seed_of(&text), creds::xkey_of(&text)] {
        if let Some(Seed::Stored(key)) = seed {
            store.delete(key).with_context(|| format!("deleting the seed of {key}; {} stays", path.display()))?;
        }
    }
    fs::remove_file(path).with_context(|| format!("removing {}", path.display()))?;
    Ok(finding)
}

/// Replace the credentials at `path` with `jwt` for the key `user`, and the encryption key `xkey` if given, keeping
/// their seeds where the earlier NKey seed is: in the file at once, as [`auth::replace_creds`] does, or in `store`,
/// once stored, read back and used to authenticate at `url`, before the earlier seeds are deleted from it. A failure
/// leaves the file and the earlier seeds as they were.
pub async fn replace(
    path: &Path,
    jwt: &str,
    user: &KeyPair,
    xkey: Option<&XKey>,
    store: &dyn Store,
    url: &str,
) -> Result<()> {
    let text = fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let Some(Seed::Stored(earlier)) = creds::seed_of(&text) else {
        return auth::replace_creds(path, jwt, user, xkey);
    };
    let key = user.public_key();
    let seed = auth::seed(user)?;
    write_stored(path, NewCreds { jwt, key: &key, seed: &seed, xkey }, store, url, Proof::Required)
        .await
        .with_context(|| format!("{} stay as they were", path.display()))?;
    let mut earlier = vec![(earlier, key)];
    if let Some(Seed::Stored(xearlier)) = creds::xkey_of(&text) {
        earlier.push((xearlier, xkey.map_or_else(String::new, XKey::public_key)));
    }
    for (earlier, now) in earlier {
        if earlier != now
            && let Err(e) = store.delete(earlier)
        {
            eprintln!("warning: the earlier seed of {earlier} stays in the OS credential store: {e:#}");
        }
    }
    Ok(())
}

/// Write new credentials of `jwt` for `user` to `path` with the seed in `store`, never in a file, as `ants add` and
/// `ants auth --store-key` do: stored (unless there already), read back, and the file written naming it once it
/// authenticates at `url`, or unproven when no server answers there ([`Proven::Unreached`]). A failure deletes the seed
/// when it was not stored before and leaves `path` as it was.
pub async fn write_new_stored(
    path: &Path,
    jwt: &str,
    user: &KeyPair,
    xkey: Option<&XKey>,
    store: &dyn Store,
    url: &str,
) -> Result<Proven> {
    let (key, seed) = (user.public_key(), auth::seed(user)?);
    write_stored(path, NewCreds { jwt, key: &key, seed: &seed, xkey }, store, url, Proof::Optional)
        .await
        .with_context(|| format!("{} not written", path.display()))
}

/// Write the new credentials of `jwt` for `user`, and a human's encryption key `xkey`, that `ants auth --store-key`
/// issued to `path`, with the seeds in the OS credential store ([`write_new_stored`]), warning when no server answered
/// at `url` to prove them.
pub async fn issue_stored(path: &Path, jwt: &str, user: &KeyPair, xkey: Option<&XKey>, url: &str) -> Result<()> {
    if write_new_stored(path, jwt, user, xkey, keystore::os(), url).await? == Proven::Unreached {
        eprintln!(
            "warning: no server answered at {url}, so the stored key {} has not authenticated yet",
            user.public_key()
        );
    }
    Ok(())
}

/// Store a new key's seed in `store`, never in a file, and check it reads back. Returns whether it was stored now
/// rather than there already.
pub fn store_new(user: &KeyPair, store: &dyn Store) -> Result<bool> {
    store_new_seed(&user.public_key(), &auth::seed(user)?, store)
}

/// [`store_new`] for the seed `seed` of the public key `key`.
fn store_new_seed(key: &str, seed: &str, store: &dyn Store) -> Result<bool> {
    let created = match store.get(key).context("reading the OS credential store")? {
        Some(stored) if stored.trim() == seed => false,
        Some(_) => bail!("the OS credential store holds another seed for {key}; left as it is"),
        None => {
            store.set(key, seed).context("storing the seed in the OS credential store")?;
            true
        }
    };
    if !matches!(store.get(key), Ok(Some(stored)) if stored.trim() == seed) {
        if created && let Err(e) = store.delete(key) {
            eprintln!("warning: the seed of {key} stays in the OS credential store: {e:#}");
        }
        bail!("the OS credential store gave back no seed, or another one, for {key}");
    }
    Ok(created)
}

/// Whether credentials whose seed is stored authenticated at the server before being written, or no server answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proven {
    Authenticated,
    Unreached,
}

/// Whether new credentials must authenticate before they are written, or may also be written when no server answers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Proof {
    Required,
    Optional,
}

/// The user JWT of new credentials, the public `key` and `seed` of their user, and the curve `xkey` they hold, if any.
#[derive(Clone, Copy)]
struct NewCreds<'a> {
    jwt: &'a str,
    key: &'a str,
    seed: &'a str,
    xkey: Option<&'a XKey>,
}

/// Store `seed` for `key`, and the seed of `xkey` under its public key, in `store` and check they read back, then write
/// `path` naming them, once it authenticates at `url`, or, with [`Proof::Optional`], also when no server answers
/// there. A failure deletes the seeds not stored before, and leaves `path` as it was.
async fn write_stored(
    path: &Path,
    NewCreds { jwt, key, seed, xkey }: NewCreds<'_>,
    store: &dyn Store,
    url: &str,
    proof: Proof,
) -> Result<Proven> {
    let user = KeyPair::from_seed(seed).map_err(|e| anyhow::anyhow!("the seed of {key}: {e}"))?;
    let mut created = Vec::new();
    let undo = |created: &[String]| {
        for key in created {
            if let Err(e) = store.delete(key) {
                eprintln!("warning: the seed of {key} stays in the OS credential store: {e:#}");
            }
        }
    };
    if store_new(&user, store)? {
        created.push(key.to_owned());
    }
    let mut text = creds::stored_text(jwt, key);
    if let Some(xkey) = xkey {
        let public = xkey.public_key();
        match auth::xseed(xkey).and_then(|xseed| store_new_seed(&public, &xseed, store)) {
            Ok(true) => created.push(public.clone()),
            Ok(false) => {}
            Err(e) => {
                undo(&created);
                return Err(e);
            }
        }
        text += &creds::xkey_block(Seed::Stored(&public));
    }
    let temporary = beside(path)?;
    let written = async {
        auth::write_secret(&temporary, &text)?;
        let proven = authenticate(url, &temporary, store, proof).await?;
        fs::rename(&temporary, path).with_context(|| format!("replacing {}", path.display()))?;
        Ok(proven)
    }
    .await;
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
        undo(&created);
    }
    written
}

/// Connect to `url` with the credentials at `path`, reading their seed from `store`, to check the server takes them;
/// with [`Proof::Optional`], a server that does not answer is [`Proven::Unreached`] rather than a failure.
async fn authenticate(url: &str, path: &Path, store: &dyn Store, proof: Proof) -> Result<Proven> {
    bus::check_url(url)?;
    let options = async_nats::ConnectOptions::new().name("ants-keys").event_callback(|_| async {});
    let client = match bus::authenticate(options, path, store)?.connect(url).await {
        Ok(client) => client,
        Err(e) if proof == Proof::Optional && bus::worth_retrying(&e) => return Ok(Proven::Unreached),
        Err(e) => return Err(e).with_context(|| format!("authenticating at {url} with the stored key")),
    };
    client.flush().await.with_context(|| format!("authenticating at {url} with the stored key"))?;
    Ok(Proven::Authenticated)
}

fn beside(path: &Path) -> Result<PathBuf> {
    let name = path.file_name().with_context(|| format!("{} names no file", path.display()))?;
    let mut temporary = name.to_owned();
    temporary.push(format!(".{}.tmp", std::process::id()));
    Ok(path.with_file_name(temporary))
}

#[cfg(test)]
mod tests {
    use ants_daemon::secrets::keystore::Memory;
    use ants_proto::{Machine, Role, UserId};

    use super::*;
    use crate::members::EnrollApprover;
    use crate::test_support::Server;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn a_stored_seed_signs_the_challenge_and_moves_back_into_its_file() {
        let Some(server) = Server::start() else { return };
        let store = Memory::default();
        let creds = server.creds_as(&p("alice"), Role::Human);
        let (jwt, user) = auth::read_creds(&creds).unwrap();
        let (seed, key) = (auth::seed(&user).unwrap(), user.public_key());
        let xkey = creds::load_xkey(&creds, &store).unwrap().expect("a human's credentials bind an encryption key");
        let (xseed, xpublic) = (auth::xseed(&xkey).unwrap(), xkey.public_key());

        assert_eq!(store_seed(&creds, &store, &server.url).await.unwrap(), Moved::Now(key.clone()));
        let text = fs::read_to_string(&creds).unwrap();
        assert!(text.contains(&jwt) && !text.contains(&seed) && !text.contains(&xseed), "{text}");
        assert_eq!(creds::seed_of(&text), Some(Seed::Stored(&key)));
        assert_eq!(creds::xkey_of(&text), Some(Seed::Stored(&xpublic)), "the encryption key moves along");
        assert_eq!(store.keys(), [key.clone(), xpublic.clone()]);
        assert_eq!(creds::load_xkey(&creds, &store).unwrap().map(|x| x.public_key()), Some(xpublic.clone()));
        let (client, _) =
            bus::connect_with(&server.url, Some(&creds), &store, None, &p("alice"), "test").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(store_seed(&creds, &store, &server.url).await.unwrap(), Moved::Before(key.clone()));

        store.lock();
        let err = bus::connect_with(&server.url, Some(&creds), &store, None, &p("alice"), "test").await.unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("locked") && !err.contains(&seed), "{err}");
        assert!(unstore_seed(&creds, &store).is_err(), "nothing to move back while locked");
        store.unlock();

        assert_eq!(unstore_seed(&creds, &store).unwrap(), Moved::Now(key.clone()));
        let text = fs::read_to_string(&creds).unwrap();
        assert!(text.contains(&seed) && text.contains(&xseed), "{text}");
        assert!(store.keys().is_empty());
        assert_eq!(unstore_seed(&creds, &store).unwrap(), Moved::Before(key));
        let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("alice"), "test").await.unwrap();
        client.flush().await.unwrap();
    }

    #[tokio::test]
    async fn a_seed_stays_in_its_file_when_the_store_or_the_server_refuses_it() {
        let Some(server) = Server::start() else { return };
        let Some(other) = Server::start() else { return };
        let store = Memory::default();
        let creds = server.creds_as(&p("dev-a"), Role::Ant);
        let before = fs::read(&creds).unwrap();
        let unchanged = || {
            assert_eq!(fs::read(&creds).unwrap(), before);
            assert!(store.keys().is_empty());
            let left: Vec<_> = fs::read_dir(server.dir())
                .unwrap()
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
                .collect();
            assert!(left.is_empty(), "{left:?}");
        };

        store.lock();
        let err = format!("{:#}", store_seed(&creds, &store, &server.url).await.unwrap_err());
        assert!(err.contains("locked") && err.contains("the seed stays in"), "{err}");
        unchanged();
        store.unlock();

        store.refuse_writes();
        let err = format!("{:#}", store_seed(&creds, &store, &server.url).await.unwrap_err());
        assert!(err.contains("denied"), "{err}");
        unchanged();
        store.allow_writes();

        // A server of another team refuses the stored key: what was stored goes.
        let err = format!("{:#}", store_seed(&creds, &store, &other.url).await.unwrap_err());
        assert!(err.contains("authenticating"), "{err}");
        unchanged();

        let uplink = server.dir().join("leaf-m1.creds");
        let m1: Machine = "m1".parse().unwrap();
        auth::issue_leaf(
            &server.dir().join("auth"),
            &m1,
            &[(p("dev-a"), Role::Ant)],
            &[],
            EnrollApprover::Without,
            None,
            &uplink,
        )
        .unwrap();
        let err = store_seed(&uplink, &store, &server.url).await.unwrap_err().to_string();
        assert!(err.contains("nats-server reads"), "{err}");
        assert!(creds::seed_of(&fs::read_to_string(&uplink).unwrap()).is_some_and(|s| matches!(s, Seed::File(_))));
    }

    #[tokio::test]
    async fn new_credentials_go_with_a_stored_seed_unless_a_server_refuses_them() {
        let Some(server) = Server::start() else { return };
        let Some(other) = Server::start() else { return };
        let store = Memory::default();
        let user = KeyPair::new_user();
        let key = user.public_key();
        let jwt = auth::sign_user(&server.dir().join("auth"), &p("dev-a"), Role::Ant, None, &key).unwrap();
        let path = server.dir().join("dev-a.creds");

        let xkey = XKey::new();
        let refused = write_new_stored(&path, &jwt, &user, Some(&xkey), &store, &other.url).await;
        let err = format!("{:#}", refused.unwrap_err());
        assert!(err.contains("not written") && err.contains("authenticating"), "{err}");
        assert!(!path.exists() && store.keys().is_empty(), "nothing half-done, the encryption key's seed neither");

        let nowhere = "nats://127.0.0.1:1";
        assert_eq!(write_new_stored(&path, &jwt, &user, None, &store, nowhere).await.unwrap(), Proven::Unreached);
        fs::remove_file(&path).unwrap();
        store.delete(&key).unwrap();

        assert_eq!(
            write_new_stored(&path, &jwt, &user, None, &store, &server.url).await.unwrap(),
            Proven::Authenticated
        );
        let text = fs::read_to_string(&path).unwrap();
        assert_eq!(creds::seed_of(&text), Some(Seed::Stored(&key)));
        assert!(!text.contains(&auth::seed(&user).unwrap()) && store.keys() == [key]);
    }

    #[tokio::test]
    async fn removing_stored_credentials_deletes_their_seed_but_revokes_nothing() {
        let Some(server) = Server::start() else { return };
        let store = Memory::default();
        let creds = server.creds_as(&p("bob"), Role::Human);
        let Moved::Now(key) = store_seed(&creds, &store, &server.url).await.unwrap() else { panic!() };

        store.lock();
        assert!(remove(&creds, &store).is_err());
        assert!(creds.exists(), "kept while the store cannot delete the seed");
        store.unlock();

        let finding = remove(&creds, &store).unwrap();
        assert_eq!((finding.name.as_str(), finding.key), ("bob", key));
        assert!(!creds.exists() && store.keys().is_empty());
    }
}
