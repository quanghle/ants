//! The text of a `.creds` file: its blocks, where its seeds are, and the keys loaded from it.

use std::path::Path;

use anyhow::{Context as _, Result, anyhow, bail};
use nkeys::{KeyPair, XKey};

use super::Finding;
use crate::secrets::keystore::{self, Store};

/// The text of the credentials file at `path`.
pub(super) fn read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))
}

/// The user JWT of the credentials at `path`.
pub fn jwt(path: &Path) -> Result<String> {
    let text = read(path)?;
    creds_jwt(&text).map(str::to_owned).with_context(|| format!("no user JWT in {}", path.display()))
}

/// The user JWT of a `.creds` file's text: the line after the `BEGIN NATS USER JWT` marker.
pub(super) fn creds_jwt(creds: &str) -> Option<&str> {
    creds_block(creds, "BEGIN NATS USER JWT")
}

const SEED_MARKER: &str = "BEGIN USER NKEY SEED";
const STORED_MARKER: &str = "BEGIN ANTS STORED NKEY";
const XKEY_SEED_MARKER: &str = "BEGIN ANTS XKEY SEED";
const STORED_XKEY_MARKER: &str = "BEGIN ANTS STORED XKEY";

fn creds_block<'a>(creds: &'a str, marker: &str) -> Option<&'a str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains(marker))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

/// Where the NKey seed of a `.creds` file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seed<'a> {
    /// In the file, as written by `ants auth` and nats tools.
    File(&'a str),
    /// In the OS credential store ([`keystore`]), under this public key.
    Stored(&'a str),
}

/// Where the seed of a `.creds` file's text is, if it says.
pub fn seed_of(creds: &str) -> Option<Seed<'_>> {
    creds_block(creds, SEED_MARKER).map(Seed::File).or_else(|| creds_block(creds, STORED_MARKER).map(Seed::Stored))
}

/// Where the encryption key seed of a human's `.creds` file text is, if it says: in an `ANTS XKEY SEED` block after
/// the NKey seed, or in the OS credential store under the public key of an `ANTS STORED XKEY` block.
pub fn xkey_of(creds: &str) -> Option<Seed<'_>> {
    creds_block(creds, XKEY_SEED_MARKER)
        .map(Seed::File)
        .or_else(|| creds_block(creds, STORED_XKEY_MARKER).map(Seed::Stored))
}

/// The block keeping an encryption key seed, or the public key of the one stored, appended to the text of a `.creds`
/// file. nats tools read only a file's first two blocks, the user JWT and the NKey seed, so they ignore it.
pub fn xkey_block(seed: Seed<'_>) -> String {
    match seed {
        Seed::File(seed) => format!(
            "\n************************* IMPORTANT *************************\n\
             The XKEY seed below opens the messages sealed for this device: only ants reads it.\n\n\
             -----BEGIN ANTS XKEY SEED-----\n{seed}\n------END ANTS XKEY SEED------\n"
        ),
        Seed::Stored(key) => format!(
            "\n************************* IMPORTANT *************************\n\
             The XKEY seed of this device is in the OS credential store under the public key below.\n\n\
             -----BEGIN ANTS STORED XKEY-----\n{key}\n------END ANTS STORED XKEY------\n"
        ),
    }
}

/// The text of a `.creds` file holding `jwt` and `seed`, as nats tools write it.
pub fn text(jwt: &str, seed: &str) -> String {
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
         ************************* IMPORTANT *************************\n\
         NKEY Seed printed below can be used to sign and prove identity.\n\
         NKEYs are sensitive and should be treated as secrets.\n\n\
         -----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n\n\
         *************************************************************\n"
    )
}

/// The text of a `.creds` file holding `jwt`, whose seed is in the OS credential store under the public key `key`.
pub fn stored_text(jwt: &str, key: &str) -> String {
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
         ************************* IMPORTANT *************************\n\
         The NKEY seed of this user is not in this file but in the OS credential store,\n\
         under the service \"{service}\" and the public key below: only ants reads it.\n\
         `ants keys unstore FILE` moves it back into the file.\n\n\
         -----BEGIN ANTS STORED NKEY-----\n{key}\n------END ANTS STORED NKEY------\n\n\
         *************************************************************\n",
        service = keystore::SERVICE
    )
}

/// The user JWT and key of the `.creds` file at `path`, whose seed is in the file or in `store`. A seed stored for
/// another key than the JWT's, or none, is an error, as is a store that cannot be read; none names the seed.
pub fn load(path: &Path, store: &dyn Store) -> Result<(String, KeyPair)> {
    let text = read(path)?;
    let jwt = creds_jwt(&text).with_context(|| format!("no user JWT in {}", path.display()))?;
    let key = match seed_of(&text) {
        Some(Seed::File(seed)) => {
            KeyPair::from_seed(seed).map_err(|e| anyhow!("the seed in {}: {e}", path.display()))?
        }
        Some(Seed::Stored(public)) => {
            let named = Finding::from_jwt(jwt).with_context(|| format!("credentials {}", path.display()))?.key;
            if named != public {
                bail!("{} name the stored key {public}, not the key {named} of their user JWT", path.display());
            }
            let seed = store
                .get(public)
                .with_context(|| {
                    format!("reading the seed of {} ({public}) from the OS credential store", path.display())
                })?
                .with_context(|| {
                    format!(
                        "the OS credential store holds no seed for {public}, which {} name: restore it from a backup \
                         of the store, or have the credentials issued again",
                        path.display()
                    )
                })?;
            let key = KeyPair::from_seed(seed.trim())
                .map_err(|e| anyhow!("the seed stored for {public} ({}): {e}", path.display()))?;
            if key.public_key() != public {
                bail!("the OS credential store holds the seed of another key under {public} ({})", path.display());
            }
            key
        }
        None => bail!("no seed in {}, nor the key of one in the OS credential store", path.display()),
    };
    Ok((jwt.to_owned(), key))
}

/// The encryption key of the `.creds` file at `path`, whose seed is in the file or in `store`, if its user JWT binds
/// one ([`ants_proto::DeviceKey::tag`]). A seed of another key than the JWT's, a missing seed, a seed the JWT does not bind and a
/// store that cannot be read are errors; none names the seed.
pub fn load_xkey(path: &Path, store: &dyn Store) -> Result<Option<XKey>> {
    let text = read(path)?;
    let bound = Finding::from_creds(&text).with_context(|| format!("credentials {}", path.display()))?.xkey;
    let (seed, bound) = match (xkey_of(&text), bound) {
        (None, None) => return Ok(None),
        (None, Some(bound)) => bail!(
            "no seed of the encryption key {bound} in {}, nor the key of one in the OS credential store",
            path.display()
        ),
        (Some(_), None) => bail!("{} hold an encryption key their user JWT does not bind", path.display()),
        (Some(Seed::File(seed)), Some(bound)) => (seed.to_owned(), bound),
        (Some(Seed::Stored(public)), Some(bound)) => {
            let seed = store
                .get(public)
                .with_context(|| format!("reading the encryption key seed of {} ({public})", path.display()))?
                .with_context(|| {
                    format!("the OS credential store holds no seed for {public}, which {} name", path.display())
                })?;
            (seed, bound)
        }
    };
    let key =
        XKey::from_seed(seed.trim()).map_err(|e| anyhow!("the encryption key seed of {}: {e}", path.display()))?;
    if key.public_key() != bound.as_str() {
        bail!("the encryption key seed of {} is not that of the key {bound} their user JWT binds", path.display());
    }
    Ok(Some(key))
}

#[cfg(test)]
mod tests {
    use ants_proto::{DeviceId, DeviceKey, PERMISSIONS_VERSION, Permissions, Role};
    use data_encoding::BASE64URL_NOPAD;

    use super::super::tests::{claims, p};
    use super::*;

    #[test]
    fn a_stored_seed_loads_only_for_the_key_its_jwt_names() {
        let dir = ants_test_support::scratch_dir("creds-load");
        let (path, store) = (dir.join("alice.creds"), keystore::Memory::default());
        let (user, other) = (KeyPair::new_user(), KeyPair::new_user());
        let (key, seed) = (user.public_key(), user.seed().unwrap());
        let tags = [PERMISSIONS_VERSION.tag(), DeviceId::generate().tag()];
        let mut named = claims("alice", &Permissions::for_user(&p("alice"), Role::Human), &tags, None);
        named["sub"] = key.clone().into();
        let jwt = format!("e30.{}.sig", BASE64URL_NOPAD.encode(named.to_string().as_bytes()));
        let load = |text: String| {
            std::fs::write(&path, text).unwrap();
            load(&path, &store).map(|(jwt, key)| (jwt, key.public_key())).map_err(|e| format!("{e:#}"))
        };

        assert_eq!(load(text(&jwt, &seed)), Ok((jwt.clone(), key.clone())));
        let err = load(stored_text(&jwt, &key)).unwrap_err();
        assert!(err.contains("holds no seed") && err.contains("backup"), "{err}");
        store.set(&key, &seed).unwrap();
        assert_eq!(load(stored_text(&jwt, &key)), Ok((jwt.clone(), key.clone())));
        let err = load(stored_text(&jwt, &other.public_key())).unwrap_err();
        assert!(err.contains("not the key"), "{err}");
        store.lock();
        let err = load(stored_text(&jwt, &key)).unwrap_err();
        assert!(err.contains("locked") && !err.contains(&seed), "{err}");
        store.unlock();
        store.set(&key, &other.seed().unwrap()).unwrap();
        let err = load(stored_text(&jwt, &key)).unwrap_err();
        assert!(err.contains("another key"), "{err}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_encryption_key_loads_only_as_the_jwt_binds_it() {
        let dir = ants_test_support::scratch_dir("creds-xkey");
        let (path, store) = (dir.join("alice.creds"), keystore::Memory::default());
        let (user, xkey, other) = (KeyPair::new_user(), XKey::new(), XKey::new());
        let (seed, xseed) = (user.seed().unwrap(), xkey.seed().unwrap());
        let jwt = |xkey: Option<&XKey>| {
            let mut tags = vec![PERMISSIONS_VERSION.tag(), DeviceId::generate().tag()];
            tags.extend(xkey.map(|x| x.public_key().parse::<DeviceKey>().unwrap().tag()));
            let mut named = claims("alice", &Permissions::for_user(&p("alice"), Role::Human), &tags, None);
            named["sub"] = user.public_key().into();
            format!("e30.{}.sig", BASE64URL_NOPAD.encode(named.to_string().as_bytes()))
        };
        let (bound, unbound) = (jwt(Some(&xkey)), jwt(None));
        let load = |text: String| {
            std::fs::write(&path, text).unwrap();
            load_xkey(&path, &store).map(|key| key.map(|key| key.public_key())).map_err(|e| format!("{e:#}"))
        };

        let file = text(&bound, &seed) + &xkey_block(Seed::File(&xseed));
        assert_eq!(xkey_of(&file), Some(Seed::File(xseed.as_str())));
        assert_eq!(seed_of(&file), Some(Seed::File(seed.as_str())), "the NKey seed stays the one nats tools read");
        assert_eq!(load(file), Ok(Some(xkey.public_key())));
        assert_eq!(load(text(&unbound, &seed)), Ok(None));
        let err = load(text(&bound, &seed)).unwrap_err();
        assert!(err.contains("no seed of the encryption key"), "{err}");
        let err = load(text(&unbound, &seed) + &xkey_block(Seed::File(&xseed))).unwrap_err();
        assert!(err.contains("does not bind"), "{err}");
        let err = load(text(&bound, &seed) + &xkey_block(Seed::File(&other.seed().unwrap()))).unwrap_err();
        assert!(err.contains("not that of the key") && !err.contains(&xseed), "{err}");

        let stored = stored_text(&bound, &user.public_key()) + &xkey_block(Seed::Stored(&xkey.public_key()));
        assert!(load(stored.clone()).unwrap_err().contains("holds no seed"));
        store.set(&xkey.public_key(), &xseed).unwrap();
        assert_eq!(load(stored), Ok(Some(xkey.public_key())));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
