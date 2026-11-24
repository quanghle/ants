//! A device of a human on a server with authentication, as a page of the web client is one: its credentials, the user
//! JWT of its NKey, read and checked before they connect ([`Credentials`]); the bus's public bootstrap credentials it
//! registers with ([`Bootstrap`]); its registration with `ants signer` as `ants login` asks for it
//! ([`registration`]); and the renewal of its credentials before they expire ([`renewal`]).
//!
//! The device's key signs where it is kept, which may hold it so that nothing reads it: what is to be signed leaves
//! here as bytes and its signature comes back, base64url without padding.

pub mod registration;
pub mod renewal;

use ants_proto::{
    DeviceId, JsDomain, JwtError, PERMISSIONS_VERSION, PermissionsVersion, UserId, is_web, js_domain_of, verified_jwt,
    verify_user_jwt,
};
use chrono::{DateTime, Utc};
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use nkeys::KeyPair;
use serde::{Deserialize, Serialize};

use crate::client::Session;

/// The name a device's connection tells the server.
pub const CONNECTION_NAME: &str = "ants-web";

/// Why a device's credentials, its registration or their renewal failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum Error {
    /// The credentials, or an answer carrying them, are not what the device may connect with, and why.
    #[error("{0}")]
    Credentials(String),
    /// The credentials carry a newer permissions version than this client knows: they were issued by a newer ants,
    /// whose page it takes.
    #[error(
        "the credentials carry permissions version {theirs}, newer than this page's {ours}: they come from a newer \
         ants, whose page is to be loaded"
    )]
    NewerPermissions { theirs: PermissionsVersion, ours: PermissionsVersion },
    /// The bus's bootstrap credentials do not read.
    #[error("the bootstrap credentials: {0}")]
    Bootstrap(String),
    /// What was asked for cannot be sent, and why.
    #[error("{0}")]
    Invalid(String),
    /// The signer refused the registration or the renewal, and why.
    #[error("refused: {0}")]
    Refused(String),
    /// The signer sent a sign-in page other than the trusted GitHub's device login page, which is not shown.
    #[error(
        "the signer sent a sign-in page other than the device login page {trusted}: not shown; the page is to be \
         exported again with the GitHub the team signs in at"
    )]
    NotShown { trusted: String },
    #[error("the registration request expired without an answer")]
    Expired,
}

/// The public NKey (`U...`) of a user whose Ed25519 public key is `raw`, as a key kept where its seed cannot be read
/// gives it.
pub fn user_key(raw: &[u8]) -> Result<String, Error> {
    const PREFIX_BYTE_USER: u8 = 20 << 3;
    if raw.len() != 32 {
        return Err(Error::Invalid(format!("an Ed25519 public key has 32 bytes, not {}", raw.len())));
    }
    let mut bytes = Vec::with_capacity(35);
    bytes.push(PREFIX_BYTE_USER);
    bytes.extend_from_slice(raw);
    let crc = crc16(&bytes);
    bytes.extend(crc.to_le_bytes());
    Ok(BASE32_NOPAD.encode(&bytes))
}

/// The CRC-16 of an NKey: XMODEM's, polynomial 0x1021 from 0.
fn crc16(data: &[u8]) -> u16 {
    data.iter().fold(0, |crc, &byte| {
        (0..8)
            .fold(crc ^ (u16::from(byte) << 8), |crc, _| if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 })
    })
}

/// The credentials of a device: the user JWT of its key, read and checked to be web credentials of a human, issued to
/// that key and of no newer permissions version than this client's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    pub human: UserId,
    /// The device's NKey, `U...`.
    pub public_key: String,
    /// The account that issued them, `A...`.
    pub account: String,
    /// The device they are for, if they name one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
    /// The hub's JetStream domain they were issued for, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<JsDomain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

impl Credentials {
    /// The credentials `jwt` holds for the key `public_key`, verified to be signed by `account` when given, else by
    /// the issuer the JWT names (the server checks that one); refused when they are expired, of another key, no web
    /// device's ([`ants_proto::WEB_TAG`]) or of a newer permissions version ([`Error::NewerPermissions`]).
    pub fn read(jwt: &str, public_key: &str, account: Option<&str>) -> Result<Self, Error> {
        let refused = |e: JwtError| Error::Credentials(e.to_string());
        let (claims, body) = verified_jwt(jwt).map_err(refused)?;
        let account = account.unwrap_or(&claims.iss);
        let now = u64::try_from(Utc::now().timestamp()).unwrap_or_default();
        let user = verify_user_jwt(jwt, [account], now).map_err(refused)?;
        if user.public_key != public_key {
            return Err(Error::Credentials(format!(
                "credentials of the key {}, not of this device's {public_key}",
                user.public_key
            )));
        }
        let tags = || user.tags.iter().map(String::as_str);
        if !is_web(tags()) {
            return Err(Error::Credentials(format!(
                "credentials of {} that are no web device's: issue them with `ants auth user {} --human --web \
                 --public-key {public_key}`",
                user.id, user.id
            )));
        }
        if let Some(theirs) = PermissionsVersion::from_tags(tags()).filter(|theirs| *theirs > PERMISSIONS_VERSION) {
            return Err(Error::NewerPermissions { theirs, ours: PERMISSIONS_VERSION });
        }
        let publish = body["nats"]["pub"]["allow"].as_array().into_iter().flatten().filter_map(|s| s.as_str());
        let time = |secs: Option<u64>| secs.and_then(|s| DateTime::from_timestamp(i64::try_from(s).ok()?, 0));
        Ok(Self {
            device: DeviceId::from_tags(tags()),
            domain: js_domain_of(publish),
            human: user.id,
            public_key: user.public_key,
            account: claims.iss,
            issued_at: time(user.issued_at),
            expires_at: time(user.expires_at),
        })
    }

    /// Who a client on these credentials acts as.
    pub fn session(&self) -> Session {
        Session {
            human: self.human.clone(),
            domain: self.domain.clone(),
            account: Some(self.account.clone()),
            name: CONNECTION_NAME.to_owned(),
        }
    }

    /// When to ask for them renewed: once two thirds of their lifetime are past. None for credentials that never
    /// expire.
    pub fn renew_at(&self) -> Option<DateTime<Utc>> {
        let expires_at = self.expires_at?;
        let issued_at = self.issued_at.unwrap_or(expires_at);
        Some(issued_at + (expires_at - issued_at) * 2 / 3)
    }

    /// The command that revokes the device, on the machine holding the account keys.
    pub fn revoke_command(&self) -> String {
        format!("ants auth revoke --key {}", self.public_key)
    }
}

/// A client on a server without authentication acting as `human`, as the CLI without credentials acts as the `human`
/// of its config.
pub fn unauthenticated(human: UserId) -> Session {
    Session { human, domain: None, account: None, name: CONNECTION_NAME.to_owned() }
}

/// The bus's public bootstrap credentials (`ants auth bootstrap`), with which a device asks `ants signer` to register
/// it. Their seed is public: the account's signature limits them to registration requests.
#[derive(Clone)]
pub struct Bootstrap {
    jwt: String,
    key: KeyPair,
    account: String,
}

impl Bootstrap {
    /// The bootstrap credentials of a `.creds` file's text, checked to be signed by their account and named
    /// [`ants_proto::BOOTSTRAP_USER`].
    pub fn read(creds: &str) -> Result<Self, Error> {
        let invalid = |why: &str| Error::Bootstrap(why.to_owned());
        let jwt = creds_block(creds, "BEGIN NATS USER JWT").ok_or_else(|| invalid("no user JWT"))?;
        let seed = creds_block(creds, "BEGIN USER NKEY SEED").ok_or_else(|| invalid("no seed"))?;
        let key = KeyPair::from_seed(seed).map_err(|e| Error::Bootstrap(format!("the seed: {e}")))?;
        let (claims, _) = verified_jwt(jwt).map_err(|e| Error::Bootstrap(e.to_string()))?;
        if claims.name != ants_proto::BOOTSTRAP_USER {
            return Err(Error::Bootstrap(format!("credentials of {}, not the bus's bootstrap ones", claims.name)));
        }
        if claims.sub != key.public_key() {
            return Err(invalid("a seed of another key than their JWT's"));
        }
        Ok(Self { jwt: jwt.to_owned(), key, account: claims.iss })
    }

    pub fn jwt(&self) -> &str {
        &self.jwt
    }

    pub fn public_key(&self) -> String {
        self.key.public_key()
    }

    /// The account of the team, which issues the JWTs a registration takes.
    pub fn account(&self) -> &str {
        &self.account
    }

    /// The signature of `nonce`, the server's, base64url without padding.
    pub fn sign(&self, nonce: &[u8]) -> Result<String, Error> {
        let signature = self.key.sign(nonce).map_err(|e| Error::Bootstrap(e.to_string()))?;
        Ok(BASE64URL_NOPAD.encode(&signature))
    }
}

/// The line after the line of a `.creds` file's text naming `marker`.
fn creds_block<'a>(creds: &'a str, marker: &str) -> Option<&'a str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains(marker))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

#[cfg(test)]
mod tests;
