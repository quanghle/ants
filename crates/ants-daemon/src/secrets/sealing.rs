//! Sealed messages between humans ([`Sealed`]), as both the CLI and an assistant's daemon seal and open them: the body
//! is encrypted under a random content key, which is sealed for each device that may open it ([`seal_envelope`],
//! [`open_message`]).
//!
//! An assistant its human enrolled as one of its devices ([`ants_proto::AssistantGrant::sealing`]) opens its human's
//! sealed messages and seals messages it sends for its human with its sealing key ([`AssistantSealing`]), whose seed
//! waits beside its credentials ([`key_path`]); only while the grant standing enrolls that key. It seals the titles
//! it sets for its human on threads whose messages are sealed the same way ([`seal_title`]).

use std::path::{Path, PathBuf};

use ants_client::sealed::State;
use ants_proto::{
    Attachment, DeviceId, DeviceKey, Envelope, SEALED_VERSION, Sealed, SealedBody, SealedKey, SealedTitle, ThreadTitle,
    UserId,
};
use anyhow::{Context as _, Result, anyhow, bail};
use crypto_secretbox::aead::{Aead as _, AeadCore as _, KeyInit as _, OsRng};
use crypto_secretbox::{Key, Nonce, XSalsa20Poly1305};
use data_encoding::BASE64;
use nkeys::XKey;

use crate::people::devices::{Directory, Lookup};
use crate::people::humans::Registry;

/// The size of a content key.
pub const KEY_LEN: usize = 32;
/// The size of the nonce a sealed body starts with.
pub const NONCE_LEN: usize = 24;

/// What opening one sealed message, or title, gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Opening<B = SealedBody> {
    Opened(B),
    Unreadable,
    /// Why.
    Forged(String),
}

impl<B> Opening<B> {
    pub fn state(&self) -> State {
        match self {
            Self::Opened(_) => State::Opened,
            Self::Unreadable => State::Unreadable,
            Self::Forged(_) => State::Forged,
        }
    }

    /// What `check` makes of what opened: forged when it says why not.
    pub fn then<C>(self, check: impl FnOnce(B) -> Result<C, String>) -> Opening<C> {
        match self {
            Self::Opened(b) => check(b).map_or_else(Opening::Forged, Opening::Opened),
            Self::Unreadable => Opening::Unreadable,
            Self::Forged(why) => Opening::Forged(why),
        }
    }
}

/// Seal the text, suggested title and quote of `envelope`, and `attachments`, the references of the files it carries,
/// under the content key `key` for `recipients`, each a device and its key, from the sending device `device` whose key
/// is `xkey`, leaving its text empty, and its title, quote and attachments none.
pub fn seal_envelope(
    envelope: &mut Envelope,
    key: &Key,
    attachments: Vec<Attachment>,
    device: DeviceId,
    xkey: &XKey,
    recipients: &[(DeviceId, &DeviceKey)],
) -> Result<()> {
    let body = SealedBody {
        from: envelope.from.clone(),
        to: envelope.to.clone(),
        thread_id: envelope.thread_id.clone(),
        id: envelope.id,
        text: std::mem::take(&mut envelope.text),
        title: envelope.title.take(),
        quote: envelope.quote.take(),
        attachments,
    };
    envelope.attachments = Vec::new();
    envelope.sealed = Some(Box::new(seal_plain(&serde_json::to_vec(&body)?, key, device, xkey, recipients)?));
    Ok(())
}

/// Seal the title of `title` for `recipients` like a message ([`seal_envelope`]), leaving it empty.
pub fn seal_title(
    title: &mut ThreadTitle,
    device: DeviceId,
    xkey: &XKey,
    recipients: &[(DeviceId, &DeviceKey)],
) -> Result<()> {
    let body = SealedTitle {
        thread_id: title.thread_id.clone(),
        by: title.by.clone(),
        title: std::mem::take(&mut title.title),
    };
    let key = XSalsa20Poly1305::generate_key(&mut OsRng);
    title.sealed = Some(Box::new(seal_plain(&serde_json::to_vec(&body)?, &key, device, xkey, recipients)?));
    Ok(())
}

/// Seal `plain` under `key` for `recipients` from the sending device `device` whose key is `xkey`.
pub fn seal_plain(
    plain: &[u8],
    key: &Key,
    device: DeviceId,
    xkey: &XKey,
    recipients: &[(DeviceId, &DeviceKey)],
) -> Result<Sealed> {
    let nonce = XSalsa20Poly1305::generate_nonce(&mut OsRng);
    let ciphertext = XSalsa20Poly1305::new(key).encrypt(&nonce, plain).map_err(|_| anyhow!("sealing"))?;
    let keys = recipients
        .iter()
        .map(|(to, public)| {
            let recipient = XKey::from_public_key(public.as_str())
                .map_err(|e| anyhow!("the encryption key {public} of device {to}: {e}"))?;
            let sealed = xkey.seal(key, &recipient).map_err(|e| anyhow!("sealing the key for device {to}: {e}"))?;
            Ok(SealedKey { device: *to, key: BASE64.encode(&sealed) })
        })
        .collect::<Result<_>>()?;
    let body = BASE64.encode(&[nonce.as_slice(), &ciphertext].concat());
    Ok(Sealed { v: SEALED_VERSION, device, keys, body })
}

/// Open the sealed part of `envelope` as device `device` whose key is `xkey`, with `sender`, the verified key of the
/// sending device it names, if any: its body, and its content key, which opens its attachments.
pub fn open_message(
    envelope: &Envelope,
    device: &DeviceId,
    xkey: &XKey,
    sender: Option<&DeviceKey>,
) -> Opening<(Key, SealedBody)> {
    let Some(sealed) = &envelope.sealed else { return Opening::Unreadable };
    open_plain(sealed, &envelope.from, device, xkey, sender).then(|(key, plain)| {
        let body: SealedBody = serde_json::from_slice(&plain).map_err(|_| "its body does not parse".to_owned())?;
        if (&body.from, &body.to, &body.thread_id, body.id)
            != (&envelope.from, &envelope.to, &envelope.thread_id, envelope.id)
        {
            return Err("its body names another message".to_owned());
        }
        Ok((key, body))
    })
}

/// The content key and plaintext of `sealed`, sent by `from`, as device `device` opens it.
pub fn open_plain(
    sealed: &Sealed,
    from: &UserId,
    device: &DeviceId,
    xkey: &XKey,
    sender: Option<&DeviceKey>,
) -> Opening<(Key, Vec<u8>)> {
    if sealed.v != SEALED_VERSION {
        return Opening::Unreadable;
    }
    let Some(entry) = sealed.key_for(device) else { return Opening::Unreadable };
    open_body(sealed, from, &entry.key, xkey, sender).map_or_else(Opening::Forged, Opening::Opened)
}

fn open_body(
    sealed: &Sealed,
    from: &UserId,
    key: &str,
    xkey: &XKey,
    sender: Option<&DeviceKey>,
) -> Result<(Key, Vec<u8>), String> {
    let Some(sender) = sender else {
        return Err(format!("the sending device {} of {from} has no verified encryption key", sealed.device));
    };
    let sender = XKey::from_public_key(sender.as_str()).map_err(|e| e.to_string())?;
    let key = BASE64.decode(key.as_bytes()).map_err(|_| "its key is not base64".to_owned())?;
    let key = xkey.open(&key, &sender).map_err(|_| "its key does not open".to_owned())?;
    if key.len() != KEY_LEN {
        return Err("its key does not open".to_owned());
    }
    let body = BASE64.decode(sealed.body.as_bytes()).map_err(|_| "its body is not base64".to_owned())?;
    if body.len() < NONCE_LEN {
        return Err("its body does not open".to_owned());
    }
    let (nonce, ciphertext) = body.split_at(NONCE_LEN);
    let key = *Key::from_slice(&key);
    let plain = XSalsa20Poly1305::new(&key)
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| "its body does not open".to_owned())?;
    Ok((key, plain))
}

/// Where the seed of the sealing key of the ant whose credentials are at `creds` waits: beside them, as `<name>.xk`.
pub fn key_path(creds: &Path) -> PathBuf {
    creds.with_extension("xk")
}

/// The sealing key of the ant whose credentials are at `creds`, if one was made ([`key_path`]).
pub fn load_key(creds: &Path) -> Result<Option<XKey>> {
    let path = key_path(creds);
    let seed = match std::fs::read_to_string(&path) {
        Ok(seed) => seed,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading the sealing key {}", path.display())),
    };
    XKey::from_seed(seed.trim()).map(Some).map_err(|e| anyhow!("the sealing key {}: {e}", path.display()))
}

/// What an assistant ant opens its human's sealed messages, and seals those it sends for its human, with: its sealing
/// key, the grants its daemon keeps, and the directory of device keys, which its credentials must read. It does
/// either only while the grant of the human standing names the ant and enrolls that key
/// ([`AssistantSealing::device_for`]), so a withdrawal, or a grant without it, ends both as soon as the daemon's
/// watch delivers it.
pub struct AssistantSealing {
    ant: UserId,
    xkey: XKey,
    humans: Registry,
    directory: Directory,
}

impl AssistantSealing {
    pub fn new(ant: UserId, xkey: XKey, humans: Registry, directory: Directory) -> Self {
        Self { ant, xkey, humans, directory }
    }

    /// The device `human` enrolled the ant as by a grant standing, if it did, for the ant's own key.
    pub fn device_for(&self, human: &UserId) -> Option<DeviceId> {
        let grant = self.humans.grant(human)?;
        let sealing = grant.sealing.filter(|s| s.xkey.as_str() == self.xkey.public_key())?;
        (grant.assistant.as_ref() == Some(&self.ant)).then_some(sealing.device)
    }

    /// Open `envelope`, a sealed message from or to `human` or one the ant sent: its body once opened; unreadable
    /// when the ant is not enrolled as a device of `human` now, the message involves neither, or was not sealed for
    /// the device; forged when it does not open with the verified key of its sending device.
    pub async fn open(&self, human: &UserId, envelope: &Envelope) -> Result<Opening> {
        let Some(sealed) = &envelope.sealed else { return Ok(Opening::Unreadable) };
        let involved = [&envelope.from, &envelope.to].iter().any(|u| **u == *human || **u == self.ant);
        let Some(device) = self.device_for(human).filter(|_| involved) else { return Ok(Opening::Unreadable) };
        if sealed.key_for(&device).is_none() {
            return Ok(Opening::Unreadable);
        }
        let sender = match self.directory.device(&envelope.from, &sealed.device).await? {
            Lookup::Verified(found) => Some(found.key),
            Lookup::Missing | Lookup::Skipped(_) => None,
        };
        let opening = open_message(envelope, &device, &self.xkey, sender.as_ref()).then(|(_, body)| Ok(body));
        if let Opening::Forged(why) = &opening {
            tracing::warn!(message = %envelope.id, from = %envelope.from, "a forged sealed message: {why}");
        }
        Ok(opening)
    }

    /// Seal `envelope`, which the ant sends for `human`, for every device of `human` and of its recipient with a
    /// verified key, the ant's own among them: refused when the ant is not enrolled as a device of `human` now, the
    /// envelope is not from the ant or carries attachments, or its recipient has no device of its own with a verified
    /// key.
    pub async fn seal(&self, human: &UserId, envelope: &mut Envelope) -> Result<()> {
        let ant = &self.ant;
        if envelope.from != *ant {
            bail!("{ant} seals only what it sends itself, not a message from {}", envelope.from);
        }
        if !envelope.attachments.is_empty() {
            bail!("{ant} does not seal attachments");
        }
        let (device, recipients) = self.recipients(human, &envelope.to.clone()).await?;
        let recipients: Vec<(DeviceId, &DeviceKey)> = recipients.iter().map(|(id, key)| (*id, key)).collect();
        let key = XSalsa20Poly1305::generate_key(&mut OsRng);
        seal_envelope(envelope, &key, Vec::new(), device, &self.xkey, &recipients)
    }

    /// Seal `title`, which the ant sets for `human` on a thread between `human` and `other` whose messages are
    /// sealed, for every device of both with a verified key, as [`AssistantSealing::seal`] seals a message.
    pub async fn seal_title(&self, human: &UserId, other: &UserId, title: &mut ThreadTitle) -> Result<()> {
        let ant = &self.ant;
        if title.by != *ant {
            bail!("{ant} seals only the titles it sets itself, not one by {}", title.by);
        }
        let (device, recipients) = self.recipients(human, other).await?;
        let recipients: Vec<(DeviceId, &DeviceKey)> = recipients.iter().map(|(id, key)| (*id, key)).collect();
        seal_title(title, device, &self.xkey, &recipients)
    }

    /// The device the ant is enrolled as for `human`, and the devices with a verified key of `human`, the ant's
    /// among them, and of `to`: refused when the ant is not enrolled now, or `to` has no device of its own.
    async fn recipients(&self, human: &UserId, to: &UserId) -> Result<(DeviceId, Vec<(DeviceId, DeviceKey)>)> {
        let ant = &self.ant;
        let Some(device) = self.device_for(human) else {
            bail!("{human} has not enrolled {ant} as one of its devices: `ants assistant seal on` does");
        };
        let devices = self.directory.devices(&[human.clone(), to.clone()]).await?;
        let own = self.xkey.public_key();
        if !devices.of(human).any(|d| d.device == device && d.key.as_str() == own) {
            bail!("the device {device} of {ant} is not verified in the directory: its grant does not read back");
        }
        if !devices.of(to).any(|d| d.assistant.is_none()) {
            bail!("{to} has no device whose encryption key is verified, so nothing is sealed");
        }
        let mut recipients: Vec<(DeviceId, DeviceKey)> = Vec::new();
        for d in devices.of(human).chain(devices.of(to)) {
            if !recipients.iter().any(|(id, _)| *id == d.device) {
                recipients.push((d.device, d.key.clone()));
            }
        }
        Ok((device, recipients))
    }
}
