//! Messages between two humans sealed so only their devices read them ([`Sealed`]): `ants ask @HUMAN` seals one for
//! every device of both humans whose encryption key the directory holds verified ([`Sealer`]), and a human's CLI opens
//! one through the entry of its own device, checked against the verified key of the sending device ([`Keyring`]). The
//! title of a thread between them is sealed the same way ([`SealedTitle`]), and the bytes of the files attached under
//! the message's content key ([`SealedAttachment`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use ants_daemon::keystore::{self, Store};
use std::ops::ControlFlow;

use ants_daemon::reading::{self, Reading};
use ants_daemon::sealing::{NONCE_LEN, open_message, open_plain, seal_envelope, seal_plain};
pub use ants_daemon::sealing::{Opening, State};
use ants_daemon::{creds, files};
use ants_proto::{
    Attachment, AttachmentHandle, DEVICES_BUCKET, DeviceId, DeviceKey, Envelope, MAX_ATTACHMENT_SIZE, Sealed,
    SealedAttachment, SealedBody, SealedTitle, ThreadTitle, UserId,
};
use anyhow::{Context as _, Result, anyhow, bail, ensure};
use async_nats::jetstream;
use async_nats::jetstream::consumer::DeliverPolicy;
use crypto_secretbox::aead::{Aead as _, AeadCore as _, KeyInit as _, OsRng};
use crypto_secretbox::{Key, Nonce, XSalsa20Poly1305};
use data_encoding::BASE64;
use nkeys::XKey;

use crate::attachments::{self, File};
use crate::devices::{self, Devices, Directory, Lookup, VerifiedDevice};

/// What `ants ask` may do instead of sealing.
pub const PLAINTEXT: &str = "`--plaintext` sends it in the clear";
/// What else may be done when `ants chat` cannot seal a message.
pub const PLAINTEXT_ASK: &str = "`ants ask --plaintext` sends it in the clear";
/// How the ciphertext of a message shows where nothing is opened: `ants watch` and dead letters.
pub fn size_note(sealed: &Sealed) -> String {
    format!("[sealed, {} bytes]", sealed.size())
}

/// Seal the text, suggested title and attachments of `envelope` for `recipients`, each a device and its key, from the
/// sending device `device` whose key is `xkey`, leaving its text empty, and its title and attachments none. `files`,
/// those the envelope carries, are sealed under the message's content key ([`seal_file`]), each becoming the file to
/// store, while their references go into the sealed body.
pub fn seal(
    envelope: &mut Envelope,
    files: &mut [File],
    device: DeviceId,
    xkey: &XKey,
    recipients: &[(DeviceId, &DeviceKey)],
) -> Result<()> {
    let key = XSalsa20Poly1305::generate_key(&mut OsRng);
    let mut attachments = std::mem::take(&mut envelope.attachments);
    for file in files.iter_mut() {
        let (reference, stored) = seal_file(&key, file)?;
        match attachments.iter_mut().find(|a| a.id == reference.id) {
            Some(attachment) => *attachment = reference,
            None => attachments.push(reference),
        }
        *file = stored;
    }
    seal_envelope(envelope, &key, attachments, device, xkey, recipients)
}

/// Seal the bytes of `file` under `key`, the content key of the message carrying it, with a nonce of their own: the
/// reference the message's sealed body carries ([`SealedAttachment`]), and the file to store, named
/// [`SealedAttachment::STORED_NAME`] of an unknown type, both with the size and SHA-256 of the ciphertext.
fn seal_file(key: &Key, file: &File) -> Result<(Attachment, File)> {
    let name = &file.attachment.name;
    let nonce = XSalsa20Poly1305::generate_nonce(&mut OsRng);
    let bytes = XSalsa20Poly1305::new(key)
        .encrypt(&nonce, file.bytes.as_slice())
        .map_err(|_| anyhow!("sealing attachment {name}"))?;
    ensure!(
        bytes.len() as u64 <= MAX_ATTACHMENT_SIZE,
        "{name} is {}: sealed, an attachment is at most {} less {} bytes; {PLAINTEXT}",
        attachments::size(file.bytes.len() as u64),
        attachments::size(MAX_ATTACHMENT_SIZE),
        SealedAttachment::OVERHEAD
    );
    let stored = Attachment {
        id: file.attachment.id,
        ..files::reference(SealedAttachment::STORED_NAME, Attachment::UNKNOWN_MIME, &bytes)?
    };
    let reference = Attachment {
        size: stored.size,
        sha256: stored.sha256.clone(),
        sealed: Some(SealedAttachment { nonce: BASE64.encode(&nonce) }),
        ..file.attachment.clone()
    };
    Ok((reference, File { attachment: stored, bytes }))
}

/// The bytes of the attachment `reference` names, sealed under `key`, the content key of the message carrying it, from
/// `stored`, those its store holds, which must match the reference's size and SHA-256.
pub fn open_attachment(key: &Key, reference: &Attachment, stored: &[u8]) -> Result<Vec<u8>> {
    let name = &reference.name;
    let sealing = reference.sealed.as_ref().with_context(|| format!("attachment {name} is not sealed"))?;
    ensure!(
        stored.len() as u64 == reference.size && files::sha256(stored) == reference.sha256,
        "attachment {name} does not match its reference: it changed or is incomplete"
    );
    let nonce = BASE64.decode(sealing.nonce.as_bytes()).ok().filter(|n| n.len() == NONCE_LEN);
    let nonce = nonce.with_context(|| format!("attachment {name} has no valid nonce: forged"))?;
    XSalsa20Poly1305::new(key)
        .decrypt(Nonce::from_slice(&nonce), stored)
        .map_err(|_| anyhow!("attachment {name} does not open: forged"))
}

/// Seal the title of `title` for `recipients` like a message ([`seal`]), leaving it empty.
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

/// Open the sealed part of `title` as [`open_message`] does a message's, giving its title.
pub fn open_title(title: &ThreadTitle, device: &DeviceId, xkey: &XKey, sender: Option<&DeviceKey>) -> Opening<String> {
    let Some(sealed) = &title.sealed else { return Opening::Unreadable };
    open_plain(sealed, &title.by, device, xkey, sender).then(|(_, plain)| {
        let body: SealedTitle = serde_json::from_slice(&plain).map_err(|_| "its body does not parse".to_owned())?;
        if (&body.thread_id, &body.by) != (&title.thread_id, &title.by) {
            return Err("its body names another title".to_owned());
        }
        Ok(body.title)
    })
}

/// What a human's device opens sealed messages and titles with: its own key, and the verified keys of the sending
/// devices.
pub struct Keyring {
    device: DeviceId,
    /// The device's key, read once a sealed message needs it; none when it cannot be read.
    xkey: OnceLock<Option<XKey>>,
    /// The credentials whose seed [`Keyring::xkey`] reads, from the file or `store`.
    creds: Option<PathBuf>,
    store: &'static dyn Store,
    /// What the directory says of a device of a human: its verified key, or none.
    keys: Mutex<HashMap<(UserId, DeviceId), Option<DeviceKey>>>,
    /// Whether [`Keyring::keys`] holds every device the directory verified, so any other has no verified key.
    complete: bool,
    /// The directory, opened when first needed through `opener` (the JetStream context and the account whose keys it
    /// takes) unless given; none when it cannot be.
    directory: tokio::sync::OnceCell<Option<Directory>>,
    opener: Option<(jetstream::Context, String)>,
}

impl Keyring {
    /// The keyring of the human's device of `nats`, reading the directory on `js`: none for others than a human's
    /// credentials binding an encryption key. A directory that cannot be read is warned of, and leaves every
    /// message sealed unreadable.
    pub async fn load(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Option<Self> {
        let own = own(nats.creds)?;
        let read = async {
            let account = nats.account.as_deref().context("credentials without an account")?;
            let directory = Directory::open(js, account).await?;
            let devices = directory.all().await?;
            anyhow::Ok((directory, devices))
        };
        let (directory, devices, complete) = match read.await {
            Ok((directory, devices)) => (Some(directory), devices, true),
            Err(e) => {
                tracing::warn!("not opening sealed messages, the directory {DEVICES_BUCKET} unread: {e:#}");
                (None, Devices::default(), false)
            }
        };
        let creds = nats.creds.map(Path::to_path_buf);
        Some(Self::new(own.device, OnceLock::new(), creds, &devices.verified, complete, directory))
    }

    /// The keyring of the human's device of the credentials `creds`, which reads the directory on `js` only once a
    /// sealed title needs a key, and the device's seed from the file or `store`: none for others than a human's
    /// credentials binding an encryption key.
    pub fn lazy(creds: Option<&Path>, store: &'static dyn Store, js: &jetstream::Context) -> Option<Self> {
        let own = own(creds)?;
        let account = devices::account_of(creds).ok().flatten()?;
        Some(Self {
            device: own.device,
            xkey: OnceLock::new(),
            creds: creds.map(Path::to_path_buf),
            store,
            keys: Mutex::default(),
            complete: false,
            directory: tokio::sync::OnceCell::new(),
            opener: Some((js.clone(), account)),
        })
    }

    fn new(
        device: DeviceId,
        xkey: OnceLock<Option<XKey>>,
        creds: Option<PathBuf>,
        verified: &[VerifiedDevice],
        complete: bool,
        directory: Option<Directory>,
    ) -> Self {
        // An assistant's device sends as its assistant, never as its human.
        let keys = verified.iter().map(|d| ((d.sender().clone(), d.device), Some(d.key.clone()))).collect();
        Self {
            device,
            xkey,
            creds,
            store: keystore::os(),
            keys: Mutex::new(keys),
            complete,
            directory: tokio::sync::OnceCell::new_with(Some(directory)),
            opener: None,
        }
    }

    fn xkey(&self) -> Option<&XKey> {
        self.xkey
            .get_or_init(|| {
                let path = self.creds.as_deref()?;
                match creds::load_xkey(path, self.store) {
                    Ok(xkey) => xkey,
                    Err(e) => {
                        tracing::warn!("not opening sealed messages: {e:#}");
                        None
                    }
                }
            })
            .as_ref()
    }

    async fn directory(&self) -> Option<&Directory> {
        let open = async {
            let (js, account) = self.opener.as_ref()?;
            Directory::open(js, account)
                .await
                .inspect_err(|e| {
                    tracing::warn!("not opening sealed titles, the directory {DEVICES_BUCKET} unread: {e:#}");
                })
                .ok()
        };
        self.directory.get_or_init(|| open).await.as_ref()
    }

    /// Look the sending device of a sealed `envelope` up in the directory unless known, as one sent after the
    /// keyring was loaded may name a device added since.
    pub async fn learn(&self, envelope: &Envelope) {
        if let Some(sealed) = &envelope.sealed {
            self.learn_device(&envelope.from, sealed.device).await;
        }
    }

    async fn learn_device(&self, human: &UserId, device: DeviceId) {
        let known = (human.clone(), device);
        if self.keys.lock().expect("keyring lock").contains_key(&known) {
            return;
        }
        let Some(directory) = self.directory().await else { return };
        let key = match directory.device(human, &device).await {
            Ok(Lookup::Verified(device)) => Some(device.key),
            Ok(Lookup::Missing | Lookup::Skipped(_)) => None,
            Err(e) => return tracing::warn!("not reading the key of device {device}: {e:#}"),
        };
        self.keys.lock().expect("keyring lock").insert(known, key);
    }

    /// The device's key and the verified key of the sending device `sealed` names, a device of `from`: none when the
    /// device cannot open it, as it holds no entry there, or no key, or the keyring does not know the sending device.
    fn keys_for(&self, from: &UserId, sealed: &Sealed) -> Option<(&XKey, Option<DeviceKey>)> {
        sealed.key_for(&self.device)?;
        let sender = match self.keys.lock().expect("keyring lock").get(&(from.clone(), sealed.device)) {
            Some(key) => key.clone(),
            None if self.complete => None,
            None => return None,
        };
        Some((self.xkey()?, sender))
    }

    /// Open the sealed part of `envelope`, if any.
    pub fn open(&self, envelope: &Envelope) -> Option<Opening> {
        Some(self.open_keyed(envelope)?.then(|(_, body)| Ok(body)))
    }

    /// [`Keyring::open`], giving the message's content key too ([`open_message`]).
    pub fn open_keyed(&self, envelope: &Envelope) -> Option<Opening<(Key, SealedBody)>> {
        let sealed = envelope.sealed.as_ref()?;
        let Some((xkey, sender)) = self.keys_for(&envelope.from, sealed) else { return Some(Opening::Unreadable) };
        let opening = open_message(envelope, &self.device, xkey, sender.as_ref());
        if let Opening::Forged(why) = &opening {
            tracing::warn!(message = %envelope.id, from = %envelope.from, "a forged sealed message: {why}");
        }
        Some(opening)
    }

    /// Open the sealed part of `title`, if any, looking its sending device up first.
    pub async fn open_title(&self, title: &ThreadTitle) -> Option<Opening<String>> {
        let sealed = title.sealed.as_ref()?;
        self.learn_device(&title.by, sealed.device).await;
        let Some((xkey, sender)) = self.keys_for(&title.by, sealed) else { return Some(Opening::Unreadable) };
        let opening = open_title(title, &self.device, xkey, sender.as_ref());
        if let Opening::Forged(why) = &opening {
            tracing::warn!(thread = %title.thread_id, by = %title.by, "a forged sealed title: {why}");
        }
        Some(opening)
    }
}

/// The device of the human's credentials `creds` if they bind an encryption key; an error reading them is warned of.
fn own(creds: Option<&Path>) -> Option<devices::Own> {
    devices::Own::from_creds(creds).unwrap_or_else(|e| {
        tracing::warn!("not opening sealed messages: {e:#}");
        None
    })
}

/// `title` as `keyring` reads it: if sealed, its title the opened one, else `[sealed]`, and no longer sealed.
pub async fn read_title(mut title: ThreadTitle, keyring: Option<&Keyring>) -> ThreadTitle {
    if title.sealed.is_none() {
        return title;
    }
    let opened = match keyring {
        Some(keyring) => keyring.open_title(&title).await,
        None => None,
    };
    title.title = match opened {
        Some(Opening::Opened(text)) => text,
        _ => State::Unreadable.placeholder().to_owned(),
    };
    title.sealed = None;
    title
}

/// `envelope` as `keyring` reads it, with what it made of it if sealed: opened, its text, suggested title and
/// attachments put back; else its text the placeholder its state shows ([`State::placeholder`]). Either way no longer
/// sealed. Without a keyring, every sealed message is unreadable.
pub fn read(mut envelope: Envelope, keyring: Option<&Keyring>) -> (Envelope, Option<State>) {
    if envelope.sealed.is_none() {
        return (envelope, None);
    }
    let opening = keyring.and_then(|k| k.open(&envelope)).unwrap_or(Opening::Unreadable);
    let state = opening.state();
    envelope.sealed = None;
    envelope.title = None;
    envelope.attachments = Vec::new();
    match opening {
        Opening::Opened(body) => {
            envelope.text = body.text;
            envelope.title = body.title;
            envelope.attachments = body.attachments;
        }
        _ => envelope.text = state.placeholder().to_owned(),
    }
    (envelope, Some(state))
}

/// What reading back the messages that may carry a sealed attachment finds ([`carrier`]).
pub enum Carrier {
    /// The message carrying it opened: its content key and the attachment's reference.
    Opened(Key, Attachment),
    /// No message opened carries it, but a sealed one that may does not open on this device: how the last such does not.
    Unopened(State),
    /// No sealed message carries it.
    None,
}

/// Look for the sealed message carrying the attachment under `handle` among those stored on the subject of its message
/// from the time in its id on, less the clocks' skew, as its bytes are stored before the message: opened with
/// `keyring`, every sealed message unopened without one.
pub async fn carrier(js: &jetstream::Context, keyring: Option<&Keyring>, handle: &AttachmentHandle) -> Result<Carrier> {
    let made = handle.id.get_timestamp().map(|t| {
        let (secs, nanos) = t.to_unix();
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::new(secs, nanos)
    });
    let start = made
        .and_then(|t| t.checked_sub(ants_daemon::channel::CLOCK_SLACK))
        .map_or(DeliverPolicy::All, |t| DeliverPolicy::ByStartTime { start_time: t.into() });
    let mut found = Carrier::None;
    reading::read_back(js, "", Reading::Gets, &handle.message_subject(), start, |_, _, subject, payload| {
        let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) else { return ControlFlow::Continue(()) };
        if envelope.sealed.is_none() || envelope.check_subject(subject).is_err() {
            return ControlFlow::Continue(());
        }
        match keyring.and_then(|k| k.open_keyed(&envelope)).unwrap_or(Opening::Unreadable) {
            Opening::Opened((key, body)) => {
                if let Some(reference) = body.attachments.into_iter().find(|a| a.id == handle.id) {
                    found = Carrier::Opened(key, reference);
                    return ControlFlow::Break(());
                }
            }
            unopened => found = Carrier::Unopened(unopened.state()),
        }
        ControlFlow::Continue(())
    })
    .await
    .with_context(|| format!("reading the messages that may carry attachment {handle}"))?;
    Ok(found)
}

/// What `ants ask` seals a message to another human with, and `ants thread title` the title of a thread between them:
/// the sending device and its key, the devices to seal for, and a keyring opening the replies.
pub struct Sealer {
    device: DeviceId,
    xkey: XKey,
    recipients: Vec<VerifiedDevice>,
    pub keyring: Keyring,
}

impl Sealer {
    /// What seals from the human of `nats` for itself and the human `to`, reading the directory on `js`: refused,
    /// with the fix and then `otherwise`, what else may be done, when the credentials bind no encryption key, the
    /// directory holds no verified key of the sending device or of any device of `to`.
    pub async fn new(nats: &crate::Nats<'_>, js: &jetstream::Context, to: &UserId, otherwise: &str) -> Result<Self> {
        Sealing::of(nats).sealer(js, to, otherwise).await
    }

    async fn with(sealing: &Sealing, js: &jetstream::Context, to: &UserId, otherwise: &str) -> Result<Self> {
        let me = &sealing.me;
        let Some(own) = devices::Own::from_creds(sealing.creds.as_deref())? else {
            bail!(
                "sealing for {me} and {to} takes an encryption key, which the credentials of {me} bind none of: \
                 `ants renew` binds one; {otherwise}"
            );
        };
        let path = sealing.creds.as_deref().context("no credentials")?;
        let xkey = creds::load_xkey(path, keystore::os())?
            .with_context(|| format!("no encryption key in {}: `ants renew` binds one; {otherwise}", path.display()))?;
        let account = sealing.account.as_deref().context("credentials without an account")?;
        let directory = Directory::open(js, account).await?;
        let humans = [me.clone(), to.clone()];
        let devices = directory
            .devices(&humans)
            .await
            .with_context(|| format!("reading the encryption keys of {me} and {to}; {otherwise}"))?;
        let own_key = crate::auth::device_key(&xkey)?;
        if !devices.of(me).any(|d| d.device == own.device && d.key == own_key) {
            bail!(
                "the encryption key of this device ({}) is not verified in {DEVICES_BUCKET}, so nothing is sealed: \
                 connecting with credentials that may put it there does (`ants renew` issues them); {otherwise}",
                own.device
            );
        }
        if !devices.of(to).any(|d| d.assistant.is_none()) {
            bail!(
                "{to} has no device whose encryption key is verified in {DEVICES_BUCKET}, so nothing is sealed: a \
                 device of {to} puts one there on connecting with credentials binding one (`ants renew`); {otherwise}"
            );
        }
        let mut recipients: Vec<VerifiedDevice> = Vec::new();
        for device in devices.of(me).chain(devices.of(to)) {
            if !recipients.iter().any(|d| d.device == device.device) {
                recipients.push(device.clone());
            }
        }
        let keyring = Keyring::new(
            own.device,
            OnceLock::from(Some(xkey.clone())),
            None,
            &devices.verified,
            false,
            Some(directory),
        );
        Ok(Self { device: own.device, xkey, recipients, keyring })
    }

    fn recipients(&self) -> Vec<(DeviceId, &DeviceKey)> {
        self.recipients.iter().map(|d| (d.device, &d.key)).collect()
    }

    /// Seal `envelope` and `files`, those it carries ([`seal`]).
    pub fn seal(&self, envelope: &mut Envelope, files: &mut [File]) -> Result<()> {
        seal(envelope, files, self.device, &self.xkey, &self.recipients())
    }

    /// Seal `title` ([`seal_title`]).
    pub fn seal_title(&self, title: &mut ThreadTitle) -> Result<()> {
        seal_title(title, self.device, &self.xkey, &self.recipients())
    }
}

/// What a view that outlives the [`crate::Nats`] it was opened with seals with ([`Sealer`]): its human, credentials
/// and their account.
#[derive(Debug, Clone)]
pub struct Sealing {
    me: UserId,
    creds: Option<PathBuf>,
    account: Option<String>,
}

impl Sealing {
    pub fn of(nats: &crate::Nats<'_>) -> Self {
        Self { me: nats.me.id.clone(), creds: nats.creds.map(Path::to_path_buf), account: nats.account.clone() }
    }

    pub fn me(&self) -> &UserId {
        &self.me
    }

    /// What seals for the human and `to` ([`Sealer::new`]).
    pub async fn sealer(&self, js: &jetstream::Context, to: &UserId, otherwise: &str) -> Result<Sealer> {
        Sealer::with(self, js, to, otherwise).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Device {
        human: UserId,
        id: DeviceId,
        xkey: XKey,
    }

    impl Device {
        fn new(human: &str) -> Self {
            Self { human: human.parse().unwrap(), id: DeviceId::generate(), xkey: XKey::new() }
        }

        fn key(&self) -> DeviceKey {
            self.xkey.public_key().parse().unwrap()
        }

        fn verified(&self) -> VerifiedDevice {
            VerifiedDevice {
                human: self.human.clone(),
                device: self.id,
                key: self.key(),
                user_key: String::new(),
                expires_at: None,
                assistant: None,
            }
        }

        /// A keyring of this device that knows `devices`, all the directory verified.
        fn keyring(&self, devices: &[&Self]) -> Keyring {
            let verified: Vec<_> = devices.iter().map(|d| d.verified()).collect();
            Keyring::new(self.id, OnceLock::from(Some(self.xkey.clone())), None, &verified, true, None)
        }
    }

    fn message(from: &Device, to: &Device) -> Envelope {
        let mut e = Envelope::new(from.human.clone(), to.human.clone(), "lunch at noon?".to_owned());
        e.title = Some("Lunch".to_owned());
        e
    }

    fn sealed(e: &Envelope, from: &Device, to: &[&Device]) -> Envelope {
        let mut e = e.clone();
        let keys: Vec<_> = to.iter().map(|d| d.key()).collect();
        let recipients: Vec<_> = to.iter().zip(&keys).map(|(d, k)| (d.id, k)).collect();
        seal(&mut e, &mut [], from.id, &from.xkey, &recipients).unwrap();
        e
    }

    #[test]
    fn every_device_sealed_for_opens_what_was_sealed() {
        let (alice, alice2, bob) = (Device::new("alice"), Device::new("alice"), Device::new("bob"));
        let plain = message(&alice, &bob);
        let e = sealed(&plain, &alice, &[&alice, &alice2, &bob]);
        assert_eq!((e.text.as_str(), &e.title), ("", &None));
        let sealing = e.sealed.as_ref().unwrap();
        assert_eq!(sealing.device, alice.id);
        assert_eq!(sealing.keys.iter().map(|k| k.device).collect::<Vec<_>>(), [alice.id, alice2.id, bob.id]);
        assert!(!serde_json::to_string(&e).unwrap().contains("lunch"));
        for reader in [&alice, &alice2, &bob] {
            let (opened, state) = read(e.clone(), Some(&reader.keyring(&[&alice, &bob])));
            assert_eq!(state, Some(State::Opened));
            assert_eq!(opened, plain);
        }
    }

    #[test]
    fn a_device_not_sealed_for_or_without_a_keyring_cannot_read_it() {
        let (alice, bob, carol) = (Device::new("alice"), Device::new("bob"), Device::new("carol"));
        let e = sealed(&message(&alice, &bob), &alice, &[&alice, &bob]);
        let (shown, state) = read(e.clone(), Some(&carol.keyring(&[&alice, &bob, &carol])));
        assert_eq!((shown.text.as_str(), state), ("[sealed]", Some(State::Unreadable)));
        assert_eq!(read(e.clone(), None).1, Some(State::Unreadable));
        let newer = Envelope { sealed: e.sealed.clone().map(|s| Box::new(Sealed { v: 2, ..*s })), ..e };
        assert_eq!(read(newer, Some(&bob.keyring(&[&alice, &bob]))).1, Some(State::Unreadable));
    }

    #[test]
    fn a_sender_device_without_its_verified_key_forges() {
        let (alice, bob, mallory) = (Device::new("alice"), Device::new("bob"), Device::new("alice"));
        let plain = message(&alice, &bob);
        // Not verified at all.
        let e = sealed(&plain, &mallory, &[&bob]);
        let reader = bob.keyring(&[&alice, &bob]);
        assert_eq!(
            read(e, Some(&reader)),
            (Envelope { text: "[forged]".into(), title: None, ..plain.clone() }, Some(State::Forged))
        );
        // Naming a verified device it does not hold the key of.
        let mut e = sealed(&plain, &mallory, &[&bob]);
        e.sealed.as_mut().unwrap().device = alice.id;
        assert_eq!(read(e, Some(&reader)).1, Some(State::Forged));
        // A device of another human than the sender.
        let bob2 = Device::new("bob");
        let e = sealed(&plain, &bob2, &[&bob]);
        assert_eq!(read(e, Some(&bob.keyring(&[&alice, &bob, &bob2]))).1, Some(State::Forged));
    }

    #[test]
    fn a_body_tampered_with_or_naming_another_message_forges() {
        let (alice, bob) = (Device::new("alice"), Device::new("bob"));
        let plain = message(&alice, &bob);
        let reader = bob.keyring(&[&alice, &bob]);
        let mut e = sealed(&plain, &alice, &[&alice, &bob]);
        let mut body = BASE64.decode(e.sealed.as_ref().unwrap().body.as_bytes()).unwrap();
        *body.last_mut().unwrap() ^= 1;
        e.sealed.as_mut().unwrap().body = BASE64.encode(&body);
        assert_eq!(read(e, Some(&reader)).1, Some(State::Forged));
        let e = sealed(&plain, &alice, &[&alice, &bob]);
        let moved = Envelope { thread_id: "other".parse().unwrap(), ..e.clone() };
        assert_eq!(read(moved, Some(&reader)).1, Some(State::Forged));
        let other = Envelope { id: ants_proto::Uuid::now_v7(), ..e };
        assert_eq!(read(other, Some(&reader)).1, Some(State::Forged));
    }

    #[test]
    fn an_unknown_sender_device_is_unreadable_until_the_directory_is_read() {
        let (alice, bob) = (Device::new("alice"), Device::new("bob"));
        let e = sealed(&message(&alice, &bob), &alice, &[&bob]);
        let reader = Keyring::new(bob.id, OnceLock::from(Some(bob.xkey)), None, &[], false, None);
        assert_eq!(read(e, Some(&reader)).1, Some(State::Unreadable));
    }

    #[tokio::test]
    async fn a_sealed_title_opens_only_for_its_devices_and_as_set() {
        let (alice, bob, carol) = (Device::new("alice"), Device::new("bob"), Device::new("carol"));
        let mut title = ThreadTitle::new(ants_proto::ThreadId::generate(), "Team lunch", alice.human.clone()).unwrap();
        let keys = [alice.key(), bob.key()];
        seal_title(&mut title, alice.id, &alice.xkey, &[(alice.id, &keys[0]), (bob.id, &keys[1])]).unwrap();
        assert!(title.title.is_empty() && !serde_json::to_string(&title).unwrap().contains("lunch"));
        let shown = async |title: &ThreadTitle, reader: Option<&Keyring>| read_title(title.clone(), reader).await.title;
        assert_eq!(shown(&title, Some(&bob.keyring(&[&alice, &bob]))).await, "Team lunch");
        assert_eq!(shown(&title, Some(&carol.keyring(&[&alice, &bob, &carol]))).await, "[sealed]");
        assert_eq!(shown(&title, None).await, "[sealed]");
        let moved = ThreadTitle { thread_id: ants_proto::ThreadId::generate(), ..title.clone() };
        let reader = bob.keyring(&[&alice, &bob]);
        assert!(matches!(reader.open_title(&moved).await, Some(Opening::Forged(_))), "names another thread");
        let claimed = ThreadTitle { by: bob.human.clone(), ..title };
        assert!(matches!(reader.open_title(&claimed).await, Some(Opening::Forged(_))), "set by another human");
    }

    /// Alice seals for both humans' devices in the directory; Bob opens it from his inbox and from the thread, a
    /// third human's device reads `[sealed]`, and a human with no verified key gets nothing sealed to it.
    #[tokio::test]
    async fn a_message_sealed_between_two_humans_opens_only_on_their_devices() {
        use ants_proto::Role;

        use crate::auth::tests::Server;

        let Some(server) = Server::start() else { return };
        let (alice, bob, carol, dave): (UserId, UserId, UserId, UserId) =
            ("alice".parse().unwrap(), "bob".parse().unwrap(), "carol".parse().unwrap(), "dave".parse().unwrap());
        let creds: Vec<_> = [&alice, &bob, &carol].iter().map(|h| server.creds_as(h, Role::Human)).collect();
        let nats: Vec<_> = creds.iter().map(|c| crate::Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let mut js = Vec::new();
        for n in &nats {
            js.push(n.connect("test").await.unwrap().1);
        }

        let sealer = Sealer::new(&nats[0], &js[0], &bob, PLAINTEXT).await.unwrap();
        let mut e = Envelope::new(alice.clone(), bob.clone(), "the launch slips a week".to_owned());
        sealer.seal(&mut e, &mut []).unwrap();
        let sealing = e.sealed.as_ref().unwrap();
        let mut sealed_for: Vec<_> = sealing.keys.iter().map(|k| k.device).collect();
        sealed_for.sort();
        let mut expected = vec![nats[0].device.unwrap(), nats[1].device.unwrap()];
        expected.sort();
        assert_eq!(sealed_for, expected, "every device of both humans, and no other");
        ants_daemon::bus::publish(&js[0], &e).await.unwrap();

        let bob_keyring = Keyring::load(&nats[1], &js[1]).await.unwrap();
        let unread = crate::inbox::Inbox::open(&js[1], &bob).await.unwrap().unread(&Default::default()).await.unwrap();
        let [got] = unread.as_slice() else { panic!("not one message: {unread:?}") };
        let (opened, state) = read(got.clone(), Some(&bob_keyring));
        assert_eq!((opened.text.as_str(), state), ("the launch slips a week", Some(State::Opened)));

        let shown = |keyring: Option<Keyring>, js: &jetstream::Context| {
            let (js, thread) = (js.clone(), e.thread_id.clone());
            async move {
                let entries =
                    crate::thread::history(&js, &thread, None, false, keyring.map(std::sync::Arc::new)).await.unwrap();
                entries.into_iter().map(|e| (e.text, e.sealed)).collect::<Vec<_>>()
            }
        };
        let opened = ("the launch slips a week".to_owned(), Some(State::Opened));
        assert_eq!(shown(Keyring::load(&nats[0], &js[0]).await, &js[0]).await, std::slice::from_ref(&opened));
        assert_eq!(shown(Some(bob_keyring), &js[1]).await, [opened]);
        let unreadable = ("[sealed]".to_owned(), Some(State::Unreadable));
        assert_eq!(shown(Keyring::load(&nats[2], &js[2]).await, &js[2]).await, std::slice::from_ref(&unreadable));
        assert_eq!(shown(None, &js[1]).await, [unreadable]);

        let Err(refused) = Sealer::new(&nats[0], &js[0], &dave, PLAINTEXT).await else {
            panic!("sealed for no device")
        };
        let refused = refused.to_string();
        assert!(refused.contains("dave has no device") && refused.contains("--plaintext"), "{refused}");
    }

    #[test]
    fn attachments_are_sealed_under_the_content_key_and_open_only_with_it() {
        let (alice, bob) = (Device::new("alice"), Device::new("bob"));
        let plain = b"quarterly numbers, not for the hub".to_vec();
        let file =
            File { attachment: files::reference("numbers.csv", "text/csv", &plain).unwrap(), bytes: plain.clone() };
        let mut e = message(&alice, &bob);
        e.attachments = vec![file.attachment.clone()];
        let mut sealed_files = vec![file.clone()];
        let keys = [alice.key(), bob.key()];
        seal(&mut e, &mut sealed_files, alice.id, &alice.xkey, &[(alice.id, &keys[0]), (bob.id, &keys[1])]).unwrap();

        let [stored] = sealed_files.as_slice() else { panic!("not one file: {sealed_files:?}") };
        let a = &stored.attachment;
        assert_eq!((a.id, a.name.as_str(), a.mime.as_str()), (file.attachment.id, "sealed", Attachment::UNKNOWN_MIME));
        assert_eq!((a.size, a.sha256.as_str()), (plain.len() as u64 + 16, files::sha256(&stored.bytes).as_str()));
        assert!(a.sealed.is_none(), "the store holds no nonce");
        assert!(!stored.bytes.windows(plain.len()).any(|w| w == plain.as_slice()));
        assert!(e.attachments.is_empty());
        let json = serde_json::to_string(&e).unwrap();
        assert!(!json.contains("numbers") && !json.contains(&file.attachment.sha256), "{json}");

        let reader = bob.keyring(&[&alice, &bob]);
        let Some(Opening::Opened((key, body))) = reader.open_keyed(&e) else { panic!("does not open") };
        let [reference] = body.attachments.as_slice() else { panic!("not one reference: {body:?}") };
        assert_eq!((reference.id, reference.name.as_str(), reference.mime.as_str()), (a.id, "numbers.csv", "text/csv"));
        assert_eq!((reference.size, &reference.sha256), (a.size, &a.sha256), "describing the bytes as stored");
        assert!(reference.sealed.is_some());
        assert_eq!(open_attachment(&key, reference, &stored.bytes).unwrap(), plain);
        assert_eq!(read(e, Some(&reader)).0.attachments, body.attachments);

        let mut tampered = stored.bytes.clone();
        tampered[0] ^= 1;
        let changed = open_attachment(&key, reference, &tampered).unwrap_err().to_string();
        assert!(changed.contains("does not match its reference"), "{changed}");
        let lying = Attachment { sha256: files::sha256(&tampered), ..reference.clone() };
        assert!(open_attachment(&key, &lying, &tampered).unwrap_err().to_string().contains("forged"));
        let other = XSalsa20Poly1305::generate_key(&mut OsRng);
        assert!(open_attachment(&other, reference, &stored.bytes).unwrap_err().to_string().contains("forged"));
    }

    #[test]
    fn a_file_too_big_once_sealed_is_refused() {
        let (alice, bob) = (Device::new("alice"), Device::new("bob"));
        let bytes = vec![0; usize::try_from(MAX_ATTACHMENT_SIZE).unwrap()];
        let file = File { attachment: files::reference("big.bin", Attachment::UNKNOWN_MIME, &bytes).unwrap(), bytes };
        let mut e = message(&alice, &bob);
        let key = bob.key();
        let refused = seal(&mut e, &mut [file], alice.id, &alice.xkey, &[(bob.id, &key)]).unwrap_err().to_string();
        assert!(refused.contains("big.bin is 8.0 MiB: sealed, an attachment is at most 8.0 MiB less 16"), "{refused}");
    }

    /// `ants ask --attach` from Alice to Bob seals the file with the message: the store holds only its ciphertext,
    /// named `sealed`, Bob's inbox lists it by its name, and `ants attachment get` saves it as attached on both
    /// humans' devices, while a third human's device writes nothing.
    #[tokio::test]
    async fn a_file_attached_to_a_sealed_message_is_saved_only_on_their_devices() {
        use ants_proto::Role;

        use crate::auth::tests::Server;

        let Some(server) = Server::start() else { return };
        let (alice, bob, carol): (UserId, UserId, UserId) =
            ("alice".parse().unwrap(), "bob".parse().unwrap(), "carol".parse().unwrap());
        let creds: Vec<_> = [&alice, &bob, &carol].iter().map(|h| server.creds_as(h, Role::Human)).collect();
        let nats: Vec<_> = creds.iter().map(|c| crate::Nats::new(&server.url, Some(c), None).unwrap()).collect();
        let mut js = Vec::new();
        for n in &nats {
            js.push(n.connect("test").await.unwrap().1);
        }
        js[0].create_stream(ants_daemon::hub::files_config()).await.unwrap();
        let dir = server.dir().join("sealed-files");
        std::fs::create_dir_all(&dir).unwrap();
        let plain = b"# Plan\nship on Friday\n".to_vec();
        std::fs::write(dir.join("plan.md"), &plain).unwrap();

        let attach = attachments::Attach { paths: vec![dir.join("plan.md")] };
        let words = vec!["@bob".to_owned(), "the plan".to_owned()];
        let (sending, replies, input) = (Default::default(), Default::default(), Default::default());
        Box::pin(crate::cmd::send::ask(&nats[0], None, sending, &replies, &input, &attach, words)).await.unwrap();

        let unread = crate::inbox::Inbox::open(&js[1], &bob).await.unwrap().unread(&Default::default()).await.unwrap();
        let [got] = unread.as_slice() else { panic!("not one message: {unread:?}") };
        assert!(got.attachments.is_empty() && got.sealed.is_some());
        let keyring = Keyring::load(&nats[1], &js[1]).await.unwrap();
        let (opened, _) = read(got.clone(), Some(&keyring));
        let [listed] = attachments::listed_on(&opened.subject(), &opened.attachments).try_into().unwrap();
        let handle = listed.handle.clone();
        assert_eq!(listed.line(), format!("📎 plan.md ({} B) {handle}", plain.len()));
        let (stored, bytes) = files::get(&js[2], &handle, None).await.unwrap();
        assert_eq!((stored.name.as_str(), stored.mime.as_str()), ("sealed", Attachment::UNKNOWN_MIME));
        assert_ne!(bytes, plain);

        let out = dir.join("bob");
        std::fs::create_dir(&out).unwrap();
        attachments::get(&nats[1], &handle, Some(&out)).await.unwrap();
        assert_eq!(std::fs::read(out.join("plan.md")).unwrap(), plain);
        attachments::get(&nats[0], &handle, Some(&dir.join("alice.md"))).await.unwrap();
        assert_eq!(std::fs::read(dir.join("alice.md")).unwrap(), plain);
        let refused = attachments::get(&nats[2], &handle, Some(&dir.join("carol.md"))).await.unwrap_err().to_string();
        assert!(refused.contains("is sealed with a message this device does not open ([sealed])"), "{refused}");
        assert!(!dir.join("carol.md").exists());
    }

    #[test]
    fn the_size_shown_is_that_of_the_ciphertext() {
        let (alice, bob) = (Device::new("alice"), Device::new("bob"));
        let e = sealed(&message(&alice, &bob), &alice, &[&bob]);
        let sealed = e.sealed.unwrap();
        let size = BASE64.decode(sealed.body.as_bytes()).unwrap().len();
        assert_eq!(size_note(&sealed), format!("[sealed, {size} bytes]"));
    }
}
