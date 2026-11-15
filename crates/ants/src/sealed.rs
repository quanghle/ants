//! Messages between two humans sealed so only their devices read them ([`Sealed`]): `ants ask @HUMAN` seals one for
//! every device of both humans whose encryption key the directory holds verified ([`Sealer`]), and a human's CLI opens
//! one through the entry of its own device, checked against the verified key of the sending device ([`Keyring`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use ants_daemon::{creds, keystore};
use ants_proto::{
    DEVICES_BUCKET, DeviceId, DeviceKey, Envelope, SEALED_VERSION, Sealed, SealedBody, SealedKey, UserId,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use crypto_secretbox::aead::{Aead as _, AeadCore as _, KeyInit as _, OsRng};
use crypto_secretbox::{Nonce, XSalsa20Poly1305};
use data_encoding::BASE64;
use nkeys::XKey;
use serde::Serialize;

use crate::devices::{self, Devices, Directory, Lookup, VerifiedDevice};

/// The size of a content key.
const KEY_LEN: usize = 32;
/// The size of the nonce a sealed body starts with.
const NONCE_LEN: usize = 24;

/// What a reader makes of a sealed message, as `--json` output says it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Opened,
    /// Not sealed for the reader's device, or not openable by this CLI.
    Unreadable,
    /// Its key or body does not open with the verified key of the sending device it names, which has none, or its
    /// body disagrees with its envelope.
    Forged,
}

impl State {
    /// What a reader shows of a message it does not open.
    pub fn placeholder(self) -> &'static str {
        match self {
            Self::Opened => "",
            Self::Unreadable => "[sealed]",
            Self::Forged => "[forged]",
        }
    }
}

/// What opening one sealed message gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Opening {
    Opened(SealedBody),
    Unreadable,
    /// Why.
    Forged(String),
}

impl Opening {
    pub fn state(&self) -> State {
        match self {
            Self::Opened(_) => State::Opened,
            Self::Unreadable => State::Unreadable,
            Self::Forged(_) => State::Forged,
        }
    }
}

/// How the ciphertext of a message shows where nothing is opened: `ants watch` and dead letters.
pub fn size_note(sealed: &Sealed) -> String {
    format!("[sealed, {} bytes]", sealed.size())
}

/// Seal the text, suggested title and attachments of `envelope` for `recipients`, each a device and its key, from the
/// sending device `device` whose key is `xkey`, leaving its text empty, and its title and attachments none.
pub fn seal(
    envelope: &mut Envelope,
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
        attachments: std::mem::take(&mut envelope.attachments),
    };
    let plain = serde_json::to_vec(&body)?;
    let key = XSalsa20Poly1305::generate_key(&mut OsRng);
    let nonce = XSalsa20Poly1305::generate_nonce(&mut OsRng);
    let ciphertext =
        XSalsa20Poly1305::new(&key).encrypt(&nonce, plain.as_slice()).map_err(|_| anyhow!("sealing the message"))?;
    let keys = recipients
        .iter()
        .map(|(to, public)| {
            let recipient = XKey::from_public_key(public.as_str())
                .map_err(|e| anyhow!("the encryption key {public} of device {to}: {e}"))?;
            let sealed =
                xkey.seal(&key, &recipient).map_err(|e| anyhow!("sealing the message's key for device {to}: {e}"))?;
            Ok(SealedKey { device: *to, key: BASE64.encode(&sealed) })
        })
        .collect::<Result<_>>()?;
    let body = BASE64.encode(&[nonce.as_slice(), &ciphertext].concat());
    envelope.sealed = Some(Box::new(Sealed { v: SEALED_VERSION, device, keys, body }));
    Ok(())
}

/// Open the sealed part of `envelope` as device `device` whose key is `xkey`, with `sender`, the verified key of the
/// sending device it names, if any.
pub fn open(envelope: &Envelope, device: &DeviceId, xkey: &XKey, sender: Option<&DeviceKey>) -> Opening {
    let Some(sealed) = &envelope.sealed else { return Opening::Unreadable };
    if sealed.v != SEALED_VERSION {
        return Opening::Unreadable;
    }
    let Some(entry) = sealed.key_for(device) else { return Opening::Unreadable };
    match open_body(envelope, sealed, &entry.key, xkey, sender) {
        Ok(body) => Opening::Opened(body),
        Err(why) => Opening::Forged(why),
    }
}

fn open_body(
    envelope: &Envelope,
    sealed: &Sealed,
    key: &str,
    xkey: &XKey,
    sender: Option<&DeviceKey>,
) -> Result<SealedBody, String> {
    let Some(sender) = sender else {
        return Err(format!(
            "the sending device {} of {} has no verified encryption key",
            sealed.device, envelope.from
        ));
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
    let cipher = XSalsa20Poly1305::new_from_slice(&key).map_err(|_| "its key does not open".to_owned())?;
    let plain =
        cipher.decrypt(Nonce::from_slice(nonce), ciphertext).map_err(|_| "its body does not open".to_owned())?;
    let body: SealedBody = serde_json::from_slice(&plain).map_err(|_| "its body does not parse".to_owned())?;
    if (&body.from, &body.to, &body.thread_id, body.id)
        != (&envelope.from, &envelope.to, &envelope.thread_id, envelope.id)
    {
        return Err("its body names another message".to_owned());
    }
    Ok(body)
}

/// What a human's device opens sealed messages with: its own key, and the verified keys of the sending devices.
pub struct Keyring {
    device: DeviceId,
    /// The device's key, read once a sealed message needs it; none when it cannot be read.
    xkey: OnceLock<Option<XKey>>,
    /// The credentials whose seed [`Keyring::xkey`] reads.
    creds: Option<PathBuf>,
    /// What the directory says of a device of a human: its verified key, or none.
    keys: Mutex<HashMap<(UserId, DeviceId), Option<DeviceKey>>>,
    /// Whether [`Keyring::keys`] holds every device the directory verified, so any other has no verified key.
    complete: bool,
    directory: Option<Directory>,
}

impl Keyring {
    /// The keyring of the human's device of `nats`, reading the directory on `js`: none for others than a human's
    /// credentials binding an encryption key. A directory that cannot be read is warned of, and leaves every
    /// message sealed unreadable.
    pub async fn load(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Option<Self> {
        let own = match devices::Own::from_creds(nats.creds) {
            Ok(own) => own?,
            Err(e) => {
                tracing::warn!("not opening sealed messages: {e:#}");
                return None;
            }
        };
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

    fn new(
        device: DeviceId,
        xkey: OnceLock<Option<XKey>>,
        creds: Option<PathBuf>,
        verified: &[VerifiedDevice],
        complete: bool,
        directory: Option<Directory>,
    ) -> Self {
        let keys = verified.iter().map(|d| ((d.human.clone(), d.device), Some(d.key.clone()))).collect();
        Self { device, xkey, creds, keys: Mutex::new(keys), complete, directory }
    }

    fn xkey(&self) -> Option<&XKey> {
        self.xkey
            .get_or_init(|| {
                let path = self.creds.as_deref()?;
                match creds::load_xkey(path, keystore::os()) {
                    Ok(xkey) => xkey,
                    Err(e) => {
                        tracing::warn!("not opening sealed messages: {e:#}");
                        None
                    }
                }
            })
            .as_ref()
    }

    /// Look the sending device of a sealed `envelope` up in the directory unless known, as one sent after the
    /// keyring was loaded may name a device added since.
    pub async fn learn(&self, envelope: &Envelope) {
        let (Some(sealed), Some(directory)) = (&envelope.sealed, &self.directory) else { return };
        let known = (envelope.from.clone(), sealed.device);
        if self.keys.lock().expect("keyring lock").contains_key(&known) {
            return;
        }
        let key = match directory.device(&envelope.from, &sealed.device).await {
            Ok(Lookup::Verified(device)) => Some(device.key),
            Ok(Lookup::Missing | Lookup::Skipped(_)) => None,
            Err(e) => return tracing::warn!("not reading the key of device {}: {e:#}", sealed.device),
        };
        self.keys.lock().expect("keyring lock").insert(known, key);
    }

    /// Open the sealed part of `envelope`, if any.
    pub fn open(&self, envelope: &Envelope) -> Option<Opening> {
        let sealed = envelope.sealed.as_ref()?;
        if sealed.key_for(&self.device).is_none() {
            return Some(Opening::Unreadable);
        }
        let sender = match self.keys.lock().expect("keyring lock").get(&(envelope.from.clone(), sealed.device)) {
            Some(key) => key.clone(),
            None if self.complete => None,
            None => return Some(Opening::Unreadable),
        };
        let Some(xkey) = self.xkey() else { return Some(Opening::Unreadable) };
        let opening = open(envelope, &self.device, xkey, sender.as_ref());
        if let Opening::Forged(why) = &opening {
            tracing::warn!(message = %envelope.id, from = %envelope.from, "a forged sealed message: {why}");
        }
        Some(opening)
    }
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

/// What `ants ask` seals a message to another human with: the sending device and its key, the devices to seal for,
/// and a keyring opening the replies.
pub struct Sealer {
    device: DeviceId,
    xkey: XKey,
    recipients: Vec<VerifiedDevice>,
    pub keyring: Keyring,
}

impl Sealer {
    /// What seals a message from the human of `nats` to the human `to`, reading the directory on `js`: refused, with
    /// the fix, when the credentials bind no encryption key, the directory holds no verified key of the sending device
    /// or of any device of `to`.
    pub async fn new(nats: &crate::Nats<'_>, js: &jetstream::Context, to: &UserId) -> Result<Self> {
        const CLEAR: &str = "`--plaintext` sends it in the clear";
        let me = &nats.me.id;
        let Some(own) = devices::Own::from_creds(nats.creds)? else {
            bail!(
                "a message to the human {to} is sealed, but the credentials of {me} bind no encryption key: `ants \
                 renew` binds one; {CLEAR}"
            );
        };
        let path = nats.creds.context("no credentials")?;
        let xkey = creds::load_xkey(path, keystore::os())?
            .with_context(|| format!("no encryption key in {}: `ants renew` binds one; {CLEAR}", path.display()))?;
        let account = nats.account.as_deref().context("credentials without an account")?;
        let directory = Directory::open(js, account).await?;
        let humans = [me.clone(), to.clone()];
        let devices = directory
            .devices(&humans)
            .await
            .with_context(|| format!("reading the encryption keys of {me} and {to}; {CLEAR}"))?;
        let own_key = crate::auth::device_key(&xkey)?;
        if !devices.of(me).any(|d| d.device == own.device && d.key == own_key) {
            bail!(
                "the encryption key of this device ({}) is not verified in {DEVICES_BUCKET}, so nothing is sealed: \
                 connecting with credentials that may put it there does (`ants renew` issues them); {CLEAR}",
                own.device
            );
        }
        if devices.of(to).next().is_none() {
            bail!(
                "{to} has no device whose encryption key is verified in {DEVICES_BUCKET}, so nothing is sealed: a \
                 device of {to} puts one there on connecting with credentials binding one (`ants renew`); {CLEAR}"
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

    /// Seal `envelope` ([`seal`]).
    pub fn seal(&self, envelope: &mut Envelope) -> Result<()> {
        let recipients: Vec<(DeviceId, &DeviceKey)> = self.recipients.iter().map(|d| (d.device, &d.key)).collect();
        seal(envelope, self.device, &self.xkey, &recipients)
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
        seal(&mut e, from.id, &from.xkey, &recipients).unwrap();
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

        let sealer = Sealer::new(&nats[0], &js[0], &bob).await.unwrap();
        let mut e = Envelope::new(alice.clone(), bob.clone(), "the launch slips a week".to_owned());
        sealer.seal(&mut e).unwrap();
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

        let Err(refused) = Sealer::new(&nats[0], &js[0], &dave).await else { panic!("sealed for no device") };
        let refused = refused.to_string();
        assert!(refused.contains("dave has no device") && refused.contains("--plaintext"), "{refused}");
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
