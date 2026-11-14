//! The OS credential store, which may hold the NKey seed of a `.creds` file in its place: the macOS keychain, or the
//! Secret Service on Linux (GNOME Keyring, KDE Wallet, KeePassXC) through `secret-tool`. Each seed is stored under the
//! service [`SERVICE`] and its user's public key, so each device's or ant's key is an entry of its own.
//!
//! A store protects seeds at rest, not from a process running with access to it: a connection keeps the seed it
//! loaded in memory, to sign the server's challenge again on every reconnect. Errors name what to do and never the
//! seed; a store locked or not running yet, as before its user logs in, fails with [`Unavailable`], which another
//! attempt may cure. Other platforms have no store here; their credentials keep the seed in the file.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail};

/// The service under which seeds are stored.
pub const SERVICE: &str = "ants";

/// A credential store of seeds, by public key.
pub trait Store: Send + Sync {
    /// The seed stored for the public key `key`, if any.
    fn get(&self, key: &str) -> Result<Option<String>>;
    /// Store `seed` for `key`, replacing any earlier one.
    fn set(&self, key: &str, seed: &str) -> Result<()>;
    /// Delete the seed of `key`; nothing to delete is no error.
    fn delete(&self, key: &str) -> Result<()>;
}

/// The error of a store that is locked or not available yet, as before its user logs in: unlike a seed missing from it,
/// another attempt may succeed once the store is unlocked.
#[derive(Debug)]
pub struct Unavailable(pub String);

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unavailable {}

/// Whether `e` comes of a store that is locked or not available yet ([`Unavailable`]).
pub fn is_unavailable(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| cause.is::<Unavailable>())
}

/// The OS credential store of this machine.
pub fn os() -> &'static dyn Store {
    &Os
}

struct Os;

#[cfg(target_os = "macos")]
impl Store for Os {
    fn get(&self, key: &str) -> Result<Option<String>> {
        use security_framework::passwords::{PasswordOptions, generic_password};
        match generic_password(PasswordOptions::new_generic_password(SERVICE, key)) {
            Ok(seed) => Ok(Some(String::from_utf8(seed).map_err(|_| anyhow::anyhow!("a stored seed is not text"))?)),
            Err(e) if e.code() == keychain::NOT_FOUND => Ok(None),
            Err(e) => Err(keychain::failure(&e)),
        }
    }

    fn set(&self, key: &str, seed: &str) -> Result<()> {
        security_framework::passwords::set_generic_password(SERVICE, key, seed.as_bytes())
            .map_err(|e| keychain::failure(&e))
    }

    fn delete(&self, key: &str) -> Result<()> {
        match security_framework::passwords::delete_generic_password(SERVICE, key) {
            Err(e) if e.code() != keychain::NOT_FOUND => Err(keychain::failure(&e)),
            _ => Ok(()),
        }
    }
}

#[cfg(target_os = "macos")]
mod keychain {
    /// `errSecItemNotFound`.
    pub const NOT_FOUND: i32 = -25300;

    pub fn failure(e: &security_framework::base::Error) -> anyhow::Error {
        let fix = match e.code() {
            // errSecInteractionNotAllowed: locked, with no session to ask in (SSH, a service before login).
            -25308 => {
                "the login keychain is locked and cannot ask to be unlocked here: unlock it with \
                 `security unlock-keychain`, or log in to the Mac's desktop"
            }
            // userCanceled, errSecAuthFailed.
            -128 | -25293 => {
                "access was denied: allow `ants` when the keychain asks (once again after each upgrade of the \
                 binary), or in Keychain Access"
            }
            // errSecNotAvailable, errSecNoDefaultKeychain.
            -25291 | -25307 => "no keychain is available: log in to the Mac's desktop to open the login keychain",
            _ => "check the login keychain in Keychain Access",
        };
        let message = format!("macOS keychain: {e} ({}): {fix}", e.code());
        match e.code() {
            -25308 | -25291 | -25307 => super::Unavailable(message).into(),
            _ => anyhow::anyhow!(message),
        }
    }
}

#[cfg(target_os = "linux")]
impl Store for Os {
    fn get(&self, key: &str) -> Result<Option<String>> {
        secret_tool(&["lookup", "service", SERVICE, "account", key], None)
    }

    fn set(&self, key: &str, seed: &str) -> Result<()> {
        let label = format!("ants NKey seed {key}");
        secret_tool(&["store", "--label", &label, "service", SERVICE, "account", key], Some(seed))?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        secret_tool(&["clear", "service", SERVICE, "account", key], None)?;
        Ok(())
    }
}

/// Run `secret-tool` with `args`, writing `input` (a seed) to its standard input, never to its arguments. Its output
/// on success; none when it fails silently, as a lookup finding nothing does.
#[cfg(target_os = "linux")]
fn secret_tool(args: &[&str], input: Option<&str>) -> Result<Option<String>> {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    const FIX: &str = "unlock the login keyring: a desktop login unlocks it, while a service without a login \
                       session, an SSH session or WSL may have no Secret Service running or unlocked";
    let mut child = Command::new("secret-tool")
        .args(args)
        .stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => anyhow::anyhow!(
                "the Secret Service needs `secret-tool`: install libsecret-tools (Debian, Ubuntu) or libsecret \
                 (Fedora, Arch)"
            ),
            _ => anyhow::anyhow!("running secret-tool: {e}"),
        })?;
    if let (Some(input), Some(mut stdin)) = (input, child.stdin.take()) {
        stdin.write_all(input.as_bytes())?;
    }
    let out = child.wait_with_output()?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    match (out.status.success(), stderr.trim()) {
        (true, _) => {
            let text = String::from_utf8(out.stdout).map_err(|_| anyhow::anyhow!("a stored seed is not text"))?;
            Ok(Some(text.trim().to_owned()).filter(|t| !t.is_empty()))
        }
        (false, "") => Ok(None),
        (false, error) => Err(Unavailable(format!("Secret Service (secret-tool {}): {error}: {FIX}", args[0])).into()),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
impl Store for Os {
    fn get(&self, _: &str) -> Result<Option<String>> {
        bail!("{UNSUPPORTED}")
    }

    fn set(&self, _: &str, _: &str) -> Result<()> {
        bail!("{UNSUPPORTED}")
    }

    fn delete(&self, _: &str) -> Result<()> {
        bail!("{UNSUPPORTED}")
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
const UNSUPPORTED: &str = "no OS credential store is supported on this platform: keep the seed in the .creds file";

/// A store in memory, standing in for the OS one in tests; [`Memory::lock`] makes it fail as a locked store does.
#[derive(Default)]
pub struct Memory {
    seeds: Mutex<BTreeMap<String, String>>,
    locked: AtomicBool,
    read_only: AtomicBool,
}

impl Memory {
    /// Fail every request from now on, as a locked store does, or no longer.
    pub fn lock(&self, locked: bool) {
        self.locked.store(locked, Ordering::SeqCst);
    }

    /// Refuse to store from now on, or no longer, while still reading and deleting.
    pub fn refuse_writes(&self, refuse: bool) {
        self.read_only.store(refuse, Ordering::SeqCst);
    }

    /// The public keys of the seeds stored.
    pub fn keys(&self) -> Vec<String> {
        self.seeds.lock().unwrap().keys().cloned().collect()
    }

    fn check(&self) -> Result<()> {
        if self.locked.load(Ordering::SeqCst) {
            return Err(Unavailable("the credential store is locked: unlock it".to_owned()).into());
        }
        Ok(())
    }
}

impl Store for Memory {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.check()?;
        Ok(self.seeds.lock().unwrap().get(key).cloned())
    }

    fn set(&self, key: &str, seed: &str) -> Result<()> {
        self.check()?;
        if self.read_only.load(Ordering::SeqCst) {
            bail!("the credential store denied access");
        }
        self.seeds.lock().unwrap().insert(key.to_owned(), seed.to_owned());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.check()?;
        self.seeds.lock().unwrap().remove(key);
        Ok(())
    }
}
