//! The OS credential store, which may hold the NKey seed of a `.creds` file in its place: the macOS keychain, the
//! Secret Service on Linux (GNOME Keyring, KDE Wallet, KeePassXC) through `secret-tool`, or the Windows Credential
//! Manager. Each seed is stored under the service [`SERVICE`] and its user's public key, so each device's or ant's key
//! is an entry of its own.
//!
//! A store protects seeds at rest, not from a process running with access to it: a connection keeps the seed it
//! loaded in memory, to sign the server's challenge again on every reconnect. Errors name what to do and never the
//! seed; a store locked or not running yet, as before its user logs in, fails with [`Unavailable`], which another
//! attempt may cure. Other platforms have no store here; their credentials keep the seed in the file.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, bail};

use crate::sync::MutexExt as _;

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
    e.chain().any(<dyn std::error::Error>::is::<Unavailable>)
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
        secret_service::SecretTool::os().get(key)
    }

    fn set(&self, key: &str, seed: &str) -> Result<()> {
        secret_service::SecretTool::os().set(key, seed)
    }

    fn delete(&self, key: &str) -> Result<()> {
        secret_service::SecretTool::os().delete(key)
    }
}

/// The Secret Service through `secret-tool`, which reports a locked keyring by failing without a message, as it does
/// when nothing matches: a silent lookup or clear is told apart by searching for the entry, which lists locked
/// entries too, and, as KeePassXC lists none of a locked database's entries, by a locked collection listing none.
#[cfg(any(target_os = "linux", test))]
mod secret_service {
    use std::io::{Read as _, Write as _};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output, Stdio};
    use std::time::{Duration, Instant};

    use anyhow::{Result, anyhow, bail};

    use super::{SERVICE, Store, Unavailable};

    /// The Secret Service's bus name, and its object.
    const BUS_NAME: &str = "org.freedesktop.secrets";
    const ROOT: &str = "/org/freedesktop/secrets";

    /// How long `secret-tool` may run, as it waits for an unlock prompt for as long as the prompt stays open.
    const TIMEOUT: Duration = Duration::from_secs(60);

    const UNSTORE: &str = "to keep the seed in the file instead, run `ants keys unstore` where the keyring is unlocked";

    /// The `secret-tool` program run, how long it may run, and the D-Bus tools tried in turn to read the Secret
    /// Service's collections (`busctl`, `gdbus`, `dbus-send`, told apart by file name).
    pub struct SecretTool {
        pub program: PathBuf,
        pub timeout: Duration,
        pub bus: Vec<PathBuf>,
    }

    impl SecretTool {
        #[cfg_attr(
            not(target_os = "linux"),
            expect(dead_code, reason = "secret-tool runs on Linux alone; elsewhere only the tests build it")
        )]
        pub fn os() -> Self {
            Self {
                program: "secret-tool".into(),
                timeout: TIMEOUT,
                bus: ["busctl", "gdbus", "dbus-send"].map(PathBuf::from).into(),
            }
        }

        /// Whether an entry for `key` exists, locked or not: the search lists each entry matching, a locked one
        /// without its secret. It prints an unlocked entry's seed, which goes no further than this.
        fn stored(&self, key: &str) -> Result<bool> {
            let found = self.run(&["search", "service", SERVICE, "account", key], None)?;
            Ok(found.is_some_and(|out| !out.trim().is_empty()))
        }

        /// A locked collection listing no entries, as KeePassXC lists none of a locked database's, and searches leave
        /// it out: an entry `secret-tool` does not find may be in it. None when every locked collection lists its
        /// entries, as GNOME Keyring's and KDE Wallet's do, or when no D-Bus tool runs.
        fn hiding_collection(&self) -> Option<String> {
            let program = self.bus.iter().find_map(|program| {
                let collections = self.property(program, ROOT, "Service", "Collections")?;
                Some((program, object_paths(&collections)))
            });
            let (program, collections) = program?;
            collections.into_iter().find(|collection| {
                self.property(program, collection, "Collection", "Locked").is_some_and(|locked| locked.contains("true"))
                    && self
                        .property(program, collection, "Collection", "Items")
                        .is_none_or(|items| object_paths(&items).is_empty())
            })
        }

        /// The property `name` of the Secret Service's `interface` (`Service`, `Collection`) at `path`, as `program`
        /// prints it; none when it fails.
        fn property(&self, program: &Path, path: &str, interface: &str, name: &str) -> Option<String> {
            let interface = format!("org.freedesktop.Secret.{interface}");
            let args: Vec<String> = match program.file_name()?.to_str()? {
                "busctl" => {
                    vec!["--user".into(), "get-property".into(), BUS_NAME.into(), path.into(), interface, name.into()]
                }
                "gdbus" => [
                    "call",
                    "--session",
                    "--dest",
                    BUS_NAME,
                    "--object-path",
                    path,
                    "--method",
                    "org.freedesktop.DBus.Properties.Get",
                    &interface,
                    name,
                ]
                .map(String::from)
                .into(),
                "dbus-send" => vec![
                    "--session".into(),
                    "--print-reply".into(),
                    format!("--dest={BUS_NAME}"),
                    path.into(),
                    "org.freedesktop.DBus.Properties.Get".into(),
                    format!("string:{interface}"),
                    format!("string:{name}"),
                ],
                _ => return None,
            };
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let output = spawn(program, &args, None, self.timeout).ok()??;
            output.status.success().then(|| String::from_utf8_lossy(&output.stdout).into_owned())
        }

        /// Run `secret-tool` with `args`, writing `input` (a seed) to its standard input, never to its arguments. Its
        /// output on success; none when it fails silently, as a lookup finding nothing does.
        fn run(&self, args: &[&str], input: Option<&str>) -> Result<Option<String>> {
            let output = spawn(&self.program, args, input, self.timeout).map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => anyhow!(
                    "the Secret Service needs `secret-tool`: install libsecret-tools (Debian, Ubuntu) or libsecret \
                     (Fedora, Arch)"
                ),
                _ => anyhow!("running secret-tool: {e}"),
            })?;
            let Some(output) = output else {
                return Err(Unavailable(format!(
                    "Secret Service (secret-tool {}): no answer within {} s: answer the keyring's unlock prompt on the \
                     desktop, or unlock the keyring before running ants",
                    args[0],
                    self.timeout.as_secs()
                ))
                .into());
            };
            let stderr = String::from_utf8_lossy(&output.stderr);
            let error = stderr.trim().trim_start_matches("secret-tool:").trim();
            match (output.status.success(), error) {
                (true, _) => {
                    String::from_utf8(output.stdout).map(Some).map_err(|_| anyhow!("a stored seed is not text"))
                }
                (false, "") => Ok(None),
                (false, error) => Err(failure(args[0], error)),
            }
        }

        /// Why `secret-tool command` found no entry for `key` although one may exist, if it may.
        fn not_found(&self, command: &str, key: &str) -> Result<Option<anyhow::Error>> {
            // Before searching, which in KeePassXC asks again to unlock the database.
            if let Some(collection) = self.hiding_collection() {
                return Ok(Some(failure(
                    command,
                    &format!("the collection {collection} is locked and lists no entries until it is unlocked"),
                )));
            }
            Ok(self.stored(key)?.then(|| locked(command)))
        }
    }

    /// Run `program` with `args`, writing `input` to its standard input; none when it runs past `timeout`.
    fn spawn(program: &Path, args: &[&str], input: Option<&str>, timeout: Duration) -> std::io::Result<Option<Output>> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        if let (Some(input), Some(mut stdin)) = (input, child.stdin.take())
            && let Err(e) = stdin.write_all(input.as_bytes())
            && e.kind() != std::io::ErrorKind::BrokenPipe
        {
            // Killing and reaping fail only for a program already exited and reaped.
            let _ = child.kill();
            let _ = child.wait();
            return Err(e);
        }
        // The output is a few lines, well within the pipes' buffers, so it is read once the program exits.
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Ok(None);
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        if let Some(mut out) = child.stdout.take() {
            out.read_to_end(&mut stdout)?;
        }
        if let Some(mut err) = child.stderr.take() {
            err.read_to_end(&mut stderr)?;
        }
        Ok(Some(Output { status, stdout, stderr }))
    }

    /// The object paths `busctl`, `gdbus` or `dbus-send` print, each quoted.
    fn object_paths(printed: &str) -> Vec<String> {
        printed
            .split(['"', '\''])
            .skip(1)
            .step_by(2)
            .filter(|quoted| quoted.starts_with('/'))
            .map(String::from)
            .collect()
    }

    impl Store for SecretTool {
        fn get(&self, key: &str) -> Result<Option<String>> {
            match self.run(&["lookup", "service", SERVICE, "account", key], None)? {
                Some(seed) => Ok(Some(seed.trim().to_owned()).filter(|s| !s.is_empty())),
                None => match self.not_found("lookup", key)? {
                    Some(error) => Err(error),
                    None => Ok(None),
                },
            }
        }

        fn set(&self, key: &str, seed: &str) -> Result<()> {
            let label = format!("ants NKey seed {key}");
            match self.run(&["store", "--label", &label, "service", SERVICE, "account", key], Some(seed))? {
                Some(_) => Ok(()),
                None => bail!(
                    "Secret Service (secret-tool store): failed without a message: check that a Secret Service runs in \
                 this session and that its keyring is unlocked"
                ),
            }
        }

        fn delete(&self, key: &str) -> Result<()> {
            if self.run(&["clear", "service", SERVICE, "account", key], None)?.is_none()
                && let Some(error) = self.not_found("clear", key)?
            {
                return Err(error);
            }
            Ok(())
        }
    }

    fn locked(command: &str) -> anyhow::Error {
        failure(command, "the keyring holding the entry is locked")
    }

    /// An error of `secret-tool command` naming what to do about it: [`Unavailable`], as a session bus, Secret
    /// Service or unlocked keyring may come once the user logs in.
    fn failure(command: &str, error: &str) -> anyhow::Error {
        let fix = if error.contains("D-Bus") || error.contains("Could not connect") {
            "no D-Bus session bus is reachable, as in a system service, cron or WSL without one: run ants in the \
             user's login session or as a systemd user service (whose bus is $XDG_RUNTIME_DIR/bus)"
        } else if error.contains("not activatable") || error.contains("not provided by any") {
            "no Secret Service is installed or running in this session: install and start one (gnome-keyring, \
             KeePassXC's Secret Service integration, KDE Wallet)"
        } else if error.contains("locked") {
            "unlock the keyring (in KeePassXC, the database, and allow access to the entry when asked): a desktop login \
             unlocks GNOME Keyring's login keyring, and an unlock prompt needs a desktop; without one, \
             `gnome-keyring-daemon --unlock` reads the password on standard input"
        } else if error.contains("does not exist at path") {
            "no default keyring exists, and creating one asks for its password in a prompt that needs a desktop: \
             create it in a desktop session, or with `gnome-keyring-daemon --unlock` reading the new password on \
             standard input"
        } else {
            "check that a Secret Service runs in this session and that its keyring is unlocked"
        };
        Unavailable(format!("Secret Service (secret-tool {command}): {error}: {fix}; {UNSTORE}")).into()
    }

    #[cfg(all(test, unix))]
    mod tests {
        use std::time::{Duration, Instant};

        use super::*;

        const KEY: &str = "UKEYTEST";

        /// A `secret-tool` running the shell `script` with its arguments, and the directory to remove after.
        fn fake(script: &str) -> (SecretTool, PathBuf) {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = ants_test_support::scratch_dir("secret-tool");
            let program = dir.join("secret-tool");
            std::fs::write(&program, format!("#!/bin/sh\n{script}\n")).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            (SecretTool { program, timeout: Duration::from_secs(10), bus: Vec::new() }, dir)
        }

        /// `tool` reading the collections through a `busctl` running the shell `script`.
        fn with_bus(mut tool: SecretTool, dir: &Path, script: &str) -> SecretTool {
            use std::os::unix::fs::PermissionsExt as _;
            let program = dir.join("busctl");
            std::fs::write(&program, format!("#!/bin/sh\n{script}\n")).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            tool.bus = vec![dir.join("gdbus-missing"), program];
            tool
        }

        /// A `busctl` printing as the real one does a collection `/c/login` listing an entry, and `/c/kp` locked or
        /// not, listing none.
        fn bus(kp_locked: bool) -> String {
            format!(
                r#"case "$4 $6" in
  "/org/freedesktop/secrets Collections") echo 'ao 2 "/c/login" "/c/kp"' ;;
  "/c/login Locked") echo 'b false' ;;
  "/c/kp Locked") echo 'b {kp_locked}' ;;
  "/c/login Items") echo 'ao 1 "/c/login/1"' ;;
  "/c/kp Items") echo 'ao 0' ;;
  *) exit 1 ;;
esac"#
            )
        }

        #[test]
        fn a_locked_entry_fails_rather_than_reading_as_missing() {
            // As secret-tool does: lookup and clear fail silently, search lists the entry without its secret.
            let (tool, dir) = fake(
                r#"case "$1" in
  search) printf '[/1]\nlabel = ants NKey seed %s\nschema = org.freedesktop.Secret.Generic\n' "$5"
          printf 'secret-tool: Cannot get secret of a locked object\nattribute.account = %s\n' "$5" >&2 ;;
  *) exit 1 ;;
esac"#,
            );
            let error = tool.get(KEY).unwrap_err();
            assert!(super::super::is_unavailable(&error), "{error}");
            let error = error.to_string();
            assert!(error.contains("secret-tool lookup") && error.contains("unlock the keyring"), "{error}");
            let error = tool.delete(KEY).unwrap_err().to_string();
            assert!(error.contains("secret-tool clear") && error.contains("locked"), "{error}");
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn a_missing_entry_reads_as_none_but_a_silent_store_fails() {
            let (tool, dir) = fake(r#"[ "$1" = search ] || exit 1"#);
            assert_eq!(tool.get(KEY).unwrap(), None);
            tool.delete(KEY).unwrap();
            let error = tool.set(KEY, "SUSEED").unwrap_err().to_string();
            assert!(error.contains("secret-tool store") && !error.contains("SUSEED"), "{error}");
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn a_locked_collection_listing_no_entries_fails_rather_than_reading_as_missing() {
            // As KeePassXC does with a locked database: nothing is found, not even by a search, which must not run
            // as it asks again to unlock the database.
            let (tool, dir) = fake(r#"[ "$1" = search ] && touch "$(dirname "$0")/searched"; exit 1"#);
            let tool = with_bus(tool, &dir, &bus(true));
            for error in [tool.get(KEY).unwrap_err(), tool.delete(KEY).unwrap_err()] {
                assert!(super::super::is_unavailable(&error), "{error}");
                let error = error.to_string();
                assert!(error.contains("/c/kp is locked") && error.contains("in KeePassXC, the database"), "{error}");
            }
            assert!(!dir.join("searched").exists());
            let tool = with_bus(tool, &dir, &bus(false));
            assert_eq!(tool.get(KEY).unwrap(), None);
            tool.delete(KEY).unwrap();
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn object_paths_read_as_each_bus_tool_prints_them() {
            let paths = ["/org/freedesktop/secrets/collection/kp", "/org/freedesktop/secrets/collection/login"];
            for printed in [
                r#"ao 2 "/org/freedesktop/secrets/collection/kp" "/org/freedesktop/secrets/collection/login""#,
                "(<[objectpath '/org/freedesktop/secrets/collection/kp', \
                 '/org/freedesktop/secrets/collection/login']>,)",
                "method return time=1.2 sender=:1.73 -> destination=:1.91 serial=201 reply_serial=2\n   variant       \
                 array [\n         object path \"/org/freedesktop/secrets/collection/kp\"\n         object path \
                 \"/org/freedesktop/secrets/collection/login\"\n      ]",
            ] {
                assert_eq!(object_paths(printed), paths, "{printed}");
            }
            for empty in
                ["ao 0", "(<@ao []>,)", "method return time=1.2 sender=:1.73\n   variant       array [\n      ]"]
            {
                assert!(object_paths(empty).is_empty(), "{empty}");
            }
        }

        #[test]
        fn errors_name_what_is_missing() {
            for (message, fix) in [
                ("Cannot autolaunch D-Bus without X11 $DISPLAY", "no D-Bus session bus"),
                ("The name is not activatable", "no Secret Service is installed"),
                ("Object does not exist at path “/org/freedesktop/secrets/collection/login”", "no default keyring"),
            ] {
                let (tool, dir) = fake(&format!("echo 'secret-tool: {message}' >&2; exit 1"));
                let error = tool.get(KEY).unwrap_err().to_string();
                assert!(error.contains(message) && error.contains(fix), "{error}");
                std::fs::remove_dir_all(dir).unwrap();
            }
        }

        #[test]
        fn an_unanswered_unlock_prompt_times_out() {
            let (mut tool, dir) = fake("exec sleep 30");
            tool.timeout = Duration::from_millis(200);
            let started = Instant::now();
            let error = tool.get(KEY).unwrap_err();
            assert!(super::super::is_unavailable(&error) && error.to_string().contains("no answer within"), "{error}");
            assert!(started.elapsed() < Duration::from_secs(10));
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}

#[cfg(windows)]
impl Store for Os {
    fn get(&self, key: &str) -> Result<Option<String>> {
        credman::get(key)
    }

    fn set(&self, key: &str, seed: &str) -> Result<()> {
        credman::set(key, seed)
    }

    fn delete(&self, key: &str) -> Result<()> {
        credman::delete(key)
    }
}

/// The Windows Credential Manager: a generic credential per key, named `ants:<public key>` and kept for the user on
/// this machine only (not roaming), encrypted by the user's logon credentials.
#[cfg(windows)]
mod credman {
    use anyhow::{Result, anyhow};
    use windows_sys::Win32::Foundation::{ERROR_NO_SUCH_LOGON_SESSION, ERROR_NOT_FOUND, GetLastError, WIN32_ERROR};
    use windows_sys::Win32::Security::Credentials::{
        CRED_PERSIST_LOCAL_MACHINE, CRED_TYPE_GENERIC, CREDENTIALW, CredDeleteW, CredFree, CredReadW, CredWriteW,
    };

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain([0]).collect()
    }

    fn target(key: &str) -> Vec<u16> {
        wide(&format!("{}:{key}", super::SERVICE))
    }

    pub fn get(key: &str) -> Result<Option<String>> {
        let target = target(key);
        let mut cred: *mut CREDENTIALW = std::ptr::null_mut();
        // SAFETY: `target` is NUL-terminated and outlives the call; on success `cred` points to a credential that
        // stays valid until freed below.
        if unsafe { CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut cred) } == 0 {
            // SAFETY: GetLastError has no preconditions; it reads the error code the failed call above set.
            return match unsafe { GetLastError() } {
                ERROR_NOT_FOUND => Ok(None),
                code => Err(failure(code)),
            };
        }
        // SAFETY: as above; the blob is `CredentialBlobSize` bytes long, copied before the credential is freed.
        let seed = unsafe {
            let blob = &*cred;
            let seed = if blob.CredentialBlob.is_null() {
                Vec::new()
            } else {
                std::slice::from_raw_parts(blob.CredentialBlob, blob.CredentialBlobSize as usize).to_vec()
            };
            CredFree(cred.cast());
            seed
        };
        let seed = String::from_utf8(seed).map_err(|_| anyhow!("a stored seed is not text"))?;
        Ok(Some(seed.trim().to_owned()).filter(|s| !s.is_empty()))
    }

    pub fn set(key: &str, seed: &str) -> Result<()> {
        let (target, user, comment) = (target(key), wide(key), wide("ants NKey seed"));
        let cred = CREDENTIALW {
            Type: CRED_TYPE_GENERIC,
            TargetName: target.as_ptr().cast_mut(),
            Comment: comment.as_ptr().cast_mut(),
            CredentialBlobSize: u32::try_from(seed.len())?,
            CredentialBlob: seed.as_ptr().cast_mut(),
            Persist: CRED_PERSIST_LOCAL_MACHINE,
            UserName: user.as_ptr().cast_mut(),
            ..Default::default()
        };
        // SAFETY: every pointer in `cred` points to a buffer that outlives the call, which only reads them.
        if unsafe { CredWriteW(&cred, 0) } == 0 {
            // SAFETY: GetLastError has no preconditions; it reads the error code the failed call above set.
            return Err(failure(unsafe { GetLastError() }));
        }
        Ok(())
    }

    pub fn delete(key: &str) -> Result<()> {
        let target = target(key);
        // SAFETY: `target` is NUL-terminated and outlives the call.
        if unsafe { CredDeleteW(target.as_ptr(), CRED_TYPE_GENERIC, 0) } == 0 {
            // SAFETY: GetLastError has no preconditions; it reads the error code the failed call above set.
            return match unsafe { GetLastError() } {
                ERROR_NOT_FOUND => Ok(()),
                code => Err(failure(code)),
            };
        }
        Ok(())
    }

    fn failure(code: WIN32_ERROR) -> anyhow::Error {
        let fix = match code {
            ERROR_NO_SUCH_LOGON_SESSION => {
                "this logon session has no credential set, as with an SSH session authenticated by key, a service \
                 running as another account or a scheduled task not storing the password: run ants as the user in \
                 a session logged on with the password, or keep the seed in the file (`ants keys unstore`)"
            }
            _ => "check the entry under Windows Credentials in Credential Manager",
        };
        let error = std::io::Error::from_raw_os_error(code as i32);
        anyhow!("Windows Credential Manager: {error}: {fix}")
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
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

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
const UNSUPPORTED: &str = "no OS credential store is supported on this platform: keep the seed in the .creds file";

/// A store in memory, standing in for the OS one in tests; [`Memory::lock`] makes it fail as a locked store does.
#[derive(Default)]
pub struct Memory {
    seeds: Mutex<BTreeMap<String, String>>,
    locked: AtomicBool,
    read_only: AtomicBool,
}

impl Memory {
    /// Fail every request from now on, as a locked store does, until [`Memory::unlock`].
    pub fn lock(&self) {
        self.locked.store(true, Ordering::SeqCst);
    }

    /// Serve requests again after [`Memory::lock`].
    pub fn unlock(&self) {
        self.locked.store(false, Ordering::SeqCst);
    }

    /// Refuse to store from now on, while still reading and deleting, until [`Memory::allow_writes`].
    pub fn refuse_writes(&self) {
        self.read_only.store(true, Ordering::SeqCst);
    }

    /// Store again after [`Memory::refuse_writes`].
    pub fn allow_writes(&self) {
        self.read_only.store(false, Ordering::SeqCst);
    }

    /// The public keys of the seeds stored.
    pub fn keys(&self) -> Vec<String> {
        self.seeds.locked().keys().cloned().collect()
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
        Ok(self.seeds.locked().get(key).cloned())
    }

    fn set(&self, key: &str, seed: &str) -> Result<()> {
        self.check()?;
        if self.read_only.load(Ordering::SeqCst) {
            bail!("the credential store denied access");
        }
        self.seeds.locked().insert(key.to_owned(), seed.to_owned());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.check()?;
        self.seeds.locked().remove(key);
        Ok(())
    }
}
