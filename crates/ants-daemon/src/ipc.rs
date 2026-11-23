//! The local endpoint of a daemon's MCP server that `ants mcp` bridges a harness's stdio to, which only the daemon's
//! user may connect to: a Unix socket with mode 0600 in the daemon's home, or, when that path is too long for a Unix
//! socket, in a directory of the user's own with mode 0700 under the runtime directory; on Windows a named pipe with an
//! unguessable name, an access list admitting only the daemon's user, and remote clients refused.

use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};

#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

#[cfg(unix)]
mod unix {
    use super::*;
    use tokio::net::{UnixListener, UnixStream};

    /// Whether a connection may be half-closed, so the other side sees the end of what was sent and still answers.
    pub const HALF_CLOSE: bool = true;

    /// The longest path a Unix socket takes, in bytes: `sun_path` holds it and its NUL.
    pub const MAX_PATH: usize = if cfg!(any(target_os = "linux", target_os = "android")) { 107 } else { 103 };

    /// Listens on `<home>/mcp.sock`, or on the socket [`address`] names when that path is too long.
    pub struct Listener(UnixListener);

    /// Where the daemon serving the ant at `home` listens: `<home>/mcp.sock`, unless that path is longer than a Unix
    /// socket takes (`MAX_PATH`); then `ants-<uid>/<hash of home>.sock` in `$XDG_RUNTIME_DIR`, else `$TMPDIR`, else
    /// `/tmp`, the first where it fits, refusing a directory another user owns or others may enter. Creates nothing:
    /// binding creates that directory, mode 0700.
    pub fn address(home: &Path) -> Result<PathBuf> {
        locate(home, false)
    }

    fn locate(home: &Path, create: bool) -> Result<PathBuf> {
        let path = home.join("mcp.sock");
        if fits(&path) {
            return Ok(path);
        }
        let runtime = ["XDG_RUNTIME_DIR", "TMPDIR"].into_iter().filter_map(std::env::var_os).map(PathBuf::from);
        let runtime = runtime.filter(|dir| dir.is_absolute()).chain([PathBuf::from("/tmp")]);
        let mut tried = Vec::new();
        for dir in runtime {
            match in_runtime(&dir, home, create) {
                Ok(path) => return Ok(path),
                Err(e) => tried.push(format!("{e:#}")),
            }
        }
        anyhow::bail!(
            "{} is longer than a Unix socket takes ({MAX_PATH} bytes), and no runtime directory serves instead: {}",
            path.display(),
            tried.join("; ")
        )
    }

    fn fits(path: &Path) -> bool {
        use std::os::unix::ffi::OsStrExt as _;
        path.as_os_str().as_bytes().len() <= MAX_PATH
    }

    /// The socket of the daemon serving the ant at `home` under the runtime directory `runtime`, its directory
    /// created if `create`.
    pub(crate) fn in_runtime(runtime: &Path, home: &Path, create: bool) -> Result<PathBuf> {
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
        let uid = rustix::process::getuid().as_raw();
        let dir = runtime.join(format!("ants-{uid}"));
        let hash = ring::digest::digest(&ring::digest::SHA256, home.as_os_str().as_bytes());
        let path = dir.join(format!("{}.sock", &data_encoding::HEXLOWER.encode(hash.as_ref())[..16]));
        anyhow::ensure!(fits(&path), "{} is too long too", path.display());
        let created = if create { std::fs::DirBuilder::new().mode(0o700).create(&dir) } else { Ok(()) };
        match created {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).with_context(|| format!("creating {}", dir.display())),
        }
        let meta = match std::fs::symlink_metadata(&dir) {
            Err(e) if !create && e.kind() == io::ErrorKind::NotFound => return Ok(path),
            meta => meta.with_context(|| format!("reading {}", dir.display()))?,
        };
        anyhow::ensure!(meta.is_dir(), "{} is no directory", dir.display());
        anyhow::ensure!(meta.uid() == uid, "{} belongs to another user", dir.display());
        anyhow::ensure!(
            meta.mode() & 0o077 == 0,
            "others may enter {} (mode {:o})",
            dir.display(),
            meta.mode() & 0o777
        );
        Ok(path)
    }

    impl Listener {
        /// Listen for the ant at `home`, replacing a socket left behind; returns the address to [`connect`] to.
        pub fn bind(home: &Path) -> Result<(Self, PathBuf)> {
            use std::os::unix::fs::PermissionsExt as _;
            let path = locate(home, true)?;
            // A socket left behind, as a rule none; one that stays fails the bind.
            let _ = std::fs::remove_file(&path);
            let listener = UnixListener::bind(&path).with_context(|| format!("binding {}", path.display()))?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            Ok((Self(listener), path))
        }

        pub async fn accept(&mut self) -> io::Result<UnixStream> {
            self.0.accept().await.map(|(stream, _)| stream)
        }
    }

    /// Remove what a listener on `address` leaves behind.
    pub fn remove(address: &Path) {
        // One that stays is replaced by the next listener.
        let _ = std::fs::remove_file(address);
    }

    pub async fn connect(address: &Path) -> io::Result<UnixStream> {
        UnixStream::connect(address).await
    }
}

#[cfg(windows)]
mod windows {
    use std::time::Duration;

    use super::*;
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions};
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_PIPE_BUSY, HANDLE, HLOCAL, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    use windows_sys::Win32::Security::{
        GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    /// Named pipes cannot be half-closed: shutting down only flushes.
    pub const HALF_CLOSE: bool = false;

    /// A named pipe, with one instance always waiting for the next client.
    pub struct Listener {
        name: PathBuf,
        /// The pipe's security descriptor, in SDDL: full access for the daemon's user, none for anyone else.
        sddl: Vec<u16>,
        next: NamedPipeServer,
    }

    impl Listener {
        /// Listen on a fresh pipe (`home` is unused); returns the address to [`connect`] to.
        pub fn bind(_home: &Path) -> Result<(Self, PathBuf)> {
            let name = PathBuf::from(format!(r"\\.\pipe\ants-mcp-{}", uuid::Uuid::new_v4().simple()));
            let sddl = format!("D:P(A;;GA;;;{})", current_user_sid().context("reading the daemon's user")?);
            let sddl: Vec<u16> = sddl.encode_utf16().chain([0]).collect();
            let next = create(&name, &sddl, Instance::First).with_context(|| format!("creating {}", name.display()))?;
            Ok((Self { name: name.clone(), sddl, next }, name))
        }

        pub async fn accept(&mut self) -> io::Result<NamedPipeServer> {
            let connected = self.next.connect().await;
            // On failure the waiting instance stays, and the next accept hands out a client that connected meanwhile.
            let next = create(&self.name, &self.sddl, Instance::Next)?;
            let server = std::mem::replace(&mut self.next, next);
            connected.map(|()| server)
        }
    }

    pub fn remove(_address: &Path) {}

    /// Connect to the pipe at `address`, waiting a while for a free instance.
    pub async fn connect(address: &Path) -> io::Result<NamedPipeClient> {
        let mut attempts = 0;
        loop {
            match ClientOptions::new().open(address) {
                Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) && attempts < 100 => attempts += 1,
                result => return result,
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Which instance of a pipe [`create`] makes: the first, which fails if the pipe exists, or one more.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Instance {
        First,
        Next,
    }

    fn create(name: &Path, sddl: &[u16], instance: Instance) -> io::Result<NamedPipeServer> {
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: `sddl` is NUL-terminated; the descriptor is allocated by the call and freed below.
        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        };
        if converted == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        let mut options = ServerOptions::new();
        options.first_pipe_instance(instance == Instance::First).reject_remote_clients(true);
        // SAFETY: `attributes` is a valid SECURITY_ATTRIBUTES whose descriptor lives until after the call.
        let server = unsafe { options.create_with_security_attributes_raw(name, (&raw mut attributes).cast()) };
        // SAFETY: allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW, no longer used.
        unsafe { LocalFree(descriptor as HLOCAL) };
        server
    }

    /// The SID of the user the process runs as, such as `S-1-5-21-…-1001`.
    fn current_user_sid() -> io::Result<String> {
        let mut token: HANDLE = std::ptr::null_mut();
        // SAFETY: the pseudo handle of the current process needs no closing; `token` is closed below.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let sid = token_user_sid(token);
        // SAFETY: opened above.
        unsafe { CloseHandle(token) };
        sid
    }

    fn token_user_sid(token: HANDLE) -> io::Result<String> {
        let mut len = 0;
        // SAFETY: a size query, which fails with the size needed.
        unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut len) };
        // u64s keep the buffer aligned for TOKEN_USER.
        let mut buffer = vec![0u64; (len as usize).div_ceil(8)];
        // SAFETY: `buffer` holds at least `len` bytes.
        if unsafe { GetTokenInformation(token, TokenUser, buffer.as_mut_ptr().cast(), len, &mut len) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: filled by GetTokenInformation with a TOKEN_USER whose SID points into `buffer`.
        let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        let mut text = std::ptr::null_mut();
        // SAFETY: `sid` is valid while `buffer` lives; the string is allocated by the call and freed below.
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a NUL-terminated UTF-16 string.
        let len = (0..).take_while(|&i| unsafe { *text.add(i) } != 0).count();
        // SAFETY: `len` units precede the NUL.
        let sid = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, len) });
        // SAFETY: allocated by ConvertSidToStringSidW, no longer used.
        unsafe { LocalFree(text as HLOCAL) };
        Ok(sid)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;

    #[test]
    fn a_home_too_long_for_a_socket_binds_in_a_directory_of_the_user_s_own() {
        // Short, as a runtime directory is.
        let runtime = Path::new("/tmp").join(format!("ants-ipc-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&runtime);
        std::fs::create_dir(&runtime).unwrap();
        let home = Path::new("/home").join("deep".repeat(30));
        assert_eq!(address(Path::new("/home/dev")).unwrap(), Path::new("/home/dev/mcp.sock"));

        let path = in_runtime(&runtime, &home, false).unwrap();
        assert!(!path.parent().unwrap().exists(), "an address alone creates nothing");
        assert_eq!(in_runtime(&runtime, &home, true).unwrap(), path);
        let dir = path.parent().unwrap();
        assert_eq!(std::fs::metadata(dir).unwrap().permissions().mode() & 0o777, 0o700);
        assert_ne!(in_runtime(&runtime, Path::new("/home/other"), false).unwrap(), path, "one socket per home");

        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let open = in_runtime(&runtime, &home, true).unwrap_err();
        assert!(format!("{open:#}").contains("others may enter"), "{open:#}");
        std::fs::remove_dir_all(&runtime).unwrap();
    }
}
