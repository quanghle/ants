//! The local endpoint of a daemon's MCP server that `ants mcp` bridges a harness's stdio to, which only the daemon's
//! user may connect to: a Unix socket with mode 0600 in the daemon's home, or on Windows a named pipe with an
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

    /// Listens on `<home>/mcp.sock`.
    pub struct Listener(UnixListener);

    impl Listener {
        /// Listen in `home`, replacing a socket left behind; returns the address to [`connect`] to.
        pub fn bind(home: &Path) -> Result<(Self, PathBuf)> {
            use std::os::unix::fs::PermissionsExt as _;
            let path = home.join("mcp.sock");
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
            let next = create(&name, &sddl, true).with_context(|| format!("creating {}", name.display()))?;
            Ok((Self { name: name.clone(), sddl, next }, name))
        }

        pub async fn accept(&mut self) -> io::Result<NamedPipeServer> {
            let connected = self.next.connect().await;
            // On failure the waiting instance stays, and the next accept hands out a client that connected meanwhile.
            let next = create(&self.name, &self.sddl, false)?;
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

    fn create(name: &Path, sddl: &[u16], first: bool) -> io::Result<NamedPipeServer> {
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
        options.first_pipe_instance(first).reject_remote_clients(true);
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
