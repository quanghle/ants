//! `ants bridge`: carry a TCP connection, a leaf node's uplink, over plain HTTP(S) requests, for networks whose
//! proxies block WebSocket.
//!
//! The worker runs `ants bridge connect <url>`, which listens on `127.0.0.1:7422`, so its leaf node's uplink is
//! `nats-leaf://127.0.0.1:7422`, as with `devtunnel connect`. Each local connection becomes a session of
//! `ants bridge serve` on the hub, behind its TLS-terminating reverse proxy, which opens a TCP connection to the hub's
//! leafnode port. Bytes go up in POST bodies and come down in long-polled GET responses, each request tagged with the
//! byte offset it starts at, so a request lost or repeated on the way is retried without losing or doubling bytes.
//! NATS authentication runs inside the stream unchanged; an optional bearer token keeps strangers from opening
//! sessions at all.
//!
//! Requests, under the URL's path:
//! - `POST <path>/open`: connects to the upstream, answers the new session's id.
//! - `POST <path>/s/<id>?off=N`: the body continues the upstream bytes at offset N; 204.
//! - `GET <path>/s/<id>?off=N`: everything before N arrived; answers the downstream bytes from N, waiting up to
//!   [`POLL`] for some (an empty body when none came), or 410 once the upstream closed and all of it was taken.
//! - `DELETE <path>/s/<id>`: closes the session.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::{BASE64, HEXLOWER};
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{self, pki_types::ServerName};

/// Where `ants bridge connect` listens: where a leaf node's `nats-leaf://` uplink goes by default.
pub const DEFAULT_CONNECT_LISTEN: &str = "127.0.0.1:7422";
/// Where `ants bridge serve` listens, for the reverse proxy in front of it.
pub const DEFAULT_SERVE_LISTEN: &str = "127.0.0.1:8423";
/// The hub's leafnode port.
pub const DEFAULT_UPSTREAM: &str = "127.0.0.1:7422";
/// Environment variable of the bearer token both ends may share.
pub const TOKEN_ENV: &str = "ANTS_BRIDGE_TOKEN";

/// How long a GET waits for downstream bytes; below the idle timeouts of common proxies (60 s).
const POLL: Duration = Duration::from_secs(20);
/// A session without requests for this long is closed. The worker gives up retrying after as long.
const IDLE: Duration = Duration::from_secs(60);
/// The most bytes in one request or response body.
const CHUNK: usize = 128 * 1024;
/// Downstream bytes the hub holds for a session before it stops reading the upstream.
const BUFFER: usize = 1024 * 1024;
const MAX_SESSIONS: usize = 256;
const UPSTREAM_CONNECT: Duration = Duration::from_secs(10);
/// Time for one request, apart from a GET's wait.
const REQUEST: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(4);

// ---- hub side --------------------------------------------------------------------------------------------------

/// `ants bridge serve`: sessions, each a connection to the upstream.
pub struct Server {
    upstream: String,
    token: Option<String>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
}

struct Session {
    up: tokio::sync::Mutex<Up>,
    down: Arc<Down>,
    reader: tokio::task::AbortHandle,
    seen: Mutex<Instant>,
}

struct Up {
    writer: OwnedWriteHalf,
    /// Upstream bytes written so far.
    written: u64,
}

#[derive(Default)]
struct Down {
    state: Mutex<DownState>,
    readable: Notify,
    room: Notify,
}

#[derive(Default)]
struct DownState {
    /// Offset of `buf[0]`: everything before it was taken.
    start: u64,
    buf: Vec<u8>,
    /// The upstream closed; `buf` is all that is left.
    eof: bool,
    /// The session was closed.
    gone: bool,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl Server {
    pub fn new(upstream: String, token: Option<String>) -> Self {
        Self { upstream, token: token.filter(|t| !t.is_empty()), sessions: Mutex::default() }
    }

    /// Serve requests from `listener` until the process ends.
    pub async fn run(self, listener: TcpListener) -> Result<()> {
        let server = Arc::new(self);
        let sweeper = server.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(IDLE / 4).await;
                sweeper.sweep();
            }
        });
        loop {
            let stream = match listener.accept().await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a bridge connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let server = server.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| {
                    let server = server.clone();
                    async move { Ok::<_, Infallible>(server.handle(request).await) }
                });
                if let Err(e) =
                    hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await
                {
                    tracing::debug!(error = %e, "bridge connection");
                }
            });
        }
    }

    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        if !self.authorized(request.headers()) {
            return reply(StatusCode::UNAUTHORIZED, "missing or wrong bridge token");
        }
        let path = request.uri().path().to_owned();
        let off = request.uri().query().and_then(offset);
        if request.method() == Method::POST && path.ends_with("/open") {
            return self.open().await;
        }
        let Some((_, id)) = path.rsplit_once("/s/") else { return reply(StatusCode::NOT_FOUND, "no such path") };
        let Some(session) = self.session(id) else { return reply(StatusCode::NOT_FOUND, "no such session") };
        match (request.method().clone(), off) {
            (Method::POST, Some(off)) => {
                let Ok(body) = Limited::new(request.into_body(), CHUNK).collect().await else {
                    return reply(StatusCode::PAYLOAD_TOO_LARGE, "body too large or cut off");
                };
                session.write(off, &body.to_bytes()).await
            }
            (Method::GET, Some(off)) => session.read(off).await,
            (Method::DELETE, _) => {
                self.close(id, "closed by the worker");
                empty(StatusCode::NO_CONTENT)
            }
            (Method::POST | Method::GET, None) => reply(StatusCode::BAD_REQUEST, "no offset"),
            _ => reply(StatusCode::METHOD_NOT_ALLOWED, "unknown method"),
        }
    }

    fn authorized(&self, headers: &hyper::http::HeaderMap) -> bool {
        let Some(token) = &self.token else { return true };
        let given = headers.get(header::AUTHORIZATION).and_then(|v| v.as_bytes().strip_prefix(b"Bearer "));
        given.is_some_and(|given| same(given, token.as_bytes()))
    }

    async fn open(&self) -> Response<Full<Bytes>> {
        if self.sessions.lock().unwrap().len() >= MAX_SESSIONS {
            return reply(StatusCode::SERVICE_UNAVAILABLE, "too many sessions");
        }
        let stream = match tokio::time::timeout(UPSTREAM_CONNECT, TcpStream::connect(&self.upstream)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return reply(StatusCode::BAD_GATEWAY, &format!("upstream {}: {e}", self.upstream)),
            Err(_) => return reply(StatusCode::GATEWAY_TIMEOUT, &format!("upstream {}: timed out", self.upstream)),
        };
        let _ = stream.set_nodelay(true);
        let (reader, writer) = stream.into_split();
        let down = Arc::new(Down::default());
        let reader = tokio::spawn(read_upstream(reader, down.clone())).abort_handle();
        let session = Session {
            up: tokio::sync::Mutex::new(Up { writer, written: 0 }),
            down,
            reader,
            seen: Mutex::new(Instant::now()),
        };
        let id = session_id();
        self.sessions.lock().unwrap().insert(id.clone(), Arc::new(session));
        tracing::info!(session = %id, upstream = %self.upstream, "bridge session opened");
        Response::new(Full::new(Bytes::from(id)))
    }

    fn session(&self, id: &str) -> Option<Arc<Session>> {
        let session = self.sessions.lock().unwrap().get(id).cloned()?;
        *session.seen.lock().unwrap() = Instant::now();
        Some(session)
    }

    fn close(&self, id: &str, why: &str) {
        if let Some(session) = self.sessions.lock().unwrap().remove(id) {
            session.close();
            tracing::info!(session = %id, "bridge session {why}");
        }
    }

    fn sweep(&self) {
        let idle: Vec<String> = {
            let sessions = self.sessions.lock().unwrap();
            sessions.iter().filter(|(_, s)| s.seen.lock().unwrap().elapsed() > IDLE).map(|(id, _)| id.clone()).collect()
        };
        for id in idle {
            self.close(&id, "expired");
        }
    }
}

impl Session {
    /// Ends the session at once: requests in flight return, and the upstream closes when the last one has.
    fn close(&self) {
        self.reader.abort();
        let mut state = self.down.state.lock().unwrap();
        state.gone = true;
        state.buf = Vec::new();
        drop(state);
        self.down.readable.notify_waiters();
    }

    async fn write(&self, off: u64, body: &[u8]) -> Response<Full<Bytes>> {
        let mut up = self.up.lock().await;
        if off > up.written {
            return reply(StatusCode::CONFLICT, &format!("offset {off} beyond {}", up.written));
        }
        // A retried request may repeat bytes already written.
        let skip = usize::try_from(up.written - off).unwrap_or(usize::MAX);
        if let Some(new) = body.get(skip..).filter(|new| !new.is_empty()) {
            if let Err(e) = up.writer.write_all(new).await {
                return reply(StatusCode::GONE, &format!("upstream closed: {e}"));
            }
            up.written += new.len() as u64;
        }
        empty(StatusCode::NO_CONTENT)
    }

    async fn read(&self, off: u64) -> Response<Full<Bytes>> {
        let deadline = tokio::time::Instant::now() + POLL;
        loop {
            let mut readable = pin!(self.down.readable.notified());
            readable.as_mut().enable();
            {
                let mut state = self.down.state.lock().unwrap();
                if state.gone {
                    return reply(StatusCode::NOT_FOUND, "no such session");
                }
                let end = state.start + state.buf.len() as u64;
                if off < state.start || off > end {
                    return reply(StatusCode::CONFLICT, &format!("offset {off} outside {}..{end}", state.start));
                }
                let taken = (off - state.start) as usize;
                if taken > 0 {
                    state.buf.drain(..taken);
                    state.start = off;
                    self.down.room.notify_one();
                }
                if !state.buf.is_empty() {
                    let n = state.buf.len().min(CHUNK);
                    return Response::new(Full::new(Bytes::copy_from_slice(&state.buf[..n])));
                }
                if state.eof {
                    return reply(StatusCode::GONE, "upstream closed");
                }
            }
            if tokio::time::timeout_at(deadline, readable).await.is_err() {
                return Response::new(Full::new(Bytes::new()));
            }
        }
    }
}

/// Reads the upstream into `down` while it has room, until the upstream closes.
async fn read_upstream(mut reader: OwnedReadHalf, down: Arc<Down>) {
    let mut chunk = vec![0; CHUNK];
    loop {
        loop {
            let mut room = pin!(down.room.notified());
            room.as_mut().enable();
            if down.state.lock().unwrap().buf.len() < BUFFER {
                break;
            }
            room.await;
        }
        let n = reader.read(&mut chunk).await.unwrap_or(0);
        let mut state = down.state.lock().unwrap();
        if n == 0 {
            state.eof = true;
        } else {
            state.buf.extend_from_slice(&chunk[..n]);
        }
        drop(state);
        down.readable.notify_waiters();
        if n == 0 {
            return;
        }
    }
}

fn offset(query: &str) -> Option<u64> {
    query.split('&').find_map(|pair| pair.strip_prefix("off=")).and_then(|v| v.parse().ok())
}

fn session_id() -> String {
    let mut id = [0; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut id).expect("system randomness");
    HEXLOWER.encode(&id)
}

/// Equal, in time independent of where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn reply(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(text.to_owned())));
    *response.status_mut() = status;
    response
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    reply(status, "")
}

// ---- worker side -----------------------------------------------------------------------------------------------

/// The hub's bridge as `ants bridge connect` reaches it.
pub struct Remote {
    /// Where to dial: the bridge, or the proxy in front of it.
    dial: String,
    /// `host:port` of the bridge, for the Host header and a proxy's CONNECT.
    authority: String,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    base: String,
    headers: Vec<(HeaderName, HeaderValue)>,
    /// Proxy-Authorization for a CONNECT proxy, which then is `dial`.
    proxy: Option<Option<String>>,
}

impl Remote {
    /// `url`: http:// or https://, with the path the bridge is mounted on. `headers`: `Name: value` pairs sent with
    /// every request. `proxy`: an HTTP CONNECT proxy, `http://[user:password@]host:port`.
    pub fn new(url: &str, token: Option<&str>, headers: &[String], proxy: Option<&str>) -> Result<Self> {
        let uri: Uri = url.parse().with_context(|| format!("bridge URL {url:?}"))?;
        let tls = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => bail!("bridge URL {url:?}: use https:// (or http:// behind a tunnel or on loopback)"),
        };
        if uri.query().is_some() {
            bail!("bridge URL {url:?}: no query string");
        }
        let host = uri.host().filter(|h| !h.is_empty()).with_context(|| format!("bridge URL {url:?}: no host"))?;
        if uri.authority().is_some_and(|a| a.as_str().contains('@')) {
            bail!("bridge URL {url:?}: give the token with --token, not in the URL");
        }
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let authority = format!("{host}:{port}");
        let tls = match tls {
            true => {
                let name = host.trim_start_matches('[').trim_end_matches(']').to_owned();
                let name = ServerName::try_from(name).with_context(|| format!("bridge URL {url:?}: host name"))?;
                Some((tls_connector()?, name))
            }
            false => None,
        };
        let mut all = Vec::new();
        if let Some(token) = token.filter(|t| !t.is_empty()) {
            all.push((header::AUTHORIZATION, HeaderValue::try_from(format!("Bearer {token}"))?));
        }
        for h in headers {
            let (name, value) = h.split_once(':').with_context(|| format!("--header {h:?}: expected Name: value"))?;
            let name = HeaderName::try_from(name.trim()).with_context(|| format!("--header {h:?}"))?;
            all.push((name, HeaderValue::try_from(value.trim()).with_context(|| format!("--header {h:?}"))?));
        }
        let (dial, proxy) = match proxy {
            None => (authority.clone(), None),
            Some(proxy) => {
                let (dial, auth) = parse_proxy(proxy)?;
                (dial, Some(auth))
            }
        };
        let base = uri.path().trim_end_matches('/').to_owned();
        Ok(Self { dial, authority, tls, base, headers: all, proxy })
    }

    async fn connect(&self) -> Result<SendRequest<Full<Bytes>>> {
        let mut tcp = TcpStream::connect(&self.dial).await.with_context(|| format!("connecting to {}", self.dial))?;
        let _ = tcp.set_nodelay(true);
        if let Some(auth) = &self.proxy {
            tunnel(&mut tcp, &self.authority, auth.as_deref()).await?;
        }
        let io: Box<dyn Io> = match &self.tls {
            Some((connector, name)) => Box::new(connector.connect(name.clone(), tcp).await.context("TLS handshake")?),
            None => Box::new(tcp),
        };
        let (send, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "bridge HTTP connection");
            }
        });
        Ok(send)
    }

    /// Accept local connections from `listener` and carry each to the hub, until the process ends.
    pub async fn run(self, listener: TcpListener) -> Result<()> {
        let remote = Arc::new(self);
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a local connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let remote = remote.clone();
            tokio::spawn(async move {
                match carry(&remote, stream).await {
                    Ok(()) => tracing::info!(%peer, "bridged connection closed"),
                    Err(e) => tracing::warn!(%peer, "bridged connection closed: {e:#}"),
                }
            });
        }
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

fn tls_connector() -> Result<TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    let (added, _) = roots.add_parsable_certificates(native.certs);
    if added == 0 {
        bail!("no trusted CA certificates found (errors: {:?}); SSL_CERT_FILE names a PEM file of them", native.errors);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsConnector::from(Arc::new(config)))
}

/// `host:port` and the Proxy-Authorization of a CONNECT proxy URL.
fn parse_proxy(proxy: &str) -> Result<(String, Option<String>)> {
    let rest =
        proxy.strip_prefix("http://").with_context(|| format!("--proxy {proxy:?}: expected http://host:port"))?;
    let authority = rest.trim_end_matches('/');
    let (auth, host) = match authority.rsplit_once('@') {
        Some((user, host)) => (Some(format!("Basic {}", BASE64.encode(user.as_bytes()))), host),
        None => (None, authority),
    };
    if host.is_empty() || host.contains('/') {
        bail!("--proxy {proxy:?}: expected http://host:port");
    }
    let dial = if host.rsplit_once(':').is_some_and(|(_, port)| port.parse::<u16>().is_ok()) {
        host.to_owned()
    } else {
        format!("{host}:80")
    };
    Ok((dial, auth))
}

/// Ask the proxy on `tcp` for a tunnel to `authority`.
async fn tunnel(tcp: &mut TcpStream, authority: &str, auth: Option<&str>) -> Result<()> {
    let mut head = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = auth {
        head += &format!("Proxy-Authorization: {auth}\r\n");
    }
    head += "\r\n";
    tcp.write_all(head.as_bytes()).await?;
    // Byte by byte, so nothing after the head is read away from the tunnel.
    let mut answer = Vec::new();
    while !answer.ends_with(b"\r\n\r\n") {
        if answer.len() > 8192 {
            bail!("proxy answer too long");
        }
        answer.push(tcp.read_u8().await.context("proxy closed the connection")?);
    }
    let status = String::from_utf8_lossy(answer.split(|b| *b == b'\n').next().unwrap_or_default()).trim().to_owned();
    if !status.split(' ').nth(1).is_some_and(|code| code.starts_with('2')) {
        bail!("proxy refused the tunnel to {authority}: {status}");
    }
    Ok(())
}

/// One HTTP/1.1 connection to the bridge, made again after any failure.
struct Http<'a> {
    remote: &'a Remote,
    send: Option<SendRequest<Full<Bytes>>>,
}

impl<'a> Http<'a> {
    fn new(remote: &'a Remote) -> Self {
        Self { remote, send: None }
    }

    async fn once(&mut self, method: &Method, path: &str, body: &Bytes, wait: Duration) -> Result<(StatusCode, Bytes)> {
        let result = tokio::time::timeout(wait, self.attempt(method, path, body)).await;
        match result {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(e)) => {
                self.send = None;
                Err(e)
            }
            Err(_) => {
                self.send = None;
                Err(anyhow!("no answer in {}s", wait.as_secs()))
            }
        }
    }

    async fn attempt(&mut self, method: &Method, path: &str, body: &Bytes) -> Result<(StatusCode, Bytes)> {
        let send = match &mut self.send {
            Some(send) if !send.is_closed() => send,
            _ => self.send.insert(self.remote.connect().await?),
        };
        send.ready().await?;
        let mut request = Request::builder()
            .method(method.clone())
            .uri(format!("{}{path}", self.remote.base))
            .header(header::HOST, &self.remote.authority)
            .header(header::CACHE_CONTROL, "no-store")
            .body(Full::new(body.clone()))?;
        for (name, value) in &self.remote.headers {
            request.headers_mut().append(name, value.clone());
        }
        let response = send.send_request(request).await?;
        let status = response.status();
        let body = Limited::new(response.into_body(), CHUNK + 4096)
            .collect()
            .await
            .map_err(|e| anyhow!("reading the answer: {e}"))?
            .to_bytes();
        Ok((status, body))
    }

    /// Like [`once`](Self::once), retried with back-off through connection failures and server errors for up to
    /// [`IDLE`], after which the hub has dropped the session anyway.
    async fn retried(
        &mut self,
        method: Method,
        path: &str,
        body: Bytes,
        wait: Duration,
    ) -> Result<(StatusCode, Bytes)> {
        let started = Instant::now();
        let mut backoff = Duration::from_millis(250);
        loop {
            let error = match self.once(&method, path, &body, wait).await {
                Ok((status, body)) if !status.is_server_error() => return Ok((status, body)),
                Ok((status, body)) => anyhow!("{status}: {}", String::from_utf8_lossy(&body)),
                Err(e) => e,
            };
            if started.elapsed() > IDLE {
                return Err(error.context(format!("{method} {path} failed for {}s", IDLE.as_secs())));
            }
            tracing::debug!("{method} {path}: {error:#}; retrying");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

/// Carry the local connection `local` through a new session.
async fn carry(remote: &Remote, local: TcpStream) -> Result<()> {
    let _ = local.set_nodelay(true);
    let (mut up, mut down) = (Http::new(remote), Http::new(remote));
    let id = match up.retried(Method::POST, "/open", Bytes::new(), REQUEST).await? {
        (StatusCode::OK, id) if !id.is_empty() && id.iter().all(u8::is_ascii_hexdigit) => {
            String::from_utf8_lossy(&id).into_owned()
        }
        (status, body) => bail!("opening a session: {status}: {}", String::from_utf8_lossy(&body)),
    };
    tracing::info!(session = %id, "bridge session opened");
    let path = format!("/s/{id}");
    let (reader, writer) = local.into_split();
    let result = tokio::select! {
        r = send_up(&mut up, &path, reader) => r,
        r = take_down(&mut down, &path, writer) => r,
    };
    let _ = Http::new(remote).once(&Method::DELETE, &path, &Bytes::new(), Duration::from_secs(5)).await;
    result
}

async fn send_up(http: &mut Http<'_>, path: &str, mut local: OwnedReadHalf) -> Result<()> {
    let mut buf = vec![0; CHUNK];
    let mut off = 0u64;
    loop {
        let n = local.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        let chunk = Bytes::copy_from_slice(&buf[..n]);
        match http.retried(Method::POST, &format!("{path}?off={off}"), chunk, REQUEST).await? {
            (status, _) if status.is_success() => off += n as u64,
            (StatusCode::GONE, _) => return Ok(()),
            (status, body) => bail!("sending: {status}: {}", String::from_utf8_lossy(&body)),
        }
    }
}

async fn take_down(http: &mut Http<'_>, path: &str, mut local: OwnedWriteHalf) -> Result<()> {
    let mut off = 0u64;
    loop {
        match http.retried(Method::GET, &format!("{path}?off={off}"), Bytes::new(), POLL + REQUEST).await? {
            (StatusCode::OK, bytes) => {
                local.write_all(&bytes).await?;
                off += bytes.len() as u64;
            }
            (StatusCode::GONE, _) => return Ok(()),
            (status, body) => bail!("receiving: {status}: {}", String::from_utf8_lossy(&body)),
        }
    }
}

/// `addr` parsed, and checked to be loopback when `local_only`.
pub fn listen_addr(addr: &str, local_only: bool) -> Result<SocketAddr> {
    let parsed: SocketAddr = addr.parse().with_context(|| format!("--listen {addr}: expected host:port"))?;
    if local_only && !parsed.ip().is_loopback() {
        bail!("--listen {addr}: the bridge serves this machine only; use a loopback address");
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_test_support::NatsServer;
    use futures::StreamExt as _;

    use super::*;

    async fn local() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    /// A bridge in front of an echo server.
    async fn echo_bridge(token: Option<&str>) -> u16 {
        let (echo, echo_port) = local().await;
        tokio::spawn(async move {
            while let Ok((stream, _)) = echo.accept().await {
                tokio::spawn(async move {
                    let (mut r, mut w) = stream.into_split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });
        let (listener, port) = local().await;
        let server = Server::new(format!("127.0.0.1:{echo_port}"), token.map(str::to_owned));
        tokio::spawn(server.run(listener));
        port
    }

    #[tokio::test]
    async fn repeated_requests_neither_lose_nor_double_bytes() {
        let port = echo_bridge(Some("s3cret")).await;
        let url = format!("http://127.0.0.1:{port}/bridge");
        let wrong = Remote::new(&url, Some("guess"), &[], None).unwrap();
        let (status, _) = Http::new(&wrong).once(&Method::POST, "/open", &Bytes::new(), REQUEST).await.unwrap();
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let remote = Remote::new(&url, Some("s3cret"), &[], None).unwrap();
        let mut http = Http::new(&remote);
        let mut call = async |method: Method, path: String, body: &'static [u8]| {
            http.once(&method, &path, &Bytes::from_static(body), REQUEST).await.unwrap()
        };
        let (status, id) = call(Method::POST, "/open".into(), b"").await;
        assert_eq!(status, StatusCode::OK);
        let s = format!("/s/{}", String::from_utf8(id.to_vec()).unwrap());

        // The second POST repeats "llo" of the first, as a retry after a lost answer would.
        assert_eq!(call(Method::POST, format!("{s}?off=0"), b"hello").await.0, StatusCode::NO_CONTENT);
        assert_eq!(call(Method::POST, format!("{s}?off=2"), b"llo world").await.0, StatusCode::NO_CONTENT);
        assert_eq!(call(Method::POST, format!("{s}?off=99"), b"gap").await.0, StatusCode::CONFLICT);

        let mut echoed = Vec::new();
        while echoed.len() < 11 {
            let (status, body) = call(Method::GET, format!("{s}?off=0"), b"").await;
            assert_eq!(status, StatusCode::OK);
            echoed = body.to_vec();
        }
        assert_eq!(echoed, b"hello world");
        // Asked again from 6, as after a lost answer: the bytes from there come again; then there is nothing new.
        assert_eq!(call(Method::GET, format!("{s}?off=6"), b"").await.1, Bytes::from_static(b"world"));
        assert_eq!(call(Method::GET, format!("{s}?off=0"), b"").await.0, StatusCode::CONFLICT);

        assert_eq!(call(Method::DELETE, s.clone(), b"").await.0, StatusCode::NO_CONTENT);
        assert_eq!(call(Method::GET, format!("{s}?off=11"), b"").await.0, StatusCode::NOT_FOUND);
    }

    #[test]
    fn proxy_urls_and_offsets_parse() {
        assert_eq!(parse_proxy("http://proxy:3128").unwrap(), ("proxy:3128".to_owned(), None));
        let (dial, auth) = parse_proxy("http://me:pw@proxy/").unwrap();
        assert_eq!((dial.as_str(), auth.as_deref()), ("proxy:80", Some("Basic bWU6cHc=")));
        assert!(parse_proxy("socks5://proxy:1080").is_err());
        assert_eq!(offset("x=1&off=42"), Some(42));
        assert!(Remote::new("wss://hub/bridge", None, &[], None).is_err());
        assert!(Remote::new("https://hub/bridge?x=1", None, &[], None).is_err());
        assert!(Remote::new("https://hub/bridge", None, &["no colon".into()], None).is_err());
    }

    /// A leaf node reaches its hub through `ants bridge connect` and `ants bridge serve` over plain HTTP, both ways,
    /// with a message larger than one request.
    #[tokio::test]
    async fn a_leaf_node_reaches_its_hub_through_the_bridge() {
        let leaf_port = ants_test_support::free_port();
        let Some(hub) = NatsServer::spawn_with(|_| {
            format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n")
        }) else {
            return;
        };
        let (serve_listener, serve_port) = local().await;
        tokio::spawn(Server::new(format!("127.0.0.1:{leaf_port}"), Some("t".into())).run(serve_listener));
        let (connect_listener, connect_port) = local().await;
        let remote = Remote::new(&format!("http://127.0.0.1:{serve_port}/bridge/"), Some("t"), &[], None).unwrap();
        tokio::spawn(remote.run(connect_listener));
        let leaf = NatsServer::spawn_with(|_| {
            format!("leafnodes {{ remotes: [{{ urls: [\"nats-leaf://127.0.0.1:{connect_port}\"] }}] }}\n")
        })
        .unwrap();

        let on_hub = async_nats::connect(&hub.url).await.unwrap();
        let on_leaf = async_nats::connect(&leaf.url).await.unwrap();
        let big = Bytes::from(vec![7u8; 3 * CHUNK]);
        for (from, to, payload) in [(&on_leaf, &on_hub, Bytes::from_static(b"up")), (&on_hub, &on_leaf, big)] {
            let mut sub = to.subscribe("bridged").await.unwrap();
            to.flush().await.unwrap();
            let got = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    from.publish("bridged", payload.clone()).await.unwrap();
                    from.flush().await.unwrap();
                    if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(250), sub.next()).await {
                        return msg.payload;
                    }
                }
            })
            .await
            .expect("no message across the bridge");
            assert_eq!(got, payload);
        }
    }
}
