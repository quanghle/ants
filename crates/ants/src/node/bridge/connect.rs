//! The worker's end of the bridge, `ants bridge connect`: each local connection carried through a session of the
//! hub's end, its bytes sent up in POST requests and taken down by long-polled GET requests, retried through
//! failures; and the probes `ants doctor` makes of a bridge.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow, bail};
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper::http::{HeaderName, HeaderValue, Method, Request, StatusCode, Uri, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject as _;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

use super::{CHUNK, IDLE, POLL, REQUEST, Traffic};
use crate::http::{Io, client_config, parse_proxy, tls_connector, tunnel};

const MAX_BACKOFF: Duration = Duration::from_secs(4);

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
    traffic: Arc<Traffic>,
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
        let tls = if tls {
            let name = host.trim_start_matches('[').trim_end_matches(']').to_owned();
            let name = ServerName::try_from(name).with_context(|| format!("bridge URL {url:?}: host name"))?;
            Some((tls_connector()?, name))
        } else {
            None
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
                let (dial, auth) = parse_proxy(proxy).context("--proxy")?;
                (dial, Some(auth))
            }
        };
        let base = uri.path().trim_end_matches('/').to_owned();
        Ok(Self { dial, authority, tls, base, headers: all, proxy, traffic: Arc::default() })
    }

    /// Trust only the CA certificates of the PEM file `ca`, as `SSL_CERT_FILE` makes the bridge's units do.
    pub fn trusting(mut self, ca: &Path) -> Result<Self> {
        if let Some((connector, _)) = &mut self.tls {
            let mut roots = rustls::RootCertStore::empty();
            let certs = CertificateDer::pem_file_iter(ca).with_context(|| format!("reading {}", ca.display()))?;
            for cert in certs {
                roots.add(cert.with_context(|| format!("reading {}", ca.display()))?)?;
            }
            *connector = client_config(roots)?;
        }
        Ok(self)
    }

    /// Whether the bridge answers, within `wait`, and takes the token: asks for a session that does not exist.
    pub async fn probe(&self, wait: Duration) -> Result<Probe> {
        let (status, body) = Http::new(self).once(&Method::GET, "/s/0?off=0", &Bytes::new(), wait).await?;
        let body = said(&body);
        Ok(match status {
            StatusCode::NOT_FOUND if body == "no such session" => Probe::Answers,
            StatusCode::UNAUTHORIZED => Probe::Refused(body),
            _ => Probe::Other(status, body),
        })
    }

    /// Open a session, take the first line its upstream sends down within `wait`, sending nothing up, and close it: a
    /// NATS server's port sends its INFO first, which is all this takes, so the hub sees a connection come and go
    /// unauthenticated.
    pub async fn first_line(&self, wait: Duration) -> Result<Carried> {
        let deadline = tokio::time::Instant::now() + wait;
        let mut http = Http::new(self);
        let (status, id) = http.once(&Method::POST, "/open", &Bytes::new(), wait).await?;
        if status != StatusCode::OK || id.is_empty() || !id.iter().all(u8::is_ascii_hexdigit) {
            return Ok(Carried::Unopened(status, said(&id)));
        }
        let path = format!("/s/{}", String::from_utf8_lossy(&id));
        let mut line = Vec::new();
        let read = async {
            while !line.contains(&b'\n') && line.len() < CHUNK {
                let off = format!("{path}?off={}", line.len());
                match http.once(&Method::GET, &off, &Bytes::new(), POLL + REQUEST).await? {
                    (StatusCode::OK, bytes) => line.extend_from_slice(&bytes),
                    (StatusCode::GONE, _) => break,
                    (status, body) => return Ok(Some(Carried::Lost(status, said(&body)))),
                }
            }
            anyhow::Ok(None)
        };
        let lost = match tokio::time::timeout_at(deadline, read).await {
            Ok(read) => read?,
            Err(_) => None,
        };
        // A session not closed here expires once idle.
        let _ = Http::new(self).once(&Method::DELETE, &path, &Bytes::new(), Duration::from_secs(5)).await;
        let end = line.iter().position(|b| *b == b'\n').unwrap_or(line.len());
        Ok(lost.unwrap_or_else(|| Carried::Line(String::from_utf8_lossy(&line[..end]).trim_end().to_owned())))
    }

    /// What the bridge carries, as it goes.
    pub fn traffic(&self) -> Arc<Traffic> {
        self.traffic.clone()
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
                    Err(e) => {
                        remote.traffic.failed();
                        tracing::warn!(%peer, "bridged connection closed: {e:#}");
                    }
                }
            });
        }
    }
}

/// What a bridge answered [`Remote::probe`].
#[derive(Debug, PartialEq, Eq)]
pub enum Probe {
    /// As a bridge taking the token does.
    Answers,
    /// The token is missing or wrong: this answer.
    Refused(String),
    /// Not as a bridge does: this status and answer.
    Other(StatusCode, String),
}

/// What a session of [`Remote::first_line`] carried down from the bridge's upstream.
#[derive(Debug, PartialEq, Eq)]
pub enum Carried {
    /// The upstream's first line, without its line end; what came of it when it ended or the wait passed first,
    /// empty for nothing.
    Line(String),
    /// The bridge opened no session, answering this status and text: its upstream unreachable, as a rule.
    Unopened(StatusCode, String),
    /// The session opened was not found by the request reading it, answering this status and text.
    Lost(StatusCode, String),
}

/// The start of an answer's body, as text.
fn said(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(200)]).trim().to_owned()
}

/// One HTTP/1.1 connection to the bridge, made again after any failure.
pub(super) struct Http<'a> {
    remote: &'a Remote,
    send: Option<SendRequest<Full<Bytes>>>,
}

impl<'a> Http<'a> {
    pub(super) fn new(remote: &'a Remote) -> Self {
        Self { remote, send: None }
    }

    pub(super) async fn once(
        &mut self,
        method: &Method,
        path: &str,
        body: &Bytes,
        wait: Duration,
    ) -> Result<(StatusCode, Bytes)> {
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
    remote.traffic.opened();
    let path = format!("/s/{id}");
    let (reader, writer) = local.into_split();
    let result = tokio::select! {
        r = send_up(&mut up, &path, reader) => r,
        r = take_down(&mut down, &path, writer) => r,
    };
    remote.traffic.closed();
    // A session not closed here expires once idle.
    if let Err(e) = Http::new(remote).once(&Method::DELETE, &path, &Bytes::new(), Duration::from_secs(5)).await {
        tracing::debug!(session = %id, "closing the bridge session: {e:#}");
    }
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
            (status, _) if status.is_success() => {
                off += n as u64;
                http.remote.traffic.up(n);
            }
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
                http.remote.traffic.down(bytes.len());
            }
            (StatusCode::GONE, _) => return Ok(()),
            (status, body) => bail!("receiving: {status}: {}", String::from_utf8_lossy(&body)),
        }
    }
}
