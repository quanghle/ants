//! The hub's end of the bridge, `ants bridge serve`: sessions, each a TCP connection to the upstream, written by the
//! POST requests of the worker's end and read by its long-polled GET requests.

use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ants_daemon::secrets::same;
use ants_daemon::sync::MutexExt as _;
use anyhow::Result;
use data_encoding::HEXLOWER;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::http::{Method, Request, Response, StatusCode, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use super::{CHUNK, IDLE, POLL, Traffic};

/// Downstream bytes the hub holds for a session before it stops reading the upstream.
const BUFFER: usize = 1024 * 1024;
const MAX_SESSIONS: usize = 256;
const UPSTREAM_CONNECT: Duration = Duration::from_secs(10);

/// `ants bridge serve`: sessions, each a connection to the upstream.
pub struct Server {
    upstream: String,
    token: Option<String>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    traffic: Arc<Traffic>,
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
        let token = token.filter(|t| !t.is_empty());
        Self { upstream, token, sessions: Mutex::default(), traffic: Arc::default() }
    }

    /// What the bridge carries, as it goes.
    pub fn traffic(&self) -> Arc<Traffic> {
        self.traffic.clone()
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
                session.write(off, &body.to_bytes(), &self.traffic).await
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
        if self.sessions.locked().len() >= MAX_SESSIONS {
            return reply(StatusCode::SERVICE_UNAVAILABLE, "too many sessions");
        }
        let stream = match tokio::time::timeout(UPSTREAM_CONNECT, TcpStream::connect(&self.upstream)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                self.traffic.failed();
                return reply(StatusCode::BAD_GATEWAY, &format!("upstream {}: {e}", self.upstream));
            }
            Err(_) => {
                self.traffic.failed();
                return reply(StatusCode::GATEWAY_TIMEOUT, &format!("upstream {}: timed out", self.upstream));
            }
        };
        let _ = stream.set_nodelay(true);
        let (reader, writer) = stream.into_split();
        let down = Arc::new(Down::default());
        let reader = tokio::spawn(read_upstream(reader, down.clone(), self.traffic.clone())).abort_handle();
        let session = Session {
            up: tokio::sync::Mutex::new(Up { writer, written: 0 }),
            down,
            reader,
            seen: Mutex::new(Instant::now()),
        };
        let id = session_id();
        self.sessions.locked().insert(id.clone(), Arc::new(session));
        self.traffic.opened();
        tracing::info!(session = %id, upstream = %self.upstream, "bridge session opened");
        Response::new(Full::new(Bytes::from(id)))
    }

    fn session(&self, id: &str) -> Option<Arc<Session>> {
        let session = self.sessions.locked().get(id).cloned()?;
        *session.seen.locked() = Instant::now();
        Some(session)
    }

    fn close(&self, id: &str, why: &str) {
        if let Some(session) = self.sessions.locked().remove(id) {
            self.traffic.closed();
            session.close();
            tracing::info!(session = %id, "bridge session {why}");
        }
    }

    fn sweep(&self) {
        let idle: Vec<String> = {
            let sessions = self.sessions.locked();
            sessions.iter().filter(|(_, s)| s.seen.locked().elapsed() > IDLE).map(|(id, _)| id.clone()).collect()
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
        let mut state = self.down.state.locked();
        state.gone = true;
        state.buf = Vec::new();
        drop(state);
        self.down.readable.notify_waiters();
    }

    async fn write(&self, off: u64, body: &[u8], traffic: &Traffic) -> Response<Full<Bytes>> {
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
            traffic.up(new.len());
        }
        empty(StatusCode::NO_CONTENT)
    }

    async fn read(&self, off: u64) -> Response<Full<Bytes>> {
        let deadline = tokio::time::Instant::now() + POLL;
        loop {
            let mut readable = pin!(self.down.readable.notified());
            readable.as_mut().enable();
            {
                let mut state = self.down.state.locked();
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
async fn read_upstream(mut reader: OwnedReadHalf, down: Arc<Down>, traffic: Arc<Traffic>) {
    let mut chunk = vec![0; CHUNK];
    loop {
        loop {
            let mut room = pin!(down.room.notified());
            room.as_mut().enable();
            if down.state.locked().buf.len() < BUFFER {
                break;
            }
            room.await;
        }
        let n = reader.read(&mut chunk).await.unwrap_or(0);
        traffic.down(n);
        let mut state = down.state.locked();
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

pub(super) fn offset(query: &str) -> Option<u64> {
    query.split('&').find_map(|pair| pair.strip_prefix("off=")).and_then(|v| v.parse().ok())
}

fn session_id() -> String {
    let mut id = [0; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut id).expect("system randomness");
    HEXLOWER.encode(&id)
}

fn reply(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(text.to_owned())));
    *response.status_mut() = status;
    response
}

fn empty(status: StatusCode) -> Response<Full<Bytes>> {
    reply(status, "")
}
