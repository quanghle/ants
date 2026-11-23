//! A minimal HTTP/1.1 client over rustls, for the few calls ants makes to web services: notification sinks, GitHub,
//! and the downloads of `ants update` ([`download`]); and what it shares with the bridge's client: the TLS client
//! and the tunnel through an HTTP CONNECT proxy.

use std::fmt::Write as _;
use std::sync::{Arc, OnceLock};

use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::BASE64;
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::http::{HeaderMap, Method, Request, StatusCode, Uri, header};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::ServerName;

fn tls() -> Result<TlsConnector> {
    static TLS: OnceLock<TlsConnector> = OnceLock::new();
    if let Some(tls) = TLS.get() {
        return Ok(tls.clone());
    }
    let tls = tls_connector()?;
    Ok(TLS.get_or_init(|| tls).clone())
}

/// A TLS client trusting the CA certificates of the system, or of the PEM file `SSL_CERT_FILE` names.
pub(crate) fn tls_connector() -> Result<TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    let (added, _) = roots.add_parsable_certificates(native.certs);
    if added == 0 {
        bail!("no trusted CA certificates found (errors: {:?}); SSL_CERT_FILE names a PEM file of them", native.errors);
    }
    client_config(roots)
}

/// A TLS client trusting `roots`, speaking HTTP/1.1.
pub(crate) fn client_config(roots: rustls::RootCertStore) -> Result<TlsConnector> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsConnector::from(Arc::new(config)))
}

/// `host:port` and the Proxy-Authorization of a CONNECT proxy URL. The error names the URL, not where it was given:
/// the flag or the variable is the caller's to add.
pub(crate) fn parse_proxy(proxy: &str) -> Result<(String, Option<String>)> {
    let rest = proxy.strip_prefix("http://").with_context(|| format!("{proxy:?}: expected http://host:port"))?;
    let authority = rest.trim_end_matches('/');
    let (auth, host) = match authority.rsplit_once('@') {
        Some((user, host)) => (Some(format!("Basic {}", BASE64.encode(user.as_bytes()))), host),
        None => (None, authority),
    };
    if host.is_empty() || host.contains('/') {
        bail!("{proxy:?}: expected http://host:port");
    }
    let dial = if host.rsplit_once(':').is_some_and(|(_, port)| port.parse::<u16>().is_ok()) {
        host.to_owned()
    } else {
        format!("{host}:80")
    };
    Ok((dial, auth))
}

/// Ask the proxy on `tcp` for a tunnel to `authority`.
pub(crate) async fn tunnel(tcp: &mut TcpStream, authority: &str, auth: Option<&str>) -> Result<()> {
    let mut head = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = auth {
        let _ = write!(head, "Proxy-Authorization: {auth}\r\n");
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

/// A connection to a server, plain or under TLS.
pub(crate) trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Io for T {}

/// `method` on `url` (https://, or http://), with `headers` and `body` of its content type if any: the status of the
/// answer and its body, empty when it is longer than `limit` bytes or breaks off. Errors never name the URL, which may
/// hold a secret.
pub(crate) async fn fetch(
    method: Method,
    url: &str,
    headers: &[(&str, String)],
    body: Option<(&str, Vec<u8>)>,
    limit: usize,
) -> Result<(StatusCode, Bytes)> {
    let (status, _, answer) = fetch_with_headers(method, url, headers, body, limit).await?;
    Ok((status, answer))
}

/// [`fetch`], with the headers of the answer.
pub(crate) async fn fetch_with_headers(
    method: Method,
    url: &str,
    headers: &[(&str, String)],
    body: Option<(&str, Vec<u8>)>,
    limit: usize,
) -> Result<(StatusCode, HeaderMap, Bytes)> {
    let uri: Uri = url.parse().map_err(|_| anyhow!("a malformed URL"))?;
    let (status, answer_headers, answer) = request(method, &uri, headers, body, limit, None).await?;
    Ok((status, answer_headers, answer.unwrap_or_default()))
}

/// Most redirects [`download`] follows.
const MAX_REDIRECTS: usize = 5;

/// `GET` `url` (https:// only), following redirects, through the HTTP CONNECT proxy `HTTPS_PROXY` names unless
/// `NO_PROXY` exempts the host: the body of the successful answer. Fails on another status and on a body longer than
/// `limit` bytes.
pub(crate) async fn download(url: &str, headers: &[(&str, String)], limit: usize) -> Result<Bytes> {
    let proxy = env_var(&["HTTPS_PROXY", "https_proxy"])
        .map(|proxy| {
            let proxy = if proxy.contains("://") { proxy } else { format!("http://{proxy}") };
            parse_proxy(&proxy).context("HTTPS_PROXY")
        })
        .transpose()?;
    let no_proxy = env_var(&["NO_PROXY", "no_proxy"]).unwrap_or_default();
    let mut uri: Uri = url.parse().map_err(|_| anyhow!("a malformed URL: {url}"))?;
    for _ in 0..=MAX_REDIRECTS {
        if uri.scheme_str() != Some("https") {
            bail!("not an https:// URL: {uri}");
        }
        let host = uri.host().context("a URL without a host")?;
        let proxy = proxy.as_ref().filter(|_| !bypasses(host, &no_proxy));
        let (status, answer_headers, body) = request(Method::GET, &uri, headers, None, limit, proxy).await?;
        if status.is_redirection() {
            let location = answer_headers.get(header::LOCATION).and_then(|l| l.to_str().ok());
            let location = location.with_context(|| format!("{status} without a location from {host}"))?;
            uri = redirected(&uri, location)?;
            continue;
        }
        if !status.is_success() {
            bail!("{status} from {host} for {}", uri.path());
        }
        return body.with_context(|| format!("the answer from {host} broke off or is longer than {limit} bytes"));
    }
    bail!("more than {MAX_REDIRECTS} redirects")
}

fn env_var(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| std::env::var(name).ok().filter(|v| !v.trim().is_empty()))
}

/// Whether `NO_PROXY`, a comma-separated list of host names and domains (`*`: all), exempts `host` from the proxy.
fn bypasses(host: &str, no_proxy: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase();
    no_proxy.split(',').map(|entry| entry.trim().to_ascii_lowercase()).filter(|entry| !entry.is_empty()).any(|entry| {
        let domain = entry.trim_start_matches("*.").trim_start_matches('.');
        entry == "*" || host == domain || host.strip_suffix(domain).is_some_and(|rest| rest.ends_with('.'))
    })
}

/// Where the redirect to `location` from `from` leads.
fn redirected(from: &Uri, location: &str) -> Result<Uri> {
    if location.starts_with('/') {
        let authority = from.authority().context("a URL without a host")?;
        let scheme = from.scheme_str().unwrap_or("https");
        return format!("{scheme}://{authority}{location}").parse().map_err(|_| anyhow!("a malformed redirect"));
    }
    location.parse().map_err(|_| anyhow!("a malformed redirect"))
}

/// One request: the status, the headers and the body of the answer, none when it is longer than `limit` bytes or
/// breaks off. Through `proxy` (its `host:port` and Proxy-Authorization), a CONNECT tunnel, for an https:// `uri`.
async fn request(
    method: Method,
    uri: &Uri,
    headers: &[(&str, String)],
    body: Option<(&str, Vec<u8>)>,
    limit: usize,
    proxy: Option<&(String, Option<String>)>,
) -> Result<(StatusCode, HeaderMap, Option<Bytes>)> {
    let https = uri.scheme_str() == Some("https");
    let host = uri.host().context("a URL without a host")?;
    let bare_host = host.trim_start_matches('[').trim_end_matches(']');
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    let authority = uri.authority().context("a URL without a host")?.as_str().to_owned();
    let io: Box<dyn Io> = match proxy {
        Some((dial, auth)) if https => {
            let mut tcp = TcpStream::connect(dial.as_str()).await.with_context(|| format!("connecting to {dial}"))?;
            tunnel(&mut tcp, &format!("{host}:{port}"), auth.as_deref()).await?;
            let name = ServerName::try_from(bare_host.to_owned()).context("the URL's host name")?;
            Box::new(tls()?.connect(name, tcp).await.context("TLS handshake")?)
        }
        _ => {
            let tcp =
                TcpStream::connect((bare_host, port)).await.with_context(|| format!("connecting to {host}:{port}"))?;
            if https {
                let name = ServerName::try_from(bare_host.to_owned()).context("the URL's host name")?;
                Box::new(tls()?.connect(name, tcp).await.context("TLS handshake")?)
            } else {
                Box::new(tcp)
            }
        }
    };
    let (mut send, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    tokio::spawn(async move {
        // A connection failing fails the request on it, which reports it.
        let _ = connection.await;
    });
    let path = uri.path_and_query().map_or("/", |p| p.as_str());
    let mut request =
        Request::builder().method(method).uri(path).header(header::HOST, authority).header(header::USER_AGENT, "ants");
    if let Some((content_type, _)) = &body {
        request = request.header(header::CONTENT_TYPE, *content_type);
    }
    for (name, value) in headers {
        request = request.header(*name, value);
    }
    let response = send
        .send_request(request.body(Full::new(Bytes::from(body.map_or_default(|(_, body)| body))))?)
        .await
        .context("sending")?;
    let (status, answer_headers) = (response.status(), response.headers().clone());
    let answer = Limited::new(response.into_body(), limit).collect().await;
    Ok((status, answer_headers, answer.ok().map(http_body_util::Collected::to_bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_proxy_exempts_hosts_and_their_subdomains() {
        let list = "localhost, .internal.example,github.com";
        assert!(bypasses("localhost", list));
        assert!(bypasses("api.github.com", list));
        assert!(bypasses("GitHub.com", list));
        assert!(bypasses("a.internal.example", list));
        assert!(!bypasses("notgithub.com", list));
        assert!(!bypasses("objects.githubusercontent.com", list));
        assert!(bypasses("anything", "*"));
        assert!(!bypasses("github.com", ""));
    }

    #[test]
    fn a_relative_redirect_stays_on_the_host() {
        let from: Uri = "https://github.com/o/r/releases/download/v1/a.tar.gz".parse().unwrap();
        assert_eq!(redirected(&from, "/x?y=1").unwrap().to_string(), "https://github.com/x?y=1");
        let to = redirected(&from, "https://objects.example/a?sig=1").unwrap();
        assert_eq!(to.host(), Some("objects.example"));
    }
}
