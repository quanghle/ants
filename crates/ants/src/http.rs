//! A minimal HTTP/1.1 client over rustls, for the few calls ants makes to web services: notification sinks and
//! GitHub.

use std::sync::OnceLock;

use anyhow::{Context as _, Result, anyhow};
use http_body_util::{BodyExt as _, Full, Limited};
use hyper::body::Bytes;
use hyper::http::{Method, Request, StatusCode, Uri, header};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;

fn tls() -> Result<TlsConnector> {
    static TLS: OnceLock<TlsConnector> = OnceLock::new();
    if let Some(tls) = TLS.get() {
        return Ok(tls.clone());
    }
    let tls = crate::bridge::tls_connector()?;
    Ok(TLS.get_or_init(|| tls).clone())
}

trait Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
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
    let uri: Uri = url.parse().map_err(|_| anyhow!("a malformed URL"))?;
    let https = uri.scheme_str() == Some("https");
    let host = uri.host().context("a URL without a host")?;
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    let authority = uri.authority().context("a URL without a host")?.as_str().to_owned();
    let tcp = TcpStream::connect((host.trim_start_matches('[').trim_end_matches(']'), port))
        .await
        .with_context(|| format!("connecting to {host}:{port}"))?;
    let io: Box<dyn Io> = if https {
        let name = ServerName::try_from(host.trim_start_matches('[').trim_end_matches(']').to_owned())
            .context("the URL's host name")?;
        Box::new(tls()?.connect(name, tcp).await.context("TLS handshake")?)
    } else {
        Box::new(tcp)
    };
    let (mut send, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    tokio::spawn(async move {
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
    let status = response.status();
    let answer = Limited::new(response.into_body(), limit).collect().await;
    Ok((status, answer.map_or_default(http_body_util::Collected::to_bytes)))
}
