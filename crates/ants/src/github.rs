//! The few GitHub calls of human registration, against GitHub's REST API or a GitHub Enterprise Server's, as the
//! signer's `[registration]` table names them ([`Registration`]).

use ants_daemon::config::registration::{GITHUB_API_URL, GithubAccount, Registration};
use anyhow::{Context as _, Result, bail};
use hyper::http::{Method, StatusCode, header};
use serde::de::DeserializeOwned;

/// Longest answer read from GitHub.
const MAX_ANSWER: usize = 256 * 1024;

/// Where GitHub is, and the signer's own token, if any.
#[derive(Debug, Clone)]
pub struct Github {
    pub api_url: String,
    token: Option<String>,
}

impl Github {
    /// GitHub as `registration` names it, with the signer's token; github.com, with no token, without a table.
    pub fn new(registration: Option<&Registration>) -> Result<Self> {
        Ok(match registration {
            Some(r) => Self { api_url: r.api_url.clone(), token: r.token()? },
            None => Self { api_url: GITHUB_API_URL.to_owned(), token: None },
        })
    }

    /// `GET` `path` of the API with `token`, else the signer's: the status, and the answer when it is a success.
    async fn get<T: DeserializeOwned>(&self, path: &str, token: Option<&str>) -> Result<(StatusCode, Option<T>)> {
        let mut headers = vec![
            (header::ACCEPT.as_str(), "application/vnd.github+json".to_owned()),
            ("x-github-api-version", "2022-11-28".to_owned()),
        ];
        if let Some(token) = token.or(self.token.as_deref()) {
            headers.push((header::AUTHORIZATION.as_str(), format!("Bearer {token}")));
        }
        let url = format!("{}{path}", self.api_url);
        let (status, body) = crate::http::fetch(Method::GET, &url, &headers, None, MAX_ANSWER)
            .await
            .with_context(|| format!("asking GitHub for {path}"))?;
        if !status.is_success() {
            return Ok((status, None));
        }
        let answer = serde_json::from_slice(&body).with_context(|| format!("GitHub's answer for {path}"))?;
        Ok((status, Some(answer)))
    }

    /// The GitHub user whose login is `login`.
    pub async fn user_by_login(&self, login: &str) -> Result<GithubAccount> {
        if !ants_daemon::config::registration::is_login(login) {
            bail!("{login:?} is no GitHub login");
        }
        match self.get::<GithubAccount>(&format!("/users/{login}"), None).await? {
            (_, Some(account)) => Ok(account),
            (StatusCode::NOT_FOUND, _) => bail!("GitHub has no user {login}"),
            (status, _) => bail!("GitHub answered {status} for the user {login}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use http_body_util::{BodyExt as _, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::http::{Request, Response};
    use hyper_util::rt::TokioIo;

    use super::*;

    /// What a fake GitHub got: `METHOD /path?query`, the authorization header and the body.
    pub(crate) type Route = dyn Fn(&str, Option<&str>, &str) -> (u16, String) + Send + Sync;

    /// A fake GitHub on a local port, answering each request with `route`: its URL, for both `github_url` and
    /// `api_url`.
    pub(crate) async fn fake(route: Arc<Route>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let route = route.clone();
                let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                    let route = route.clone();
                    async move {
                        let what = format!("{} {}", request.method(), request.uri());
                        let auth = request.headers().get(header::AUTHORIZATION).map(|v| v.to_str().unwrap().to_owned());
                        let body = request.into_body().collect().await?.to_bytes();
                        let (status, answer) = route(&what, auth.as_deref(), &String::from_utf8_lossy(&body));
                        let response = Response::builder().status(status).body(Full::new(Bytes::from(answer)));
                        Ok::<_, hyper::Error>(response.unwrap())
                    }
                });
                tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tcp), service));
            }
        });
        url
    }

    pub(crate) fn at(url: &str, token: Option<&str>) -> Github {
        Github { api_url: url.to_owned(), token: token.map(str::to_owned) }
    }

    #[tokio::test]
    async fn users_are_looked_up_by_login_with_the_signers_token() {
        let url = fake(Arc::new(|what, auth, _| match (what, auth) {
            ("GET /users/octocat", Some("Bearer signer")) => (200, r#"{"id": 583231, "login": "octocat"}"#.into()),
            _ => (404, r#"{"message": "Not Found"}"#.into()),
        }))
        .await;
        let github = at(&url, Some("signer"));
        let account = github.user_by_login("octocat").await.unwrap();
        assert_eq!(account, GithubAccount { id: 583_231, login: "octocat".into() });
        let err = github.user_by_login("nobody").await.unwrap_err().to_string();
        assert!(err.contains("GitHub has no user nobody"), "{err}");
        let err = github.user_by_login("../x").await.unwrap_err().to_string();
        assert!(err.contains("no GitHub login"), "{err}");
    }
}
