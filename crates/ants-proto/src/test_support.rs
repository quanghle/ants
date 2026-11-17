//! Test helpers shared by the test suites of this crate: user JWTs signed by an account.

use data_encoding::BASE64URL_NOPAD;
use nkeys::KeyPair;
use serde_json::{Value, json};

pub(crate) const NOW: u64 = 1_800_000_000;

pub(crate) fn jwt(signer: &KeyPair, body: &Value) -> String {
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(body.to_string().as_bytes()));
    let signature = BASE64URL_NOPAD.encode(&signer.sign(signed.as_bytes()).unwrap());
    format!("{signed}.{signature}")
}

pub(crate) fn user_jwt(account: &KeyPair, user: &KeyPair, name: &str, tags: &[&str], exp: Option<u64>) -> String {
    let mut body = json!({
        "iss": account.public_key(),
        "sub": user.public_key(),
        "name": name,
        "iat": NOW - 10,
        "nats": { "type": "user", "tags": tags, "version": 2 },
    });
    if let Some(exp) = exp {
        body["exp"] = json!(exp);
    }
    jwt(account, &body)
}
