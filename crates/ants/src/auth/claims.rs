//! User JWTs and their claims, signed by the bus's account and verified, and signatures of user keys.

use std::time::{SystemTime, UNIX_EPOCH};

use ants_proto::{PERMISSIONS_VERSION, Permissions};
use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use nkeys::KeyPair;
use serde_json::{Value, json};

use super::uplink::allowed;

/// Whether the user JWT `jwt` is for the key of `other` and allows everything `other` allows.
pub fn allows_all(jwt: &str, other: &str) -> Result<bool> {
    let ((claims, body), (other_claims, other_body)) = (verified_body(jwt)?, verified_body(other)?);
    let (have, need) = (allowed(&body), allowed(&other_body));
    let covers = |have: &[String], need: &[String]| need.iter().all(|s| have.contains(s));
    Ok(claims.sub == other_claims.sub
        && covers(&have.publish, &need.publish)
        && covers(&have.subscribe, &need.subscribe))
}

/// The signature of `data` by `key`, base64url without padding.
pub fn sign(key: &KeyPair, data: &[u8]) -> Result<String> {
    Ok(BASE64URL_NOPAD.encode(&key.sign(data).map_err(|e| anyhow!("signing: {e}"))?))
}

/// Check that `signature`, from [`sign`], is the signature of `data` by the key `public_key`.
pub fn check_signature(public_key: &str, data: &[u8], signature: &str) -> Result<()> {
    let key = KeyPair::from_public_key(public_key).map_err(|e| anyhow!("public key {public_key}: {e}"))?;
    let signature = BASE64URL_NOPAD.decode(signature.as_bytes()).context("a signature that is not base64url")?;
    key.verify(data, &signature).map_err(|_| anyhow!("not signed by {public_key}"))
}

/// Fail unless `public_key` is a user's public NKey, `U...`.
pub fn check_user_key(public_key: &str) -> Result<()> {
    let key = KeyPair::from_public_key(public_key).map_err(|e| anyhow!("public key {public_key}: {e}"))?;
    if key.key_pair_type() != nkeys::KeyPairType::User {
        bail!("{public_key} is not the public key of a user (U...)");
    }
    Ok(())
}

/// The claims of a user JWT that matter to enrollment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The user's public key.
    pub sub: String,
    /// The account that signed it.
    pub iss: String,
    pub name: String,
    /// Expiry, in seconds since the epoch.
    pub exp: Option<u64>,
}

/// Decode a JWT and check that its issuer signed it.
pub fn verified(jwt: &str) -> Result<Claims> {
    verified_body(jwt).map(|(claims, _)| claims)
}

/// [`verified`], with all of the JWT's claims.
pub(crate) fn verified_body(jwt: &str) -> Result<(Claims, Value)> {
    let (claims, body) = ants_proto::verified_jwt(jwt)?;
    Ok((Claims { sub: claims.sub, iss: claims.iss, name: claims.name, exp: claims.exp }, body))
}

pub(crate) fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

/// A user JWT for the public key `user` with `permissions`, plus the `extra` claims, expiring at `exp` (seconds since
/// the epoch) if given. Its tags carry [`PERMISSIONS_VERSION`].
pub(super) fn user_jwt(
    account: &KeyPair,
    user: &str,
    name: &str,
    permissions: Permissions,
    extra: Value,
    exp: Option<u64>,
) -> Result<String> {
    let Permissions { publish, subscribe } = permissions;
    let mut nats = json!({
        "pub": { "allow": publish },
        "sub": { "allow": subscribe },
        "subs": -1, "data": -1, "payload": -1,
        "type": "user",
        "version": 2,
    });
    if let (Value::Object(nats), Value::Object(extra)) = (&mut nats, extra) {
        nats.extend(extra);
    }
    let mut tags = nats["tags"].as_array().cloned().unwrap_or_default();
    tags.push(PERMISSIONS_VERSION.tag().into());
    nats["tags"] = tags.into();
    jwt(account, user, name, nats, exp)
}

/// A NATS v2 JWT for `subject`, signed by `issuer`, expiring at `exp` if given.
pub(super) fn jwt(issuer: &KeyPair, subject: &str, name: &str, nats: Value, exp: Option<u64>) -> Result<String> {
    let mut claims = json!({ "iat": now()?, "iss": issuer.public_key(), "name": name, "sub": subject, "nats": nats });
    if let Some(exp) = exp {
        claims["exp"] = exp.into();
    }
    // As in nats-io/jwt: the id is the hash of the claims without it.
    let id = ring::digest::digest(&ring::digest::SHA512_256, &serde_json::to_vec(&claims)?);
    claims["jti"] = BASE32_NOPAD.encode(id.as_ref()).into();
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims)?));
    let signature = issuer.sign(signed.as_bytes()).map_err(|e| anyhow!("signing: {e}"))?;
    Ok(format!("{signed}.{}", BASE64URL_NOPAD.encode(&signature)))
}
