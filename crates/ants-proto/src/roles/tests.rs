//! Tests of roles: tags parse in either case, team members' user JWTs verify and others are refused, and admins' signed
//! acts and assistants of a human verify.

use serde_json::json;

use super::*;
use crate::test_support::{NOW, jwt, user_jwt};

fn sign(key: &KeyPair, payload: &[u8]) -> String {
    BASE64URL_NOPAD.encode(&key.sign(payload).unwrap())
}

#[test]
fn tags_parse_in_either_case() {
    let alice: UserId = "alice".parse().unwrap();
    assert_eq!(AssistantOf::tag(&alice), "ants-assistant-of:alice");
    assert_eq!(AssistantOf::from_tags(["ants-perms:2", "ants-assistant-of:alice"]), Some(alice.clone()));
    assert_eq!(AssistantOf::from_tags(["ANTS-ASSISTANT-OF:ALICE"]), Some(alice));
    assert_eq!(AssistantOf::from_tags(["ants-assistant-of:"]), None);
    assert_eq!(AssistantOf::from_tags(["ants-assistant-of:no_such"]), None);
    assert_eq!(AssistantOf::from_tags(["ants-admin"]), None);
    assert!(is_admin(["ants-device:d1", "ANTS-ADMIN"]));
    assert!(AdminRole::from_tags([AdminRole::TAG]));
    assert!(!is_admin(["ants-admin-not", "admin"]));
}

#[test]
fn verifies_a_team_members_user_jwt() {
    let account = KeyPair::new_account();
    let user = KeyPair::new_user();
    let token = user_jwt(&account, &user, "alice", &["ants-admin", "ants-perms:2"], Some(NOW + 60));
    let verified = verify_user_jwt(&token, [account.public_key().as_str()], NOW).unwrap();
    assert_eq!(verified.id.as_str(), "alice");
    assert_eq!(verified.public_key, user.public_key());
    assert_eq!(verified.issued_at, Some(NOW - 10));
    assert_eq!(verified.expires_at, Some(NOW + 60));
    assert!(verified.is_admin());
    assert_eq!(verified.assistant_of(), None);
    let web = user_jwt(&account, &user, "alice", &["ants-admin", "ants-web"], Some(NOW + 60));
    let web = verify_user_jwt(&web, [account.public_key().as_str()], NOW).unwrap();
    assert!(!web.is_admin(), "a web device never holds the admin role");

    let other = KeyPair::new_account();
    let accounts = [other.public_key(), account.public_key()];
    assert!(verify_user_jwt(&token, accounts.iter().map(String::as_str), NOW).is_ok());
    assert_eq!(
        verify_user_jwt(&token, [other.public_key().as_str()], NOW),
        Err(JwtError::ForeignAccount(account.public_key()))
    );
    assert_eq!(verify_user_jwt(&token, [account.public_key().as_str()], NOW + 60), Err(JwtError::Expired(NOW + 60)));
}

#[test]
fn refuses_what_is_no_team_members_user_jwt() {
    let account = KeyPair::new_account();
    let accounts = [account.public_key()];
    let accounts = || accounts.iter().map(String::as_str);
    let user = KeyPair::new_user();
    let check = |token: &str| verify_user_jwt(token, accounts(), NOW);

    let token = user_jwt(&account, &user, "alice", &[], None);
    let (signed, _) = token.rsplit_once('.').unwrap();
    let forged = format!("{signed}.{}", sign(&KeyPair::new_account(), signed.as_bytes()));
    assert_eq!(check(&forged), Err(JwtError::NotSignedByIssuer(account.public_key())));
    assert_eq!(check("garbage"), Err(JwtError::Malformed));

    let not_user = KeyPair::new_account();
    assert_eq!(
        check(&user_jwt(&account, &not_user, "alice", &[], None)),
        Err(JwtError::NotUserKey(not_user.public_key()))
    );
    assert_eq!(check(&user_jwt(&account, &user, "Alice!", &[], None)), Err(JwtError::InvalidName("Alice!".into())));
    let account_jwt = jwt(
        &account,
        &json!({ "iss": account.public_key(), "sub": user.public_key(), "name": "alice", "nats": { "type": "account" } }),
    );
    assert_eq!(check(&account_jwt), Err(JwtError::NotUser));
}

#[test]
fn verifies_an_admins_signed_act() {
    let account = KeyPair::new_account();
    let accounts = [account.public_key()];
    let accounts = || accounts.iter().map(String::as_str);
    let admin = KeyPair::new_user();
    let token = user_jwt(&account, &admin, "alice", &["ANTS-ADMIN"], None);
    let order = br#"{"ant":"pa","change":"model"}"#;

    let verified = verify_admin(&token, accounts(), order, &sign(&admin, order), NOW).unwrap();
    assert_eq!(verified.id.as_str(), "alice");

    let stranger = KeyPair::new_user();
    assert_eq!(
        verify_admin(&token, accounts(), order, &sign(&stranger, order), NOW),
        Err(JwtError::BadSignature(admin.public_key()))
    );
    assert_eq!(
        verify_admin(&token, accounts(), b"another order", &sign(&admin, order), NOW),
        Err(JwtError::BadSignature(admin.public_key()))
    );
    let plain = user_jwt(&account, &admin, "bob", &[], None);
    assert_eq!(
        verify_admin(&plain, accounts(), order, &sign(&admin, order), NOW),
        Err(JwtError::NotAdmin("bob".parse().unwrap()))
    );
}

#[test]
fn verifies_an_assistant_of_a_human() {
    let account = KeyPair::new_account();
    let accounts = [account.public_key()];
    let accounts = || accounts.iter().map(String::as_str);
    let pa = KeyPair::new_user();
    let alice: UserId = "alice".parse().unwrap();
    let bob: UserId = "bob".parse().unwrap();
    let token = user_jwt(&account, &pa, "pa", &["ants-assistant-of:alice"], None);

    let verified = verify_assistant_of(&token, accounts(), &alice, NOW).unwrap();
    assert_eq!(verified.assistant_of(), Some(alice));
    assert_eq!(
        verify_assistant_of(&token, accounts(), &bob, NOW),
        Err(JwtError::NotAssistant { user: "pa".parse().unwrap(), human: bob })
    );
}
