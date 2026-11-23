//! Snapshots of how machines, humans and ants come to their credentials: enrollment, login and ant requests.

use super::*;

#[test]
fn enrollment() {
    snap(
        "enroll_request",
        &EnrollRequest {
            ant: user("dev-a"),
            public_key: "UABC".to_owned(),
            host: "laptop.local".to_owned(),
            sent_at: at(0),
            enrollment: "eyJ0eXAiOiJKV1QifQ.enroll".to_owned(),
            signature: "c2lnLWVucm9sbA".to_owned(),
            leaf: Some(enroll_leaf()),
            order: None,
            host_creds: Some(EnrollHost { machine: machine(), public_key: "UHOST".to_owned() }),
        },
    );
    snap("enroll_leaf", &enroll_leaf());
    snap(
        "enroll_reply_variants",
        &[
            EnrollReply::Approved {
                ant: user("dev-a"),
                public_key: "UABC".to_owned(),
                jwt: "eyJ0eXAiOiJKV1QifQ.user".to_owned(),
                leaf_jwt: Some("eyJ0eXAiOiJKV1QifQ.leaf".to_owned()),
                host_jwt: Some("eyJ0eXAiOiJKV1QifQ.host".to_owned()),
            },
            EnrollReply::Rejected {
                ant: user("dev-a"),
                public_key: Some("UABC".to_owned()),
                reason: Some("unknown machine".to_owned()),
            },
            EnrollReply::Retired {
                ant: user("dev-a"),
                public_key: "UABC".to_owned(),
                revoked: vec!["UABC".to_owned()],
                push_error: None,
                leaf_jwt: Some("eyJ0eXAiOiJKV1QifQ.leaf".to_owned()),
                leaf_error: None,
            },
        ],
    );
    snap(
        "renew_request",
        &RenewRequest {
            name: "dev-a".to_owned(),
            public_key: "UDEF".to_owned(),
            host: "laptop.local".to_owned(),
            sent_at: at(0),
            current: "eyJ0eXAiOiJKV1QifQ.current".to_owned(),
            xkey: None,
            signature: "c2lnLXJlbmV3".to_owned(),
        },
    );
    snap(
        "renew_request_xkey",
        &RenewRequest {
            name: "alice".to_owned(),
            public_key: "UDEF".to_owned(),
            host: "laptop.local".to_owned(),
            sent_at: at(0),
            current: "eyJ0eXAiOiJKV1QifQ.current".to_owned(),
            xkey: Some("XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap()),
            signature: "c2lnLXJlbmV3".to_owned(),
        },
    );
    snap("renew_reply", &RenewReply::signed("dev-a", "UDEF", "eyJ0eXAiOiJKV1QifQ.renewed".to_owned()));
    snap(
        "renew_reply_refused",
        &RenewReply::refused("alice", "UDEF", "GitHub user octocat is no member of acme any more".to_owned()),
    );
}

#[test]
fn login() {
    snap(
        "login_request",
        &LoginRequest {
            name: user("alice"),
            public_key: "UDEF".to_owned(),
            reply_xkey: "XREPLY".to_owned(),
            xkey: Some("XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap()),
            web: false,
            label: Some("Alice's laptop".to_owned()),
            made_at: at(0),
            expires_at: at(15),
            signature: "c2lnLWxvZ2lu".to_owned(),
        },
    );
    snap(
        "login_request_web",
        &LoginRequest {
            name: user("alice"),
            public_key: "UDEF".to_owned(),
            reply_xkey: "XREPLY".to_owned(),
            xkey: None,
            web: true,
            label: Some("Alice's phone browser".to_owned()),
            made_at: at(0),
            expires_at: at(15),
            signature: "c2lnLWxvZ2lu".to_owned(),
        },
    );
    snap(
        "login_reply",
        &LoginReply {
            name: user("alice"),
            public_key: "UDEF".to_owned(),
            sender: "XSIGNER".to_owned(),
            sealed: "c2VhbGVk".to_owned(),
        },
    );
    snap(
        "login_answer_variants",
        &vec![
            LoginAnswer::Verify {
                url: "https://github.com/login/device".to_owned(),
                user_code: "WDJB-MJHT".to_owned(),
                expires_at: at(15),
            },
            LoginAnswer::Waiting { login: "octocat".to_owned(), reason: "outside the registration policy".to_owned() },
            LoginAnswer::Refused { reason: "alice is bound to another GitHub user".to_owned() },
            LoginAnswer::Signed { jwt: "eyJ0eXAiOiJKV1QifQ.alice".to_owned() },
        ],
    );
}

#[test]
fn ant_requests() {
    snap(
        "ant_request",
        &AntRequest {
            manager: user("lead"),
            name: user("dev-c"),
            template: "rust-dev".to_owned(),
            request: r#"{"whoami":"Parser work"}"#.to_owned(),
            reason: "the parser needs a hand".to_owned(),
            thread_id: thread(),
            public_key: "UGHI".to_owned(),
            host: "studio.local".to_owned(),
            sent_at: at(0),
            expires_at: at(10),
            credentials: "eyJ0eXAiOiJKV1QifQ.manager".to_owned(),
            signature: "c2lnLWFudA".to_owned(),
            leaf: Some(enroll_leaf()),
            retire: true,
        },
    );
    snap(
        "ant_reply_variants",
        &[
            AntReply::Approved {
                manager: user("lead"),
                name: user("dev-c"),
                public_key: "UGHI".to_owned(),
                jwt: "eyJ0eXAiOiJKV1QifQ.user".to_owned(),
                leaf_jwt: Some("eyJ0eXAiOiJKV1QifQ.leaf".to_owned()),
            },
            AntReply::Refused {
                manager: user("lead"),
                name: user("dev-c"),
                public_key: "UGHI".to_owned(),
                reason: "outside the template".to_owned(),
            },
            AntReply::Retired {
                manager: user("lead"),
                name: user("dev-c"),
                public_key: "UGHI".to_owned(),
                revoked: vec!["UGHI".to_owned()],
                push_error: Some("hub unreachable".to_owned()),
                leaf_jwt: None,
                leaf_error: Some("no uplink of m1".to_owned()),
            },
        ],
    );
}
