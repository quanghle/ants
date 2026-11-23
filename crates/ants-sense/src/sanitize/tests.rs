//! Tests of the sanitizer: each kind of secret redacted, ordinary text kept, hidden characters stripped, one-line
//! titles, limits on the text as given, multibyte text, and idempotence.

use super::*;

const BODY: Limit = Limit::Bytes(4096);

fn body(text: &str) -> Sanitized {
    sanitize_body(text, BODY).expect("within the limit")
}

fn redacted(text: &str) -> String {
    body(text).text
}

/// `n` characters cycling through `alphabet`, so that no test secret appears whole in the source.
fn fill(alphabet: &str, n: usize) -> String {
    alphabet.chars().cycle().take(n).collect()
}

fn alnum(n: usize) -> String {
    fill("aB3dE5gH7jK9mN1pQ", n)
}

fn assert_redacts(secret: &str, kind: SecretKind) {
    let text = format!("before {secret} after");
    let out = body(&text);
    assert_eq!(out.text, format!("before [redacted:{kind}] after"), "{secret}");
    assert_eq!(out.redactions, [Redaction { kind, count: 1 }]);
}

fn assert_untouched(text: &str) {
    let out = body(text);
    assert_eq!(out.text, text);
    assert!(out.redactions.is_empty(), "{text}: {:?}", out.redactions);
}

#[test]
fn private_keys() {
    for label in ["", "RSA ", "EC ", "OPENSSH ", "ENCRYPTED ", "PGP "] {
        let block = if label == "PGP " { " BLOCK" } else { "" };
        let key = format!(
            "-----BEGIN {label}PRIVATE KEY{block}-----\n{}\n{}\n-----END {label}PRIVATE KEY{block}-----",
            alnum(64),
            alnum(20)
        );
        assert_redacts(&key, SecretKind::PrivateKey);
    }
    let truncated = format!("key:\n-----BEGIN PRIVATE KEY-----\n{}\n", alnum(64));
    assert_eq!(redacted(&truncated), "key:\n[redacted:private-key]");
    assert_untouched("-----BEGIN PUBLIC KEY-----\nMFkwEwYH\n-----END PUBLIC KEY-----");
}

#[test]
fn nats_creds_and_seeds() {
    let seed = format!("SU{}", fill("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", 56));
    assert_redacts(&seed, SecretKind::NkeySeed);
    assert_redacts(&format!("SA{}", &seed[2..]), SecretKind::NkeySeed);
    // A public key, or one character short of a seed.
    assert_untouched(&format!("U{}", &seed[2..]));
    assert_untouched(&seed[..57]);

    let jwt = format!("eyJ0eXAiOiJKV1Qi.eyJqdGkiOiJ{}.{}", alnum(30), alnum(40));
    let creds = format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
         ************************* IMPORTANT *************************\n\n\
         -----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
    );
    let out = body(&creds);
    assert_eq!(
        out.text,
        "[redacted:nats-creds]\n\n************************* IMPORTANT *************************\n\n\
         [redacted:nats-creds]\n"
    );
    assert_eq!(out.redactions, [Redaction { kind: SecretKind::NatsCreds, count: 2 }]);
}

#[test]
fn jwts() {
    let jwt = format!("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOi{}.{}", alnum(20), fill("a-b_c", 43));
    assert_redacts(&jwt, SecretKind::Jwt);
    assert_untouched("eyJhbGciOiJIUzI1NiJ9 alone is a header");
}

#[test]
fn aws_keys() {
    assert_redacts(&format!("AKIA{}", fill("Z7Y6X5W4", 16)), SecretKind::AwsAccessKey);
    assert_redacts(&format!("ASIA{}", fill("Q2R3", 16)), SecretKind::AwsAccessKey);
    assert_untouched(&format!("AKIA{}", fill("Z7", 15)));

    let secret = fill("wJalrXUtnFEMI/K7MDENG/bPxRfiCY+", 40);
    for line in [
        format!("aws_secret_access_key = {secret}"),
        format!("AWS_SECRET_ACCESS_KEY={secret}"),
        format!("export AWS_SECRET_ACCESS_KEY=\"{secret}\""),
    ] {
        let out = body(&line);
        assert_eq!(out.text, line.replace(&secret, "[redacted:aws-secret-key]"));
        assert_eq!(out.redactions, [Redaction { kind: SecretKind::AwsSecretKey, count: 1 }]);
    }
}

#[test]
fn vendor_tokens() {
    for prefix in ["ghp_", "gho_", "ghu_", "ghs_", "ghr_"] {
        assert_redacts(&format!("{prefix}{}", alnum(36)), SecretKind::GithubToken);
    }
    assert_redacts(&format!("github_pat_{}_{}", alnum(22), alnum(59)), SecretKind::GithubToken);
    for prefix in ["xoxb-", "xoxp-", "xoxa-", "xoxs-", "xoxr-"] {
        assert_redacts(&format!("{prefix}1234567890-{}", alnum(24)), SecretKind::SlackToken);
    }
    assert_redacts(&format!("sk-ant-api03-{}", fill("aB3-_x", 93)), SecretKind::AnthropicKey);
    assert_redacts(&format!("sk-proj-{}", fill("Zz9_-y", 120)), SecretKind::OpenaiKey);
    assert_redacts(&format!("sk-{}", alnum(48)), SecretKind::OpenaiKey);
    assert_redacts(&format!("AIza{}", fill("Sy0-_b", 35)), SecretKind::GoogleApiKey);
    assert_redacts(&format!("sk_live_{}", alnum(24)), SecretKind::StripeKey);
    assert_redacts(&format!("rk_live_{}", alnum(24)), SecretKind::StripeKey);
}

#[test]
fn bearer_tokens() {
    let token = alnum(32);
    for (text, expected) in [
        (format!("Authorization: Bearer {token}"), "Authorization: Bearer [redacted:bearer-token]"),
        (format!("curl -H 'authorization: bearer {token}'"), "curl -H 'authorization: bearer [redacted:bearer-token]'"),
        ("Authorization: Bearer xyz".to_owned(), "Authorization: Bearer [redacted:bearer-token]"),
        (format!("send Bearer {token} along"), "send Bearer [redacted:bearer-token] along"),
        ("Authorization: Basic dXNlcjpwYXNz".to_owned(), "Authorization: Basic [redacted:authorization]"),
    ] {
        assert_eq!(redacted(&text), expected);
    }
    for text in [
        "Bearer authentication sends a token in a header.",
        "Authorization: Bearer <token>",
        "Authorization: Bearer $TOKEN",
        "Authorization: Bearer ${GITHUB_TOKEN}",
        "Authorization: Bearer ***",
        "Authorization: required",
    ] {
        assert_untouched(text);
    }
}

#[test]
fn connection_strings_keep_all_but_the_password() {
    for (text, expected) in [
        (
            "postgres://app:hunter2@db.internal:5432/app",
            "postgres://app:[redacted:connection-string]@db.internal:5432/app",
        ),
        ("redis://:s3cret@cache:6379", "redis://:[redacted:connection-string]@cache:6379"),
        (
            "mongodb+srv://u:p@ss@cluster0.example.net/x",
            "mongodb+srv://u:[redacted:connection-string]@cluster0.example.net/x",
        ),
        ("amqp://guest:guest@localhost", "amqp://guest:[redacted:connection-string]@localhost"),
    ] {
        assert_eq!(redacted(text), expected);
    }
    for text in [
        "https://example.com:8443/path?q=a@b",
        "postgres://app@db/app",
        "postgres://app:@db/app",
        "postgres://app:<password>@db/app",
        "postgres://app:${DB_PASSWORD}@db/app",
        "postgres://app:***@db/app",
        "mail user@example.com: see ssh://git@github.com/x",
    ] {
        assert_untouched(text);
    }
}

#[test]
fn secret_assignments_keep_the_name() {
    for (text, expected) in [
        ("API_KEY=abc123", "API_KEY=[redacted:secret]"),
        ("export GH_TOKEN='abc def'", "export GH_TOKEN='[redacted:secret]'"),
        ("DB_PASSWORD = \"p4ss word\"", "DB_PASSWORD = \"[redacted:secret]\""),
        ("STRIPE_WEBHOOK_SECRET=whsec_1\nNEXT=1", "STRIPE_WEBHOOK_SECRET=[redacted:secret]\nNEXT=1"),
        ("SMTP_PASSWD=x1", "SMTP_PASSWD=[redacted:secret]"),
        ("GOOGLE_CREDENTIALS=blob", "GOOGLE_CREDENTIALS=[redacted:secret]"),
        ("GITLAB_PAT=glpat-abc", "GITLAB_PAT=[redacted:secret]"),
        ("SENTRY_KEY=abc", "SENTRY_KEY=[redacted:secret]"),
        ("password: hunter2", "password: [redacted:secret]"),
        ("  client_secret: abc # rotated", "  client_secret: [redacted:secret] # rotated"),
        ("{\"apiKey\":\"abc\",\"user\":\"me\"}", "{\"apiKey\":\"[redacted:secret]\",\"user\":\"me\"}"),
        ("'password' => 'abc',", "'password' => '[redacted:secret]',"),
        ("mytool --api-key=abc123 run", "mytool --api-key=[redacted:secret] run"),
        ("Server=db;User Id=sa;Password=Pa55;", "Server=db;User Id=sa;Password=[redacted:secret];"),
        ("x-api-key: abc123", "x-api-key: [redacted:secret]"),
    ] {
        assert_eq!(redacted(text), expected, "{text}");
    }
}

#[test]
fn ordinary_text_stays() {
    for text in [
        "The password must be rotated monthly.",
        "Password: rotate it monthly",
        "Ask for a token in the settings page.",
        "API_KEY=",
        "API_KEY=<your key>",
        "API_KEY=${API_KEY}",
        "API_KEY=$API_KEY",
        "API_KEY=***",
        "PASSWORD=xxxxxxxx",
        "TOKEN=[redacted:secret]",
        "max_tokens: 4096",
        "MAX_TOKENS=4096",
        "TOKEN_URL=https://auth.example.com/token",
        "PASSWORD_FILE=/run/secrets/db",
        "tokenizer: cl100k",
        "primary_key=True",
        "sort_key = \"date\"",
        "REQUIRE_PASSWORD=true",
        "password = os.getenv(\"PASSWORD\")",
        "token = settings.API_TOKEN",
        "password: String,",
        "fn login(password: &str)",
        "if password == expected {",
        "if token != None:",
        "the risk-skewed tasks: desk-sk-learning-rate-scheduler",
        "use sk-learn, not sk-",
        "PWD=/home/ant",
        "This is fine: AKIA is a prefix.",
    ] {
        assert_untouched(text);
    }
}

#[test]
fn several_secrets_and_overlaps() {
    let gh = format!("ghp_{}", alnum(36));
    let jwt = format!("eyJhbGciOi.eyJzdWIiOi{}.{}", alnum(10), alnum(20));
    let text =
        format!("GITHUB_TOKEN={gh}\nAuthorization: Bearer {jwt}\nuse {gh} twice\nDATABASE_URL=postgres://u:pw@h/db\n");
    let out = body(&text);
    assert_eq!(
        out.text,
        "GITHUB_TOKEN=[redacted:github-token]\nAuthorization: Bearer [redacted:jwt]\nuse [redacted:github-token] \
         twice\nDATABASE_URL=postgres://u:[redacted:connection-string]@h/db\n"
    );
    assert_eq!(
        out.redactions,
        [
            Redaction { kind: SecretKind::Jwt, count: 1 },
            Redaction { kind: SecretKind::GithubToken, count: 2 },
            Redaction { kind: SecretKind::ConnectionString, count: 1 },
        ]
    );
    let key = format!("PRIVATE_KEY=\"-----BEGIN PRIVATE KEY-----\\n{}\\n-----END PRIVATE KEY-----\\n\"", alnum(40));
    assert_eq!(redacted(&key), "PRIVATE_KEY=\"[redacted:private-key]\\n\"");
}

#[test]
fn hidden_characters_are_stripped() {
    let out = body("a\u{0}b\u{7}c\u{1b}[31md\u{7f}e\u{85}f");
    assert_eq!(out.text, "abc[31mdef");
    assert_eq!(out.stripped, 5);
    assert_eq!(redacted("x\u{202E}cba\u{202C}y\u{2066}z\u{2069}\u{200E}\u{200F}\u{061C}"), "xcbayz");
    assert_eq!(redacted("zero\u{200B}width\u{FEFF}\u{2060}\u{00AD}"), "zerowidth");
    assert_eq!(redacted("tag\u{E0041}\u{E0042}s"), "tags");
    assert_eq!(redacted("line\r\nbreaks\rand\u{2028}more\n\ttab"), "line\nbreaks\nand\nmore\n\ttab");
}

#[test]
fn joiners_stay_only_between_non_ascii() {
    let family = "👨\u{200D}👩\u{200D}👧";
    let persian = "می\u{200C}خواهم";
    assert_eq!(redacted(family), family);
    assert_eq!(redacted(persian), persian);
    assert_eq!(redacted("a\u{200D}b \u{200C}é é\u{200D} \u{200D}"), "ab é é ");
}

#[test]
fn hidden_characters_cannot_split_a_secret() {
    let gh = alnum(36);
    let text = format!("ghp_\u{200B}{}\u{200D}{}", &gh[..10], &gh[10..]);
    assert_eq!(redacted(&text), "[redacted:github-token]");
}

#[test]
fn titles_are_one_line() {
    let out = sanitize_title("fix\r\nthe\tbuild\u{2029}now", Limit::Chars(200)).expect("within the limit");
    assert_eq!(out.text, "fix the build now");
    assert_eq!(out.stripped, 0);
    let token = format!("GH_TOKEN=ghp_{}", alnum(36));
    assert_eq!(sanitize_title(&token, Limit::Chars(200)).expect("fits").text, "GH_TOKEN=[redacted:github-token]");
}

#[test]
fn limits_are_measured_on_the_text_as_given() {
    let title = "é".repeat(200);
    assert!(sanitize_title(&title, Limit::Chars(200)).is_ok());
    let refused = sanitize_title(&format!("{title}a"), Limit::Chars(200)).expect_err("over the limit");
    assert_eq!(refused, Refused { limit: Limit::Chars(200), size: 201 });
    assert_eq!(refused.to_string(), "text of 201 characters exceeds the limit of 200 characters");

    let refused = sanitize_body(&title, Limit::Bytes(399)).expect_err("over the limit");
    assert_eq!(refused, Refused { limit: Limit::Bytes(399), size: 400 });
    // Stripped characters count: the limit is on what the ant sent.
    let hidden = format!("{}\u{200B}", "a".repeat(4096));
    assert!(sanitize_body(&hidden, BODY).is_err());
    assert!(sanitize_body(&"a".repeat(4096), BODY).is_ok());
}

#[test]
fn multibyte_text_around_secrets() {
    let gh = format!("ghp_{}", alnum(36));
    let text = format!("日本語{gh}é\nパスワード: hunter2\nPASSWORD=ünïcödé🙂 ok\npostgres://ü:pässwörd@hôst/db");
    assert_eq!(
        redacted(&text),
        "日本語[redacted:github-token]é\nパスワード: hunter2\nPASSWORD=[redacted:secret] ok\n\
         postgres://ü:[redacted:connection-string]@hôst/db"
    );
}

#[test]
fn sanitizing_twice_changes_nothing() {
    let texts = [
        format!("GITHUB_TOKEN={}", format_args!("ghp_{}", alnum(36))),
        format!("postgres://u:pw@h/db and Bearer {}", alnum(30)),
        format!("-----BEGIN RSA PRIVATE KEY-----\n{}", alnum(50)),
        "API_KEY=abc\npassword: hunter2\n\u{200B}a\u{200D}b 👨\u{200D}👩\r\n".to_owned(),
        format!("Authorization: Basic {} {}", alnum(20), format_args!("sk-{}", alnum(40))),
    ];
    for text in texts {
        let once = body(&text);
        assert!(!once.redactions.is_empty() || once.stripped > 0, "{text}");
        let twice = body(&once.text);
        assert_eq!(twice.text, once.text);
        assert!(twice.redactions.is_empty());
        assert_eq!(twice.stripped, 0);
    }
}
