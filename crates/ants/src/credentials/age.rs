//! Passphrase encryption in the age v1 file format (age-encryption.org/v1, with its scrypt recipient and ASCII
//! armor), for `ants auth backup`: `age -d` decrypts what [`encrypt`] writes, and [`decrypt`] reads what `age -p`
//! writes, armored or not.
//!
//! A random file key encrypts the payload, in chunks of 64 KiB with ChaCha20-Poly1305 (the STREAM construction);
//! the passphrase, through scrypt, wraps the file key; an HMAC of the header under the file key authenticates the
//! header.

use std::fmt::Write as _;

use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::{BASE64, BASE64_NOPAD};
use ring::rand::{SecureRandom as _, SystemRandom};
use ring::{aead, hkdf, hmac};

const INTRO: &str = "age-encryption.org/v1";
const SCRYPT_LABEL: &[u8] = b"age-encryption.org/v1/scrypt";
const BEGIN: &str = "-----BEGIN AGE ENCRYPTED FILE-----";
const END: &str = "-----END AGE ENCRYPTED FILE-----";
const CHUNK: usize = 64 * 1024;
const TAG: usize = 16;
/// The scrypt work factor (log2 of its cost) of what [`encrypt`] writes, as `age -p` uses: about 256 MiB of memory.
pub const WORK_FACTOR: u8 = 18;
/// The highest work factor [`decrypt`] accepts, as age does, so a file cannot make it work for hours.
const MAX_WORK_FACTOR: u8 = 22;

/// Why [`decrypt`] refused a file it could read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The passphrase does not open the file key.
    WrongPassphrase,
    /// The header or the payload fails authentication or is cut short: damaged or altered.
    Damaged,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WrongPassphrase => "wrong passphrase",
            Self::Damaged => "damaged or altered",
        })
    }
}

impl std::error::Error for Refused {}

/// `plaintext` encrypted with `passphrase` at scrypt's `work_factor`, ASCII-armored.
pub fn encrypt(plaintext: &[u8], passphrase: &[u8], work_factor: u8) -> Result<String> {
    let rng = SystemRandom::new();
    let file_key: [u8; 16] = random(&rng)?;
    let salt: [u8; 16] = random(&rng)?;
    let mut body = file_key.to_vec();
    seal(&wrap_key(passphrase, &salt, work_factor)?, [0; 12], &mut body)?;
    let mut header =
        format!("{INTRO}\n-> scrypt {} {work_factor}\n{}\n---", BASE64_NOPAD.encode(&salt), BASE64_NOPAD.encode(&body));
    let mac = hmac::sign(&header_key(&file_key), header.as_bytes());
    writeln!(header, " {}", BASE64_NOPAD.encode(mac.as_ref()))?;
    let mut out = header.into_bytes();
    let nonce: [u8; 16] = random(&rng)?;
    out.extend_from_slice(&nonce);
    let key = payload_key(&file_key, &nonce);
    let chunks: Vec<&[u8]> = if plaintext.is_empty() { vec![&[]] } else { plaintext.chunks(CHUNK).collect() };
    let last = chunks.len() - 1;
    for (n, chunk) in chunks.into_iter().enumerate() {
        let mut sealed = chunk.to_vec();
        seal(&key, chunk_nonce(n as u64, n == last), &mut sealed)?;
        out.extend_from_slice(&sealed);
    }
    Ok(armor(&out))
}

/// The plaintext of `file`, armored or not, encrypted with `passphrase`. A wrong passphrase or a damaged file fails
/// with a [`Refused`].
pub fn decrypt(file: &[u8], passphrase: &[u8]) -> Result<Vec<u8>> {
    let binary = dearmor(file)?;
    let not_age = || anyhow!("not a file encrypted with a passphrase by ants auth backup or age");
    let mac_at = binary.windows(5).position(|w| w == b"\n--- ").ok_or_else(not_age)?;
    let header = std::str::from_utf8(&binary[..mac_at + 4]).map_err(|_| not_age())?;
    let rest = &binary[mac_at + 5..];
    let line_end = rest.iter().position(|&b| b == b'\n').ok_or_else(not_age)?;
    let mac = BASE64_NOPAD.decode(&rest[..line_end]).map_err(|_| not_age())?;
    let payload = &rest[line_end + 1..];
    let lines: Vec<&str> = header.split('\n').collect();
    let [INTRO, stanza, body, "---"] = lines.as_slice() else {
        if lines.first() == Some(&INTRO) && lines.iter().any(|l| l.starts_with("-> ") && !l.starts_with("-> scrypt ")) {
            bail!("the file is encrypted to age recipients, not with a passphrase");
        }
        return Err(not_age());
    };
    let args: Vec<&str> = stanza.split(' ').collect();
    let ["->", "scrypt", salt, work_factor] = args.as_slice() else {
        bail!("the file is encrypted to age recipients, not with a passphrase");
    };
    let salt = BASE64_NOPAD.decode(salt.as_bytes()).ok().filter(|s| s.len() == 16).ok_or_else(not_age)?;
    let work_factor: u8 = work_factor.parse().ok().filter(|_| !work_factor.starts_with('0')).ok_or_else(not_age)?;
    if work_factor > MAX_WORK_FACTOR {
        bail!("the file asks for scrypt work factor {work_factor}, more than the {MAX_WORK_FACTOR} allowed");
    }
    let mut file_key = BASE64_NOPAD.decode(body.as_bytes()).ok().filter(|b| b.len() == 32).ok_or_else(not_age)?;
    open(&wrap_key(passphrase, &salt, work_factor)?, [0; 12], &mut file_key).ok_or(Refused::WrongPassphrase)?;
    hmac::verify(&header_key(&file_key), header.as_bytes(), &mac)
        .map_err(|_| anyhow!(Refused::Damaged).context("its header fails authentication"))?;
    let (nonce, mut rest) = payload.split_at_checked(16).ok_or(Refused::Damaged).context("no payload")?;
    let key = payload_key(&file_key, nonce);
    let mut plaintext = Vec::new();
    for n in 0u64.. {
        let (chunk, after) = rest.split_at(rest.len().min(CHUNK + TAG));
        let last = after.is_empty();
        let mut opened = chunk.to_vec();
        if chunk.len() < TAG
            || (last && n > 0 && chunk.len() == TAG)
            || open(&key, chunk_nonce(n, last), &mut opened).is_none()
        {
            return Err(anyhow!(Refused::Damaged).context(format!("its payload fails authentication at chunk {n}")));
        }
        plaintext.extend_from_slice(&opened);
        if last {
            break;
        }
        rest = after;
    }
    Ok(plaintext)
}

fn random<const N: usize>(rng: &SystemRandom) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    rng.fill(&mut bytes).map_err(|_| anyhow!("no random numbers from the system"))?;
    Ok(bytes)
}

/// The key wrapping the file key: scrypt of `passphrase`, N = 2^work_factor, r = 8, p = 1.
fn wrap_key(passphrase: &[u8], salt: &[u8], work_factor: u8) -> Result<[u8; 32]> {
    let params =
        scrypt::Params::new(work_factor, 8, 1).map_err(|e| anyhow!("scrypt work factor {work_factor}: {e}"))?;
    let salt = [SCRYPT_LABEL, salt].concat();
    let mut key = [0; 32];
    scrypt::scrypt(passphrase, &salt, &params, &mut key).map_err(|e| anyhow!("scrypt: {e}"))?;
    Ok(key)
}

fn header_key(file_key: &[u8]) -> hmac::Key {
    hmac::Key::new(hmac::HMAC_SHA256, &hkdf_sha256(&[], file_key, b"header"))
}

fn payload_key(file_key: &[u8], nonce: &[u8]) -> [u8; 32] {
    hkdf_sha256(nonce, file_key, b"payload")
}

struct Len(usize);

impl hkdf::KeyType for Len {
    fn len(&self) -> usize {
        self.0
    }
}

fn hkdf_sha256(salt: &[u8], ikm: &[u8], info: &[u8]) -> [u8; 32] {
    let mut out = [0; 32];
    let info = [info];
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, salt).extract(ikm);
    // 32 bytes are well within what HKDF-SHA256 derives.
    prk.expand(&info, Len(out.len())).and_then(|okm| okm.fill(&mut out)).expect("HKDF-SHA256 derives 32 bytes");
    out
}

/// The nonce of STREAM chunk `n`: its 11-byte big-endian counter, then 1 for the last chunk.
fn chunk_nonce(n: u64, last: bool) -> [u8; 12] {
    let mut nonce = [0; 12];
    nonce[3..11].copy_from_slice(&n.to_be_bytes());
    nonce[11] = u8::from(last);
    nonce
}

fn cipher(key: &[u8; 32]) -> aead::LessSafeKey {
    aead::LessSafeKey::new(aead::UnboundKey::new(&aead::CHACHA20_POLY1305, key).expect("a 32-byte ChaCha20 key"))
}

fn seal(key: &[u8; 32], nonce: [u8; 12], data: &mut Vec<u8>) -> Result<()> {
    let nonce = aead::Nonce::assume_unique_for_key(nonce);
    cipher(key).seal_in_place_append_tag(nonce, aead::Aad::empty(), data).map_err(|_| anyhow!("encrypting"))
}

fn open(key: &[u8; 32], nonce: [u8; 12], data: &mut Vec<u8>) -> Option<()> {
    let nonce = aead::Nonce::assume_unique_for_key(nonce);
    let len = cipher(key).open_in_place(nonce, aead::Aad::empty(), data).ok()?.len();
    data.truncate(len);
    Some(())
}

/// `binary` in age's ASCII armor: padded base64 in lines of 64 columns between the BEGIN and END lines.
fn armor(binary: &[u8]) -> String {
    let encoded = BASE64.encode(binary);
    let mut text = format!("{BEGIN}\n");
    for line in encoded.as_bytes().chunks(64) {
        text.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        text.push('\n');
    }
    text.push_str(END);
    text.push('\n');
    text
}

/// The binary age file of `file`, armored or not.
fn dearmor(file: &[u8]) -> Result<Vec<u8>> {
    let trimmed = file.trim_ascii();
    let Some(armored) = trimmed.strip_prefix(BEGIN.as_bytes()) else {
        return Ok(file.to_vec());
    };
    let inner = armored.strip_suffix(END.as_bytes()).context("an armored age file without its END line")?;
    let inner = std::str::from_utf8(inner).context("an armored age file that is not text")?;
    let encoded: String = inner.lines().map(str::trim).collect();
    BASE64.decode(encoded.as_bytes()).map_err(|e| anyhow!("an armored age file of invalid base64: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(e: &anyhow::Error) -> Option<Refused> {
        e.downcast_ref::<Refused>().copied()
    }

    #[test]
    fn what_is_encrypted_decrypts_with_the_passphrase_only_and_unaltered() {
        let long: Vec<u8> = (0..CHUNK * 2 + 7).map(|n| n as u8).collect();
        for plaintext in [&b""[..], b"team keys", &long[..CHUNK], &long] {
            let file = encrypt(plaintext, b"correct horse", 10).unwrap();
            assert!(file.starts_with(BEGIN) && file.lines().all(|l| l.len() <= 64), "{file}");
            assert_eq!(decrypt(file.as_bytes(), b"correct horse").unwrap(), plaintext);
            let wrong = decrypt(file.as_bytes(), b"correct horse!").unwrap_err();
            assert_eq!(refused(&wrong), Some(Refused::WrongPassphrase), "{wrong:#}");
        }
        let binary = dearmor(encrypt(&long, b"pass", 10).unwrap().as_bytes()).unwrap();
        let mac_at = binary.windows(5).position(|w| w == b"\n--- ").unwrap() + 5;
        let payload = mac_at + binary[mac_at..].iter().position(|&b| b == b'\n').unwrap() + 1;
        let mut forged_mac = binary[..mac_at].to_vec();
        forged_mac.extend_from_slice(format!("{}\n", BASE64_NOPAD.encode(&[0; 32])).as_bytes());
        forged_mac.extend_from_slice(&binary[payload..]);
        let mut cut = binary.clone();
        cut.truncate(binary.len() - (7 + TAG));
        let mut altered = vec![forged_mac, cut];
        for at in [payload + 20, binary.len() - 1] {
            altered.push(binary.clone());
            altered.last_mut().unwrap()[at] ^= 1;
        }
        for altered in altered {
            let e = decrypt(&altered, b"pass").unwrap_err();
            assert_eq!(refused(&e), Some(Refused::Damaged), "{e:#}");
        }
    }

    #[test]
    fn a_file_of_another_kind_is_not_taken_for_a_damaged_one() {
        let e = decrypt(b"operator: eyJ...\n", b"pass").unwrap_err();
        assert_eq!(refused(&e), None, "{e:#}");
        let recipients = "age-encryption.org/v1\n-> X25519 abc\nAAAA\n--- AAAA\n";
        let e = decrypt(recipients.as_bytes(), b"pass").unwrap_err();
        assert!(format!("{e:#}").contains("recipients"), "{e:#}");
    }
}
