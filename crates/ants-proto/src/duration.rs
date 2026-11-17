//! Durations as the CLI flags and the config file write them: whole numbers with a unit, `s`, `m`, `h` or `d`,
//! largest unit first and each unit at most once, such as `90s`, `10m` or `1h30m`. A bare `0` is zero; any other
//! number needs a unit.

use std::fmt::Write as _;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

const UNITS: [(char, u64); 4] = [('d', 86_400), ('h', 3_600), ('m', 60), ('s', 1)];

/// Why [`parse`] refuses a duration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid duration {text:?}: {why} (such as 90s, 10m, 1h30m or 0)")]
pub struct ParseError {
    /// The text refused.
    pub text: String,
    pub why: Invalid,
}

/// What is wrong with a duration [`parse`] refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    #[error("empty")]
    Empty,
    #[error("unknown unit {0:?}")]
    UnknownUnit(char),
    #[error("a unit without a number")]
    UnitWithoutNumber,
    #[error("units out of order or repeated")]
    OutOfOrder,
    #[error("too long")]
    TooLong,
    #[error("a number without a unit")]
    NumberWithoutUnit,
}

/// Parse a duration such as `90s`, `10m`, `1h30m` or `0`.
pub fn parse(text: &str) -> Result<Duration, ParseError> {
    let invalid = |why| ParseError { text: text.to_owned(), why };
    if text == "0" {
        return Ok(Duration::ZERO);
    }
    if text.is_empty() {
        return Err(invalid(Invalid::Empty));
    }
    let mut secs: u64 = 0;
    let mut digits = String::new();
    let mut next_unit = 0;
    for c in text.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        let Some(at) = UNITS.iter().position(|(unit, _)| *unit == c) else {
            return Err(invalid(Invalid::UnknownUnit(c)));
        };
        if digits.is_empty() {
            return Err(invalid(Invalid::UnitWithoutNumber));
        }
        if at < next_unit {
            return Err(invalid(Invalid::OutOfOrder));
        }
        let count: u64 = digits.parse().map_err(|_| invalid(Invalid::TooLong))?;
        secs = count
            .checked_mul(UNITS[at].1)
            .and_then(|s| secs.checked_add(s))
            .ok_or_else(|| invalid(Invalid::TooLong))?;
        digits.clear();
        next_unit = at + 1;
    }
    if !digits.is_empty() {
        return Err(invalid(Invalid::NumberWithoutUnit));
    }
    Ok(Duration::from_secs(secs))
}

/// Write a duration as [`parse`] reads it, whole seconds only: `0`, `90s` as `1m30s`, `1h`.
pub fn format(duration: Duration) -> String {
    let mut secs = duration.as_secs();
    if secs == 0 {
        return "0".to_owned();
    }
    let mut out = String::new();
    for (unit, size) in UNITS {
        if secs >= size {
            let _ = write!(out, "{}{unit}", secs / size);
            secs %= size;
        }
    }
    out
}

/// Deserialize a duration written as [`parse`] reads it, for `#[serde(deserialize_with = ...)]`.
pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    parse(&text).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_add_up_largest_first() {
        let secs = |text| parse(text).map(|d| d.as_secs());
        assert_eq!(secs("0"), Ok(0));
        assert_eq!(secs("0s"), Ok(0));
        assert_eq!(secs("90s"), Ok(90));
        assert_eq!(secs("10m"), Ok(600));
        assert_eq!(secs("1h30m"), Ok(5_400));
        assert_eq!(secs("2d"), Ok(172_800));
        assert_eq!(secs("1d2h3m4s"), Ok(93_784));
    }

    #[test]
    fn bare_numbers_unknown_units_and_disorder_are_rejected() {
        for bad in
            ["", "600", "10", "m", "10x", "1.5h", "-5s", "30m1h", "1m1m", "10 m", "1h 30m", "99999999999999999999d"]
        {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
        let refused = parse("600").unwrap_err();
        assert_eq!(refused.why, Invalid::NumberWithoutUnit);
        assert_eq!(
            refused.to_string(),
            "invalid duration \"600\": a number without a unit (such as 90s, 10m, 1h30m or 0)"
        );
    }

    #[test]
    fn format_reads_back() {
        for secs in [0, 1, 59, 60, 90, 3_600, 5_400, 86_400, 93_784] {
            let d = Duration::from_secs(secs);
            assert_eq!(parse(&format(d)), Ok(d), "{}", format(d));
        }
        assert_eq!(format(Duration::from_secs(90)), "1m30s");
    }
}
