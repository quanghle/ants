//! Durations as the CLI flags and the config file write them: whole numbers with a unit, `s`, `m`, `h` or `d`,
//! largest unit first and each unit at most once, such as `90s`, `10m` or `1h30m`. A bare `0` is zero; any other
//! number needs a unit.

use std::time::Duration;

use serde::{Deserialize, Deserializer};

const UNITS: [(char, u64); 4] = [('d', 86_400), ('h', 3_600), ('m', 60), ('s', 1)];

/// Parse a duration such as `90s`, `10m`, `1h30m` or `0`.
pub fn parse(text: &str) -> Result<Duration, String> {
    let invalid = |why: &str| format!("invalid duration {text:?}: {why} (such as 90s, 10m, 1h30m or 0)");
    if text == "0" {
        return Ok(Duration::ZERO);
    }
    if text.is_empty() {
        return Err(invalid("empty"));
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
            return Err(invalid(&format!("unknown unit {c:?}")));
        };
        if digits.is_empty() {
            return Err(invalid("a unit without a number"));
        }
        if at < next_unit {
            return Err(invalid("units out of order or repeated"));
        }
        let count: u64 = digits.parse().map_err(|_| invalid("too long"))?;
        secs = count.checked_mul(UNITS[at].1).and_then(|s| secs.checked_add(s)).ok_or_else(|| invalid("too long"))?;
        digits.clear();
        next_unit = at + 1;
    }
    if !digits.is_empty() {
        return Err(invalid("a number without a unit"));
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
            out.push_str(&format!("{}{unit}", secs / size));
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
        assert!(parse("600").unwrap_err().contains("a number without a unit"));
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
