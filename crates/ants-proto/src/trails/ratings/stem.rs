//! A light English stemmer, so that a search for "reviewing changes" finds a trail that "reviews a change": a word
//! loses one inflection (a plural, `-ing`, `-ed`), then one derivation (`-ation`, `-ment`, `-ness`, `-er`, `-ly`, …),
//! then a final `e`, each only when a stem of 3 letters or more is left. Deterministic and dictionary-free, it maps
//! most forms of a word to one stem, and leaves words with digits or other than ASCII letters as they are.

/// The fewest letters a stem keeps.
const MIN_STEM: usize = 3;

/// The derivational suffixes [`stem`] takes off, longest first among those ending alike, with what replaces each.
const DERIVATIONS: [(&str, &str); 9] = [
    ("ization", "ize"),
    ("isation", "ise"),
    ("ation", "ate"),
    ("iness", "y"),
    ("ment", ""),
    ("ness", ""),
    ("ily", "y"),
    ("er", ""),
    ("ly", ""),
];

/// The stem of a lowercase word.
pub fn stem(word: &str) -> String {
    if word.len() < MIN_STEM || !word.bytes().all(|b| b.is_ascii_lowercase()) {
        return word.to_owned();
    }
    let mut stem = derivation(&inflection(word));
    if stem.len() > MIN_STEM && stem.ends_with('e') {
        stem.pop();
    }
    stem
}

/// `word` without its plural, `-ing` or `-ed`.
fn inflection(word: &str) -> String {
    if let Some(rest) = word.strip_suffix("ies").or_else(|| word.strip_suffix("ied"))
        && rest.len() + 1 >= MIN_STEM
    {
        return format!("{rest}y");
    }
    if let Some(rest) = word.strip_suffix("sses") {
        return format!("{rest}ss");
    }
    if let Some(rest) = word.strip_suffix("es")
        && ["x", "z", "ch", "sh"].iter().any(|end| rest.ends_with(end))
        && rest.len() >= MIN_STEM
    {
        return rest.to_owned();
    }
    if let Some(rest) = word.strip_suffix('s')
        && !rest.ends_with(['s', 'u', 'i'])
        && stem_left(rest)
    {
        return rest.to_owned();
    }
    for suffix in ["ing", "ed"] {
        if let Some(rest) = word.strip_suffix(suffix)
            && !(suffix == "ed" && rest.ends_with('e'))
            && stem_left(rest)
        {
            return undoubled(rest);
        }
    }
    word.to_owned()
}

/// `word` without one of its [`DERIVATIONS`].
fn derivation(word: &str) -> String {
    for (suffix, replacement) in DERIVATIONS {
        if let Some(rest) = word.strip_suffix(suffix)
            && stem_left(rest)
        {
            return if suffix == "er" { undoubled(rest) } else { format!("{rest}{replacement}") };
        }
    }
    word.to_owned()
}

/// Whether `rest` is long enough, with a vowel, to be a stem.
fn stem_left(rest: &str) -> bool {
    rest.len() >= MIN_STEM && rest.contains(['a', 'e', 'i', 'o', 'u', 'y'])
}

/// `stem` with a doubled final consonant made single ("runn" of "running"), but for `l`, `s` and `z`, which the word
/// itself doubles ("install", "pass", "buzz"), and when too short a stem would be left ("add" of "adding").
fn undoubled(stem: &str) -> String {
    match stem.as_bytes() {
        [.., a, b] if a == b && !b"aeioulsz".contains(b) && stem.len() > MIN_STEM => stem[..stem.len() - 1].to_owned(),
        _ => stem.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn same(words: &[&str]) {
        let stems: Vec<_> = words.iter().map(|word| stem(word)).collect();
        assert!(stems.iter().all(|s| *s == stems[0]), "{words:?} stem to {stems:?}");
    }

    #[test]
    fn the_forms_of_a_word_share_a_stem() {
        same(&["review", "reviews", "reviewing", "reviewed", "reviewer", "reviewers"]);
        same(&["change", "changes", "changing", "changed"]);
        same(&["fix", "fixes", "fixed", "fixing"]);
        same(&["test", "tests", "testing", "tested", "tester"]);
        same(&["dependency", "dependencies"]);
        same(&["copy", "copies", "copied"]);
        same(&["match", "matches", "matching", "matched"]);
        same(&["pass", "passes", "passing", "passed"]);
        same(&["run", "running", "runner"]);
        same(&["stop", "stopped", "stopping"]);
        same(&["deploy", "deploys", "deploying", "deployment", "deployments"]);
        same(&["migrate", "migrates", "migrating", "migration", "migrations"]);
        same(&["optimize", "optimization"]);
        same(&["update", "updates", "updating", "updated"]);
        same(&["install", "installing", "installed"]);
        same(&["quick", "quickly"]);
        same(&["ready", "readiness"]);
        same(&["order", "orders", "ordered"]);
    }

    #[test]
    fn short_and_unlike_words_keep_their_letters() {
        for word in ["status", "analysis", "class", "string", "thing", "need", "use", "user", "bus", "go"] {
            assert_eq!(stem(word), word);
        }
        assert_eq!(stem("adding"), "add");
    }

    #[test]
    fn words_with_digits_or_other_letters_are_left_as_they_are() {
        for word in ["utf8s", "ipv6ing", "cafés"] {
            assert_eq!(stem(word), word);
        }
    }

    #[test]
    fn stems_are_stable() {
        let stems: Vec<_> = ["reviewing", "changes", "dependencies", "deployment", "migration", "running", "quickly"]
            .iter()
            .map(|word| stem(word))
            .collect();
        assert_eq!(stems, ["review", "chang", "dependency", "deploy", "migrat", "run", "quick"]);
    }
}
