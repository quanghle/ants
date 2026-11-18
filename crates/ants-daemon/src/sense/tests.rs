use std::path::{Path, PathBuf};

/// The files of the ants crate naming `ants_sense`, for the files of an ant's home it lays out, and their tests.
const ANTS_NAMING: [&str; 6] = [
    "src/add.rs",
    "src/admin/orders/clone/tests.rs",
    "src/auth/tests/machines.rs",
    "src/doctor/ants.rs",
    "src/doctor/tests.rs",
    "src/export.rs",
];

fn sources(dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
}

/// Whether `text` names the crate `ants_sense`, as against a subject of its stream (`ants_sense.<id>`).
fn names_ants_sense(text: &str) -> bool {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    text.match_indices("ants_sense").any(|(at, name)| {
        let before = text[..at].chars().next_back();
        let after = text[at + name.len()..].chars().next();
        !before.is_some_and(ident) && !after.is_some_and(|c| ident(c) || c == '.')
    })
}

/// The files of the crate at `dir` naming `ants_sense`, relative to `dir`.
fn naming(dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    sources(&dir.join("src"), &mut found);
    sources(&dir.join("tests"), &mut found);
    let mut naming: Vec<String> = found
        .into_iter()
        .filter(|file| names_ants_sense(&std::fs::read_to_string(file).unwrap()))
        .map(|file| file.strip_prefix(dir).unwrap().to_string_lossy().replace('\\', "/"))
        .collect();
    naming.sort();
    naming
}

#[test]
fn only_the_sense_module_and_the_files_listed_name_ants_sense() {
    let daemon = Path::new(env!("CARGO_MANIFEST_DIR"));
    let outside: Vec<_> =
        naming(daemon).into_iter().filter(|file| file != "src/sense.rs" && !file.starts_with("src/sense/")).collect();
    assert_eq!(outside, Vec::<String>::new(), "the daemon reaches ants_sense through src/sense alone");
    let ants = naming(&daemon.join("../ants"));
    assert_eq!(ants, ANTS_NAMING, "the ants crate reaches ants_sense in the files listed, all naming it");
}
