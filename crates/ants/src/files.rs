//! Files a command writes where its user said: new ones, never over one already there.

use std::io::Write as _;
use std::path::Path;

use anyhow::{Context as _, Result};

/// Write `bytes` to a new file at `path`, refusing to overwrite one; on a failure, nothing is left there.
pub fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    write(path, bytes, &mut std::fs::OpenOptions::new())
}

/// [`write_new`], the file being one only its owner can read.
pub fn write_new_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    write(path, bytes, &mut options)
}

fn write(path: &Path, bytes: &[u8], options: &mut std::fs::OpenOptions) -> Result<()> {
    let mut file = options.write(true).create_new(true).open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            anyhow::anyhow!("{} exists: not overwriting it (-o names another path)", path.display())
        } else {
            anyhow::Error::new(e).context(format!("creating {}", path.display()))
        }
    })?;
    if let Err(e) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(e).with_context(|| format!("writing {}", path.display()));
    }
    Ok(())
}
