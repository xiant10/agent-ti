//! Files that hold the last good copy of what t10-agent-control answered, one per tenant and
//! feed, in the format agent-pi keeps: `{"etag", "at", "body"}`, with `at` in Unix milliseconds.
//! They're configuration a tenant published, so they're read here, the one place configuration
//! is read. The runtime decides when to use them.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// One saved answer: its ETag, how long ago control sent it, and the body.
pub struct SavedFile {
    pub etag: Option<String>,
    pub age: Duration,
    pub body: Value,
}

/// One feed's directory of copies.
pub struct ControlCopyFiles {
    dir: PathBuf,
}

impl ControlCopyFiles {
    pub fn open(dir: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// The saved answer for a key, or `None` when there's none or it isn't readable as one.
    pub fn load(&self, key: &str) -> io::Result<Option<SavedFile>> {
        let raw = match std::fs::read(self.file(key)) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let Ok(saved) = serde_json::from_slice::<Value>(&raw) else {
            return Ok(None);
        };
        let Some(at_ms) = saved["at"].as_u64() else {
            return Ok(None);
        };
        let age = SystemTime::now()
            .duration_since(UNIX_EPOCH + Duration::from_millis(at_ms))
            .unwrap_or_default();
        Ok(Some(SavedFile {
            etag: saved["etag"].as_str().map(str::to_owned),
            age,
            body: saved.get("body").cloned().unwrap_or(Value::Null),
        }))
    }

    /// Keeps an answer control just sent. Written beside the old one, then renamed over it.
    pub fn save(&self, key: &str, etag: Option<&str>, body: &Value) -> io::Result<()> {
        let file = self.file(key);
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut saved = json!({ "at": at, "body": body });
        if let Some(etag) = etag {
            saved["etag"] = json!(etag);
        }
        let tmp = file.with_extension("json.tmp");
        std::fs::write(&tmp, saved.to_string())?;
        std::fs::rename(&tmp, &file)
    }

    /// Tenant IDs and principals aren't file names, so each key is hashed.
    fn file(&self, key: &str) -> PathBuf {
        let hash = Sha256::digest(key.as_bytes());
        let name: String = hash[..16].iter().map(|b| format!("{b:02x}")).collect();
        self.dir.join(format!("{name}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_saved_answer_reads_back_with_its_age() {
        let dir = tempfile::tempdir().unwrap();
        let files = ControlCopyFiles::open(dir.path().join("skills")).unwrap();
        assert!(files.load("acme").unwrap().is_none());
        files
            .save("acme", Some("\"v1\""), &json!({ "skills": [] }))
            .unwrap();
        let again = ControlCopyFiles::open(dir.path().join("skills")).unwrap();
        let saved = again.load("acme").unwrap().unwrap();
        assert_eq!(saved.etag.as_deref(), Some("\"v1\""));
        assert_eq!(saved.body, json!({ "skills": [] }));
        assert!(saved.age < Duration::from_secs(5));
        assert!(again.load("globex").unwrap().is_none());
    }
}
