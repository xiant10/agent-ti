//! Configuration: config sources and their snapshots, the host file, and agent-pi's `agent.json`.
//! Nothing else in agent-ti opens a config file.
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::Mutex,
};
use thiserror::Error;

mod agent;
mod host;
mod http;
pub use agent::*;
pub use host::*;
pub use http::HttpSource;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub const MAX_FILE_BYTES: usize = 1024 * 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ConfigError(pub String);
pub type Result<T> = std::result::Result<T, ConfigError>;
pub(crate) fn fail<T>(message: impl Into<String>) -> Result<T> {
    Err(ConfigError(message.into()))
}

/// Reads an agent at a location as one consistent version.
pub trait ConfigSource: Send + Sync {
    fn load<'a>(&'a self, location: &'a str) -> BoxFuture<'a, Result<Snapshot>>;
}

/// An immutable, versioned set of documents. Paths given to `read` and `list` are relative to the
/// agent file's directory, as in agent-pi, and may not leave the snapshot.
pub struct Snapshot {
    root: String,
    content: Content,
    /// Set when a source served an older copy, for a startup warning.
    pub notice: Option<String>,
}
enum Content {
    Files {
        version: String,
        files: BTreeMap<String, Vec<u8>>,
    },
    /// A local directory, read on first use; the version is a hash of the files read.
    Directory {
        base: PathBuf,
        read: Mutex<BTreeMap<String, Vec<u8>>>,
    },
}
/// Normalizes `relative` against the directory of `from`, refusing anything that leaves the snapshot.
fn join(from: &str, relative: &str) -> Result<String> {
    if relative.is_empty() || relative.contains('\\') || Path::new(relative).is_absolute() {
        return fail(format!("{relative:?} is not a relative path"));
    }
    let mut parts: Vec<&str> = from.split('/').filter(|p| !p.is_empty()).collect();
    parts.pop();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => parts.push(
                part.to_str()
                    .ok_or_else(|| ConfigError("paths must be UTF-8".into()))?,
            ),
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() {
                    return fail(format!("{relative:?} leaves the agent's config snapshot"));
                }
            }
            _ => return fail(format!("{relative:?} is not a relative path")),
        }
    }
    if parts.is_empty() {
        return fail(format!("{relative:?} names the snapshot itself"));
    }
    Ok(parts.join("/"))
}
fn hash(files: &BTreeMap<String, Vec<u8>>) -> String {
    let mut h = Sha256::new();
    for (path, data) in files {
        h.update((path.len() as u64).to_be_bytes());
        h.update(path.as_bytes());
        h.update((data.len() as u64).to_be_bytes());
        h.update(data);
    }
    format!("sha256:{}", hex(&h.finalize()))
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
impl Snapshot {
    /// A snapshot held in memory, as a remote source returns it.
    pub fn from_files(
        version: String,
        root: String,
        files: BTreeMap<String, Vec<u8>>,
    ) -> Result<Self> {
        if version.is_empty() || version.len() > 256 {
            return fail("snapshot version must be 1 to 256 characters");
        }
        for path in files.keys() {
            if join("x", path)? != *path {
                return fail(format!("snapshot path {path:?} is not normalized"));
            }
        }
        if files.values().map(Vec::len).sum::<usize>() > MAX_SNAPSHOT_BYTES {
            return fail("snapshot is larger than 16 MiB");
        }
        if !files.contains_key(&root) {
            return fail(format!("snapshot has no agent file {root:?}"));
        }
        Ok(Self {
            root,
            content: Content::Files { version, files },
            notice: None,
        })
    }
    /// A local directory; `root` is the agent file within it.
    pub fn directory(base: PathBuf, root: String) -> Result<Self> {
        let base = base
            .canonicalize()
            .map_err(|e| ConfigError(format!("{}: {e}", base.display())))?;
        join("x", &root)?;
        Ok(Self {
            root,
            content: Content::Directory {
                base,
                read: Mutex::new(BTreeMap::new()),
            },
            notice: None,
        })
    }
    /// The agent file's path within the snapshot.
    pub fn root(&self) -> &str {
        &self.root
    }
    pub fn version(&self) -> String {
        match &self.content {
            Content::Files { version, .. } => version.clone(),
            Content::Directory { read, .. } => hash(&read.lock().unwrap()),
        }
    }
    /// Every file this snapshot holds or has read, for a cache of it.
    pub fn files(&self) -> BTreeMap<String, Vec<u8>> {
        match &self.content {
            Content::Files { files, .. } => files.clone(),
            Content::Directory { read, .. } => read.lock().unwrap().clone(),
        }
    }
    fn get(&self, path: &str) -> Result<Vec<u8>> {
        match &self.content {
            Content::Files { files, .. } => files
                .get(path)
                .cloned()
                .ok_or_else(|| ConfigError(format!("{path}: not in the config snapshot"))),
            Content::Directory { base, read } => {
                if let Some(data) = read.lock().unwrap().get(path) {
                    return Ok(data.clone());
                }
                let file = base.join(path);
                // ConfigMap keys are symlinks into the same directory, so follow links but stay inside it.
                let real = file
                    .canonicalize()
                    .map_err(|e| ConfigError(format!("{}: {e}", file.display())))?;
                if !real.starts_with(base) || !real.is_file() {
                    return fail(format!(
                        "{}: not a file in the agent's directory",
                        file.display()
                    ));
                }
                let data = std::fs::read(&real)
                    .map_err(|e| ConfigError(format!("{}: {e}", file.display())))?;
                if data.len() > MAX_FILE_BYTES {
                    return fail(format!("{}: larger than 1 MiB", file.display()));
                }
                let mut read = read.lock().unwrap();
                if read.values().map(Vec::len).sum::<usize>() + data.len() > MAX_SNAPSHOT_BYTES {
                    return fail("config snapshot is larger than 16 MiB");
                }
                read.insert(path.to_owned(), data.clone());
                Ok(data)
            }
        }
    }
    /// A file, by a path relative to the agent file.
    pub fn read(&self, relative: &str) -> Result<Vec<u8>> {
        self.get(&join(&self.root, relative)?)
    }
    pub fn read_text(&self, relative: &str) -> Result<String> {
        String::from_utf8(self.read(relative)?)
            .map_err(|_| ConfigError(format!("{relative}: not UTF-8 text")))
    }
    pub(crate) fn read_root(&self) -> Result<String> {
        String::from_utf8(self.get(&self.root)?)
            .map_err(|_| ConfigError(format!("{}: not UTF-8 text", self.root)))
    }
    /// The files under a directory (relative to the agent file), as paths relative to that directory.
    pub fn list(&self, relative: &str) -> Result<Vec<String>> {
        let dir = join(&self.root, relative)?;
        let prefix = format!("{dir}/");
        match &self.content {
            Content::Files { files, .. } => Ok(files
                .keys()
                .filter_map(|p| p.strip_prefix(&prefix))
                .map(str::to_owned)
                .collect()),
            Content::Directory { base, .. } => {
                fn walk(
                    dir: &Path,
                    prefix: &str,
                    out: &mut Vec<String>,
                    depth: usize,
                ) -> std::io::Result<()> {
                    if depth > 8 {
                        return Ok(());
                    }
                    for entry in std::fs::read_dir(dir)? {
                        let entry = entry?;
                        let name = entry.file_name().to_string_lossy().into_owned();
                        if name.starts_with('.') {
                            continue;
                        }
                        let path = entry.path();
                        if path.is_dir() {
                            walk(&path, &format!("{prefix}{name}/"), out, depth + 1)?;
                        } else if path.is_file() {
                            out.push(format!("{prefix}{name}"));
                        }
                    }
                    Ok(())
                }
                let mut out = Vec::new();
                let path = base.join(&dir);
                if path.is_dir() {
                    walk(&path, "", &mut out, 0)
                        .map_err(|e| ConfigError(format!("{}: {e}", path.display())))?;
                }
                out.sort();
                Ok(out)
            }
        }
    }
}

/// Local files: the image, a mounted ConfigMap or a development checkout.
pub struct FileSource;
impl FileSource {
    /// The agent's app directory: `<app>` for agent-pi's `<app>/config/agent.json` layout, otherwise
    /// the agent file's own directory.
    pub fn snapshot(path: &Path) -> Result<Snapshot> {
        // The directory, not the file: a ConfigMap key is a symlink into a timestamped directory.
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let dir = parent
            .canonicalize()
            .map_err(|e| ConfigError(format!("{}: {e}", parent.display())))?;
        let dir = dir.as_path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| ConfigError("agent file name must be UTF-8".into()))?;
        let (base, root) = match (dir.file_name().and_then(|n| n.to_str()), dir.parent()) {
            (Some("config"), Some(app)) => (app.to_path_buf(), format!("config/{name}")),
            _ => (dir.to_path_buf(), name.to_owned()),
        };
        Snapshot::directory(base, root)
    }
}
impl ConfigSource for FileSource {
    fn load<'a>(&'a self, location: &'a str) -> BoxFuture<'a, Result<Snapshot>> {
        Box::pin(async move {
            Self::snapshot(Path::new(
                location.strip_prefix("file://").unwrap_or(location),
            ))
        })
    }
}

/// agent-pi's `${VAR}` and `${VAR:-default}` in every string value; an unset variable without a
/// default is an error.
pub fn substitute(value: Value, env: &dyn Fn(&str) -> Option<String>, at: &str) -> Result<Value> {
    Ok(match value {
        Value::String(s) => {
            let mut out = String::new();
            let mut rest = s.as_str();
            while let Some(start) = rest.find("${") {
                out.push_str(&rest[..start]);
                let tail = &rest[start + 2..];
                let end = tail
                    .find('}')
                    .ok_or_else(|| ConfigError(format!("{at}: unclosed ${{")))?;
                let body = &tail[..end];
                let (name, default) = match body.split_once(":-") {
                    Some((name, default)) => (name, Some(default)),
                    None => (body, None),
                };
                let valid = name
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                    && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if !valid {
                    out.push_str(&rest[start..start + 3 + end]);
                } else {
                    match (env(name), default) {
                        (Some(v), _) => out.push_str(&v),
                        (None, Some(d)) => out.push_str(d),
                        (None, None) => {
                            return fail(format!(
                                "{at}: ${{{name}}} is not set and has no default"
                            ));
                        }
                    }
                }
                rest = &tail[end + 1..];
            }
            out.push_str(rest);
            Value::String(out)
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|v| substitute(v, env, at))
                .collect::<Result<_>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| Ok((k, substitute(v, env, at)?)))
                .collect::<Result<Map<_, _>>>()?,
        ),
        other => other,
    })
}
pub fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Field readers that fail with the field's path, as agent-pi's. Substituted values arrive as
/// strings, so numbers and booleans accept their string forms.
pub(crate) struct Reader<'a> {
    pub source: &'a str,
}
pub(crate) type Raw = Map<String, Value>;
static EMPTY: std::sync::LazyLock<Raw> = std::sync::LazyLock::new(Raw::new);
impl Reader<'_> {
    pub fn fail<T>(&self, path: &str, message: &str) -> Result<T> {
        fail(format!("{}: {path} {message}", self.source))
    }
    /// An object whose keys must all be known.
    pub fn object<'v>(&self, value: &'v Value, path: &str, known: &[&str]) -> Result<&'v Raw> {
        let Some(object) = value.as_object() else {
            return self.fail(path, "must be an object");
        };
        if let Some(key) = object.keys().find(|k| !known.contains(&k.as_str())) {
            return self.fail(&format!("{path}.{key}"), "is not a known field");
        }
        Ok(object)
    }
    /// An object with any keys, such as a map of names.
    pub fn map<'v>(&self, value: Option<&'v Value>, path: &str) -> Result<&'v Raw> {
        match value {
            None => Ok(&EMPTY),
            Some(Value::Object(map)) => Ok(map),
            Some(_) => self.fail(path, "must be an object"),
        }
    }
    pub fn optional_object<'v>(
        &self,
        value: Option<&'v Value>,
        path: &str,
        known: &[&str],
    ) -> Result<&'v Raw> {
        match value {
            None => Ok(&EMPTY),
            Some(v) => self.object(v, path, known),
        }
    }
    pub fn string(&self, value: Option<&Value>, path: &str) -> Result<String> {
        match value.and_then(Value::as_str) {
            Some(s) if !s.is_empty() => Ok(s.to_owned()),
            _ => self.fail(path, "must be a non-empty string"),
        }
    }
    /// Absent or empty is `None`, so an unset variable turns a setting off.
    pub fn optional_string(&self, value: Option<&Value>, path: &str) -> Result<Option<String>> {
        match value {
            None => Ok(None),
            Some(Value::String(s)) if s.is_empty() => Ok(None),
            Some(v) => self.string(Some(v), path).map(Some),
        }
    }
    pub fn number(&self, value: Option<&Value>, path: &str, fallback: f64) -> Result<f64> {
        let n = match value {
            None => return Ok(fallback),
            Some(Value::Number(n)) => n.as_f64(),
            Some(Value::String(s)) => s.parse::<f64>().ok(),
            _ => None,
        };
        match n {
            Some(n) if n.is_finite() => Ok(n),
            _ => self.fail(path, "must be a number"),
        }
    }
    pub fn count(&self, value: Option<&Value>, path: &str, fallback: u64) -> Result<u64> {
        let n = self.number(value, path, fallback as f64)?;
        if n < 0.0 || n.fract() != 0.0 || n > u32::MAX as f64 {
            return self.fail(path, "must be a whole number");
        }
        Ok(n as u64)
    }
    pub fn boolean(&self, value: Option<&Value>, path: &str, fallback: bool) -> Result<bool> {
        match value {
            None => Ok(fallback),
            Some(Value::Bool(b)) => Ok(*b),
            Some(Value::String(s)) if s == "true" => Ok(true),
            Some(Value::String(s)) if s == "false" => Ok(false),
            _ => self.fail(path, "must be a boolean"),
        }
    }
    /// A list, or a comma-separated string as an environment variable gives it.
    pub fn strings(&self, value: Option<&Value>, path: &str) -> Result<Vec<String>> {
        match value {
            None => Ok(vec![]),
            Some(Value::String(s)) => Ok(s
                .split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect()),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().map(str::to_owned).ok_or(()))
                .collect::<std::result::Result<_, _>>()
                .or_else(|_| self.fail(path, "must be a list of strings")),
            _ => self.fail(path, "must be a list of strings"),
        }
    }
    pub fn one_of(
        &self,
        value: Option<&Value>,
        path: &str,
        options: &[&str],
        fallback: Option<&str>,
    ) -> Result<String> {
        match (value.and_then(Value::as_str), fallback) {
            (None, Some(f)) if value.is_none() => Ok(f.into()),
            (Some(v), _) if options.contains(&v) => Ok(v.into()),
            _ => self.fail(path, &format!("must be one of {}", options.join(", "))),
        }
    }
}
