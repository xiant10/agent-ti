//! Carries the repo's docs in the binary, so the image serves the docs of its own
//! commit at /docs/mcp (src/docs.rs). Records stay out.

use std::{fmt::Write, path::Path};

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
    println!("cargo:rerun-if-changed={}", root.display());
    let mut files = Vec::new();
    let mut dirs = vec![root.clone()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let rel = path
                .strip_prefix(&root)
                .expect("under docs")
                .to_string_lossy()
                .replace('\\', "/");
            if path.is_dir() {
                if rel != "records" {
                    println!("cargo:rerun-if-changed={}", path.display());
                    dirs.push(path);
                }
            } else if rel.ends_with(".md") {
                files.push((rel, path));
            }
        }
    }
    files.sort();
    let mut out = String::from("pub static DOCS: &[(&str, &str)] = &[\n");
    for (rel, path) in files {
        writeln!(
            out,
            "    ({rel:?}, include_str!({:?})),",
            path.display().to_string()
        )
        .expect("write");
    }
    out.push_str("];\n");
    let dest = Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR")).join("docs.rs");
    std::fs::write(dest, out).expect("write docs.rs");
}
