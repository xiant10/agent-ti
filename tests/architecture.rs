use std::{collections::BTreeMap, process::Command};
#[test]
fn cargo_dependency_boundaries() {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let allowed: BTreeMap<&str, &[&str]> = [
        // The one place configuration is read.
        (
            "agent-ti-config",
            &[
                "serde",
                "serde_json",
                "sha2",
                "thiserror",
                "base64",
                "reqwest",
                "tempfile",
            ][..],
        ),
        (
            "agent-ti-core",
            &["futures-util", "serde", "serde_json", "tokio"][..],
        ),
        (
            "agent-ti-plugin-sdk",
            &["agent-ti-config", "agent-ti-core", "serde", "serde_json"][..],
        ),
        (
            "agent-ti-store",
            &[
                "agent-ti-core",
                "serde",
                "serde_json",
                "sha2",
                "sqlx",
                "tokio",
                "thiserror",
                "fs2",
                "uuid",
            ][..],
        ),
    ]
    .into();
    for package in metadata["packages"].as_array().unwrap() {
        let name = package["name"].as_str().unwrap();
        if let Some(dependencies) = allowed.get(name) {
            for dependency in package["dependencies"].as_array().unwrap() {
                let dep = dependency["name"].as_str().unwrap();
                assert!(
                    dependencies.contains(&dep),
                    "unexpected dependency {name} -> {dep}; review architecture before adding it"
                );
            }
        }
    }
}
/// Nothing outside the config crate opens a config file: no `std::fs` reads in the runtime.
#[test]
fn only_config_sources_read_configuration() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for dir in [
        "src",
        "crates/agent-ti-core/src",
        "crates/agent-ti-plugin-sdk/src",
        "crates/agent-ti-store/src",
    ] {
        for entry in std::fs::read_dir(root.join(dir)).unwrap() {
            let path = entry.unwrap().path();
            let text = std::fs::read_to_string(&path).unwrap();
            for forbidden in ["fs::read", "read_to_string", "File::open", "load_bundle"] {
                assert!(
                    !text.contains(forbidden),
                    "{} uses {forbidden}; read configuration through a config source",
                    path.display()
                );
            }
        }
    }
}
