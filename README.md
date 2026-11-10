# agent-ti

High-performance Rust implementation of the agent-pi harness.

## Project Description

**agent-ti** is a high-performance reimplementation of `agent-pi` in Rust. It strictly follows agent-pi's network protocols, `agent.json` schema, plugin hooks, skills architecture, and tool loading conventions, while providing multi-agent per-process concurrency and an internal SQLite write-ahead log journal.

## Documents

Design (the target):

- [Design](docs/design.md): how agent-ti follows agent-pi, where and why it differs
- [Configuration](docs/configuration.md): config sources (local files or a remote API), the host file and agent-pi's `agent.json`
- [Plugins](docs/plugins.md): agent-pi's hooks in Rust, replay rules
- [Storage and recovery](docs/storage.md): the SQLite journal that replaces pi-durable

Work and operation:

- [Implementation plan](docs/implementation-plan.md): what is built, how the code differs from the design, what to fix, what to port next
- [Deployment](deploy/README.md): Compose and Kubernetes test environments
- [Validation record](docs/validation.md): test runs and their results

## Build and verify

Run from this directory with a current Rust toolchain supporting edition 2024:

```sh
cargo build --locked
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo doc --workspace --no-deps --locked
node scripts/test-local.mjs
```

From the parent workspace, run the shared HTTP contract:

```sh
node contracts/agent/conformance.ts agent-ti
```

The local test runs the smoke against a real child process, kills it with SIGKILL during a model call and again during an unsafe tool call, and checks recovery. Kubernetes tests repeat that against the container and PVC:

```sh
docker build -t t10/agent-ti:dev .
./scripts/test-k8s.sh
```

`agent-ti serve <host file>` starts the server, and `agent-ti check <host file>` loads every agent and exits. [`examples/agent-ti.json`](examples/agent-ti.json) runs agent-pi's assistant from the workspace's agent-pi checkout:

```sh
cargo run --locked -- check examples/agent-ti.json
```
