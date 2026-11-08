# agent-ti

agent-pi written in Rust: the runtime and the building blocks an agent is made of, with no agents of its own. It copies agent-pi's protocol, `agent.json` format, plugin hooks, skills and tool loading, grants and voice output, and adds several agents per process and a SQLite journal of its own in place of pi-durable. Where the docs don't say otherwise, agent-pi's behavior is the specification.

An agent is a folder of config (`agent.json`, its prompts, skills and plugin options). The image's host file loads every agent folder in `/etc/agent-ti/agents`, which a deployment fills. t10's agents, Ti Assistant and the property agent, are in [t10-agents](../t10-agents); a new agent goes there, not here. This repo keeps only the test agents of its smoke overlay and examples.

**Status:** a durable text agent that passes the shared agent contract and runs an unmodified agent-pi `agent.json` (agent-pi's assistant loads as is), read through config sources: local files or a remote config API. Ti Assistant runs on it in the local platform picker. Live text, steering, skills, grants, tenant skills and connectors, the rest of the plugin hooks and voice are still to port. The [implementation plan](docs/implementation-plan.md) is the one place that tracks what is built.

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
