# Deploy agent-ti

One process serves the agents its host file names, each with its own SQLite file (`<agent>.sqlite`) on the data volume. Use one StatefulSet replica with its PVC; more replicas follow agent-pi's model (step 11 of the [plan](../docs/implementation-plan.md)). The component uses a separate pool and store from agent-pi/agent-sales. The [Ti Assistant setup](../docs/platform-agent.md) registers its picker entry through the platform control plane.

Deploy configs are plain files: a host file (`agent-ti.json`) and one directory per agent in agent-pi's format (`agent.json`, prompts, plugin files). Kubernetes mounts each as a ConfigMap where the host file names it; the file source follows ConfigMap symlinks within the agent's directory, so no staging step is needed. See [configuration](../docs/configuration.md).

| Environment | Host file | Agents |
| --- | --- | --- |
| Platform base (`t10`) | `k8s/base/config/agent-ti.json` | `k8s/base/agents/ti/` |
| Smoke (Kubernetes, Compose, `scripts/test-local.mjs`) | `k8s/overlays/smoke/agent-ti.json` | `k8s/overlays/smoke/agents/{ti-demo,ti-plain}/` |

## Build

From the agent-ti directory:

```sh
cargo build --locked
docker build -t t10/agent-ti:dev .
```

The image builds from this repository only. The Docker target cache retains third-party dependencies and rebuilds workspace crates when a source layer changes.

## Kubernetes smoke environment

Requires the Docker Desktop Kubernetes context, with local Docker images available to its node. From this directory:

```sh
./scripts/test-k8s.sh
```

The script applies `deploy/k8s/overlays/smoke` to the explicitly selected `docker-desktop` context and separate `agent-ti-test` namespace. `AGENT_TI_KUBE_CONTEXT` can select another local context only if it exposes Docker container IDs and uses the same Docker daemon. The crash test checks that runtime requirement explicitly. It creates a StatefulSet, 2 GiB PVC, Services, ConfigMaps, an ingress NetworkPolicy and a restricted test-only issuer/model/MCP fixture. There is no provider billing or dependency on real service credentials. The fixture is reachable inside its namespace and through a loopback port-forward during tests; it must never be used as a production issuer.

Tests run HTTP calls through `kubectl port-forward` on loopback ports 29120 and 29121. They verify JWT validation, tenant/user/agent ownership, two agents in one process, idempotency, plugin replies without the model, guardrails, tool calls and tool errors, UTF-16 text offsets, replay, cancellation and the session's config version. Then the script resolves **agent-ti-test/agent-ti-0**'s exact Docker container ID and sends SIGKILL through Docker Desktop's daemon, once during a model call and once during an unsafe tool call, waits for kubelet to restart the container, reconnects, and verifies recovery and durable state on the same PVC. The script checks that the restart count increased. This crash test does not kill or change an existing t10 workload.

The namespace and PVC remain available for inspection; test port-forwards stop on exit. Log locations are printed by the script. To remove the test environment and its data explicitly:

```sh
kubectl --context docker-desktop delete namespace agent-ti-test
```

Config mounts are read-only, and all containers run as UID 10001 with dropped capabilities, seccomp and no service account token.

## Docker Compose smoke environment

The Compose file runs the same scripted services, persists SQLite in `agent-ti_agent-ti-data`, and exposes agent-ti at `127.0.0.1:28120` and the test issuer at `127.0.0.1:28121`:

```sh
docker compose up -d --build --wait
node scripts/smoke.mjs
docker compose down
```

`down` retains the named data volume. Compose mounts the smoke overlay's host file and agents, and the fixture source, read-only. This is a development stack with a token-minting fixture; it is not a production authentication configuration.

## Platform base

`deploy/k8s/base` is an optional component for the existing `t10` namespace. It uses `auth-server` for JWKS and `ai-gateway-llm` for model calls. Before applying it:

1. Provision an `agent-ti` Secret containing `LLM_API_KEY` for the LLM gateway. No secrets are checked in.
2. Ti is `base/agents/ti/agent.json` (profile `chat`, `qwen3.5-flash` through `ai-gateway-llm`). Which tenants may use it is the platform catalog's decision, as for agent-pi ([registration](../docs/platform-agent.md)); the runtime keeps no tenant list.
3. Set issuer, audience, JWKS and gateway endpoints to match the deployment, through the host file's and `agent.json`'s `${VAR:-default}` settings. Plain `http://` is accepted only for cluster-local hosts; use HTTPS elsewhere.
4. Override the image with a versioned image or digest and provision a suitable local persistent volume/storage class. Keep SQLite off network filesystems.

```sh
kubectl --context docker-desktop apply -k deploy/k8s/base
kubectl --context docker-desktop -n t10 rollout status statefulset/agent-ti
```

The base's ingress policy permits the agent gateway and test clients labelled `app.kubernetes.io/name=agent-ti-test`. An administrative client needs its own allowed ingress rule. Enforce NetworkPolicy with a compatible CNI; Docker Desktop does not provide that enforcement. TLS termination, external DNS and production credentials are operator responsibilities. No public ingress is created.

A changed `agent.json` or prompt takes effect at the next restart, as with agent-pi: update the ConfigMap (`kubectl apply -k`) and restart the StatefulSet. Each session records the config version it started under (the agent's `bundle`, or its snapshot's hash), shown in `session.get` and `/v1/runtime`.

The local platform now includes the component and its `ti` catalog seed. Live registration uses the normal routing control plane as described in the [Ti Assistant setup](../docs/platform-agent.md). The isolated deployment smoke does not register a platform route.
