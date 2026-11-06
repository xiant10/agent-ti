# Ti Assistant in the platform picker

Ti Assistant is a separately configured agent on the Rust agent-ti runtime. Its public agent/pool name is `ti`; it does not replace the `assistant` agent on agent-pi.

Open [Workbench](http://127.0.0.1:18060), sign in to **Acme**, **Sample Acme**, or **Sample Globex**, and choose **Ti Assistant** in the **Agent** selector. Refresh a Workbench tab that was open before registration. New conversations and follow-ups go through the existing agent gateway; session IDs begin `ti.agent-ti-0.`.

This configuration enables chat through `ai-gateway-llm` using `qwen3.5-flash`, with a `chat` profile and a `voice` profile that emits `speech` events. It has no business tools or skills installed. It advertises `durable_runs`, `steer`, `interrupt`, `voice` and `retry`. A call through `t10-livekit-agent` also needs voice grants to name the `agent-ti` audience, which they don't yet ([plan](implementation-plan.md#current-code-against-the-design)).

Ti is one agent-pi `agent.json`, [`deploy/k8s/base/agents/ti/agent.json`](../deploy/k8s/base/agents/ti/agent.json), with its instructions in `base.md` beside it; the host file is [`deploy/k8s/base/config/agent-ti.json`](../deploy/k8s/base/config/agent-ti.json). Tenant access comes only from the platform catalog, as for agent-pi.

## Deploy

| File | Purpose |
| --- | --- |
| [`deploy/k8s/base/agents/ti/`](../deploy/k8s/base/agents/ti/) | Ti's `agent.json`, instructions (`base.md`) and voice instructions (`voice.md`) |
| [`deploy/k8s/base/config/agent-ti.json`](../deploy/k8s/base/config/agent-ti.json) | Host file: port, instance ID, storage, JWT issuer and audience, limits, the agents to load |
| [`deploy/agent-registration.json`](../deploy/agent-registration.json) | Picker title, tenant access, pool, replica address and capabilities |
| [`deploy/k8s/overlays/platform-local/kustomization.yaml`](../deploy/k8s/overlays/platform-local/kustomization.yaml) | Standalone local-platform deploy using image `t10/agent-ti:picker` |
| Parent `deploy/k8s/base/kustomization.yaml` and `overlays/local/agents.json` | Include the component in platform deploys and seed imports |

From the agent-ti directory, build and deploy the local component:

```sh
docker build -t t10/agent-ti:picker .
kubectl --context docker-desktop apply -k deploy/k8s/overlays/platform-local
kubectl --context docker-desktop -n t10 rollout restart statefulset/agent-ti
```

The parent workspace's `deploy/k8s/overlays/local` includes the same base with the same image tag, so a platform-wide apply gives the same result. The agent's model credential is a dedicated `t10/agent-ti` Secret containing the same authorized gateway key used by the local agent-pi deployment; no secret value is checked in. The pod carries the auth-client and llm-client labels required by the platform NetworkPolicies.

To change Ti's prompt or settings, edit its files, re-apply the overlay and restart the StatefulSet; sessions record the config version they started under. Ti's sessions are in `/data/ti.sqlite`. The `agent-ti.sqlite` on the same volume is from the interim format and is no longer read; it can be deleted once nobody needs its old conversations.

## Register without replacing the live catalog

The active catalog can differ from the checked-in seed. Publish just this agent and pool using the normal platform API. The helper saves drafts, reads their findings, and publishes with a reason and the draft's expected base revision. It preserves other agents, replicas and the current default.

```sh
# Start separate port-forward terminals:
kubectl --context docker-desktop -n t10 port-forward svc/t10-agent-control 29110:18010
kubectl --context docker-desktop -n t10 port-forward svc/agent-ti 29130:18120

# Supply your own short-lived, verified platform-admin token in a protected file:
python3 scripts/register-platform-agent.py \
  --control-url http://127.0.0.1:29110 \
  --manifest-url http://127.0.0.1:29130/v1/runtime \
  --token-file /path/to/protected-platform-admin-token \
  --reason 'Enable Ti Assistant for text conversations'
```

The helper checks the running manifest before publication. On first registration it creates a closed agent, publishes its ready pool, then enables routing for the three bound tenants. Routers pick it up on their normal poll. Re-running the helper with matching data makes no change. The runtime currently accepts verified user tokens for the `t10` audience; delegated service grants are a later runtime milestone.

## Add a replica

Each replica owns its sessions on its own volume for life, so replicas can be added and not removed ([agent-pi's replicas](../../agent-pi/docs/replicas.md) apply as written). Raise `spec.replicas` on the StatefulSet, add `"agent-ti-1": "http://agent-ti-1.agent-ti.t10.svc.cluster.local:18120"` to `pool.replicas` in [`deploy/agent-registration.json`](../deploy/agent-registration.json), and run the helper above. A pod's name is its instance ID, through `AGENT_INSTANCE_ID`. Session and run limits are per replica. `scripts/test-replicas-k8s.sh` checks routing, listing and losing a replica in a namespace of its own.

## Browser test

A real-browser test is opt-in because it calls the configured model provider:

```sh
cd ../workbench/web
AGENT_TI_E2E=1 E2E_PORT=18060 npx playwright test agent-ti.spec.ts
```

It signs in as the existing development member, selects Ti Assistant, waits for an actual assistant reply, sends a follow-up, reloads and verifies both replies without duplicate user messages or browser runtime errors. Results are in the [validation record](validation.md#2026-11-05-platform-picker).
