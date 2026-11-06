#!/bin/sh
# scripts/replicas.mjs on the local cluster: three replicas behind ai-gateway-agent. Uses a namespace
# of its own, deleted afterwards. Taking a replica down is a scale, which keeps its volume.
set -eu
cd "$(dirname "$0")/.."
context=${AGENT_TI_KUBE_CONTEXT:-docker-desktop}
ns=agent-ti-replicas
k() { kubectl --context "$context" -n "$ns" "$@"; }
base=${TEST_BASE_PORT:-29150}
state=$(mktemp -d "${TMPDIR:-/tmp}/agent-ti-replicas.XXXXXX")
forwards=''
last_forward=''
forward() {
  # Not through `k`: backgrounding a function would leave `$!` naming a subshell, and kubectl running.
  kubectl --context "$context" -n "$ns" port-forward "$1" "$2" > "$state/forward-$2.log" 2>&1 &
  last_forward=$!
  forwards="$forwards $last_forward"
}
cleanup() {
  for pid in $forwards; do kill "$pid" 2>/dev/null || true; done
  kubectl --context "$context" delete namespace "$ns" --wait=false >/dev/null 2>&1 || true
  rm -rf "$state"
}
trap cleanup EXIT INT TERM
wait_http() {
  i=0
  until curl -fsS "$1" >/dev/null 2>&1; do
    i=$((i+1)); if [ "$i" -gt 150 ]; then echo "Not ready: $1" >&2; return 1; fi
    sleep 0.2
  done
}
# A previous run's namespace may still be going away.
kubectl --context "$context" wait --for=delete "namespace/$ns" --timeout=300s >/dev/null 2>&1 || true
kubectl --context "$context" apply -k deploy/k8s/overlays/replicas >/dev/null
k rollout status deploy/agent-ti-fixture --timeout=120s
k rollout status statefulset/agent-ti --timeout=240s
k rollout status deploy/ai-gateway-agent --timeout=120s
forward svc/ai-gateway-agent "$base:18090"
forward svc/agent-ti-fixture "$((base+1)):8080"
forward pod/agent-ti-0 "$((base+2)):18120"
forward pod/agent-ti-1 "$((base+3)):18120"
forward pod/agent-ti-2 "$((base+4)):18120"
export GATEWAY_URL="http://127.0.0.1:$base" AGENT_TI_FIXTURE_URL="http://127.0.0.1:$((base+1))"
export REPLICA_URLS="http://127.0.0.1:$((base+2)),http://127.0.0.1:$((base+3)),http://127.0.0.1:$((base+4))"
wait_http "$GATEWAY_URL/health"
wait_http "$AGENT_TI_FIXTURE_URL/healthz"
for url in $(echo "$REPLICA_URLS" | tr ',' ' '); do wait_http "$url/readyz"; done
node scripts/replicas.mjs up "$state/state.json"
k scale statefulset/agent-ti --replicas=2 >/dev/null
k wait --for=delete pod/agent-ti-2 --timeout=120s
node scripts/replicas.mjs down "$state/state.json"
k scale statefulset/agent-ti --replicas=3 >/dev/null
k rollout status statefulset/agent-ti --timeout=240s
node scripts/replicas.mjs back "$state/state.json"
echo "PASS three replicas behind the gateway in context $context"
