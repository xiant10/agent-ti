#!/bin/sh
# Uses a separate namespace and never modifies existing t10 routes or workloads.
set -eu
cd "$(dirname "$0")/.."
context=${AGENT_TI_KUBE_CONTEXT:-docker-desktop}
image=${AGENT_TI_IMAGE:-t10/agent-ti:dev}
if [ "$image" != "t10/agent-ti:dev" ]; then
  echo 'This local smoke overlay expects t10/agent-ti:dev.' >&2
  exit 2
fi
artifact_dir=$(mktemp -d "${TMPDIR:-/tmp}/agent-ti-k8s.XXXXXX")
app_forward=''
fixture_forward=''
cleanup() {
  if [ -n "$app_forward" ]; then kill "$app_forward" 2>/dev/null || true; fi
  if [ -n "$fixture_forward" ]; then kill "$fixture_forward" 2>/dev/null || true; fi
  printf 'Kubernetes smoke logs: %s\n' "$artifact_dir"
}
trap cleanup EXIT INT TERM
kubectl --context "$context" cluster-info > "$artifact_dir/cluster.txt"
kubectl --context "$context" apply -f deploy/k8s/overlays/smoke/namespace.yaml
kubectl --context "$context" apply --dry-run=server -k deploy/k8s/overlays/smoke > "$artifact_dir/validation.txt"
kubectl --context "$context" apply -k deploy/k8s/overlays/smoke
# Local image tags are mutable; explicitly start a fresh pod after each build.
kubectl --context "$context" -n agent-ti-test rollout restart statefulset/agent-ti
kubectl --context "$context" -n agent-ti-test rollout status deploy/agent-ti-fixture --timeout=120s
# Each fixture pod signs with a key of its own; wait until the old one has gone.
until [ "$(kubectl --context "$context" -n agent-ti-test get pods -l app.kubernetes.io/name=agent-ti-fixture --no-headers | wc -l | tr -d ' ')" = 1 ]; do sleep 1; done
kubectl --context "$context" -n agent-ti-test rollout status statefulset/agent-ti --timeout=180s
kubectl --context "$context" -n agent-ti-test port-forward svc/agent-ti 29120:18120 > "$artifact_dir/app-forward.log" 2>&1 &
app_forward=$!
kubectl --context "$context" -n agent-ti-test port-forward svc/agent-ti-fixture 29121:8080 > "$artifact_dir/fixture-forward.log" 2>&1 &
fixture_forward=$!
export AGENT_TI_URL=http://127.0.0.1:29120
export AGENT_TI_FIXTURE_URL=http://127.0.0.1:29121
wait_http() {
  i=0
  until curl -fsS "$1" >/dev/null 2>&1; do
    i=$((i+1)); if [ "$i" -gt 100 ]; then echo "Not ready: $1" >&2; return 1; fi
    sleep 0.2
  done
}
wait_http "$AGENT_TI_URL/readyz"
wait_http "$AGENT_TI_FIXTURE_URL/healthz"
node scripts/smoke.mjs > "$artifact_dir/http-smoke.txt"
cat "$artifact_dir/http-smoke.txt"
# Kill the container during a model call, then during an unsafe tool call, and check recovery.
crash() {
  node scripts/smoke.mjs "prepare-$1" "$artifact_dir/$1.json"
  prior=$(kubectl --context "$context" -n agent-ti-test get pod agent-ti-0 -o jsonpath='{.status.containerStatuses[0].restartCount}')
  # Signal from the ancestor namespace: Linux protects namespace PID 1 from a
  # self-issued SIGKILL. Docker Desktop exposes this pod's exact Docker container.
  container_id=$(kubectl --context "$context" -n agent-ti-test get pod agent-ti-0 -o jsonpath='{.status.containerStatuses[0].containerID}')
  case "$container_id" in
    docker://*) docker kill --signal KILL "${container_id#docker://}" ;;
    *) echo "Crash test needs Docker Desktop's Docker runtime, got: $container_id" >&2; exit 2 ;;
  esac
  kill "$app_forward" 2>/dev/null || true
  app_forward=''
  i=0
  while :; do
    current=$(kubectl --context "$context" -n agent-ti-test get pod agent-ti-0 -o jsonpath='{.status.containerStatuses[0].restartCount}')
    if [ "$current" -gt "$prior" ]; then break; fi
    i=$((i+1)); if [ "$i" -gt 60 ]; then echo 'container did not restart' >&2; exit 1; fi
    sleep 1
  done
  kubectl --context "$context" -n agent-ti-test wait --for=condition=Ready pod/agent-ti-0 --timeout=120s
  kubectl --context "$context" -n agent-ti-test port-forward svc/agent-ti 29120:18120 > "$artifact_dir/app-forward-$1.log" 2>&1 &
  app_forward=$!
  wait_http "$AGENT_TI_URL/readyz"
  node scripts/smoke.mjs "verify-$1" "$artifact_dir/$1.json" >> "$artifact_dir/recovery.txt"
}
crash restart
crash tool-restart
cat "$artifact_dir/recovery.txt"
kubectl --context "$context" -n agent-ti-test get pods,pvc -o wide > "$artifact_dir/resources.txt"
kubectl --context "$context" -n agent-ti-test logs agent-ti-0 > "$artifact_dir/runtime.log"
printf 'PASS Kubernetes smoke and crash recovery in context %s, namespace agent-ti-test\n' "$context"
