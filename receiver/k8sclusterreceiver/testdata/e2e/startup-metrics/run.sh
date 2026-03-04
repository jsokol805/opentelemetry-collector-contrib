#!/usr/bin/env bash
# End-to-end test for pod/container startup duration metrics using kind.
#
# Usage:
#   ./run.sh          # full setup: create cluster, build, deploy, tail logs
#   ./run.sh setup    # only create cluster + build + load image
#   ./run.sh deploy   # only apply manifests (cluster must exist)
#   ./run.sh logs     # only tail collector logs (already deployed)
#   ./run.sh cleanup  # tear down the kind cluster
#
# Prerequisites: kind, docker, kubectl, make (at repo root)

set -euo pipefail

CLUSTER_NAME="otelcol-startup-e2e"
KUBECONFIG_PATH="/tmp/kube-config-${CLUSTER_NAME}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
MANIFESTS="${SCRIPT_DIR}/manifests.yaml"

export KUBECONFIG="${KUBECONFIG_PATH}"

log() { echo "==> $*"; }

cmd_setup() {
    log "Creating kind cluster '${CLUSTER_NAME}'..."
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log "Cluster already exists, skipping creation."
    else
        kind create cluster --name "${CLUSTER_NAME}" --kubeconfig="${KUBECONFIG_PATH}"
    fi

    log "Building otelcontribcol docker image..."
    (cd "${REPO_ROOT}" && make docker-otelcontribcol)

    log "Loading image into kind..."
    kind load docker-image otelcontribcol:latest --name "${CLUSTER_NAME}"
}

cmd_deploy() {
    log "Applying manifests..."
    kubectl apply -f "${MANIFESTS}"

    log "Waiting for collector pod to be ready..."
    kubectl wait --for=condition=ready pod -l app=otelcol-startup-test \
        --timeout=120s

    log "Waiting for test workload pods to be ready..."
    kubectl wait --for=condition=ready pod -l app=startup-test-workload \
        --timeout=120s

    log "All pods are running. Use '$0 logs' to see startup metrics."
}

cmd_logs() {
    log "Tailing collector logs (filtering for startup/duration metrics)..."
    log "Press Ctrl+C to stop."
    echo ""
    COLLECTOR_POD=$(kubectl get pod -l app=otelcol-startup-test -o jsonpath='{.items[0].metadata.name}')
    kubectl logs -f "${COLLECTOR_POD}" | grep -E '(startup_duration|scheduling_duration|initializing_duration|containers_ready_duration|Name:|Value:)' || true
}

cmd_logs_raw() {
    log "Tailing raw collector logs..."
    COLLECTOR_POD=$(kubectl get pod -l app=otelcol-startup-test -o jsonpath='{.items[0].metadata.name}')
    kubectl logs -f "${COLLECTOR_POD}"
}

cmd_cleanup() {
    log "Deleting kind cluster '${CLUSTER_NAME}'..."
    kind delete cluster --name "${CLUSTER_NAME}"
    rm -f "${KUBECONFIG_PATH}"
    log "Done."
}

cmd_all() {
    cmd_setup
    cmd_deploy
    # Give the collector two scrape intervals to pick up the metrics
    log "Waiting 25s for metrics collection..."
    sleep 25
    cmd_logs
}

case "${1:-all}" in
    setup)   cmd_setup ;;
    deploy)  cmd_deploy ;;
    logs)    cmd_logs ;;
    logs-raw) cmd_logs_raw ;;
    cleanup) cmd_cleanup ;;
    all)     cmd_all ;;
    *)
        echo "Usage: $0 {setup|deploy|logs|logs-raw|cleanup|all}"
        exit 1
        ;;
esac
