#!/bin/bash
# Deploy test workloads to Kind cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo "Deploying test workloads"
echo "=============================================="

# Check if kubectl is configured
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: kubectl is not configured or cluster is not running."
    echo "Run './scripts/setup-cluster.sh' first."
    exit 1
fi

# Apply test workloads
echo "Applying test-workloads.yaml..."
kubectl apply -f "$PROJECT_DIR/test-workloads.yaml"

# Wait for deployments to be ready
echo "Waiting for deployments to be ready..."
kubectl wait --for=condition=Available deployment/simple-app -n drain-demo --timeout=60s
kubectl wait --for=condition=Available deployment/pdb-protected-app -n drain-demo --timeout=60s
kubectl wait --for=condition=Available deployment/critical-app -n drain-demo --timeout=60s

# Wait for DaemonSet
echo "Waiting for DaemonSet to be ready..."
kubectl rollout status daemonset/node-monitor -n drain-demo --timeout=60s

# Show deployed workloads
echo ""
echo "=============================================="
echo "Test workloads deployed!"
echo "=============================================="
echo ""

echo "Pods by node:"
kubectl get pods -n drain-demo -o wide

echo ""
echo "Pod Disruption Budgets:"
kubectl get pdb -n drain-demo

echo ""
echo "Scenario summary:"
echo "  - simple-app (4 replicas): No PDB, quick termination"
echo "  - pdb-protected-app (3 replicas): PDB allows 1 unavailable"
echo "  - critical-app (2 replicas): PDB blocks all evictions"
echo "  - long-running-job (1 pod): 5-minute termination grace period"
echo "  - node-monitor (DaemonSet): Present on all nodes"
echo ""
echo "Ready to test drain scenarios!"
