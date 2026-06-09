#!/bin/bash
# Setup Kind cluster for Node Drain Orchestrator demo

set -e

CLUSTER_NAME="drain-demo"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo "Setting up Kind cluster: $CLUSTER_NAME"
echo "=============================================="

# Check if kind is installed
if ! command -v kind &> /dev/null; then
    echo "Error: 'kind' is not installed."
    echo "Install it from: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
    exit 1
fi

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo "Error: 'kubectl' is not installed."
    echo "Install it from: https://kubernetes.io/docs/tasks/tools/"
    exit 1
fi

# Delete existing cluster if it exists
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo "Deleting existing cluster '$CLUSTER_NAME'..."
    kind delete cluster --name "$CLUSTER_NAME"
fi

# Create the cluster
echo "Creating Kind cluster with 3 nodes..."
kind create cluster --config "$PROJECT_DIR/kind-config.yaml"

# Wait for nodes to be ready
echo "Waiting for nodes to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=120s

# Show cluster info
echo ""
echo "=============================================="
echo "Cluster created successfully!"
echo "=============================================="
echo ""
kubectl get nodes -o wide
echo ""
echo "Next steps:"
echo "  1. Deploy test workloads: ./scripts/deploy-workloads.sh"
echo "  2. Start Resonate server: resonate dev"
echo "  3. Start worker: go run ./cmd/worker"
echo "  4. Start gateway: go run ./cmd/gateway"
echo "  5. Trigger drain: curl -X POST http://localhost:3000/drain"
