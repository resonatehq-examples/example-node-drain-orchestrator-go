#!/bin/bash
# Cleanup Kind cluster and resources

set -e

CLUSTER_NAME="drain-demo"

echo "=============================================="
echo "Cleaning up Node Drain Orchestrator demo"
echo "=============================================="

# Delete workloads first (in case cluster still exists)
if kubectl cluster-info &> /dev/null 2>&1; then
    echo "Deleting test workloads..."
    kubectl delete namespace drain-demo --ignore-not-found=true --wait=false
fi

# Delete Kind cluster
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Deleting Kind cluster '$CLUSTER_NAME'..."
    kind delete cluster --name "$CLUSTER_NAME"
    echo "Cluster deleted."
else
    echo "Cluster '$CLUSTER_NAME' not found."
fi

# Clean up local files
echo "Cleaning up local files..."
rm -f resonate.db 2>/dev/null || true

echo ""
echo "=============================================="
echo "Cleanup complete!"
echo "=============================================="
