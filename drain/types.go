// Package drain holds the durable node-drain workflow and its checkpointed
// steps. The workflow is registered on the worker under FuncName and started
// remotely by the gateway via r.RPC.
package drain

import "github.com/resonatehq-examples/example-node-drain-orchestrator-go/internal/k8s"

// FuncName is the registered workflow name. The worker registers the workflow
// under this name; the gateway targets it with r.RPC. Keeping it in one place
// stops the two binaries from drifting.
const FuncName = "drainAllNodes"

// WorkerGroup is the Resonate routing group the worker joins (the SDK default).
// The gateway dispatches the workflow to "poll://any@" + WorkerGroup, and runs
// in its own distinct group so it never receives — and drops — workflow tasks.
const WorkerGroup = "default"

// GatewayGroup is the distinct group the gateway uses. It only creates and
// settles promises; it registers no functions, so it must not share the
// worker's group or the server would round-robin workflow dispatches to it.
const GatewayGroup = "drain-gateway"

// Decision is an operator's answer when a node drain is blocked by a PDB.
type Decision string

const (
	DecisionSkip  Decision = "skip"  // move on to the next node
	DecisionRetry Decision = "retry" // re-run the drain as-is
	DecisionAbort Decision = "abort" // stop the whole operation
	DecisionForce Decision = "force" // re-run the drain force-deleting pods
)

// Valid reports whether d is one of the four known decisions.
func (d Decision) Valid() bool {
	switch d {
	case DecisionSkip, DecisionRetry, DecisionAbort, DecisionForce:
		return true
	default:
		return false
	}
}

// Options tune a drain operation. Timeouts are milliseconds to mirror the
// TypeScript example's shape; GracePeriodSeconds is the pod termination grace.
//
// DeleteLocalData is accepted for parity with the TypeScript example but is not
// enforced here (nor in the TS original) — the Eviction API handles local-data
// cleanup at the kubelet level.
type Options struct {
	EvictionTimeoutMs  int  `json:"evictionTimeoutMs"`
	DrainTimeoutMs     int  `json:"drainTimeoutMs"`
	IgnoreDaemonSets   bool `json:"ignoreDaemonSets"`
	DeleteLocalData    bool `json:"deleteLocalData"`
	Force              bool `json:"force"`
	GracePeriodSeconds int  `json:"gracePeriodSeconds"`
}

// DefaultOptions returns the same defaults as the TypeScript example.
func DefaultOptions() Options {
	return Options{
		EvictionTimeoutMs:  60000,  // 1 minute per pod
		DrainTimeoutMs:     300000, // 5 minutes per node
		IgnoreDaemonSets:   true,
		DeleteLocalData:    true,
		Force:              false,
		GracePeriodSeconds: 30,
	}
}

// Args is the single struct passed to the workflow. resonate.Register is
// generic over func(*Context, A) (R, error) — exactly one argument after the
// context — so the TypeScript workflow's three parameters (operationId,
// options, nodeSelector) collapse into one struct here.
type Args struct {
	OperationID  string            `json:"operationId"`
	Options      Options           `json:"options"`
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// nodeArgs is the single struct passed to the drainSingleNode step, for the
// same single-argument reason as Args.
type nodeArgs struct {
	NodeName string  `json:"nodeName"`
	Options  Options `json:"options"`
}

// getNodesArgs is the single struct passed to the getNodes step.
type getNodesArgs struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// NodeResult is the outcome of draining a single node.
type NodeResult struct {
	Node        string       `json:"node"`
	Success     bool         `json:"success"`
	StartedAt   string       `json:"startedAt"`
	CompletedAt string       `json:"completedAt,omitempty"`
	PodsEvicted int          `json:"podsEvicted"`
	Error       string       `json:"error,omitempty"`
	BlockedBy   *k8s.PodInfo `json:"blockedBy,omitempty"`
}

// Result is the workflow's return value, stored on the root durable promise.
// Status is "completed" when every node drained, otherwise "failed".
type Result struct {
	OperationID string       `json:"operationId"`
	Status      string       `json:"status"`
	StartedAt   string       `json:"startedAt"`
	CompletedAt string       `json:"completedAt,omitempty"`
	Nodes       []NodeResult `json:"nodes"`
}
