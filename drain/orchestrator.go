package drain

import (
	"context"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"

	"github.com/resonatehq-examples/example-node-drain-orchestrator-go/internal/k8s"
)

// Orchestrator owns the Kubernetes client and exposes the workflow and its
// checkpointed steps as methods. Holding the client on the receiver keeps it
// out of the workflow arguments — only JSON-serializable data crosses the
// durability boundary, and a *k8s.Client is neither serializable nor stable
// across worker restarts. The worker constructs one Orchestrator and registers
// DrainAllNodes; ctx.Run calls below pass the unexported step methods by value,
// which the SDK accepts as ordinary func(*Context, A) (R, error) values.
type Orchestrator struct {
	cluster *k8s.Client
}

// NewOrchestrator returns an Orchestrator bound to a Kubernetes client.
func NewOrchestrator(cluster *k8s.Client) *Orchestrator {
	return &Orchestrator{cluster: cluster}
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }

// timestamp is a checkpointed step that records the current time. Calling
// time.Now() directly in the workflow body would re-read the clock on every
// replay; wrapping it in ctx.Run records the value once on the durable promise
// so the operation's start/end times stay stable across suspensions.
func (o *Orchestrator) timestamp(_ *resonate.Context) (string, error) {
	return nowRFC3339(), nil
}

func (o *Orchestrator) checkpointNow(ctx *resonate.Context) (string, error) {
	f, err := ctx.Run(o.timestamp, nil)
	if err != nil {
		return "", err
	}
	var ts string
	if err := f.Await(&ts); err != nil {
		return "", err
	}
	return ts, nil
}

// getNodes is a checkpointed step that lists the worker nodes to drain.
func (o *Orchestrator) getNodes(_ *resonate.Context, args getNodesArgs) ([]k8s.NodeInfo, error) {
	kctx := context.Background()
	if len(args.NodeSelector) > 0 {
		return o.cluster.NodesBySelector(kctx, args.NodeSelector)
	}
	return o.cluster.WorkerNodes(kctx)
}

// drainSingleNode is a checkpointed step that cordons one node and evicts its
// pods. A PodDisruptionBudget block (or an eviction timeout) is returned as a
// NodeResult with Success=false and BlockedBy set — NOT as an error — so the
// workflow can branch into the human-in-the-loop path instead of retrying.
// Genuine "give the operator a choice" outcomes are values; the step only ever
// returns a nil error, mirroring the TypeScript example's try/catch shape.
func (o *Orchestrator) drainSingleNode(_ *resonate.Context, args nodeArgs) (NodeResult, error) {
	kctx := context.Background()
	opts := args.Options
	res := NodeResult{Node: args.NodeName, StartedAt: nowRFC3339()}

	fail := func(msg string, blockedBy *k8s.PodInfo, evicted int) (NodeResult, error) {
		res.Success = false
		res.Error = msg
		res.BlockedBy = blockedBy
		res.PodsEvicted = evicted
		res.CompletedAt = nowRFC3339()
		return res, nil
	}

	if err := o.cluster.Cordon(kctx, args.NodeName); err != nil {
		return fail(err.Error(), nil, 0)
	}

	pods, err := o.cluster.PodsOnNode(kctx, args.NodeName, opts.IgnoreDaemonSets)
	if err != nil {
		return fail(err.Error(), nil, 0)
	}
	log.Printf("[drain] node %s: %d pod(s) to evict", args.NodeName, len(pods))

	evicted := 0
	evictTimeout := time.Duration(opts.EvictionTimeoutMs) * time.Millisecond
	for i := range pods {
		pod := pods[i]
		if pod.Phase == "Succeeded" || pod.Phase == "Failed" {
			continue
		}

		ev := o.cluster.Evict(kctx, pod.Name, pod.Namespace, int64(opts.GracePeriodSeconds))
		if !ev.Success {
			if !opts.Force {
				// Blocked by a PDB (or another eviction error): hand control
				// back to the workflow, which will ask a human what to do.
				return fail(ev.Error, &pod, evicted)
			}
			// Force mode: bypass the PDB with a direct delete.
			if err := o.cluster.Delete(kctx, pod.Name, pod.Namespace, 0); err != nil {
				return fail(err.Error(), &pod, evicted)
			}
		}
		evicted++

		deleted, err := o.cluster.WaitForPodDeletion(kctx, pod.Name, pod.Namespace, evictTimeout)
		if err != nil {
			return fail(err.Error(), &pod, evicted)
		}
		if !deleted && !opts.Force {
			return fail(fmt.Sprintf("timeout waiting for pod %s/%s to terminate", pod.Namespace, pod.Name), &pod, evicted)
		}
	}

	drainTimeout := time.Duration(opts.DrainTimeoutMs) * time.Millisecond
	empty, err := o.cluster.WaitForNodeEmpty(kctx, args.NodeName, opts.IgnoreDaemonSets, drainTimeout)
	if err != nil {
		return fail(err.Error(), nil, evicted)
	}
	if !empty && !opts.Force {
		return fail("timeout waiting for node to be empty", nil, evicted)
	}

	res.Success = true
	res.PodsEvicted = evicted
	res.CompletedAt = nowRFC3339()
	log.Printf("[drain] node %s drained (%d pod(s) evicted)", args.NodeName, evicted)
	return res, nil
}

// DrainAllNodes is the durable workflow. It lists the worker nodes, drains each
// one through a checkpointed step, and — when a drain is blocked and force is
// off — creates a latent durable promise and parks on it until a human resolves
// the decision via the gateway. Every step survives worker crashes: on resume
// the workflow re-runs from the top and each settled child promise
// short-circuits, so completed work is never repeated.
func (o *Orchestrator) DrainAllNodes(ctx *resonate.Context, args Args) (Result, error) {
	log.Printf("[drain] starting operation %s", args.OperationID)

	startedAt, err := o.checkpointNow(ctx)
	if err != nil {
		return Result{}, err
	}

	nodesF, err := ctx.Run(o.getNodes, getNodesArgs{NodeSelector: args.NodeSelector})
	if err != nil {
		return Result{}, err
	}
	var nodes []k8s.NodeInfo
	if err := nodesF.Await(&nodes); err != nil {
		return Result{}, err
	}
	log.Printf("[drain] %d worker node(s) to drain", len(nodes))

	results := make([]NodeResult, 0, len(nodes))

	for _, node := range nodes {
		res, err := o.drainNode(ctx, node.Name, args.Options)
		if err != nil {
			return Result{}, err
		}
		results = append(results, res)

		// A successful drain, or force mode, needs no human input.
		if res.Success || args.Options.Force {
			continue
		}

		// Blocked: create a latent durable promise and suspend until an
		// operator resolves it. Future.ID() is the promise ID the gateway
		// settles; it is logged so an operator can act on it.
		decisionF, err := ctx.Promise()
		if err != nil {
			return Result{}, err
		}
		log.Printf("[drain] node %s blocked — resolve promise %q with skip|retry|abort|force",
			node.Name, decisionF.ID())

		var decision Decision
		if err := decisionF.Await(&decision); err != nil {
			return Result{}, err
		}
		log.Printf("[drain] operator decision for %s: %s", node.Name, decision)

		switch decision {
		case DecisionAbort:
			completedAt, err := o.checkpointNow(ctx)
			if err != nil {
				return Result{}, err
			}
			return Result{
				OperationID: args.OperationID,
				Status:      "failed",
				StartedAt:   startedAt,
				CompletedAt: completedAt,
				Nodes:       results,
			}, nil

		case DecisionRetry:
			retry, err := o.drainNode(ctx, node.Name, args.Options)
			if err != nil {
				return Result{}, err
			}
			results[len(results)-1] = retry

		case DecisionForce:
			forced := args.Options
			forced.Force = true
			res, err := o.drainNode(ctx, node.Name, forced)
			if err != nil {
				return Result{}, err
			}
			results[len(results)-1] = res

		case DecisionSkip:
			// Leave the failed result in place and move on.
		}
	}

	completedAt, err := o.checkpointNow(ctx)
	if err != nil {
		return Result{}, err
	}

	status := "completed"
	for _, r := range results {
		if !r.Success {
			status = "failed"
			break
		}
	}
	log.Printf("[drain] operation %s %s", args.OperationID, status)

	return Result{
		OperationID: args.OperationID,
		Status:      status,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Nodes:       results,
	}, nil
}

// drainNode runs the drainSingleNode step as a durable checkpoint and returns
// its result.
func (o *Orchestrator) drainNode(ctx *resonate.Context, nodeName string, opts Options) (NodeResult, error) {
	f, err := ctx.Run(o.drainSingleNode, nodeArgs{NodeName: nodeName, Options: opts})
	if err != nil {
		return NodeResult{}, err
	}
	var res NodeResult
	if err := f.Await(&res); err != nil {
		return NodeResult{}, err
	}
	return res, nil
}
