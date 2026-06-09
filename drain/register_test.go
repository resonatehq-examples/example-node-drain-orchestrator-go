package drain

import (
	"testing"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"

	"github.com/resonatehq-examples/example-node-drain-orchestrator-go/internal/k8s"
)

// Compile-time assertions that the workflow and its checkpointed steps have the
// shapes resonate.Register and ctx.Run accept — func(*resonate.Context, A)
// (R, error), or func(*resonate.Context) (R, error) for the no-arg step.
// ctx.Run takes its function as `any` and validates the signature only at
// runtime (durableFunctionFor), so without these guards a bad step signature
// would surface as a workflow-execution error rather than a build error.
var (
	_ func(*resonate.Context, Args) (Result, error)                 = (*Orchestrator)(nil).DrainAllNodes
	_ func(*resonate.Context, getNodesArgs) ([]k8s.NodeInfo, error) = (*Orchestrator)(nil).getNodes
	_ func(*resonate.Context, nodeArgs) (NodeResult, error)         = (*Orchestrator)(nil).drainSingleNode
	_ func(*resonate.Context) (string, error)                       = (*Orchestrator)(nil).timestamp
)

// TestRegisterDrainWorkflow confirms the workflow registers without error
// against the SDK pin — the runtime half of the signature check above.
func TestRegisterDrainWorkflow(t *testing.T) {
	pid := "drain-register-test"
	r, err := resonate.New(resonate.Config{
		Network:   localnet.NewLocal("default", &pid),
		Heartbeat: resonate.NoopHeartbeat{},
	})
	if err != nil {
		t.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	// Registration only needs the function value, not a live cluster.
	o := NewOrchestrator(nil)
	if _, err := resonate.Register(r, FuncName, o.DrainAllNodes); err != nil {
		t.Fatalf("register %s: %v", FuncName, err)
	}
}
