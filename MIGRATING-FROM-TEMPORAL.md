# Coming from Temporal: Checkpointed Steps + Human-in-the-Loop Approval

This example is a composite of two `temporalio/samples-go` patterns:
[`saga`](https://github.com/temporalio/samples-go/tree/main/saga) (sequential steps with compensation) and
[`await-signals`](https://github.com/temporalio/samples-go/tree/main/await-signals) (a workflow paused until
an external actor sends it data). If you are porting either of those patterns — or combining them — this guide
maps the mechanics side by side.

## The pattern

A durable orchestrator drains Kubernetes nodes one at a time. Each drain attempt is a **checkpointed step**:
it runs exactly once across crashes and replays. When a drain is blocked by a PodDisruptionBudget the workflow
creates a **latent durable promise** and parks on it; the operator resolves the promise through a separate
gateway process, and the workflow immediately resumes. A separate gateway binary starts operations and settles
decisions — it never runs workflow code itself.

In Temporal terms: the checkpointed steps correspond to activity executions in the `saga` sample; the
operator gate corresponds to the signal-receive pattern in `await-signals`; and the worker/gateway split
corresponds to a Temporal worker process plus a standalone client that calls `SignalWorkflow`.

## Side by side

### Part 1: Checkpointed steps — `saga` workflow.go

The Temporal `saga` sample runs three activities in sequence, deferring compensating activities if any step
fails:

```go
// saga/workflow.go (temporalio/samples-go)
func TransferMoney(ctx workflow.Context, transferDetails TransferDetails) (err error) {
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
	}

	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	err = workflow.ExecuteActivity(ctx, Withdraw, transferDetails).Get(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			errCompensation := workflow.ExecuteActivity(ctx, WithdrawCompensation, transferDetails).Get(ctx, nil)
			err = multierr.Append(err, errCompensation)
		}
	}()

	err = workflow.ExecuteActivity(ctx, Deposit, transferDetails).Get(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			errCompensation := workflow.ExecuteActivity(ctx, DepositCompensation, transferDetails).Get(ctx, nil)
			err = multierr.Append(err, errCompensation)
		}
	}()

	err = workflow.ExecuteActivity(ctx, StepWithError, transferDetails).Get(ctx, nil)
	if err != nil {
		return err
	}

	return nil
}
```

### Part 1: Checkpointed steps — this example (`drain/orchestrator.go`)

Each node drain is a `ctx.Run` step. `ctx.Run` takes a plain function, runs it once, and records the result
on a child durable promise. On replay after a crash the SDK checks whether that child promise has already
settled; if it has, the result is returned immediately without re-executing the function. No activity
registration, no `ActivityOptions`, no task-queue wiring.

```go
// drain/orchestrator.go
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

		// Blocked: suspend at the human-approval gate (see Part 2 below).
		// ...
	}
	// ...
}

// drainNode wraps drainSingleNode in a ctx.Run checkpoint.
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
```

The `drainSingleNode` method is a plain Go function — the only thing that makes it durable is being called
via `ctx.Run`. There is no `@activity` decorator, no separate registration step, and no timeout required
to start the function.

---

### Part 2: External approval gate — `await-signals` workflow.go

The Temporal `await-signals` sample listens for external signals in a background goroutine and uses
`workflow.Await` to park the main goroutine until one arrives:

```go
// await-signals/await_signals_workflow.go (temporalio/samples-go)
type AwaitSignals struct {
	FirstSignalTime time.Time
	Signal1Received bool
	Signal2Received bool
	Signal3Received bool
}

func (a *AwaitSignals) Listen(ctx workflow.Context) {
	for {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(workflow.GetSignalChannel(ctx, "Signal1"), func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			a.Signal1Received = true
		})
		// ... Signal2, Signal3 ...
		selector.Select(ctx)
	}
}

func AwaitSignalsWorkflow(ctx workflow.Context) (err error) {
	var a AwaitSignals
	workflow.Go(ctx, a.Listen) // background goroutine receives signals

	err = workflow.Await(ctx, func() bool {
		return a.Signal1Received
	})
	// ...
}
```

The external client that delivers a signal:

```go
// await-signals/starter/main.go (temporalio/samples-go)
err = c.SignalWorkflow(context.Background(), we.GetID(), we.GetRunID(), "Signal1", nil)
```

### Part 2: External approval gate — this example (`drain/orchestrator.go` + `cmd/gateway/main.go`)

Instead of a signal channel and a shared boolean, the workflow creates a **latent durable promise** with
`ctx.Promise()`, logs its ID, and calls `f.Await` to park. The promise replaces the signal definition,
the background goroutine, the shared flag, and the condition function — one call does the work of all four.

```go
// drain/orchestrator.go (the gate inside DrainAllNodes)

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
    // ...
case DecisionRetry:
    // ...
case DecisionForce:
    // ...
case DecisionSkip:
    // Leave the failed result in place and move on.
}
```

The gateway settles the promise from outside the workflow:

```go
// cmd/gateway/main.go — resolve() method
rec, err := gw.r.Sender().PromiseSettle(ctx, resonate.PromiseSettleReq{
    ID:    promiseID,
    State: resonate.SettleStateResolved,
    Value: value,
})
```

Where `value` is built by the gateway's `encodeSettleValue` helper (see
[Notes & coverage](#notes--coverage) for the encoding detail).

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `w.RegisterActivity(fn)` + `workflow.ExecuteActivity(ctx, fn, args)` | `ctx.Run(fn, args)` | No separate activity type; durability comes from `ctx.Run`, not from registration. |
| `workflow.ActivityOptions{StartToCloseTimeout, RetryPolicy}` | `resonate.RunOpts{Timeout, RetryPolicy}` | Options are passed per-call as a variadic arg; a timeout is optional, not required. Note: `RunOpts` is the in-workflow type (`ctx.Run`); the top-level type (`RegisteredFunc.Run`) uses `resonate.RunOptions` — same fields, different type. |
| `workflow.ExecuteActivity(...).Get(ctx, &out)` | `f, err := ctx.Run(...); if err != nil {...}; f.Await(&out)` | Both block until the step settles; the Resonate form returns a `(*Future, error)` pair. Always check `err` before calling `f.Await` — `ctx.Run` returns a nil `*Future` on error, and calling `Await` on nil panics. |
| `defer` + compensating `ExecuteActivity` | Ordinary Go `defer` + `ctx.Run` | Compensation in Resonate is plain Go code; the deferred `ctx.Run` is durable the same way the forward step is. |
| `workflow.GetSignalChannel` + `workflow.Go` + `workflow.Await` | `ctx.Promise()` + `f.Await` | One latent promise replaces the signal definition, background goroutine, shared flag, and condition function. |
| `client.SignalWorkflow(ctx, workflowID, runID, signalName, payload)` | `r.Sender().PromiseSettle(ctx, PromiseSettleReq{ID, State, Value})` | Both are called from outside the workflow. Resonate targets the promise by ID; Temporal targets it by workflow+run ID plus a named signal channel. |
| `w.RegisterWorkflow(fn)` + task-queue routing | `resonate.Register(r, "name", fn)` + group routing | Groups replace task queues. The worker's group (`"default"`) receives dispatches; the gateway's group (`"drain-gateway"`) does not. |
| `client.ExecuteWorkflow(ctx, opts, fn, args)` | `r.RPC(ctx, id, funcName, args, RPCOptions{Target})` | Both start the workflow and return a handle. `r.RPC` targets the worker group via `Target: "poll://any@default"`. |
| Workflow ID (stable, caller-supplied) | Promise ID (the `id` arg to `r.RPC`) | Both are the stable idempotency key for the operation. |
| `workflow.Context` (deterministic sandbox) | `*resonate.Context` | Passed to the workflow function; I/O that must be checkpointed goes through it. |
| `context.Context` (activity I/O) | Plain Go function body (any context) | The step function (`drainSingleNode`) uses `context.Background()` freely — it is not a deterministic sandbox. |

## Porting it, step by step

**From Temporal `saga` → `ctx.Run` checkpointed steps:**

1. Remove `workflow.ActivityOptions` and `workflow.WithActivityOptions`. Per-call options move to
   `resonate.RunOpts{Timeout: d}` as the last variadic arg to `ctx.Run`, and are optional.
2. Replace each `workflow.ExecuteActivity(ctx, Fn, args).Get(ctx, &out)` with:
   ```go
   f, err := ctx.Run(fn, args)
   if err != nil { return ..., err }
   if err := f.Await(&out); err != nil { return ..., err }
   ```
3. There is no `RegisterActivity` equivalent. `ctx.Run` accepts functions in four forms: no args
   (`func() (R, error)`), context only (`func(*resonate.Context) (R, error)`), args only
   (`func(A) (R, error)`), or context plus args (`func(*resonate.Context, A) (R, error)`). This
   example uses the context-only form for `timestamp` — no dummy arg struct is needed. Pass method
   values, closures, or package-level functions as needed.
4. Compensation defers are identical Go code. Replace the activity call inside the `defer` the same way as
   step 2; the `defer` mechanics are unchanged.
5. Remove `w.RegisterWorkflow`. Register the workflow function once with `resonate.Register(r, "name", fn)`.
   Handle both return values — a bare call discarding both returns compiles but silently drops the error
   and the `*RegisteredFunc` handle; a partial one-of-two assignment is a compile error. Use
   `_, err := resonate.Register(...)` at minimum.

**From Temporal `await-signals` → `ctx.Promise`:**

6. Remove the signal channel definitions, the background `workflow.Go` goroutine, the shared boolean fields,
   and the `workflow.Await` / `workflow.AwaitWithTimeout` condition functions. Replace the whole group with:
   ```go
   decisionF, err := ctx.Promise()    // creates a latent durable promise
   log.Println("resolve:", decisionF.ID())
   var out MyType
   if err := decisionF.Await(&out); err != nil { ... }
   ```
7. In the external client (the gateway), replace `client.SignalWorkflow` with
   `r.Sender().PromiseSettle(ctx, resonate.PromiseSettleReq{ID: promiseID, State: resonate.SettleStateResolved, Value: value})`.
   The promise ID is the one logged in step 6. See [Notes & coverage](#notes--coverage) for how to build
   `value` correctly.
8. The external client must use a **distinct Resonate group** from the worker. If the gateway shared the
   worker's group, the server would round-robin workflow dispatches to it, and the gateway would drop them
   ("function not registered"). Set the group in `httpnet.HTTPOptions{Group: "drain-gateway"}`.

**Gateway (external client) setup:**

This is the process that replaces `client.ExecuteWorkflow` and `client.SignalWorkflow`. Build it with
its own `resonate.New` instance and call `r.RPC` to start a workflow:

   ```go
   r, err := resonate.New(resonate.Config{
       Network: httpnet.NewHTTP(url, httpnet.HTTPOptions{
           PID:   pid,
           Group: "drain-gateway", // distinct group — never receives workflow dispatches
       }),
   })
   // start a workflow (equivalent to client.ExecuteWorkflow):
   _, err = r.RPC(ctx, operationID, drain.FuncName, args, resonate.RPCOptions{
       Target: "poll://any@default", // route to the worker group
   })
   ```

   The same `*resonate.Resonate` instance is used for `r.RPC` (start), `r.Sender().PromiseGet` (status),
   and `r.Sender().PromiseSettle` (approve/reject). No separate client dial is needed.

**Worker setup:**

9. The worker calls `resonate.New(resonate.Config{Network: httpnet.NewHTTP(url, httpnet.HTTPOptions{Group: "default", PID: pid})})`.
   There is no separate `client.Dial`; the same `*Resonate` instance is used for both the poll loop and any
   top-level `r.RPC` / `r.Get` calls.
10. Raise `Config.TTL` if individual steps can take longer than the default 60-second task lease.

## What's different (and why)

**No workflow/activity distinction.** In Temporal, `workflow.Context` and `context.Context` mark the
boundary between the deterministic sandbox and free I/O. In Resonate that boundary is structural: the
workflow function receives `*resonate.Context` and must route all durable I/O through it; the step function
receives `*resonate.Context` as well, but is a plain Go function that can use `context.Background()` freely
for Kubernetes API calls. The SDK does not enforce determinism inside the step — the developer is
responsible for keeping side effects inside `ctx.Run` calls.

**Promise ID is the idempotency key.** In Temporal the workflow ID is the stable business key and the
run ID identifies a particular attempt. In Resonate the caller-supplied `id` arg to `r.RPC` (or
`f.ID()` for child promises) is the single stable identifier. Re-submitting `r.RPC` with the same ID
and `Target` is idempotent: the server returns the existing promise without dispatching a second execution.

**Groups replace task queues.** A Temporal task queue determines which workers pick up which work. Resonate
groups do the same job. The `Target: "poll://any@default"` string in `r.RPC` routes the dispatch to any
worker in the `"default"` group. The gateway uses `"drain-gateway"` to opt out of receiving dispatches.

**Signals vs. durable promises.** A Temporal signal is a named channel attached to a running workflow
instance; delivering it requires the workflow ID (run ID is optional — omitting it targets the latest
run). The Temporal server durably records the signal in the workflow's event history the moment it is
sent — no worker needs to be online at that instant — but a worker must eventually run the workflow
execution to consume it from history. A latent Resonate promise is a first-class server-side object: it can be settled before
the workflow reaches `Await` — the value is durably stored on the server and survives crashes and
replays — though the `ctx.Promise()` call that creates it must execute before any settlement takes
effect. The `Await` call simply blocks until the server reports the promise is settled. There is no
signal-name registry, no goroutine, and no condition function.

**Replay model.** Both systems re-execute the workflow function body from the top on resume. In Temporal
the event history is the authoritative record; in Resonate the settled child promises are. The net behavior
is the same: completed work is never repeated, and side effects outside the checkpointed boundary re-run.
Keep non-idempotent I/O inside `ctx.Run`.

## Notes & coverage

**Value encoding for `PromiseSettle`.** This is the roughest edge in the Go SDK today. The spec signature
in the cheat-sheet is `r.Sender().PromiseSettle(ctx, resonate.PromiseSettleReq{ID, State, Value})` with
`Value` built via `resonate.NewValue(x)`. In practice, `resonate.NewValue(v)` stores raw JSON without the
base64 wrapping the SDK codec expects when it decodes the value inside `f.Await`. The gateway works around
this with a manual `encodeSettleValue` helper: JSON → base64 → quoted JSON string, assigned to
`resonate.Value{Data: json.RawMessage(quoted)}`. A high-level `r.Promises().Resolve(id, value)` that folds
encoding and the settle RPC into one call is tracked in
[resonate-sdk-go#28](https://github.com/resonatehq/resonate-sdk-go/issues/28). Until that lands, follow
the pattern in `cmd/gateway/main.go` — or use the `resonate promises resolve` CLI for ad-hoc resolution.

**Compensation shape.** The `saga` sample uses `defer` + a compensating `ExecuteActivity` to reverse
completed steps. This example uses a different compensation shape: `drainSingleNode` returns a structured
`NodeResult` (never a Go error) when a PDB blocks, and the workflow branches into the human-approval gate.
When the operator chooses `abort` the workflow returns early; `retry` and `force` re-invoke `drainNode`
for the same node. The `ctx.Run` call in the retry path uses a new child promise ID (SDK-generated), so
the retry step is itself checkpointed independently of the original failed attempt.

**`ctx.Run` vs. `r.RPC`.** This codebase uses two dispatch mechanisms. `ctx.Run` (used inside `DrainAllNodes`) executes a local Go function in the same process — the step runs on the worker that owns the workflow and the result is recorded on a child durable promise. `r.RPC` (used in the gateway's `drain` handler) dispatches to a named registered function on a remote worker group; it is the top-level entry point that starts a new workflow execution. The drain steps (`getNodes`, `drainSingleNode`, `timestamp`) all use `ctx.Run` because the `*k8s.Client` they need is held on the receiver and is not serializable. `ctx.RPC` also exists in the SDK (in-workflow cross-worker dispatch), but is not used in this example.

**`resonate.Register` multi-return.** The call `resonate.Register(r, drain.FuncName, orchestrator.DrainAllNodes)`
returns `(*RegisteredFunc, error)`. A bare call discarding both returns compiles but silently drops the
error and the `*RegisteredFunc` handle; assigning only one of the two values is a compile error. The
worker handles this correctly with `_, err := resonate.Register(...)`.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal `saga` sample: https://github.com/temporalio/samples-go/tree/main/saga
- Temporal `await-signals` sample: https://github.com/temporalio/samples-go/tree/main/await-signals
- This example's README
