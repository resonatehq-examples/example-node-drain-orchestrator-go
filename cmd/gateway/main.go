// Command gateway is the HTTP control plane for the drain orchestrator. It
// starts drain operations (r.RPC, fire-and-forget), reports status
// (Sender().PromiseGet), and resolves the human-in-the-loop decision promise
// (Sender().PromiseSettle). It registers no workflow functions, so it joins a
// distinct Resonate group — see the group note on the client below.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"

	"github.com/resonatehq-examples/example-node-drain-orchestrator-go/drain"
)

type gateway struct {
	r      *resonate.Resonate
	target string

	mu          sync.Mutex
	operationID string // last operation started through this gateway
}

func main() {
	url := flag.String("url", envOr("RESONATE_URL", "http://localhost:8001"), "Resonate server URL")
	port := flag.Int("port", envInt("GATEWAY_PORT", 3000), "HTTP listen port")
	flag.Parse()

	pid := fmt.Sprintf("drain-gateway-%d", os.Getpid())
	r, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(*url, httpnet.HTTPOptions{
			PID: pid,
			// The gateway only *creates* and *settles* promises — it never runs
			// a registered function. It must therefore use a group distinct
			// from the worker's. If it shared the worker's group, the server
			// would round-robin workflow task dispatches to the gateway too,
			// and the gateway would drop them ("function not registered"),
			// intermittently stalling the workflow.
			Group: drain.GatewayGroup,
		}),
	})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	gw := &gateway{r: r, target: "poll://any@" + drain.WorkerGroup}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", gw.health)
	mux.HandleFunc("GET /status", gw.status)
	mux.HandleFunc("GET /status/{operationId}", gw.status)
	mux.HandleFunc("POST /drain", gw.drain)
	mux.HandleFunc("POST /decision", gw.decision)
	mux.HandleFunc("POST /skip/{promiseId}", gw.shortcut(drain.DecisionSkip))
	mux.HandleFunc("POST /retry/{promiseId}", gw.shortcut(drain.DecisionRetry))
	mux.HandleFunc("POST /abort/{promiseId}", gw.shortcut(drain.DecisionAbort))
	mux.HandleFunc("POST /force/{promiseId}", gw.shortcut(drain.DecisionForce))
	mux.HandleFunc("DELETE /operation", gw.clearOperation)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[gateway pid=%s group=%s] listening on %s — Resonate at %s", pid, drain.GatewayGroup, addr, *url)
	log.Println("  POST /drain                 start a drain operation")
	log.Println("  GET  /status[/:operationId] operation status")
	log.Println("  POST /decision              {\"decision\":\"skip|retry|abort|force\",\"promiseId\":\"...\"}")
	log.Println("  POST /{skip,retry,abort,force}/:promiseId")
	log.Println("  (the promise ID to resolve is printed in the worker logs when a node blocks)")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("http server: %v", err)
	}
}

func (gw *gateway) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// drain starts a drain operation. r.RPC creates the root durable promise and
// dispatches the workflow to the worker group; the handle is intentionally
// discarded (fire-and-forget) because the operation's state lives on the server
// and is read back via /status, not by blocking here.
func (gw *gateway) drain(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	var body struct {
		Options      *drain.Options    `json:"options"`
		NodeSelector map[string]string `json:"nodeSelector"`
	}
	if req.Body != nil {
		// An empty body is fine; only reject malformed JSON.
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
			return
		}
	}

	gw.mu.Lock()
	defer gw.mu.Unlock()

	// Reject a new drain while one is still running.
	if gw.operationID != "" {
		if rec, err := gw.r.Sender().PromiseGet(ctx, gw.operationID); err == nil && rec.State == resonate.PromiseStatePending {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":       "a drain operation is already in progress",
				"operationId": gw.operationID,
			})
			return
		}
	}

	opts := drain.DefaultOptions()
	if body.Options != nil {
		opts = *body.Options
	}

	operationID := fmt.Sprintf("drain-%d", time.Now().UnixNano())
	args := drain.Args{OperationID: operationID, Options: opts, NodeSelector: body.NodeSelector}

	if _, err := gw.r.RPC(ctx, operationID, drain.FuncName, args, resonate.RPCOptions{Target: gw.target}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	gw.operationID = operationID
	log.Printf("[gateway] started drain operation %s", operationID)

	writeJSON(w, http.StatusAccepted, map[string]string{
		"message":     "drain operation started",
		"operationId": operationID,
		"statusUrl":   "/status/" + operationID,
	})
}

// status reports the operation state by reading the root promise. A pending
// promise means the workflow is running (or parked on a human decision); a
// settled promise carries the final Result, base64-decoded out of the value.
func (gw *gateway) status(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	operationID := req.PathValue("operationId")
	if operationID == "" {
		gw.mu.Lock()
		operationID = gw.operationID
		gw.mu.Unlock()
	}
	if operationID == "" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "idle", "message": "no active drain operation"})
		return
	}

	rec, err := gw.r.Sender().PromiseGet(ctx, operationID)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":      "idle",
			"operationId": operationID,
			"message":     "operation not found",
		})
		return
	}

	if rec.State == resonate.PromiseStatePending {
		writeJSON(w, http.StatusOK, map[string]any{
			"operationId": operationID,
			"status":      "in_progress",
			"message":     "workflow running; check back when completed for detailed results",
		})
		return
	}

	// Settled. A resolved promise holds the Result; a rejected one means the
	// workflow failed.
	resp := map[string]any{"operationId": operationID}
	if rec.State == resonate.PromiseStateResolved {
		var result drain.Result
		if err := decodePromiseValue(rec.Value, &result); err != nil {
			log.Printf("[gateway] failed to decode result for %s: %v", operationID, err)
			resp["status"] = "completed"
		} else {
			resp["status"] = result.Status
			resp["result"] = result
		}
	} else {
		resp["status"] = "failed"
	}
	writeJSON(w, http.StatusOK, resp)
}

// decision resolves the human-in-the-loop promise from a JSON body.
func (gw *gateway) decision(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Decision  drain.Decision `json:"decision"`
		PromiseID string         `json:"promiseId"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if body.PromiseID == "" || !body.Decision.Valid() {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":          "missing or invalid fields: decision, promiseId",
			"validDecisions": []string{"skip", "retry", "abort", "force"},
		})
		return
	}
	gw.resolve(w, req.Context(), body.PromiseID, body.Decision)
}

// shortcut returns a handler that resolves the {promiseId} path with a fixed
// decision (POST /skip/:id, /retry/:id, /abort/:id, /force/:id).
func (gw *gateway) shortcut(decision drain.Decision) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		gw.resolve(w, req.Context(), req.PathValue("promiseId"), decision)
	}
}

// resolve settles a latent decision promise with the operator's choice.
//
// This is the heart of the human-in-the-loop hand-off, and the one place the Go
// SDK shows its prerelease edges. There is no high-level
// r.Promises().Resolve(id, value) yet (resonate-sdk-go#28), so resolution goes
// through the low-level Sender().PromiseSettle, and the value must be encoded
// to the wire form by hand (see encodeSettleValue).
func (gw *gateway) resolve(w http.ResponseWriter, ctx context.Context, promiseID string, decision drain.Decision) {
	if promiseID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing promiseId"})
		return
	}

	value, err := encodeSettleValue(string(decision))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	rec, err := gw.r.Sender().PromiseSettle(ctx, resonate.PromiseSettleReq{
		ID:    promiseID,
		State: resonate.SettleStateResolved,
		Value: value,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	log.Printf("[gateway] resolved promise %s with %q (state=%s)", promiseID, decision, rec.State)
	writeJSON(w, http.StatusOK, map[string]any{
		"message":   "decision recorded",
		"decision":  decision,
		"promiseId": promiseID,
	})
}

func (gw *gateway) clearOperation(w http.ResponseWriter, _ *http.Request) {
	gw.mu.Lock()
	gw.operationID = ""
	gw.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"message": "operation id cleared"})
}

// encodeSettleValue builds the wire value for a promise.settle: JSON → base64 →
// quoted JSON string, matching what the SDK codec writes and what the worker's
// Future.Await expects to read back.
//
// Friction note (resonate-sdk-go#28): resonate.NewValue(v) is the wrong call
// here — it stores the raw JSON without the base64 wrap, which surfaces only
// later as a decode error inside the worker. A future r.Promises().Resolve(id,
// v) would fold this encoding and the settle RPC into one call.
func encodeSettleValue(v any) (resonate.Value, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return resonate.Value{}, err
	}
	b64 := base64.StdEncoding.EncodeToString(raw)
	quoted, err := json.Marshal(b64)
	if err != nil {
		return resonate.Value{}, err
	}
	return resonate.Value{Data: json.RawMessage(quoted)}, nil
}

// decodePromiseValue reverses encodeSettleValue: it reads the quoted-base64
// Value.Data that Sender().PromiseGet returns back into a Go value. Same #28
// gap, on the read side.
func decodePromiseValue(v resonate.Value, out any) error {
	if len(v.Data) == 0 {
		return nil
	}
	var b64 string
	if err := json.Unmarshal(v.Data, &b64); err != nil {
		return err
	}
	if b64 == "" {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
