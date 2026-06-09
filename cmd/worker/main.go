// Command worker runs the durable node-drain workflow. It joins the default
// Resonate group, registers drainAllNodes, and blocks until interrupted. The
// server pushes workflow tasks here; all drain state lives on the server, so
// the worker is stateless and any number of replicas can share the load.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/httpnet"

	"github.com/resonatehq-examples/example-node-drain-orchestrator-go/drain"
	"github.com/resonatehq-examples/example-node-drain-orchestrator-go/internal/k8s"
)

func main() {
	url := flag.String("url", defaultURL(), "Resonate server URL")
	flag.Parse()

	cluster, err := k8s.NewClient()
	if err != nil {
		log.Fatalf("kubernetes client: %v", err)
	}

	pid := fmt.Sprintf("drain-worker-%d", os.Getpid())
	r, err := resonate.New(resonate.Config{
		Network: httpnet.NewHTTP(*url, httpnet.HTTPOptions{
			PID:   pid,
			Group: drain.WorkerGroup,
		}),
		// A node drain can take a while (cordon + evict + wait per node). The
		// task lease must outlive a single drain step, so raise it above the
		// 60s default; the worker still suspends (releasing the lease) the
		// moment it parks on a human-decision promise.
		TTL: 10 * time.Minute,
	})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	orchestrator := drain.NewOrchestrator(cluster)
	if _, err := resonate.Register(r, drain.FuncName, orchestrator.DrainAllNodes); err != nil {
		log.Fatalf("register %s: %v", drain.FuncName, err)
	}

	log.Printf("[worker pid=%s group=%s] ready at %s — waiting for drain operations (Ctrl-C to exit)",
		pid, drain.WorkerGroup, *url)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("[worker] shutting down")
}

func defaultURL() string {
	if u := os.Getenv("RESONATE_URL"); u != "" {
		return u
	}
	return "http://localhost:8001"
}
