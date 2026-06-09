// Package k8s wraps the subset of the Kubernetes API the drain orchestrator
// needs: listing worker nodes, cordoning, eviction (PDB-aware), force-delete,
// and the poll loops that wait for a pod or node to drain. It is the Go
// equivalent of the TypeScript example's src/k8s.ts, built on the canonical
// k8s.io/client-go instead of @kubernetes/client-node.
package k8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NodeInfo is a trimmed view of a Kubernetes node.
type NodeInfo struct {
	Name           string            `json:"name"`
	IsControlPlane bool              `json:"isControlPlane"`
	IsSchedulable  bool              `json:"isSchedulable"`
	Labels         map[string]string `json:"labels"`
}

// PodInfo is a trimmed view of a pod, including the two facts the drain logic
// keys off: whether it is owned by a DaemonSet and what its grace period is.
type PodInfo struct {
	Name                    string `json:"name"`
	Namespace               string `json:"namespace"`
	NodeName                string `json:"nodeName"`
	Phase                   string `json:"phase"`
	IsControlledByDaemonSet bool   `json:"isControlledByDaemonSet"`
	TerminationGracePeriod  int64  `json:"terminationGracePeriod"`
}

// EvictResult reports the outcome of an eviction attempt. Blocked is true when
// the API server rejected the eviction with 429 Too Many Requests — the signal
// that a PodDisruptionBudget would be violated.
type EvictResult struct {
	Success bool
	Blocked bool
	Error   string
}

// Client is a thin wrapper over a client-go clientset.
type Client struct {
	clientset kubernetes.Interface
}

// NewClient builds a Client from the in-cluster config when running inside a
// pod, otherwise from the default kubeconfig (honoring $KUBECONFIG, then
// ~/.kube/config) — mirroring the TS example's kc.loadFromDefault().
func NewClient() (*Client, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("load kube config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	return &Client{clientset: clientset}, nil
}

func loadConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func isControlPlane(labels map[string]string) bool {
	if _, ok := labels["node-role.kubernetes.io/control-plane"]; ok {
		return true
	}
	_, ok := labels["node-role.kubernetes.io/master"]
	return ok
}

func nodeInfo(n corev1.Node) NodeInfo {
	return NodeInfo{
		Name:           n.Name,
		IsControlPlane: isControlPlane(n.Labels),
		IsSchedulable:  !n.Spec.Unschedulable,
		Labels:         n.Labels,
	}
}

// WorkerNodes returns every node that is not a control-plane node.
func (c *Client) WorkerNodes(ctx context.Context) ([]NodeInfo, error) {
	list, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	out := make([]NodeInfo, 0, len(list.Items))
	for _, n := range list.Items {
		if info := nodeInfo(n); !info.IsControlPlane {
			out = append(out, info)
		}
	}
	return out, nil
}

// NodesBySelector returns nodes matching a label selector (e.g.
// {"drain-target": "true"}), excluding control-plane nodes.
func (c *Client) NodesBySelector(ctx context.Context, selector map[string]string) ([]NodeInfo, error) {
	parts := make([]string, 0, len(selector))
	for k, v := range selector {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	list, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: strings.Join(parts, ","),
	})
	if err != nil {
		return nil, fmt.Errorf("list nodes by selector: %w", err)
	}
	out := make([]NodeInfo, 0, len(list.Items))
	for _, n := range list.Items {
		if info := nodeInfo(n); !info.IsControlPlane {
			out = append(out, info)
		}
	}
	return out, nil
}

// Cordon marks a node unschedulable via a strategic-merge patch — the same call
// `kubectl cordon` makes.
func (c *Client) Cordon(ctx context.Context, nodeName string) error {
	patch := []byte(`{"spec":{"unschedulable":true}}`)
	_, err := c.clientset.CoreV1().Nodes().Patch(
		ctx, nodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("cordon %s: %w", nodeName, err)
	}
	return nil
}

// Uncordon marks a node schedulable again. Not used by the happy path but kept
// for symmetry with the TS example and for operators recovering a cluster.
func (c *Client) Uncordon(ctx context.Context, nodeName string) error {
	patch := []byte(`{"spec":{"unschedulable":false}}`)
	_, err := c.clientset.CoreV1().Nodes().Patch(
		ctx, nodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("uncordon %s: %w", nodeName, err)
	}
	return nil
}

// PodsOnNode lists the evictable pods on a node, skipping mirror (static) pods
// always and DaemonSet-owned pods when ignoreDaemonSets is set.
func (c *Client) PodsOnNode(ctx context.Context, nodeName string, ignoreDaemonSets bool) ([]PodInfo, error) {
	list, err := c.clientset.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods on %s: %w", nodeName, err)
	}

	pods := make([]PodInfo, 0, len(list.Items))
	for _, p := range list.Items {
		// Skip mirror pods (static pods managed directly by the kubelet).
		if _, ok := p.Annotations["kubernetes.io/config.mirror"]; ok {
			continue
		}

		controlledByDaemonSet := false
		for _, ref := range p.OwnerReferences {
			if ref.Kind == "DaemonSet" {
				controlledByDaemonSet = true
				break
			}
		}
		if ignoreDaemonSets && controlledByDaemonSet {
			continue
		}

		grace := int64(30)
		if p.Spec.TerminationGracePeriodSeconds != nil {
			grace = *p.Spec.TerminationGracePeriodSeconds
		}

		pods = append(pods, PodInfo{
			Name:                    p.Name,
			Namespace:               p.Namespace,
			NodeName:                p.Spec.NodeName,
			Phase:                   string(p.Status.Phase),
			IsControlledByDaemonSet: controlledByDaemonSet,
			TerminationGracePeriod:  grace,
		})
	}
	return pods, nil
}

// Evict requests eviction through the Eviction API, which respects Pod
// Disruption Budgets. A 429 (IsTooManyRequests) means a PDB blocks the
// eviction — surfaced as Blocked so the workflow can pause for a human. A 404
// means the pod is already gone, treated as success.
func (c *Client) Evict(ctx context.Context, podName, namespace string, gracePeriodSeconds int64) EvictResult {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		DeleteOptions: &metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriodSeconds,
		},
	}

	err := c.clientset.PolicyV1().Evictions(namespace).Evict(ctx, eviction)
	switch {
	case err == nil:
		return EvictResult{Success: true}
	case apierrors.IsTooManyRequests(err):
		return EvictResult{Blocked: true, Error: "blocked by PodDisruptionBudget"}
	case apierrors.IsNotFound(err):
		return EvictResult{Success: true}
	default:
		return EvictResult{Error: err.Error()}
	}
}

// Delete force-deletes a pod, bypassing PDBs. Used when an operator chooses
// "force" for a blocked node.
func (c *Client) Delete(ctx context.Context, podName, namespace string, gracePeriodSeconds int64) error {
	err := c.clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodSeconds,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s: %w", namespace, podName, err)
	}
	return nil
}

// WaitForPodDeletion polls until the pod is gone (404) or the timeout elapses.
// Returns true if the pod was deleted within the timeout.
//
// Like the other calls here it uses the caller's context only for the API
// requests; the poll deadline is the timeout, so on worker shutdown an
// in-flight wait runs to its timeout before the process exits.
func (c *Client) WaitForPodDeletion(ctx context.Context, podName, namespace string, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := c.clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("get %s/%s: %w", namespace, podName, err)
		}
		time.Sleep(time.Second)
	}
	return false, nil
}

// WaitForNodeEmpty polls until the node has no active (non-Succeeded,
// non-Failed) evictable pods or the timeout elapses.
func (c *Client) WaitForNodeEmpty(ctx context.Context, nodeName string, ignoreDaemonSets bool, timeout time.Duration) (bool, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods, err := c.PodsOnNode(ctx, nodeName, ignoreDaemonSets)
		if err != nil {
			return false, err
		}
		active := 0
		for _, p := range pods {
			if p.Phase != "Succeeded" && p.Phase != "Failed" {
				active++
			}
		}
		if active == 0 {
			return true, nil
		}
		time.Sleep(2 * time.Second)
	}
	return false, nil
}
