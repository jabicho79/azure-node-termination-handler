//go:build e2e

// Package e2e runs the agent on a kind cluster against the fake IMDS.
// Run it with scripts/e2e.sh (make e2e); it needs KUBECONFIG pointing at the
// anth-e2e cluster. The tests run in order and some build on earlier state.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
)

const (
	fakeURL        = "http://localhost:30080"
	clusterName    = "anth-e2e"
	agentNamespace = "anth-e2e"
	agentSelector  = "app.kubernetes.io/name=azure-node-termination-handler"
	workloadNS     = "default"
	worker         = "anth-e2e-worker"
	worker2        = "anth-e2e-worker2"
	nginxImage     = "nginx:1.27-alpine"
	normalUptime   = "999999.0"
)

var (
	workers = []string{worker, worker2}
	kube    kubernetes.Interface
	dyn     dynamic.Interface
)

var (
	crdGVR     = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	machineGVR = schema.GroupVersionResource{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machines"}
)

func TestMain(m *testing.M) {
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		fmt.Fprintln(os.Stderr, "KUBECONFIG must point at the anth-e2e kind cluster (run scripts/e2e.sh)")
		os.Exit(1)
	}
	raw, err := clientcmd.LoadFromFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading %s: %v\n", path, err)
		os.Exit(1)
	}
	if raw.CurrentContext != "kind-"+clusterName {
		fmt.Fprintf(os.Stderr, "refusing to run: KUBECONFIG context is %q, want %q\n", raw.CurrentContext, "kind-"+clusterName)
		os.Exit(1)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "building client config: %v\n", err)
		os.Exit(1)
	}
	if kube, err = kubernetes.NewForConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "building clientset: %v\n", err)
		os.Exit(1)
	}
	if dyn, err = dynamic.NewForConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "building dynamic client: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// ---------------------------------------------------------------- scenarios

func TestRedeployDrainsAndApproves(t *testing.T) {
	begin(t)
	ctx := context.Background()

	createDeployment(t, "nginx-e2e", &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
			Weight: 100,
			Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key: "kubernetes.io/hostname", Operator: corev1.NodeSelectorOpIn, Values: []string{worker},
			}}},
		}},
	}}, nil)
	waitForReadyPodOn(t, "nginx-e2e", worker)

	id := eventID("redeploy")
	setDocument(t, scheduled(id, imds.EventRedeploy, worker, time.Now().Add(15*time.Minute)))

	if !waitFor(func() bool { return unschedulable(t, worker) }, 30*time.Second) {
		t.Fatalf("%s was not cordoned within 30s", worker)
	}
	ok := waitFor(func() bool {
		pods, err := kube.CoreV1().Pods(workloadNS).List(ctx, metav1.ListOptions{
			LabelSelector: "app=nginx-e2e", FieldSelector: "spec.nodeName=" + worker,
		})
		return err == nil && len(pods.Items) == 0 && slices.Contains(approvals(t), id)
	}, 60*time.Second)
	if !ok {
		t.Fatalf("within 60s: want no nginx pod on %s and approval of %s; approvals=%v", worker, id, approvals(t))
	}
}

// TestRebootUncordons continues from TestRedeployDrainsAndApproves: worker is
// cordoned and its Redeploy event approved. It simulates the reboot.
func TestRebootUncordons(t *testing.T) {
	dumpLogsOnFailure(t, worker)
	n := getNode(t, worker)
	id := n.Annotations[node.AnnApprovedEventID]
	if id == "" || n.Annotations[node.AnnAction] != node.ActionUncordonAfterReboot || !n.Spec.Unschedulable {
		t.Fatalf("needs the state left by TestRedeployDrainsAndApproves; %s annotations=%v unschedulable=%v",
			worker, n.Annotations, n.Spec.Unschedulable)
	}

	waitPastActionTime(t, worker, 6*time.Second)
	setUptime(t, worker, "5.0")
	t.Cleanup(func() { setUptime(t, worker, normalUptime) })
	restartAgentOn(t, worker)

	ok := waitFor(func() bool {
		n := getNode(t, worker)
		return !n.Spec.Unschedulable && n.Annotations[node.AnnCompletedEventID] == id
	}, 60*time.Second)
	if !ok {
		n := getNode(t, worker)
		t.Fatalf("within 60s: want %s schedulable with completed-event-id=%s; unschedulable=%v annotations=%v",
			worker, id, n.Spec.Unschedulable, n.Annotations)
	}
}

func TestWithdrawnEventUncordons(t *testing.T) {
	begin(t)
	dumpLogsOnFailure(t, worker2)

	id := eventID("reboot")
	setDocument(t, scheduled(id, imds.EventReboot, worker2, time.Now().Add(15*time.Minute)))
	waitForApproval(t, id, 60*time.Second)
	if !unschedulable(t, worker2) {
		t.Fatalf("%s should be cordoned after the approval", worker2)
	}

	setDocument(t)
	if !waitFor(func() bool { return !unschedulable(t, worker2) }, 30*time.Second) {
		t.Fatalf("%s was not uncordoned within 30s of the event being withdrawn", worker2)
	}
}

func TestFreezeDoesNothing(t *testing.T) {
	begin(t)

	id := eventID("freeze")
	setDocument(t, scheduled(id, imds.EventFreeze, worker, time.Now().Add(15*time.Minute)))

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if unschedulable(t, worker) {
			t.Fatalf("%s was cordoned for a Freeze event", worker)
		}
		time.Sleep(time.Second)
	}
	if a := approvals(t); len(a) != 0 {
		t.Fatalf("Freeze must not be approved; approvals=%v", a)
	}
	if !waitFor(func() bool { return hasNodeEvent(t, worker, "Freeze", id) }, 15*time.Second) {
		t.Fatalf("no Node Event with reason Freeze for %s on %s", id, worker)
	}
}

func TestPDBBlockedApprovesAtDeadline(t *testing.T) {
	begin(t)
	ctx := context.Background()

	createDeployment(t, "pdb-e2e", nil, map[string]string{"kubernetes.io/hostname": worker2})
	minAvailable := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "pdb-e2e", Namespace: workloadNS},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pdb-e2e"}},
		},
	}
	if _, err := kube.PolicyV1().PodDisruptionBudgets(workloadNS).Create(ctx, pdb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating PDB: %v", err)
	}
	t.Cleanup(func() {
		_ = kube.PolicyV1().PodDisruptionBudgets(workloadNS).Delete(context.Background(), "pdb-e2e", metav1.DeleteOptions{})
	})
	waitForReadyPodOn(t, "pdb-e2e", worker2)
	ok := waitFor(func() bool {
		p, err := kube.PolicyV1().PodDisruptionBudgets(workloadNS).Get(ctx, "pdb-e2e", metav1.GetOptions{})
		return err == nil && p.Status.ObservedGeneration == p.Generation && p.Status.CurrentHealthy == 1 && p.Status.DisruptionsAllowed == 0
	}, 60*time.Second)
	if !ok {
		t.Fatal("PDB did not report 1 healthy pod and 0 allowed disruptions")
	}

	id := eventID("redeploy-pdb")
	start := time.Now()
	setDocument(t, scheduled(id, imds.EventRedeploy, worker2, time.Now().Add(15*time.Minute)))

	for time.Since(start) < 15*time.Second {
		if slices.Contains(approvals(t), id) {
			t.Fatalf("%s approved after %s although the PDB blocks the drain", id, time.Since(start).Round(time.Second))
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !waitFor(func() bool { return slices.Contains(approvals(t), id) }, 60*time.Second-time.Since(start)) {
		t.Fatalf("%s not approved within 60s", id)
	}
	if !waitFor(func() bool { return hasNodeEvent(t, worker2, "DrainTimeout", id) }, 15*time.Second) {
		t.Fatalf("no Node Event with reason DrainTimeout for %s on %s", id, worker2)
	}
}

func TestPreemptApprovesQuickly(t *testing.T) {
	begin(t)

	id := eventID("preempt")
	setDocument(t, scheduled(id, imds.EventPreempt, worker, time.Now().Add(30*time.Second)))
	waitForApproval(t, id, 30*time.Second)
}

func TestMachineDeletingKeepsCordon(t *testing.T) {
	begin(t)
	dumpLogsOnFailure(t, worker2)
	ctx := context.Background()

	applyMachineCRD(t)
	m := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cluster.x-k8s.io/v1beta2",
		"kind":       "Machine",
		"metadata": map[string]any{
			"name":       "m1",
			"namespace":  "default",
			"finalizers": []any{"e2e.azure-node-termination-handler/hold"},
		},
	}}
	if _, err := dyn.Resource(machineGVR).Namespace("default").Create(ctx, m, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating Machine m1: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		patchNode(t, worker2, `{"metadata":{"annotations":{"cluster.x-k8s.io/machine":null,"cluster.x-k8s.io/cluster-namespace":null}}}`)
		if _, err := dyn.Resource(machineGVR).Namespace("default").Patch(ctx, "m1", types.MergePatchType,
			[]byte(`{"metadata":{"finalizers":null}}`), metav1.PatchOptions{}); err != nil && !apierrors.IsNotFound(err) {
			t.Errorf("removing the Machine finalizer: %v", err)
		}
		_ = dyn.Resource(machineGVR).Namespace("default").Delete(ctx, "m1", metav1.DeleteOptions{})
		patchNode(t, worker2, `{"spec":{"unschedulable":false}}`)
		setUptime(t, worker2, normalUptime)
	})
	patchNode(t, worker2, `{"metadata":{"annotations":{"cluster.x-k8s.io/machine":"m1","cluster.x-k8s.io/cluster-namespace":"default"}}}`)

	id := eventID("reboot-machine")
	setDocument(t, scheduled(id, imds.EventReboot, worker2, time.Now().Add(15*time.Minute)))
	waitForApproval(t, id, 60*time.Second)

	if err := dyn.Resource(machineGVR).Namespace("default").Delete(ctx, "m1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting Machine m1: %v", err)
	}
	u, err := dyn.Resource(machineGVR).Namespace("default").Get(ctx, "m1", metav1.GetOptions{})
	if err != nil || u.GetDeletionTimestamp() == nil {
		t.Fatalf("Machine m1 should be terminating: err=%v", err)
	}

	waitPastActionTime(t, worker2, 6*time.Second)
	setUptime(t, worker2, "5.0")
	restartAgentOn(t, worker2)

	// The restarted agent must take the reboot path and skip the uncordon.
	if !waitFor(func() bool { return hasNodeEvent(t, worker2, "UncordonSkipped", id) }, 30*time.Second) {
		t.Fatalf("no UncordonSkipped Node Event for %s on %s", id, worker2)
	}
	time.Sleep(30 * time.Second)
	if !unschedulable(t, worker2) {
		t.Fatalf("%s was uncordoned although its Machine is being deleted", worker2)
	}
}

// ------------------------------------------------------------------ helpers

// begin resets the fake and waits until every worker is schedulable.
func begin(t *testing.T) {
	t.Helper()
	resetFake(t)
	ok := waitFor(func() bool {
		for _, w := range workers {
			if unschedulable(t, w) {
				return false
			}
		}
		return true
	}, 90*time.Second)
	if !ok {
		t.Fatal("workers did not become schedulable before the test")
	}
	for _, w := range workers {
		dumpLogsOnFailure(t, w)
	}
}

// waitFor polls cond every 500ms until it is true or timeout elapses.
func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForApproval(t *testing.T, id string, timeout time.Duration) {
	t.Helper()
	if !waitFor(func() bool { return slices.Contains(approvals(t), id) }, timeout) {
		t.Fatalf("%s not approved within %s; approvals=%v", id, timeout, approvals(t))
	}
}

func eventID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func scheduled(id string, typ imds.EventType, vm string, notBefore time.Time) imds.Event {
	return imds.Event{
		EventID:      id,
		EventType:    typ,
		ResourceType: "VirtualMachine",
		Resources:    []string{vm},
		EventStatus:  imds.StatusScheduled,
		NotBefore:    notBefore.UTC().Format(http.TimeFormat),
		Description:  "e2e " + string(typ),
		EventSource:  "Platform",
	}
}

// setDocument replaces the fake's Scheduled Events.
func setDocument(t *testing.T, events ...imds.Event) {
	t.Helper()
	if events == nil {
		events = []imds.Event{}
	}
	body, err := json.Marshal(imds.Document{Events: events})
	if err != nil {
		t.Fatal(err)
	}
	fakeRequest(t, http.MethodPut, "/control/document", body)
}

// approvals returns the event IDs approved so far.
func approvals(t *testing.T) []string {
	t.Helper()
	var ids []string
	if err := json.Unmarshal(fakeRequest(t, http.MethodGet, "/control/approvals", nil), &ids); err != nil {
		t.Fatalf("decoding approvals: %v", err)
	}
	return ids
}

// resetFake empties the document and forgets every approval.
func resetFake(t *testing.T) {
	t.Helper()
	setDocument(t)
	fakeRequest(t, http.MethodDelete, "/control/approvals", nil)
}

func fakeRequest(t *testing.T, method, path string, body []byte) []byte {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	var resp *http.Response
	var err error
	for attempt := 0; ; attempt++ {
		var req *http.Request
		req, err = http.NewRequest(method, fakeURL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("building request: %v", err)
		}
		resp, err = c.Do(req)
		if err == nil || attempt == 4 {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: %s: %s", method, path, resp.Status, buf.String())
	}
	return buf.Bytes()
}

// setUptime writes a fake /proc/uptime value read by the agent on node.
func setUptime(t *testing.T, nodeName, seconds string) {
	t.Helper()
	if !strings.HasPrefix(nodeName, clusterName+"-") {
		t.Fatalf("refusing to docker exec into %q", nodeName)
	}
	if _, err := strconv.ParseFloat(seconds, 64); err != nil {
		t.Fatalf("invalid uptime %q", seconds)
	}
	out, err := exec.Command("docker", "exec", nodeName, "sh", "-c",
		fmt.Sprintf(`echo "%s 0" > /var/lib/anth-e2e/uptime`, seconds)).CombinedOutput()
	if err != nil {
		t.Fatalf("setting uptime on %s: %v: %s", nodeName, err, out)
	}
}

// restartAgentOn deletes the agent pod on nodeName and waits until its
// replacement is ready (its Startup has succeeded).
func restartAgentOn(t *testing.T, nodeName string) {
	t.Helper()
	ctx := context.Background()
	old := agentPod(t, nodeName)
	if err := kube.CoreV1().Pods(agentNamespace).Delete(ctx, old.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting agent pod %s: %v", old.Name, err)
	}
	ok := waitFor(func() bool {
		pods, err := kube.CoreV1().Pods(agentNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: agentSelector, FieldSelector: "spec.nodeName=" + nodeName,
		})
		if err != nil {
			return false
		}
		for _, p := range pods.Items {
			if p.UID != old.UID && p.DeletionTimestamp == nil && podReady(&p) {
				return true
			}
		}
		return false
	}, 90*time.Second)
	if !ok {
		t.Fatalf("agent on %s did not become ready after the restart", nodeName)
	}
}

func agentPod(t *testing.T, nodeName string) *corev1.Pod {
	t.Helper()
	pods, err := kube.CoreV1().Pods(agentNamespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: agentSelector, FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		t.Fatalf("listing agent pods on %s: %v", nodeName, err)
	}
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp == nil {
			return &pods.Items[i]
		}
	}
	t.Fatalf("no agent pod on %s", nodeName)
	return nil
}

// dumpLogsOnFailure logs the agent's output on nodeName if t fails.
func dumpLogsOnFailure(t *testing.T, nodeName string) {
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		pods, err := kube.CoreV1().Pods(agentNamespace).List(context.Background(), metav1.ListOptions{
			LabelSelector: agentSelector, FieldSelector: "spec.nodeName=" + nodeName,
		})
		if err != nil {
			t.Logf("listing agent pods on %s: %v", nodeName, err)
			return
		}
		for _, p := range pods.Items {
			b, err := kube.CoreV1().Pods(agentNamespace).GetLogs(p.Name, &corev1.PodLogOptions{}).DoRaw(context.Background())
			t.Logf("---- agent logs %s on %s (err=%v) ----\n%s", p.Name, nodeName, err, b)
		}
	})
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func getNode(t *testing.T, name string) *corev1.Node {
	t.Helper()
	n, err := kube.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting node %s: %v", name, err)
	}
	return n
}

func unschedulable(t *testing.T, name string) bool {
	t.Helper()
	return getNode(t, name).Spec.Unschedulable
}

func patchNode(t *testing.T, name, patch string) {
	t.Helper()
	if _, err := kube.CoreV1().Nodes().Patch(context.Background(), name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		t.Errorf("patching node %s: %v", name, err)
	}
}

// waitPastActionTime waits until at least d has passed since the agent
// recorded the uncordon-after-reboot action, so a fake uptime of a few
// seconds reads as a reboot after the approval.
func waitPastActionTime(t *testing.T, nodeName string, d time.Duration) {
	t.Helper()
	raw := getNode(t, nodeName).Annotations[node.AnnActionTime]
	s, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("%s has no valid %s annotation: %q", nodeName, node.AnnActionTime, raw)
	}
	if wait := time.Until(time.Unix(s, 0).Add(d)); wait > 0 {
		time.Sleep(wait)
	}
}

// hasNodeEvent reports whether a Kubernetes Event with reason exists on the
// node and mentions eventID.
func hasNodeEvent(t *testing.T, nodeName, reason, eventID string) bool {
	t.Helper()
	evs, err := kube.CoreV1().Events(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{
		FieldSelector: "involvedObject.kind=Node,involvedObject.name=" + nodeName + ",reason=" + reason,
	})
	if err != nil {
		t.Logf("listing events: %v", err)
		return false
	}
	for _, e := range evs.Items {
		if strings.Contains(e.Message, eventID) {
			return true
		}
	}
	return false
}

// createDeployment creates a 1-replica nginx Deployment labelled app=name and
// deletes it when the test ends.
func createDeployment(t *testing.T, name string, affinity *corev1.Affinity, nodeSelector map[string]string) {
	t.Helper()
	replicas := int32(1)
	grace := int64(5)
	labels := map[string]string{"app": name}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: workloadNS},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Affinity:                      affinity,
					NodeSelector:                  nodeSelector,
					TerminationGracePeriodSeconds: &grace,
					Containers: []corev1.Container{{
						Name:            "nginx",
						Image:           nginxImage,
						ImagePullPolicy: corev1.PullIfNotPresent,
					}},
				},
			},
		},
	}
	if _, err := kube.AppsV1().Deployments(workloadNS).Create(context.Background(), d, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating deployment %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = kube.AppsV1().Deployments(workloadNS).Delete(ctx, name, metav1.DeleteOptions{})
		waitFor(func() bool {
			pods, err := kube.CoreV1().Pods(workloadNS).List(ctx, metav1.ListOptions{LabelSelector: "app=" + name})
			return err == nil && len(pods.Items) == 0
		}, 60*time.Second)
	})
}

func waitForReadyPodOn(t *testing.T, app, nodeName string) {
	t.Helper()
	ok := waitFor(func() bool {
		pods, err := kube.CoreV1().Pods(workloadNS).List(context.Background(), metav1.ListOptions{
			LabelSelector: "app=" + app, FieldSelector: "spec.nodeName=" + nodeName,
		})
		if err != nil {
			return false
		}
		for i := range pods.Items {
			if podReady(&pods.Items[i]) {
				return true
			}
		}
		return false
	}, 180*time.Second)
	if !ok {
		t.Fatalf("no ready %s pod on %s", app, nodeName)
	}
}

// applyMachineCRD installs testdata/machine-crd.yaml and waits until it is
// established.
func applyMachineCRD(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	b, err := os.ReadFile("testdata/machine-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := yaml.Unmarshal(b, &obj); err != nil {
		t.Fatalf("parsing CRD: %v", err)
	}
	crd := &unstructured.Unstructured{Object: obj}
	if _, err := dyn.Resource(crdGVR).Create(ctx, crd, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating Machine CRD: %v", err)
	}
	ok := waitFor(func() bool {
		u, err := dyn.Resource(crdGVR).Get(ctx, crd.GetName(), metav1.GetOptions{})
		if err != nil {
			return false
		}
		conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
		for _, c := range conds {
			m, _ := c.(map[string]any)
			if m["type"] == "Established" && m["status"] == "True" {
				return true
			}
		}
		return false
	}, 60*time.Second)
	if !ok {
		t.Fatal("Machine CRD not established")
	}
}
