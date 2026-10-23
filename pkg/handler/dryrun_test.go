package handler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// dryRunEnv is an env whose handler uses the real node.Ops in dry-run mode,
// which never persists anything.
func dryRunEnv(t *testing.T) (*env, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset(testNode(nodeA, vmA))
	e := newEnv(t, nil, nil)
	o := e.h.o
	o.DryRun = true
	e.reg = prometheus.NewRegistry()
	e.h = New(Deps{
		IMDS:     e.imds,
		Nodes:    node.NewOps(client, true),
		Machines: e.machines,
		Notifier: e.notifier,
		Metrics:  observability.NewMetrics(e.reg),
		Now:      e.clock.Now,
		Uptime:   func() (time.Duration, error) { return time.Hour, nil },
	}, o)
	return e, client
}

func assertRealNodeUntouched(t *testing.T, client *fake.Clientset) {
	t.Helper()
	n, err := client.CoreV1().Nodes().Get(context.Background(), nodeA, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n.Spec.Unschedulable || len(n.Annotations) != 0 {
		t.Errorf("real node changed in dry-run: unschedulable=%v annotations=%v", n.Spec.Unschedulable, n.Annotations)
	}
}

func TestDryRunActsOncePerEvent(t *testing.T) {
	e, client := dryRunEnv(t)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	for range 5 {
		e.tick(t)
		e.clock.Advance(3 * time.Minute)
	}

	for stage, want := range map[observability.Stage]int{
		observability.StageDetected: 1,
		observability.StageCordoned: 1,
		observability.StageDrained:  1,
		observability.StageApproved: 0,
	} {
		if got := e.notifier.count(stage); got != want {
			t.Errorf("%s notifications = %d, want %d (stages %v)", stage, got, want, e.notifier.stages())
		}
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v, want none", got)
	}
	if got := e.metric(t, "anth_approvals_total", "result", "dry_run"); got != 1 {
		t.Errorf("approvals_total{result=dry_run} = %v, want 1", got)
	}
	assertRealNodeUntouched(t, client)
}

func TestDryRunWithdrawnEventReleasesOnce(t *testing.T) {
	e, client := dryRunEnv(t)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))
	e.tick(t)

	e.imds.setEvents()
	e.clock.Advance(time.Minute)
	e.tick(t) // records event-gone-since
	e.clock.Advance(5 * time.Minute)
	for range 3 {
		e.tick(t)
		e.clock.Advance(time.Minute)
	}

	if got := e.notifier.count(observability.StageUncordoned); got != 1 {
		t.Errorf("Uncordoned notifications = %d, want 1 (stages %v)", got, e.notifier.stages())
	}
	if got := e.notifier.count(observability.StageDetected); got != 1 {
		t.Errorf("Detected notifications = %d, want 1", got)
	}
	if e.machines.calls != 1 {
		t.Errorf("Machine checks = %d, want 1 (one release)", e.machines.calls)
	}
	assertRealNodeUntouched(t, client)
}
