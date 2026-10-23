package handler

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// approvedRebootNode is node-a as left by an approved Reboot event E1 that
// the agent cordoned, with the uncordon marker set at actionTime.
func approvedRebootNode(actionTime time.Time) *corev1.Node {
	n := testNode(nodeA, vmA,
		node.AnnEventID, "E1",
		node.AnnEventType, "Reboot",
		node.AnnFirstSeen, unix(actionTime.Add(-time.Minute)),
		node.AnnCordoned, "true",
		node.AnnDrainedEventID, "E1",
		node.AnnApprovedEventID, "E1",
		node.AnnAction, node.ActionUncordonAfterReboot,
		node.AnnActionTime, unix(actionTime),
	)
	n.Spec.Unschedulable = true
	return n
}

func (e *env) setUptime(d time.Duration) {
	e.h.d.Uptime = func() (time.Duration, error) { return d, nil }
}

func (e *env) startup(t *testing.T) {
	t.Helper()
	if err := e.h.Startup(context.Background()); err != nil {
		t.Fatalf("Startup: %v", err)
	}
}

func assertNoWorkingAnnotations(t *testing.T, n *corev1.Node) {
	t.Helper()
	for _, k := range node.WorkingAnnotations {
		if v, ok := n.Annotations[k]; ok {
			t.Errorf("annotation %s = %q, want absent", k, v)
		}
	}
}

func TestStartupUncordonsAfterReboot(t *testing.T) {
	e := newEnv(t, []*corev1.Node{approvedRebootNode(t0.Add(-10 * time.Minute))}, nil)
	e.setUptime(2 * time.Minute)
	if e.h.Ready() {
		t.Fatal("Ready() = true before Startup")
	}

	e.startup(t)

	n := e.nodes.node(nodeA)
	if n.Spec.Unschedulable {
		t.Error("node should be schedulable")
	}
	assertNoWorkingAnnotations(t, n)
	if got := n.Annotations[node.AnnCompletedEventID]; got != "E1" {
		t.Errorf("completed-event-id = %q, want E1", got)
	}
	if got := e.notifier.stages(); !reflect.DeepEqual(got, []observability.Stage{observability.StageUncordoned}) {
		t.Errorf("stages = %v, want [Uncordoned]", got)
	}
	if got := e.notifier.got; len(got) > 0 && (got[0].EventID != "E1" || got[0].EventType != "Reboot") {
		t.Errorf("notification = %+v, want event E1 of type Reboot", got[0])
	}
	if !e.h.Ready() {
		t.Error("Ready() = false after Startup succeeded")
	}
}

func TestStartupNoRebootLeavesNode(t *testing.T) {
	before := approvedRebootNode(t0.Add(-10 * time.Minute))
	e := newEnv(t, []*corev1.Node{before}, nil)
	e.setUptime(3 * time.Hour)

	e.startup(t)

	after := e.nodes.node(nodeA)
	if !reflect.DeepEqual(after.Annotations, before.Annotations) || !after.Spec.Unschedulable {
		t.Errorf("node changed: annotations=%v unschedulable=%v", after.Annotations, after.Spec.Unschedulable)
	}
	for _, op := range []string{"Uncordon", "SetAnnotations", "RemoveAnnotations"} {
		if n := len(e.nodes.callsOf(op)); n != 0 {
			t.Errorf("%s calls = %d, want 0", op, n)
		}
	}
	if !e.h.Ready() {
		t.Error("Ready() = false after Startup succeeded")
	}
}

func TestStartedEventAfterRebootNotReprocessed(t *testing.T) {
	e := newEnv(t, []*corev1.Node{approvedRebootNode(t0.Add(-10 * time.Minute))}, nil)
	e.setUptime(2 * time.Minute)
	e.startup(t)
	calls := len(e.nodes.allCalls())

	ev := scheduled("E1", imds.EventReboot, time.Time{}, vmA)
	ev.EventStatus = imds.StatusStarted
	e.imds.setEvents(ev)
	e.tick(t)
	e.tick(t)

	for _, c := range e.nodes.allCalls()[calls:] {
		if c.Op != "Get" {
			t.Errorf("unexpected call after release: %+v", c)
		}
	}
	if e.nodes.node(nodeA).Spec.Unschedulable {
		t.Error("node re-cordoned")
	}
	if len(e.notifier.stages()) != 1 {
		t.Errorf("stages = %v, want only the release's Uncordoned", e.notifier.stages())
	}
}

func TestStartupSkipsUncordonWhenMachineDeleting(t *testing.T) {
	e := newEnv(t, []*corev1.Node{approvedRebootNode(t0.Add(-10 * time.Minute))}, nil)
	e.setUptime(2 * time.Minute)
	e.machines.deleting = true

	e.startup(t)

	n := e.nodes.node(nodeA)
	if !n.Spec.Unschedulable {
		t.Error("node should stay unschedulable")
	}
	if c := len(e.nodes.callsOf("Uncordon")); c != 0 {
		t.Errorf("Uncordon calls = %d, want 0", c)
	}
	assertNoWorkingAnnotations(t, n)
	if got := e.notifier.stages(); !reflect.DeepEqual(got, []observability.Stage{observability.StageUncordonSkipped}) {
		t.Errorf("stages = %v, want [UncordonSkipped]", got)
	}
}

func TestStartupMachineCheckErrorLeavesNode(t *testing.T) {
	before := approvedRebootNode(t0.Add(-10 * time.Minute))
	e := newEnv(t, []*corev1.Node{before}, nil)
	e.setUptime(2 * time.Minute)
	errAPI := errors.New("machines API unavailable")
	e.machines.err = errAPI

	if err := e.h.Startup(context.Background()); !errors.Is(err, errAPI) {
		t.Fatalf("Startup err = %v, want %v", err, errAPI)
	}

	after := e.nodes.node(nodeA)
	if !reflect.DeepEqual(after.Annotations, before.Annotations) || !after.Spec.Unschedulable {
		t.Errorf("node changed: annotations=%v unschedulable=%v", after.Annotations, after.Spec.Unschedulable)
	}
	if got := e.notifier.stages(); len(got) != 0 {
		t.Errorf("stages = %v, want none", got)
	}
	if e.h.Ready() {
		t.Error("Ready() = true after Startup failed")
	}

	// Once the API recovers, a retry releases the node.
	e.machines.err = nil
	e.startup(t)
	if e.nodes.node(nodeA).Spec.Unschedulable {
		t.Error("node should be schedulable after the retry")
	}
	if !e.h.Ready() {
		t.Error("Ready() = false after the retry succeeded")
	}
}

func TestNeverUncordonsForeignCordon(t *testing.T) {
	n := testNode(nodeA, vmA)
	n.Spec.Unschedulable = true
	e := newEnv(t, []*corev1.Node{n}, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	e.tick(t)
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Fatalf("approvals = %v, want [E1]", got)
	}

	// The VM reboots; the agent restarts.
	e.clock.Advance(10 * time.Minute)
	e.setUptime(2 * time.Minute)
	e.startup(t)

	for _, c := range e.nodes.callsOf("SetAnnotations") {
		if _, ok := c.Annotations[node.AnnCordoned]; ok {
			t.Errorf("cordoned annotation set: %+v", c)
		}
	}
	after := e.nodes.node(nodeA)
	if !after.Spec.Unschedulable {
		t.Error("foreign cordon was lifted")
	}
	if c := len(e.nodes.callsOf("Uncordon")); c != 0 {
		t.Errorf("Uncordon calls = %d, want 0", c)
	}
	assertNoWorkingAnnotations(t, after)
	if got := after.Annotations[node.AnnCompletedEventID]; got != "E1" {
		t.Errorf("completed-event-id = %q, want E1", got)
	}
	if e.notifier.count(observability.StageUncordoned) != 0 {
		t.Errorf("Uncordoned notified: %v", e.notifier.stages())
	}
}

// approveThenWithdraw processes and approves Reboot E1, then removes it from
// the document.
func approveThenWithdraw(t *testing.T, e *env) imds.Event {
	t.Helper()
	ev := scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA)
	e.imds.setEvents(ev)
	e.tick(t)
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Fatalf("approvals = %v, want [E1]", got)
	}
	e.imds.setEvents()
	return ev
}

func TestWithdrawnEventUncordonsAfterGrace(t *testing.T) {
	e := newEnv(t, nil, nil)
	approveThenWithdraw(t, e)

	e.clock.Advance(time.Minute)
	goneAt := e.clock.Now()
	e.tick(t)
	if got := e.ann(nodeA, node.AnnEventGoneSince); got != unix(goneAt) {
		t.Errorf("event-gone-since = %q, want %q", got, unix(goneAt))
	}
	if !e.nodes.node(nodeA).Spec.Unschedulable {
		t.Fatal("node uncordoned as soon as the event disappeared")
	}

	e.clock.Advance(5*time.Minute - time.Second)
	e.tick(t)
	if !e.nodes.node(nodeA).Spec.Unschedulable {
		t.Fatal("node uncordoned before the grace period ended")
	}

	e.clock.Advance(time.Second)
	e.tick(t)
	n := e.nodes.node(nodeA)
	if n.Spec.Unschedulable {
		t.Error("node should be schedulable after the grace period")
	}
	assertNoWorkingAnnotations(t, n)
	if got, ok := n.Annotations[node.AnnCompletedEventID]; ok {
		t.Errorf("completed-event-id = %q, want absent", got)
	}
	if e.notifier.count(observability.StageUncordoned) != 1 {
		t.Errorf("stages = %v, want one Uncordoned", e.notifier.stages())
	}
}

func TestEventReappearsClearsGoneSince(t *testing.T) {
	e := newEnv(t, nil, nil)
	ev := approveThenWithdraw(t, e)

	e.clock.Advance(time.Minute)
	e.tick(t)
	if got := e.ann(nodeA, node.AnnEventGoneSince); got == "" {
		t.Fatal("event-gone-since not set")
	}

	e.clock.Advance(time.Minute)
	e.imds.setEvents(ev)
	e.tick(t)
	n := e.nodes.node(nodeA)
	if got, ok := n.Annotations[node.AnnEventGoneSince]; ok {
		t.Errorf("event-gone-since = %q, want absent", got)
	}
	if !n.Spec.Unschedulable {
		t.Error("node should still be cordoned")
	}
	if got := n.Annotations[node.AnnEventID]; got != "E1" {
		t.Errorf("event-id = %q, want E1", got)
	}
	if got := e.imds.getApprovals(); len(got) != 1 {
		t.Errorf("approvals = %v, want exactly one", got)
	}

	// Gone again: the grace period restarts.
	e.clock.Advance(time.Minute)
	e.imds.setEvents()
	e.tick(t)
	if got := e.ann(nodeA, node.AnnEventGoneSince); got != unix(e.clock.Now()) {
		t.Errorf("event-gone-since = %q, want %q", got, unix(e.clock.Now()))
	}
}

// An overdue event listed while the current one is gone takes over and is
// approved at once (it used to be left to the sweep while the gone grace ran).
func TestOverdueEventTakesOverWhileCurrentEventGone(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeB, vmB)}, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(30*time.Minute), vmA, vmB))
	e.tick(t) // E1 becomes current and waits for node-b

	nb2 := t0.Add(5 * time.Minute)
	e.imds.setEvents(scheduled("E2", imds.EventReboot, nb2, vmA))
	e.clock.Advance(4 * time.Minute) // E2's NotBefore - 60s
	e.tick(t)

	if got, ok := e.nodes.node(nodeA).Annotations[node.AnnEventGoneSince]; ok {
		t.Errorf("event-gone-since = %q, want absent", got)
	}
	if got := e.ann(nodeA, node.AnnEventID); got != "E2" {
		t.Errorf("event-id = %q, want E2", got)
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E2"}) {
		t.Errorf("approvals = %v, want [E2]", got)
	}
	if n := len(e.nodes.callsOf("Uncordon")); n != 0 {
		t.Errorf("Uncordon calls = %d, want 0", n)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	e := newEnv(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := e.h.Run(ctx, time.Hour); err != nil {
		t.Errorf("Run = %v, want nil", err)
	}
}

func TestRunRetriesStartupThenTicks(t *testing.T) {
	e := newEnv(t, []*corev1.Node{approvedRebootNode(t0.Add(-10 * time.Minute))}, nil)
	var uptimeCalls atomic.Int32
	e.h.d.Uptime = func() (time.Duration, error) {
		if uptimeCalls.Add(1) == 1 {
			return 0, errors.New("uptime unavailable")
		}
		return 2 * time.Minute, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.h.Run(ctx, time.Millisecond) }()

	// Startup fails once (Get), succeeds (Get), then Ticks call Get.
	waitUntil := time.Now().Add(5 * time.Second)
	for len(e.nodes.callsOf("Get")) < 4 {
		if time.Now().After(waitUntil) {
			t.Fatal("timed out waiting for ticks")
		}
		time.Sleep(time.Millisecond)
	}
	if !e.h.Ready() {
		t.Error("Ready() = false while ticking")
	}
	if e.nodes.node(nodeA).Spec.Unschedulable {
		t.Error("node should be schedulable after Startup")
	}
	if got := uptimeCalls.Load(); got != 2 {
		t.Errorf("uptime calls = %d, want 2", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// A reboot for an event approved by the sweep completes that event, not the
// current one: the current event is released and processed again from
// scratch after the reboot.
func TestSweepRebootDoesNotCompleteCurrentEvent(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeB, vmB)}, nil)
	e1 := scheduled("E1", imds.EventReboot, t0.Add(30*time.Minute), vmA, vmB)
	e2 := scheduled("E2", imds.EventReboot, t0.Add(5*time.Minute), vmA)
	e.imds.setEvents(e1)
	e.tick(t) // E1 becomes current and waits for node-b
	e.imds.setEvents(e1, e2)
	e.clock.Advance(4 * time.Minute) // E2's NotBefore - 60s
	e.tick(t)
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E2"}) {
		t.Fatalf("approvals = %v, want [E2]", got)
	}
	if got := e.ann(nodeA, node.AnnActionEventID); got != "E2" {
		t.Fatalf("action-event-id = %q, want E2", got)
	}

	// The VM reboots for E2; the agent restarts.
	e.clock.Advance(2 * time.Minute)
	e.setUptime(time.Minute)
	e.startup(t)

	n := e.nodes.node(nodeA)
	if got := n.Annotations[node.AnnCompletedEventID]; got != "E2" {
		t.Errorf("completed-event-id = %q, want E2", got)
	}
	assertNoWorkingAnnotations(t, n)
	if n.Spec.Unschedulable {
		t.Error("node should be uncordoned after the reboot")
	}

	// E1 is still pending and is processed again; the Started E2 is not.
	started := e2
	started.EventStatus = imds.StatusStarted
	started.NotBefore = ""
	e.imds.setEvents(e1, started)
	e.tick(t)
	if got := e.ann(nodeA, node.AnnEventID); got != "E1" {
		t.Errorf("event-id = %q, want E1", got)
	}
	if !e.nodes.node(nodeA).Spec.Unschedulable {
		t.Error("node should be cordoned again for E1")
	}
}
