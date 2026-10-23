package handler

import (
	"context"
	"errors"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"

	"github.com/jabicho79/azure-node-termination-handler/pkg/decision"
	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	os.Exit(m.Run())
}

var t0 = time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

const (
	nodeA = "node-a"
	vmA   = "vm-a"
	nodeB = "node-b"
	vmB   = "vm-b"
)

type env struct {
	imds     *fakeIMDS
	nodes    *fakeNodes
	machines *fakeMachines
	notifier *recordingNotifier
	clock    *fakeClock
	reg      *prometheus.Registry
	h        *Handler
}

func testSettings() decision.Settings {
	return decision.Settings{
		Enabled: map[imds.EventType]bool{
			imds.EventReboot:    true,
			imds.EventRedeploy:  true,
			imds.EventPreempt:   true,
			imds.EventTerminate: true,
		},
		AutoApprove:           true,
		SafetyMargin:          60 * time.Second,
		PreemptSafetyMargin:   5 * time.Second,
		MaxDrainTime:          60 * time.Minute,
		PodGracePeriod:        -1,
		PreemptPodGracePeriod: 20,
	}
}

// newEnv builds a handler for nodeA/vmA over the given nodes (nodeA is
// added if absent). mutate may adjust the options.
func newEnv(t *testing.T, nodes []*corev1.Node, mutate func(*Options)) *env {
	t.Helper()
	fn := newFakeNodes(nodes...)
	if _, ok := fn.nodes[nodeA]; !ok {
		fn.nodes[nodeA] = testNode(nodeA, vmA)
	}
	e := &env{
		imds:     &fakeIMDS{},
		nodes:    fn,
		machines: &fakeMachines{},
		notifier: &recordingNotifier{},
		clock:    newFakeClock(t0),
		reg:      prometheus.NewRegistry(),
	}
	o := Options{
		NodeName:           nodeA,
		VMName:             vmA,
		Decision:           testSettings(),
		DeleteEmptyDirData: true,
		IgnoreDaemonSets:   true,
		EventGoneGrace:     5 * time.Minute,
	}
	if mutate != nil {
		mutate(&o)
	}
	e.h = New(Deps{
		IMDS:     e.imds,
		Nodes:    e.nodes,
		Machines: e.machines,
		Notifier: e.notifier,
		Metrics:  observability.NewMetrics(e.reg),
		Now:      e.clock.Now,
		Uptime:   func() (time.Duration, error) { return time.Hour, nil },
	}, o)
	return e
}

func (e *env) tick(t *testing.T) {
	t.Helper()
	if err := e.h.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
}

func (e *env) ann(name, key string) string {
	return e.nodes.node(name).Annotations[key]
}

// metric returns the value of the counter name with the given label pair.
func (e *env) metric(t *testing.T, name, label, value string) float64 {
	t.Helper()
	mfs, err := e.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func scheduled(id string, typ imds.EventType, notBefore time.Time, resources ...string) imds.Event {
	nb := ""
	if !notBefore.IsZero() {
		nb = notBefore.Format(http.TimeFormat)
	}
	return imds.Event{
		EventID:     id,
		EventType:   typ,
		EventStatus: imds.StatusScheduled,
		Resources:   resources,
		NotBefore:   nb,
	}
}

func unix(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

func TestRedeployCordonDrainApprove(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(scheduled("E1", imds.EventRedeploy, t0.Add(7*24*time.Hour), vmA))

	e.tick(t)

	if !e.nodes.node(nodeA).Spec.Unschedulable {
		t.Error("node should be unschedulable")
	}
	drains := e.nodes.callsOf("Drain")
	if len(drains) != 1 {
		t.Fatalf("drain calls = %d, want 1", len(drains))
	}
	if want := t0.Add(60 * time.Minute); !drains[0].Drain.Deadline.Equal(want) {
		t.Errorf("drain deadline = %v, want %v", drains[0].Drain.Deadline, want)
	}
	if !drains[0].Drain.DeleteEmptyDirData || !drains[0].Drain.IgnoreDaemonSets || drains[0].Drain.PodGracePeriodSeconds != -1 {
		t.Errorf("drain opts = %+v", drains[0].Drain)
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
	if got := e.ann(nodeA, node.AnnAction); got != node.ActionUncordonAfterReboot {
		t.Errorf("action = %q", got)
	}
	if got := e.ann(nodeA, node.AnnActionTime); got != unix(t0) {
		t.Errorf("action-time = %q, want %q", got, unix(t0))
	}
	for k, want := range map[string]string{
		node.AnnEventID:         "E1",
		node.AnnEventType:       "Redeploy",
		node.AnnFirstSeen:       unix(t0),
		node.AnnCordoned:        "true",
		node.AnnDrainedEventID:  "E1",
		node.AnnApprovedEventID: "E1",
		node.AnnActionEventID:   "E1",
	} {
		if got := e.ann(nodeA, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	want := []observability.Stage{observability.StageDetected, observability.StageCordoned, observability.StageDrained, observability.StageApproved}
	if got := e.notifier.stages(); !reflect.DeepEqual(got, want) {
		t.Errorf("stages = %v, want %v", got, want)
	}
	if got := e.metric(t, "anth_events_total", "type", "Redeploy"); got != 1 {
		t.Errorf("events_total = %v", got)
	}
	if got := e.metric(t, "anth_approvals_total", "result", "ok"); got != 1 {
		t.Errorf("approvals_total = %v", got)
	}

	// A second tick does nothing new.
	e.tick(t)
	if len(e.nodes.callsOf("Drain")) != 1 || len(e.imds.getApprovals()) != 1 || len(e.notifier.stages()) != 4 {
		t.Errorf("second tick acted again: drains=%d approvals=%v stages=%v",
			len(e.nodes.callsOf("Drain")), e.imds.getApprovals(), e.notifier.stages())
	}
}

func TestIgnoresEventsForOtherVMs(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmB))

	e.tick(t)

	for _, op := range []string{"Cordon", "Drain", "SetAnnotations"} {
		if n := len(e.nodes.callsOf(op)); n != 0 {
			t.Errorf("%s calls = %d, want 0", op, n)
		}
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v", got)
	}
	if got := e.notifier.stages(); len(got) != 0 {
		t.Errorf("notifications = %v", got)
	}
}

func TestFreezeNoAction(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(scheduled("F1", imds.EventFreeze, t0.Add(10*time.Minute), vmA))

	e.tick(t)
	e.tick(t)

	for _, op := range []string{"Cordon", "Drain", "SetAnnotations"} {
		if n := len(e.nodes.callsOf(op)); n != 0 {
			t.Errorf("%s calls = %d, want 0", op, n)
		}
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v", got)
	}
	if got := e.notifier.stages(); !reflect.DeepEqual(got, []observability.Stage{observability.StageFreeze}) {
		t.Errorf("stages = %v, want exactly one Freeze", got)
	}
}

func TestDrainTimeoutApprovesAtDeadline(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.nodes.drain = drainDeadlineExceeded
	nb := t0.Add(10 * time.Minute)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, nb, vmA))

	e.tick(t)
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Fatalf("approved before deadline: %v", got)
	}
	if e.notifier.count(observability.StageDrainTimeout) != 1 {
		t.Errorf("stages = %v, want one DrainTimeout", e.notifier.stages())
	}
	if got := e.ann(nodeA, node.AnnDrainedEventID); got != "" {
		t.Errorf("drained-event-id = %q, want empty", got)
	}

	e.clock.Advance(10*time.Minute - 60*time.Second) // reach nb - safety margin
	e.tick(t)

	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
	if n := len(e.nodes.callsOf("Drain")); n != 1 {
		t.Errorf("drain calls = %d, want 1 (no drain after the deadline)", n)
	}
}

func TestDrainCancelledByParentIsNotTimeout(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.nodes.drain = drainBlock
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.h.Tick(ctx) }()
	waitUntil := time.Now().Add(5 * time.Second)
	for len(e.nodes.callsOf("Drain")) == 0 {
		if time.Now().After(waitUntil) {
			cancel()
			t.Fatal("timed out waiting for Drain to be called")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Tick err = %v, want context.Canceled", err)
	}
	if e.notifier.count(observability.StageDrainTimeout) != 0 {
		t.Errorf("DrainTimeout notified on shutdown: %v", e.notifier.stages())
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v", got)
	}
}

func TestDrainErrorReturned(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.nodes.drain = func(context.Context, string, node.DrainOptions) error { return errDrainFailed }
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	if err := e.h.Tick(context.Background()); !errors.Is(err, errDrainFailed) {
		t.Fatalf("Tick err = %v, want %v", err, errDrainFailed)
	}
	if got := e.metric(t, "anth_errors_total", "op", "drain"); got != 1 {
		t.Errorf("errors_total{op=drain} = %v, want 1", got)
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v", got)
	}
}

func TestWaitsForPeersBeforeApproving(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeB, vmB)}, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(30*time.Minute), vmA, vmB))

	e.tick(t)
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Fatalf("approved before peer drained: %v", got)
	}
	if got := e.ann(nodeA, node.AnnDrainedEventID); got != "E1" {
		t.Fatalf("own drained-event-id = %q", got)
	}

	e.nodes.annotate(nodeB, map[string]string{node.AnnDrainedEventID: "E1"})
	e.clock.Advance(5 * time.Second)
	e.tick(t)

	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
}

func TestPeersIgnoredWhenNotInCluster(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(30*time.Minute), vmA, "vm-x"))

	e.tick(t)

	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
}

func TestAutoApproveOff(t *testing.T) {
	e := newEnv(t, nil, func(o *Options) { o.Decision.AutoApprove = false })
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	e.tick(t)
	if got := e.ann(nodeA, node.AnnDrainedEventID); got != "E1" {
		t.Errorf("drained-event-id = %q, want E1", got)
	}
	e.clock.Advance(time.Hour)
	e.tick(t)

	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v, want none", got)
	}
	if got := e.ann(nodeA, node.AnnApprovedEventID); got != "" {
		t.Errorf("approved-event-id = %q", got)
	}
}

func TestStartedEventNotApproved(t *testing.T) {
	e := newEnv(t, nil, nil)
	ev := scheduled("E1", imds.EventReboot, time.Time{}, vmA)
	ev.EventStatus = imds.StatusStarted
	e.imds.setEvents(ev)

	e.tick(t)

	if !e.nodes.node(nodeA).Spec.Unschedulable {
		t.Error("node should be cordoned")
	}
	if n := len(e.nodes.callsOf("Drain")); n != 0 {
		t.Errorf("drain calls = %d, want 0", n)
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v", got)
	}
}

func TestPreemptUsesShortGrace(t *testing.T) {
	e := newEnv(t, nil, nil)
	nb := t0.Add(30 * time.Second)
	e.imds.setEvents(scheduled("E1", imds.EventPreempt, nb, vmA))

	e.tick(t)

	drains := e.nodes.callsOf("Drain")
	if len(drains) != 1 {
		t.Fatalf("drain calls = %d, want 1", len(drains))
	}
	if got := drains[0].Drain.PodGracePeriodSeconds; got != 20 {
		t.Errorf("PodGracePeriodSeconds = %d, want 20", got)
	}
	if want := nb.Add(-5 * time.Second); !drains[0].Drain.Deadline.Equal(want) {
		t.Errorf("deadline = %v, want %v", drains[0].Drain.Deadline, want)
	}
}

func TestIMDSErrorNoNodeAction(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.err = errors.New("connection refused")

	if err := e.h.Tick(context.Background()); err == nil {
		t.Fatal("Tick returned nil, want error")
	}
	if calls := e.nodes.allCalls(); len(calls) != 0 {
		t.Errorf("node calls = %+v, want none", calls)
	}
	if got := e.metric(t, "anth_errors_total", "op", "imds"); got != 1 {
		t.Errorf("errors_total{op=imds} = %v, want 1", got)
	}
}

func TestRestartResumesDrainWithoutDoubleApprove(t *testing.T) {
	firstSeen := t0.Add(-2 * time.Minute)
	n := testNode(nodeA, vmA,
		node.AnnEventID, "E1",
		node.AnnEventType, "Reboot",
		node.AnnFirstSeen, unix(firstSeen),
		node.AnnCordoned, "true",
	)
	n.Spec.Unschedulable = true
	e := newEnv(t, []*corev1.Node{n}, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	e.tick(t)

	drains := e.nodes.callsOf("Drain")
	if len(drains) != 1 {
		t.Fatalf("drain calls = %d, want 1", len(drains))
	}
	if want := t0.Add(10*time.Minute - 60*time.Second); !drains[0].Drain.Deadline.Equal(want) {
		t.Errorf("deadline = %v, want %v", drains[0].Drain.Deadline, want)
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Fatalf("approvals = %v, want [E1]", got)
	}
	if got := e.ann(nodeA, node.AnnFirstSeen); got != unix(firstSeen) {
		t.Errorf("first-seen rewritten: %q", got)
	}
	if e.notifier.count(observability.StageDetected) != 0 {
		t.Errorf("Detected re-notified after restart: %v", e.notifier.stages())
	}

	// Simulate another restart: a fresh handler over the same node state.
	h2 := New(Deps{
		IMDS: e.imds, Nodes: e.nodes, Machines: e.machines, Notifier: e.notifier,
		Metrics: observability.NewMetrics(prometheus.NewRegistry()), Now: e.clock.Now,
	}, e.h.o)
	e.clock.Advance(time.Minute)
	if err := h2.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := e.imds.getApprovals(); len(got) != 1 {
		t.Errorf("approvals = %v, want exactly one", got)
	}
	if n := len(e.nodes.callsOf("Drain")); n != 1 {
		t.Errorf("drain calls = %d, want 1", n)
	}
}

func TestDryRunNeverApproves(t *testing.T) {
	e := newEnv(t, nil, func(o *Options) { o.DryRun = true })
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	e.tick(t)
	e.clock.Advance(time.Hour)
	e.tick(t)

	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Errorf("approvals = %v, want none", got)
	}
}

func TestCompletedEventIgnored(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeA, vmA, node.AnnCompletedEventID, "E1")}, nil)
	ev := scheduled("E1", imds.EventReboot, time.Time{}, vmA)
	ev.EventStatus = imds.StatusStarted
	e.imds.setEvents(ev)

	e.tick(t)

	for _, op := range []string{"Cordon", "Drain", "SetAnnotations"} {
		if n := len(e.nodes.callsOf(op)); n != 0 {
			t.Errorf("%s calls = %d, want 0", op, n)
		}
	}
}

func TestPicksEarliestEvent(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(
		scheduled("LATE", imds.EventReboot, t0.Add(20*time.Minute), vmA),
		scheduled("EARLY", imds.EventRedeploy, t0.Add(10*time.Minute), vmA),
	)

	e.tick(t)

	if got := e.ann(nodeA, node.AnnEventID); got != "EARLY" {
		t.Errorf("event-id = %q, want EARLY", got)
	}
}

func TestForeignCordonNotClaimed(t *testing.T) {
	n := testNode(nodeA, vmA)
	n.Spec.Unschedulable = true
	e := newEnv(t, []*corev1.Node{n}, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	e.tick(t)

	if n := len(e.nodes.callsOf("Cordon")); n != 0 {
		t.Errorf("cordon calls = %d, want 0", n)
	}
	if got := e.ann(nodeA, node.AnnCordoned); got != "" {
		t.Errorf("cordoned = %q, want empty", got)
	}
}

func TestCordonErrorStillApprovesAtDeadline(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.nodes.errs["Cordon"] = errors.New("admission webhook denied the request")
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))

	if err := e.h.Tick(context.Background()); err == nil {
		t.Fatal("Tick returned nil before the deadline, want the cordon error")
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Fatalf("approved before the deadline: %v", got)
	}
	if n := len(e.nodes.callsOf("Drain")); n != 0 {
		t.Errorf("drain calls = %d, want 0 while cordon fails", n)
	}

	e.clock.Advance(10*time.Minute - 60*time.Second) // reach NotBefore - safety margin
	err := e.h.Tick(context.Background())
	if err == nil {
		t.Error("Tick returned nil, want the cordon error after the approve attempt")
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
	if got := e.ann(nodeA, node.AnnApprovedEventID); got != "E1" {
		t.Errorf("approved-event-id = %q, want E1", got)
	}
	if got := e.metric(t, "anth_errors_total", "op", "cordon"); got != 2 {
		t.Errorf("errors_total{op=cordon} = %v, want 2", got)
	}
}

func TestPeerNotDrainedApprovesAtDeadline(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeB, vmB)}, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA, vmB))

	e.tick(t)
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Fatalf("approved while peer not drained: %v", got)
	}

	e.clock.Advance(10*time.Minute - 60*time.Second)
	e.tick(t)

	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
}

func TestDisabledEventNotAnnotated(t *testing.T) {
	e := newEnv(t, nil, func(o *Options) {
		o.Decision.Enabled = map[imds.EventType]bool{imds.EventReboot: true}
	})
	e.imds.setEvents(scheduled("E1", imds.EventRedeploy, t0.Add(10*time.Minute), vmA))

	e.tick(t)

	for _, op := range []string{"SetAnnotations", "RemoveAnnotations", "Cordon", "Drain"} {
		if n := len(e.nodes.callsOf(op)); n != 0 {
			t.Errorf("%s calls = %d, want 0", op, n)
		}
	}
	if got := e.notifier.stages(); len(got) != 0 {
		t.Errorf("notifications = %v, want none", got)
	}
}

func TestPreemptTakesOverCurrentEvent(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeB, vmB)}, nil)
	redeploy := scheduled("E1", imds.EventRedeploy, t0.Add(7*24*time.Hour), vmA, vmB)
	e.imds.setEvents(redeploy)

	e.tick(t)
	if got := e.ann(nodeA, node.AnnEventID); got != "E1" {
		t.Fatalf("event-id = %q, want E1", got)
	}
	if got := e.ann(nodeA, node.AnnDrainedEventID); got != "E1" {
		t.Fatalf("drained-event-id = %q, want E1", got)
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Fatalf("approved while waiting for peers: %v", got)
	}

	e.clock.Advance(time.Minute)
	nb := e.clock.Now().Add(30 * time.Second)
	e.imds.setEvents(redeploy, scheduled("P1", imds.EventPreempt, nb, vmA))
	e.tick(t)

	for k, want := range map[string]string{
		node.AnnEventID:         "P1",
		node.AnnEventType:       "Preempt",
		node.AnnFirstSeen:       unix(e.clock.Now()),
		node.AnnDrainedEventID:  "P1",
		node.AnnApprovedEventID: "P1",
	} {
		if got := e.ann(nodeA, k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	drains := e.nodes.callsOf("Drain")
	if len(drains) != 2 {
		t.Fatalf("drain calls = %d, want 2", len(drains))
	}
	if d := drains[1].Drain; d.PodGracePeriodSeconds != 20 || !d.Deadline.Equal(nb.Add(-5*time.Second)) {
		t.Errorf("preempt drain opts = %+v, want grace 20 and deadline %v", d, nb.Add(-5*time.Second))
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"P1"}) {
		t.Errorf("approvals = %v, want [P1]", got)
	}
}

func TestSweepApprovesOverdueSecondaryEvent(t *testing.T) {
	e := newEnv(t, []*corev1.Node{testNode(nodeB, vmB)}, nil)
	e1 := scheduled("E1", imds.EventReboot, t0.Add(30*time.Minute), vmA, vmB)
	e.imds.setEvents(e1)
	e.tick(t) // E1 becomes current and waits for node-b

	nb2 := t0.Add(5 * time.Minute)
	e.imds.setEvents(e1, scheduled("E2", imds.EventReboot, nb2, vmA))
	e.tick(t)
	if got := e.ann(nodeA, node.AnnEventID); got != "E1" {
		t.Fatalf("event-id = %q, want E1 to stay current", got)
	}
	if got := e.imds.getApprovals(); len(got) != 0 {
		t.Fatalf("approvals = %v, want none before E2's deadline", got)
	}

	e.clock.Advance(4 * time.Minute) // NotBefore - 60s
	e.tick(t)
	e.clock.Advance(5 * time.Second)
	e.tick(t)
	e.tick(t)

	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E2"}) {
		t.Errorf("approvals = %v, want exactly [E2]", got)
	}
	if got := e.ann(nodeA, node.AnnEventID); got != "E1" {
		t.Errorf("event-id = %q, want E1", got)
	}
	if got := e.ann(nodeA, node.AnnAction); got != node.ActionUncordonAfterReboot {
		t.Errorf("action = %q", got)
	}
}

func TestResourcesMatchCaseInsensitive(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), "VM-A"))

	e.tick(t)

	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
	if n := len(e.nodes.callsOf("ListNodes")); n != 0 {
		t.Errorf("ListNodes calls = %d, want 0 (own VM is not a peer)", n)
	}
}

func TestDisabledCurrentEventDoesNotHideActionableEvent(t *testing.T) {
	// E1 was current before a restart that disabled Redeploy events.
	n := testNode(nodeA, vmA,
		node.AnnEventID, "E1",
		node.AnnEventType, "Redeploy",
		node.AnnFirstSeen, unix(t0.Add(-time.Minute)),
	)
	e := newEnv(t, []*corev1.Node{n}, func(o *Options) {
		o.Decision.Enabled = map[imds.EventType]bool{imds.EventReboot: true}
	})
	e.imds.setEvents(
		scheduled("E1", imds.EventRedeploy, t0.Add(5*time.Minute), vmA),
		scheduled("E2", imds.EventReboot, t0.Add(10*time.Minute), vmA),
	)

	e.tick(t)

	if got := e.ann(nodeA, node.AnnEventID); got != "E2" {
		t.Errorf("event-id = %q, want E2", got)
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E2"}) {
		t.Errorf("approvals = %v, want [E2]", got)
	}
}

func TestRescheduledEventTakesOverWithoutUncordon(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents(scheduled("A", imds.EventRedeploy, t0.Add(7*24*time.Hour), vmA))
	e.tick(t)
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"A"}) {
		t.Fatalf("approvals = %v, want [A]", got)
	}

	// Azure reschedules: A is replaced by B.
	e.clock.Advance(time.Minute)
	e.imds.setEvents(scheduled("B", imds.EventRedeploy, t0.Add(8*24*time.Hour), vmA))
	e.tick(t)

	if n := len(e.nodes.callsOf("Uncordon")); n != 0 {
		t.Errorf("Uncordon calls = %d, want 0", n)
	}
	if n := len(e.nodes.callsOf("Cordon")); n != 1 {
		t.Errorf("Cordon calls = %d, want 1", n)
	}
	if n := len(e.nodes.callsOf("Drain")); n != 2 {
		t.Errorf("Drain calls = %d, want 2 (B is drained)", n)
	}
	n := e.nodes.node(nodeA)
	if !n.Spec.Unschedulable {
		t.Error("node should stay cordoned")
	}
	for k, want := range map[string]string{
		node.AnnEventID:        "B",
		node.AnnCordoned:       "true",
		node.AnnDrainedEventID: "B",
	} {
		if got := n.Annotations[k]; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if got, ok := n.Annotations[node.AnnEventGoneSince]; ok {
		t.Errorf("event-gone-since = %q, want absent", got)
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Errorf("approvals = %v, want [A B]", got)
	}
}

func TestIdleTicksSkipNodeGet(t *testing.T) {
	e := newEnv(t, nil, nil)
	e.imds.setEvents() // empty document

	for range 4 {
		e.tick(t)
	}
	if n := len(e.nodes.callsOf("Get")); n != 1 {
		t.Errorf("Get calls = %d, want 1 (only the first tick)", n)
	}

	// Events for other VMs do not need the node either.
	e.imds.setEvents(scheduled("X", imds.EventReboot, t0.Add(10*time.Minute), vmB))
	e.tick(t)
	if n := len(e.nodes.callsOf("Get")); n != 1 {
		t.Errorf("Get calls = %d, want 1", n)
	}

	// An event for this VM makes the next tick fetch the node again.
	e.imds.setEvents(scheduled("E1", imds.EventReboot, t0.Add(10*time.Minute), vmA))
	e.tick(t)
	if n := len(e.nodes.callsOf("Get")); n != 2 {
		t.Errorf("Get calls = %d, want 2", n)
	}
	if got := e.imds.getApprovals(); !reflect.DeepEqual(got, []string{"E1"}) {
		t.Errorf("approvals = %v, want [E1]", got)
	}
}

func TestNodeGetContinuesWhileWorkingAnnotationsRemain(t *testing.T) {
	e := newEnv(t, nil, nil)
	approveThenWithdraw(t, e) // leaves E1's annotations; the document is empty

	e.tick(t)
	e.tick(t)

	if n := len(e.nodes.callsOf("Get")); n != 3 {
		t.Errorf("Get calls = %d, want 3", n)
	}
}
