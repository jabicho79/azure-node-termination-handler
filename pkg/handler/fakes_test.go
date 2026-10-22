package handler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// fakeClock is a controllable clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{t: t} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeIMDS serves scripted documents: each call consumes the next document,
// and the last one repeats. If err is set it is returned instead.
type fakeIMDS struct {
	mu         sync.Mutex
	docs       []imds.Document
	err        error
	approveErr error
	approvals  []string
}

func (f *fakeIMDS) ScheduledEvents(context.Context) (imds.Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return imds.Document{}, f.err
	}
	if len(f.docs) == 0 {
		return imds.Document{}, nil
	}
	d := f.docs[0]
	if len(f.docs) > 1 {
		f.docs = f.docs[1:]
	}
	return d, nil
}

func (f *fakeIMDS) Approve(_ context.Context, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.approveErr != nil {
		return f.approveErr
	}
	f.approvals = append(f.approvals, eventID)
	return nil
}

// setEvents replaces the scripted documents with a single document.
func (f *fakeIMDS) setEvents(events ...imds.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.docs = []imds.Document{{DocumentIncarnation: 1, Events: events}}
}

func (f *fakeIMDS) getApprovals() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.approvals...)
}

// call is one recorded fakeNodes method call.
type call struct {
	Op   string // "Get", "ListNodes", "Cordon", "Uncordon", "Drain", "SetAnnotations", "RemoveAnnotations"
	Node string
	// Drain holds the options of a Drain call.
	Drain node.DrainOptions
	// Annotations holds the kv of SetAnnotations; Keys the keys of RemoveAnnotations.
	Annotations map[string]string
	Keys        []string
}

// fakeNodes is an in-memory Nodes. Nodes are stored by name; every method
// call is recorded in order.
type fakeNodes struct {
	mu    sync.Mutex
	nodes map[string]*corev1.Node
	calls []call
	// drain, if set, decides the Drain result (e.g. return nil,
	// context.DeadlineExceeded, or block on ctx). nil means success.
	drain func(ctx context.Context, name string, opts node.DrainOptions) error
	// errs injects an error per Op name.
	errs map[string]error
}

func newFakeNodes(nodes ...*corev1.Node) *fakeNodes {
	f := &fakeNodes{nodes: map[string]*corev1.Node{}, errs: map[string]error{}}
	for _, n := range nodes {
		f.nodes[n.Name] = n.DeepCopy()
	}
	return f
}

// testNode builds a node whose providerID maps to vmName. kv are
// alternating annotation keys and values.
func testNode(name, vmName string, kv ...string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{}},
		Spec: corev1.NodeSpec{
			ProviderID: "azure:///subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/" + vmName,
		},
	}
	for i := 0; i+1 < len(kv); i += 2 {
		n.Annotations[kv[i]] = kv[i+1]
	}
	return n
}

func (f *fakeNodes) record(c call) error {
	f.calls = append(f.calls, c)
	return f.errs[c.Op]
}

func (f *fakeNodes) lookup(name string) (*corev1.Node, error) {
	n, ok := f.nodes[name]
	if !ok {
		return nil, fmt.Errorf("node %q not found", name)
	}
	return n, nil
}

func (f *fakeNodes) Get(_ context.Context, name string) (*corev1.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.record(call{Op: "Get", Node: name}); err != nil {
		return nil, err
	}
	n, err := f.lookup(name)
	if err != nil {
		return nil, err
	}
	return n.DeepCopy(), nil
}

func (f *fakeNodes) ListNodes(context.Context) ([]corev1.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.record(call{Op: "ListNodes"}); err != nil {
		return nil, err
	}
	out := make([]corev1.Node, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, *n.DeepCopy())
	}
	return out, nil
}

func (f *fakeNodes) Cordon(_ context.Context, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.record(call{Op: "Cordon", Node: name}); err != nil {
		return false, err
	}
	n, err := f.lookup(name)
	if err != nil {
		return false, err
	}
	already := n.Spec.Unschedulable
	n.Spec.Unschedulable = true
	return already, nil
}

func (f *fakeNodes) Uncordon(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.record(call{Op: "Uncordon", Node: name}); err != nil {
		return err
	}
	n, err := f.lookup(name)
	if err != nil {
		return err
	}
	n.Spec.Unschedulable = false
	return nil
}

func (f *fakeNodes) Drain(ctx context.Context, name string, opts node.DrainOptions) error {
	f.mu.Lock()
	err := f.record(call{Op: "Drain", Node: name, Drain: opts})
	fn := f.drain
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if fn != nil {
		return fn(ctx, name, opts)
	}
	return nil
}

func (f *fakeNodes) SetAnnotations(_ context.Context, name string, kv map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[string]string, len(kv))
	for k, v := range kv {
		cp[k] = v
	}
	if err := f.record(call{Op: "SetAnnotations", Node: name, Annotations: cp}); err != nil {
		return err
	}
	n, err := f.lookup(name)
	if err != nil {
		return err
	}
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}
	for k, v := range kv {
		n.Annotations[k] = v
	}
	return nil
}

func (f *fakeNodes) RemoveAnnotations(_ context.Context, name string, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.record(call{Op: "RemoveAnnotations", Node: name, Keys: append([]string(nil), keys...)}); err != nil {
		return err
	}
	n, err := f.lookup(name)
	if err != nil {
		return err
	}
	for _, k := range keys {
		delete(n.Annotations, k)
	}
	return nil
}

// node returns a copy of the stored node (test inspection; not recorded).
func (f *fakeNodes) node(name string) *corev1.Node {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nodes[name].DeepCopy()
}

// annotate sets annotations directly (test setup; not recorded).
func (f *fakeNodes) annotate(name string, kv map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range kv {
		f.nodes[name].Annotations[k] = v
	}
}

// callsOf returns the recorded calls with the given Op.
func (f *fakeNodes) callsOf(op string) []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []call
	for _, c := range f.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeNodes) allCalls() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

// drainDeadlineExceeded mimics node.Ops.Drain timing out.
func drainDeadlineExceeded(context.Context, string, node.DrainOptions) error {
	return fmt.Errorf("draining node: %w", context.DeadlineExceeded)
}

// drainBlock blocks until ctx is done and then reports it the way
// node.Ops.Drain does: any context error is wrapped as DeadlineExceeded.
func drainBlock(ctx context.Context, _ string, _ node.DrainOptions) error {
	<-ctx.Done()
	return fmt.Errorf("draining node: %w (%v)", context.DeadlineExceeded, ctx.Err())
}

var errDrainFailed = errors.New("eviction failed")

// fakeMachines reports a fixed BeingDeleted result.
type fakeMachines struct {
	mu       sync.Mutex
	deleting bool
	err      error
	calls    int
}

func (f *fakeMachines) BeingDeleted(context.Context, *corev1.Node) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.deleting, f.err
}

// recordingNotifier records every notification in order.
type recordingNotifier struct {
	mu  sync.Mutex
	got []observability.Notification
}

func (r *recordingNotifier) Notify(_ context.Context, n observability.Notification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, n)
}

func (r *recordingNotifier) stages() []observability.Stage {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]observability.Stage, len(r.got))
	for i, n := range r.got {
		out[i] = n.Stage
	}
	return out
}

func (r *recordingNotifier) count(s observability.Stage) int {
	n := 0
	for _, x := range r.stages() {
		if x == s {
			n++
		}
	}
	return n
}
