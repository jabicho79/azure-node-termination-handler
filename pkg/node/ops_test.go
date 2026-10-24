package node

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	os.Exit(m.Run())
}

func newNode(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{"other/x": "keep"}}}
}

func TestCordonReportsPriorState(t *testing.T) {
	c := fake.NewSimpleClientset(newNode("n1"))
	o := NewOps(c, false)
	ctx := context.Background()
	already, err := o.Cordon(ctx, "n1")
	if err != nil || already {
		t.Fatalf("first cordon = (%v, %v), want (false, nil)", already, err)
	}
	n, _ := o.Get(ctx, "n1")
	if !n.Spec.Unschedulable {
		t.Fatal("node should be unschedulable")
	}
	already, err = o.Cordon(ctx, "n1")
	if err != nil || !already {
		t.Fatalf("second cordon = (%v, %v), want (true, nil)", already, err)
	}
}

func TestUncordon(t *testing.T) {
	n := newNode("n1")
	n.Spec.Unschedulable = true
	c := fake.NewSimpleClientset(n)
	o := NewOps(c, false)
	if err := o.Uncordon(context.Background(), "n1"); err != nil {
		t.Fatal(err)
	}
	got, _ := o.Get(context.Background(), "n1")
	if got.Spec.Unschedulable {
		t.Fatal("node should be schedulable")
	}
}

func TestAnnotationsRoundTrip(t *testing.T) {
	c := fake.NewSimpleClientset(newNode("n1"))
	o := NewOps(c, false)
	ctx := context.Background()
	if err := o.SetAnnotations(ctx, "n1", map[string]string{AnnEventID: "E1", AnnFirstSeen: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := o.RemoveAnnotations(ctx, "n1", AnnFirstSeen); err != nil {
		t.Fatal(err)
	}
	got, _ := o.Get(ctx, "n1")
	want := map[string]string{AnnEventID: "E1", "other/x": "keep"}
	if len(got.Annotations) != len(want) {
		t.Fatalf("annotations = %v, want %v", got.Annotations, want)
	}
	for k, v := range want {
		if got.Annotations[k] != v {
			t.Fatalf("annotations = %v, want %v", got.Annotations, want)
		}
	}
}

func pod(name string, owner *metav1.OwnerReference) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: "n1"},
	}
	if owner != nil {
		p.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	return p
}

func drainClient() (*fake.Clientset, *Ops) {
	yes := true
	ds := &metav1.OwnerReference{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "ds", UID: "u1", Controller: &yes}
	c := fake.NewSimpleClientset(newNode("n1"), pod("app", nil), pod("ds-pod", ds),
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "default", UID: "u1"}})
	// Advertise policy/v1 eviction support so the drain helper evicts instead of deleting.
	c.Resources = []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{Name: "pods/eviction", Kind: "Eviction", Group: "policy", Version: "v1"}},
	}}
	return c, NewOps(c, false)
}

func TestDrainEvictsPods(t *testing.T) {
	c, o := drainClient()
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		ev := a.(k8stesting.CreateAction).GetObject().(*policyv1.Eviction)
		return true, nil, c.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), ev.Namespace, ev.Name)
	})
	err := o.Drain(context.Background(), "n1", DrainOptions{
		Deadline: time.Now().Add(10 * time.Second), IgnoreDaemonSets: true, DeleteEmptyDirData: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	pods, _ := c.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 1 || pods.Items[0].Name != "ds-pod" {
		t.Fatalf("remaining pods = %v, want only ds-pod", pods.Items)
	}
}

func TestDrainTimesOut(t *testing.T) {
	c, o := drainClient()
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewTooManyRequests("pdb", 1)
	})
	err := o.Drain(context.Background(), "n1", DrainOptions{
		Deadline: time.Now().Add(time.Second), IgnoreDaemonSets: true, DeleteEmptyDirData: true,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

// The client rate limiter fails a request early, before the context is done,
// when waiting would pass the drain deadline. That is a drain timeout too.
func TestDrainRateLimiterDeadlineIsTimeout(t *testing.T) {
	c, o := drainClient()
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, errors.New("client rate limiter Wait returned an error: rate: Wait(n=1) would exceed context deadline")
	})
	err := o.Drain(context.Background(), "n1", DrainOptions{
		Deadline: time.Now().Add(10 * time.Second), IgnoreDaemonSets: true, DeleteEmptyDirData: true,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

// A cancelled parent (agent shutdown) is not a drain timeout.
func TestDrainParentCancelledIsNotTimeout(t *testing.T) {
	c, o := drainClient()
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewTooManyRequests("pdb", 1)
	})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	err := o.Drain(ctx, "n1", DrainOptions{
		Deadline: time.Now().Add(30 * time.Second), IgnoreDaemonSets: true, DeleteEmptyDirData: true,
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want Canceled and not DeadlineExceeded", err)
	}
}

func TestDrainUnrelatedErrorIsNotTimeout(t *testing.T) {
	c, o := drainClient()
	c.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewInternalError(errors.New("boom"))
	})
	err := o.Drain(context.Background(), "n1", DrainOptions{
		Deadline: time.Now().Add(30 * time.Second), IgnoreDaemonSets: true, DeleteEmptyDirData: true,
	})
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want a non-timeout error", err)
	}
}

func TestDryRunDoesNotMutate(t *testing.T) {
	c := fake.NewSimpleClientset(newNode("n1"))
	o := NewOps(c, true)
	ctx := context.Background()
	if _, err := o.Cordon(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	if err := o.SetAnnotations(ctx, "n1", map[string]string{AnnEventID: "E1"}); err != nil {
		t.Fatal(err)
	}
	got, _ := o.Get(ctx, "n1")
	if got.Spec.Unschedulable || got.Annotations[AnnEventID] != "" {
		t.Fatalf("node mutated in dry-run: %+v", got)
	}
}
