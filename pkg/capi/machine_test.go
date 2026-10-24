package capi

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func machine(version string, deleting bool) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("cluster.x-k8s.io/" + version)
	u.SetKind("Machine")
	u.SetName("m1")
	u.SetNamespace("ns1")
	if deleting {
		now := metav1.NewTime(time.Now())
		u.SetDeletionTimestamp(&now)
	}
	return u
}

func annotatedNode() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        "n1",
		Annotations: map[string]string{AnnMachine: "m1", AnnClusterNamespace: "ns1"},
	}}
}

func newClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objs...)
}

func TestNoAnnotations(t *testing.T) {
	got, err := NewMachineChecker(newClient()).BeingDeleted(context.Background(), &corev1.Node{})
	if err != nil || got {
		t.Fatalf("got %v, %v; want false, nil", got, err)
	}
}

func TestMachineDeleting(t *testing.T) {
	got, err := NewMachineChecker(newClient(machine("v1beta2", true))).BeingDeleted(context.Background(), annotatedNode())
	if err != nil || !got {
		t.Fatalf("got %v, %v; want true, nil", got, err)
	}
}

func TestMachineNotDeleting(t *testing.T) {
	got, err := NewMachineChecker(newClient(machine("v1beta2", false))).BeingDeleted(context.Background(), annotatedNode())
	if err != nil || got {
		t.Fatalf("got %v, %v; want false, nil", got, err)
	}
}

func TestV1beta1Fallback(t *testing.T) {
	got, err := NewMachineChecker(newClient(machine("v1beta1", true))).BeingDeleted(context.Background(), annotatedNode())
	if err != nil || !got {
		t.Fatalf("got %v, %v; want true, nil", got, err)
	}
}

func TestNoCRD(t *testing.T) {
	got, err := NewMachineChecker(newClient()).BeingDeleted(context.Background(), annotatedNode())
	if err != nil || got {
		t.Fatalf("got %v, %v; want false, nil", got, err)
	}
}

func TestOtherErrorPropagates(t *testing.T) {
	c := newClient()
	c.PrependReactor("get", "machines", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})
	got, err := NewMachineChecker(c).BeingDeleted(context.Background(), annotatedNode())
	if err == nil || got {
		t.Fatalf("got %v, %v; want false, error", got, err)
	}
}
