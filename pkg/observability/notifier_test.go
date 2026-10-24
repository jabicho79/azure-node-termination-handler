package observability

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type recorder struct{ got []Notification }

func (r *recorder) Notify(_ context.Context, n Notification) { r.got = append(r.got, n) }

func TestMultiFansOut(t *testing.T) {
	a, b := &recorder{}, &recorder{}
	Multi{a, b}.Notify(context.Background(), Notification{Stage: StageDrained, NodeName: "n1"})
	if len(a.got) != 1 || len(b.got) != 1 || a.got[0].NodeName != "n1" {
		t.Fatalf("a=%v b=%v", a.got, b.got)
	}
}

func TestEventRecorderEmitsNodeEvent(t *testing.T) {
	cs := fake.NewSimpleClientset()
	n := NewEventRecorder(cs, "node-1")
	n.Notify(context.Background(), Notification{Stage: StageDrainTimeout, NodeName: "node-1", Message: "too slow"})

	deadline := time.Now().Add(2 * time.Second)
	for {
		list, err := cs.CoreV1().Events(metav1.NamespaceDefault).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list.Items) > 0 {
			ev := list.Items[0]
			if ev.InvolvedObject.Kind != "Node" || ev.InvolvedObject.Name != "node-1" {
				t.Errorf("involvedObject = %+v", ev.InvolvedObject)
			}
			if ev.Reason != "DrainTimeout" || ev.Type != corev1.EventTypeWarning {
				t.Errorf("reason=%q type=%q", ev.Reason, ev.Type)
			}
			if ev.Source.Component != "azure-node-termination-handler" {
				t.Errorf("source = %+v", ev.Source)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no event emitted within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
