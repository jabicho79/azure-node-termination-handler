package observability

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

// Stage is a point in the lifecycle of a scheduled event on this node.
type Stage string

const (
	StageDetected        Stage = "Detected"
	StageCordoned        Stage = "Cordoned"
	StageDrained         Stage = "Drained"
	StageDrainTimeout    Stage = "DrainTimeout"
	StageApproved        Stage = "Approved"
	StageUncordoned      Stage = "Uncordoned"
	StageUncordonSkipped Stage = "UncordonSkipped"
	StageFreeze          Stage = "Freeze"
)

// Notification describes a lifecycle stage reached for a node.
type Notification struct {
	Stage                                                                 Stage
	NodeName, VMName, EventID, EventType, Description, NotBefore, Message string
}

// Notifier receives lifecycle notifications. Implementations must not block for long and never fail the caller.
type Notifier interface {
	Notify(ctx context.Context, n Notification)
}

// Multi fans a notification out to every notifier.
type Multi []Notifier

func (m Multi) Notify(ctx context.Context, n Notification) {
	for _, x := range m {
		x.Notify(ctx, n)
	}
}

const eventSource = "azure-node-termination-handler"

type eventRecorder struct {
	rec record.EventRecorder
	ref *corev1.ObjectReference
}

// NewEventRecorder returns a Notifier that emits Kubernetes Events on the Node.
func NewEventRecorder(client kubernetes.Interface, nodeName string) Notifier {
	b := record.NewBroadcaster()
	b.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	return &eventRecorder{
		rec: b.NewRecorder(scheme.Scheme, corev1.EventSource{Component: eventSource}),
		ref: &corev1.ObjectReference{Kind: "Node", Name: nodeName, UID: types.UID(nodeName)},
	}
}

func (e *eventRecorder) Notify(_ context.Context, n Notification) {
	typ := corev1.EventTypeNormal
	if n.Stage == StageDrainTimeout || n.Stage == StageUncordonSkipped {
		typ = corev1.EventTypeWarning
	}
	msg := n.Message
	if msg == "" {
		msg = fmt.Sprintf("%s event %s: %s", n.EventType, n.EventID, n.Description)
	}
	e.rec.Event(e.ref, typ, string(n.Stage), msg)
}
