package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
)

// DrainOptions configures a single node drain.
type DrainOptions struct {
	Deadline              time.Time
	PodGracePeriodSeconds int
	DeleteEmptyDirData    bool
	IgnoreDaemonSets      bool
}

// Ops performs Kubernetes node operations. In dry-run mode every mutating
// method logs and returns nil.
type Ops struct {
	client kubernetes.Interface
	dryRun bool
}

// NewOps returns an Ops backed by client.
func NewOps(client kubernetes.Interface, dryRun bool) *Ops {
	return &Ops{client: client, dryRun: dryRun}
}

// Get returns the named node.
func (o *Ops) Get(ctx context.Context, name string) (*corev1.Node, error) {
	return o.client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
}

// ListNodes returns all nodes in the cluster.
func (o *Ops) ListNodes(ctx context.Context) ([]corev1.Node, error) {
	list, err := o.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// Cordon marks the node unschedulable and reports whether it already was.
func (o *Ops) Cordon(ctx context.Context, name string) (bool, error) {
	n, err := o.Get(ctx, name)
	if err != nil {
		return false, err
	}
	already := n.Spec.Unschedulable
	if o.dryRun {
		log.Info().Str("node", name).Msg("Would have cordoned node, but dry-run was set")
		return already, nil
	}
	if already {
		return true, nil
	}
	return false, o.setUnschedulable(ctx, name, true)
}

// Uncordon marks the node schedulable.
func (o *Ops) Uncordon(ctx context.Context, name string) error {
	if o.dryRun {
		log.Info().Str("node", name).Msg("Would have uncordoned node, but dry-run was set")
		return nil
	}
	return o.setUnschedulable(ctx, name, false)
}

func (o *Ops) setUnschedulable(ctx context.Context, name string, v bool) error {
	patch := []byte(fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, v))
	_, err := o.client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// Drain evicts pods from the node. A timeout is reported as an error wrapping
// context.DeadlineExceeded.
func (o *Ops) Drain(ctx context.Context, name string, opts DrainOptions) error {
	if o.dryRun {
		log.Info().Str("node", name).Msg("Would have drained node, but dry-run was set")
		return nil
	}
	ctx, cancel := context.WithDeadline(ctx, opts.Deadline)
	defer cancel()

	logger := log.Logger.With().Str("node", name).Logger()
	helper := &drain.Helper{
		Ctx:                 ctx,
		Client:              o.client,
		Force:               true,
		GracePeriodSeconds:  opts.PodGracePeriodSeconds,
		IgnoreAllDaemonSets: opts.IgnoreDaemonSets,
		DeleteEmptyDirData:  opts.DeleteEmptyDirData,
		Timeout:             time.Until(opts.Deadline),
		Out:                 &zerologWriter{logger: logger, error: false},
		ErrOut:              &zerologWriter{logger: logger, error: true},
	}
	err := drain.RunNodeDrain(helper, name)
	if err == nil {
		return nil
	}
	// A cancelled parent (shutdown) is not a drain timeout, whatever the
	// drain helper reports.
	if errors.Is(ctx.Err(), context.Canceled) {
		if errors.Is(err, context.Canceled) {
			return fmt.Errorf("draining node %s: %w", name, err)
		}
		return fmt.Errorf("draining node %s: %w (%v)", name, context.Canceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// client-go's rate limiter fails early with "would exceed context
	// deadline" (not wrapping DeadlineExceeded) before ctx is done.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || strings.Contains(err.Error(), "global timeout reached") ||
		strings.Contains(err.Error(), "would exceed context deadline") {
		return fmt.Errorf("draining node %s: %w (%v)", name, context.DeadlineExceeded, err)
	}
	return fmt.Errorf("draining node %s: %w", name, err)
}

// SetAnnotations merge-patches the given annotations onto the node.
func (o *Ops) SetAnnotations(ctx context.Context, name string, kv map[string]string) error {
	m := make(map[string]any, len(kv))
	for k, v := range kv {
		m[k] = v
	}
	if o.dryRun {
		log.Info().Str("node", name).Interface("annotations", kv).Msg("Would have set node annotations, but dry-run was set")
		return nil
	}
	return o.patchAnnotations(ctx, name, m)
}

// RemoveAnnotations removes the given annotation keys from the node.
func (o *Ops) RemoveAnnotations(ctx context.Context, name string, keys ...string) error {
	m := make(map[string]any, len(keys))
	for _, k := range keys {
		m[k] = nil
	}
	if o.dryRun {
		log.Info().Str("node", name).Strs("keys", keys).Msg("Would have removed node annotations, but dry-run was set")
		return nil
	}
	return o.patchAnnotations(ctx, name, m)
}

func (o *Ops) patchAnnotations(ctx context.Context, name string, anns map[string]any) error {
	if len(anns) == 0 {
		return nil
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": anns}})
	if err != nil {
		return err
	}
	_, err = o.client.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// zerologWriter adapts the drain helper's Out/ErrOut writers to zerolog.
type zerologWriter struct {
	logger zerolog.Logger
	error  bool
}

func (w *zerologWriter) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	if msg == "" {
		return len(p), nil
	}
	if w.error {
		w.logger.Warn().Msg(msg)
	} else {
		w.logger.Info().Msg(msg)
	}
	return len(p), nil
}
