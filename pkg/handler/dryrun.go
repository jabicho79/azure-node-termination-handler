package handler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// dryRunNodes wraps Nodes in dry-run mode. The wrapped node.Ops only logs
// mutations, so dryRunNodes keeps the agent's would-be writes to its own node
// in memory (an annotation overlay plus the unschedulable flag) and merges
// them onto every copy of that node it returns. The handler then sees its own
// progress, and the gone and uncordon paths work, instead of detecting,
// cordoning and draining the same event on every poll. The overlay lives only
// as long as the process.
//
// It is used only by the handler's goroutine and is not safe for concurrent
// use.
type dryRunNodes struct {
	Nodes
	name string
	// ann maps annotation keys to their would-be value; nil means removed.
	ann           map[string]*string
	unschedulable *bool
}

func newDryRunNodes(inner Nodes, name string) *dryRunNodes {
	return &dryRunNodes{Nodes: inner, name: name, ann: map[string]*string{}}
}

// merge applies the overlay to n if it is the agent's own node.
func (d *dryRunNodes) merge(n *corev1.Node) {
	if n == nil || n.Name != d.name {
		return
	}
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}
	for k, v := range d.ann {
		if v == nil {
			delete(n.Annotations, k)
		} else {
			n.Annotations[k] = *v
		}
	}
	if d.unschedulable != nil {
		n.Spec.Unschedulable = *d.unschedulable
	}
}

func (d *dryRunNodes) Get(ctx context.Context, name string) (*corev1.Node, error) {
	n, err := d.Nodes.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	d.merge(n)
	return n, nil
}

func (d *dryRunNodes) ListNodes(ctx context.Context) ([]corev1.Node, error) {
	nodes, err := d.Nodes.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	for i := range nodes {
		d.merge(&nodes[i])
	}
	return nodes, nil
}

func (d *dryRunNodes) Cordon(ctx context.Context, name string) (bool, error) {
	already, err := d.Nodes.Cordon(ctx, name)
	if err != nil || name != d.name {
		return already, err
	}
	if d.unschedulable != nil {
		already = *d.unschedulable
	}
	t := true
	d.unschedulable = &t
	return already, nil
}

func (d *dryRunNodes) Uncordon(ctx context.Context, name string) error {
	if err := d.Nodes.Uncordon(ctx, name); err != nil || name != d.name {
		return err
	}
	f := false
	d.unschedulable = &f
	return nil
}

func (d *dryRunNodes) SetAnnotations(ctx context.Context, name string, kv map[string]string) error {
	if err := d.Nodes.SetAnnotations(ctx, name, kv); err != nil || name != d.name {
		return err
	}
	for k, v := range kv {
		d.ann[k] = &v
	}
	return nil
}

func (d *dryRunNodes) RemoveAnnotations(ctx context.Context, name string, keys ...string) error {
	if err := d.Nodes.RemoveAnnotations(ctx, name, keys...); err != nil || name != d.name {
		return err
	}
	for _, k := range keys {
		d.ann[k] = nil
	}
	return nil
}
