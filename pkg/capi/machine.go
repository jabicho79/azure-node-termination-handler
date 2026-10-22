// Package capi inspects Cluster API Machines without depending on cluster-api.
package capi

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// Annotations Cluster API sets on Nodes it manages.
const (
	AnnMachine          = "cluster.x-k8s.io/machine"
	AnnClusterNamespace = "cluster.x-k8s.io/cluster-namespace"
)

// Preferred API versions first.
var machineGVRs = []schema.GroupVersionResource{
	{Group: "cluster.x-k8s.io", Version: "v1beta2", Resource: "machines"},
	{Group: "cluster.x-k8s.io", Version: "v1beta1", Resource: "machines"},
}

// MachineChecker looks up the Cluster API Machine backing a node.
type MachineChecker struct {
	client dynamic.Interface
}

// NewMachineChecker returns a MachineChecker using the given dynamic client.
func NewMachineChecker(client dynamic.Interface) *MachineChecker {
	return &MachineChecker{client: client}
}

// BeingDeleted reports true only if the node's Machine exists and has a
// deletionTimestamp. Missing annotations, a missing CRD or a missing Machine
// yield (false, nil).
func (m *MachineChecker) BeingDeleted(ctx context.Context, node *corev1.Node) (bool, error) {
	name := node.Annotations[AnnMachine]
	ns := node.Annotations[AnnClusterNamespace]
	if name == "" || ns == "" {
		return false, nil
	}
	for _, gvr := range machineGVRs {
		u, err := m.client.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("get machine %s/%s (%s): %w", ns, name, gvr.Version, err)
		}
		return u.GetDeletionTimestamp() != nil, nil
	}
	return false, nil
}
