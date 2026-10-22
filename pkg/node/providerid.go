// Package node maps Kubernetes nodes to Azure VMs.
package node

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// VMNameFromProviderID extracts the Azure VM name from a node's providerID.
// Uniform scale set instances (.../virtualMachineScaleSets/<vmss>/virtualMachines/<id>)
// yield "<vmss>_<id>"; Flexible and standalone VMs (.../virtualMachines/<name>) yield "<name>".
// Segment names are matched case-insensitively.
func VMNameFromProviderID(providerID string) (string, bool) {
	rest, ok := strings.CutPrefix(providerID, "azure://")
	if !ok {
		return "", false
	}
	segs := strings.Split(rest, "/")

	// Uniform scale set: virtualMachineScaleSets/<vmss>/virtualMachines/<id>.
	for i := 0; i+3 < len(segs); i++ {
		if strings.EqualFold(segs[i], "virtualMachineScaleSets") &&
			strings.EqualFold(segs[i+2], "virtualMachines") &&
			segs[i+1] != "" && segs[i+3] != "" {
			return segs[i+1] + "_" + segs[i+3], true
		}
	}

	// Otherwise the last virtualMachines/<name> pair wins.
	for i := len(segs) - 2; i >= 0; i-- {
		if strings.EqualFold(segs[i], "virtualMachines") && segs[i+1] != "" {
			return segs[i+1], true
		}
	}
	return "", false
}

// NodesForVMs matches VM names to nodes by providerID. Keys of found are the
// lowercased VM names; missing keeps the original spelling of unmatched names.
func NodesForVMs(nodes []corev1.Node, vmNames []string) (found map[string]*corev1.Node, missing []string) {
	byVM := make(map[string]*corev1.Node, len(nodes))
	for i := range nodes {
		if name, ok := VMNameFromProviderID(nodes[i].Spec.ProviderID); ok {
			byVM[strings.ToLower(name)] = &nodes[i]
		}
	}
	found = make(map[string]*corev1.Node)
	for _, vm := range vmNames {
		key := strings.ToLower(vm)
		if n, ok := byVM[key]; ok {
			found[key] = n
		} else {
			missing = append(missing, vm)
		}
	}
	return found, missing
}
