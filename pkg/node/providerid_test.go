package node

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestVMNameFromProviderID(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"uniform", "azure:///subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/pool/virtualMachines/3", "pool_3", true},
		{"flexible", "azure:///subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/pool_a1b2c3", "pool_a1b2c3", true},
		{"mixed case", "azure:///subscriptions/s/resourceGroups/rg/providers/microsoft.compute/VirtualMachineScaleSets/pool/VirtualMachines/3", "pool_3", true},
		{"kind", "kind://docker/x/y", "", false},
		{"empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := VMNameFromProviderID(tc.in)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestNodesForVMs(t *testing.T) {
	mk := func(name, pid string) corev1.Node {
		return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{ProviderID: pid}}
	}
	nodes := []corev1.Node{
		mk("n1", "azure:///subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/pool/virtualMachines/3"),
		mk("n2", "azure:///subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/md-abc"),
		mk("n3", "kind://docker/x/y"),
	}
	found, missing := NodesForVMs(nodes, []string{"POOL_3", "md-abc", "other"})
	if len(found) != 2 || found["pool_3"] == nil || found["pool_3"].Name != "n1" || found["md-abc"] == nil || found["md-abc"].Name != "n2" {
		t.Fatalf("unexpected found: %v", found)
	}
	if !reflect.DeepEqual(missing, []string{"other"}) {
		t.Fatalf("missing = %v", missing)
	}
}
