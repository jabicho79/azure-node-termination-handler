#!/usr/bin/env bash
# Runs the end-to-end suite on a dedicated kind cluster named anth-e2e.
#
# It never uses the current kubectl context: the cluster's kubeconfig is
# written to its own file and passed explicitly to kubectl, helm and the tests.
# The cluster is deleted at the end (also on failure) unless KEEP_CLUSTER=1.
set -euo pipefail

CLUSTER=anth-e2e
NAMESPACE=anth-e2e
IMAGE=azure-node-termination-handler:dev
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
E2E="$ROOT/test/e2e"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/anth-e2e.XXXXXX")"
KUBECONFIG_FILE="$WORKDIR/kubeconfig"

cleanup() {
	local rc=$?
	if [[ "${KEEP_CLUSTER:-}" == "1" ]]; then
		echo "KEEP_CLUSTER=1: keeping kind cluster $CLUSTER (kubeconfig: $KUBECONFIG_FILE)"
	else
		echo "Deleting kind cluster $CLUSTER"
		kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" || true
		rm -rf "$WORKDIR"
	fi
	exit "$rc"
}
trap cleanup EXIT

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
	echo "Reusing existing kind cluster $CLUSTER"
else
	kind create cluster --name "$CLUSTER" --config "$E2E/kind-config.yaml" --kubeconfig "$KUBECONFIG_FILE" --wait 120s
fi
kind get kubeconfig --name "$CLUSTER" > "$KUBECONFIG_FILE"

kc() { kubectl --kubeconfig "$KUBECONFIG_FILE" "$@"; }

for node in $(kind get nodes --name "$CLUSTER"); do
	case "$node" in
		"$CLUSTER"-*) ;;
		*) echo "refusing to touch unexpected node container $node" >&2; exit 1 ;;
	esac
	docker exec "$node" sh -c 'mkdir -p /var/lib/anth-e2e && echo "999999.0 0" > /var/lib/anth-e2e/uptime'
done

make -C "$ROOT" docker-build IMAGE="$IMAGE"
kind load docker-image "$IMAGE" --name "$CLUSTER"

kc create namespace "$NAMESPACE" --dry-run=client -o yaml | kc apply -f -
kc apply -f "$E2E/manifests/fake-imds.yaml"
kc -n "$NAMESPACE" rollout status deployment/fake-imds --timeout=120s

helm upgrade --install anth "$ROOT/charts/azure-node-termination-handler" \
	--kubeconfig "$KUBECONFIG_FILE" \
	--namespace "$NAMESPACE" \
	--values "$E2E/values-e2e.yaml" \
	--wait --timeout 180s
kc -n "$NAMESPACE" rollout status daemonset/anth-azure-node-termination-handler --timeout=120s

cd "$ROOT"
KUBECONFIG="$KUBECONFIG_FILE" go test -tags e2e ./test/e2e/ -v -count=1 -timeout 20m
