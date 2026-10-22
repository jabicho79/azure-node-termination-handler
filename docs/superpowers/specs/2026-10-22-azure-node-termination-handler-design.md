# Azure Node Termination Handler — Design

Date: 2026-10-22
Status: Draft for review

## Intent and why

We run self-managed Kubernetes clusters on Azure, provisioned with Cluster API Provider Azure (CAPZ). Nodes come from both **MachinePools** (`AzureMachinePool`, backed by VM Scale Set instances) and **MachineDeployments** (`AzureMachineTemplate`, standalone VMs).

Cluster API already cordons and drains a node when *it* removes a Machine (scale-down, upgrade, remediation). Nothing handles events that **Azure** initiates. The trigger for this project was a platform **Redeploy** event for degraded hardware: Azure gave advance notice (days), nobody acted, and the VM was redeployed without the node being drained.

We want the equivalent of [aws-node-termination-handler](https://github.com/aws/aws-node-termination-handler) (NTH) in IMDS mode, built for Azure: a per-node agent that sees Azure's advance warnings and moves workloads off the node cleanly before the VM goes down.

## Goals

- Detect Azure Scheduled Events for the node the agent runs on, on both scale set instances and standalone VMs.
- On a disruptive event: cordon the node, drain it immediately (whatever the event's deadline), then **automatically approve** the event so Azure proceeds right away.
- Uncordon the node automatically when it comes back from a Reboot or Redeploy.
- Observability: logs, Prometheus metrics, Kubernetes Events on the node, optional webhook.
- Easy install: container image + Helm chart.

## Non-goals (v1)

- A central queue mode (Event Grid → Service Bus/Storage Queue). NTH has one, but we don't need it for v1.
- AKS-specific behaviour. AKS already handles some of this itself.
- Windows nodes.
- Changing Cluster API objects. The agent only reads a node's own Machine (see "Coexisting with Cluster API").
- Replacing the VM through Cluster API instead of letting Azure redeploy it.

## Anti-goals (failure even if it "works")

- Letting the event's deadline pass while the agent waits, so Azure acts without our approval. Approving late is better than never approving.
- Fighting Cluster API: competing drains, uncordoning a node CAPI is deleting, or blocking CAPI-driven deletes.
- Needing Azure credentials, managed identities, or cloud-side infrastructure for the core path.
- Draining nodes for events that don't take the VM down for long, such as Freeze.

## Constraints and facts about the platform

- **Endpoint:** `GET http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01` with header `Metadata: true`. Approve with `POST` to the same URL, body `{"StartRequests":[{"EventId":"<id>"}]}`.
- **Response:** `DocumentIncarnation`, and `Events[]` with `EventId`, `EventType` (`Reboot`, `Redeploy`, `Freeze`, `Preempt`, `Terminate`), `ResourceType`, `Resources[]` (VM names), `EventStatus` (`Scheduled`, `Started`), `NotBefore`, `Description`, `EventSource` (`Platform`, `User`), `DurationInSeconds`.
- **Activation:** the service turns on for a VM the first time it's queried, and turns off after about 24 hours without queries. Events that were scheduled before activation may be missed, so the agent must poll continuously from node start.
- **Notice periods:** Freeze and Reboot/Redeploy have at least 10–15 minutes of notice; degraded-hardware Redeploy can have hours to days; Preempt (Spot) has about 30 seconds; Terminate has the scale set's configured terminate-notification timeout (5–15 min; CAPZ exposes it on `AzureMachinePool`). Terminate events only appear if terminate notifications are enabled on the scale set.
- **One event can list several VMs** in `Resources` (for example, VMs that share an update domain). Approving the event lets it proceed for **every** VM listed, not just the one that approved it.
- Pods can reach 169.254.169.254 on Azure by default. The chart should still offer `hostNetwork` for clusters whose network policy or CNI blocks it.

## Decided by the user

- Platform: self-managed CAPZ clusters with both AzureMachinePools and MachineDeployments.
- Main case: maintenance, especially degraded-hardware **Redeploy**.
- On Redeploy/Reboot: **drain right away and approve the event automatically.**
- Standalone project, in its own repository at `~/Documents/Repos/azure-node-termination-handler`, not a fork of NTH.

## Behaviour (my calls, agreed in conversation unless marked)

### Deployment shape

A Go binary running as a **DaemonSet** on Linux nodes, one pod per node. Each pod handles only its own node. It finds its node name through the downward API (`spec.nodeName`) and its VM name from IMDS (`/metadata/instance/compute/name`). On a scale set instance this is the instance name, for example `vmss_3`, which is the form used in `Resources[]`.

### Per event type

| Event | Action | After |
|---|---|---|
| Redeploy | cordon → drain → approve | uncordon when node returns |
| Reboot | cordon → drain → approve | uncordon when node returns |
| Preempt (Spot) | cordon → fast drain (fit the ~30 s) → approve | uncordon if the VM comes back (CAPZ Spot `Deallocate` policy) |
| Terminate | cordon → drain → approve | none; VM is gone |
| Freeze | log + Kubernetes Event only; no cordon, no approve | — |

Each action can be turned on or off through Helm values. Auto-approve can be turned off globally.

Events whose `EventStatus` is already `Started` get best-effort handling: cordon, plus a drain if there's time. They are never approved.

### Drain

- Uses `k8s.io/kubectl/pkg/drain`, like NTH: respects PodDisruptionBudgets and pod termination grace periods, ignores DaemonSet pods, and has configurable handling for emptyDir and unmanaged pods.
- **Drain deadline** is the earlier of `NotBefore − safety margin` (default 60 s) and a configurable maximum drain time. When it's reached and pods are still left (for example, blocked by a PDB), the agent logs a warning, emits a Kubernetes Event and a metric, and **approves anyway**. *(My call: never let the deadline pass without approving.)*
- For Preempt, the grace period is capped so the drain can finish inside the notice.

### Multi-VM events *(my call)*

Because approval applies to every VM in `Resources[]`, a pod approves an event only when every listed VM that maps to a node in this cluster has been drained for that event. Each pod records "drained for event X" as an annotation on its own node. It maps VM names to nodes through `spec.providerID`, which for CAPZ ends in `/virtualMachines/<name>` or `/virtualMachineScaleSets/<vmss>/virtualMachines/<id>`. Both scale set orchestration modes must work *(my call, builder check)*. Uniform uses instance names like `vmss_3` and `/virtualMachineScaleSets/<vmss>/virtualMachines/<id>` providerIDs. Flexible uses plain VM names and `/virtualMachines/<name>` providerIDs. Matching tries both forms, and the fake server and e2e fixtures cover both. If a listed VM name matches no node and the agent can't tell whether it belongs to the cluster, it logs that at warn level. Listed VMs that aren't nodes in this cluster are ignored. If the drain deadline is reached first, the deadline rule wins and the pod approves.

### Uncordon after reboot

This follows NTH's approach. Before approving a Reboot or Redeploy, the agent annotates the node with the event ID, the action (`uncordon-after-reboot`), and a timestamp. When the agent starts, if that annotation is present and the node's uptime is shorter than the time since the timestamp, it uncordons the node and removes its own annotations. It only uncordons nodes that it cordoned itself. The same marker is written for Preempt, so a deallocated Spot VM that is started again rejoins schedulable.

**Event disappears without a reboot** *(my call, builder check)*. Azure has no "cancelled" status: an event just drops out of `Events[]`. This happens when Azure withdraws or reschedules the event, or live-migrates the VM instead of redeploying it. If an event the agent acted on has been gone from the document for a grace period (default 5 min) and the node never rebooted, the agent uncordons the node and clears its annotations. A missing event means Azure isn't about to act on that VM, and this matches NTH's handling of cancelled events. If the event later reappears, it is handled as a new event.

### Coexisting with Cluster API *(my call)*

Handling is idempotent. The agent never writes CAPI objects. Before it uncordons a node, it reads the node's own Machine, which it finds through the `cluster.x-k8s.io/machine` and `cluster.x-k8s.io/cluster-namespace` annotations that CAPI puts on the node. If the Machine has a deletion timestamp, the agent skips the uncordon. If the CAPI CRDs or annotations are missing, it skips the check. *(My call, from the builder check: this covers a node that reboots while CAPI is deleting it.)* If CAPI is already deleting the Machine, the node is already cordoned and drained, so cordon and drain are no-ops. Approving a resulting Terminate event just lets the delete finish sooner. The agent never uncordons a node it didn't cordon itself.

### Polling and state

- Poll every 5 s by default (configurable). Skip unchanged documents by tracking `DocumentIncarnation`.
- Store everything the agent needs to survive a pod restart as node annotations (event in progress, drained-for-event, uncordon marker). Keep nothing important only in memory.
- Prefix all annotations, labels and taints with `azure-node-termination-handler/`.

### Observability

- Structured logs (zerolog, like NTH).
- Prometheus metrics: events seen by type, actions taken, drain duration, drain timeouts, approvals, errors.
- Kubernetes Events on the Node object for each step.
- Optional webhook (generic JSON template, Slack-compatible) after drain and after approval, modelled on NTH's `pkg/webhook`.
- Liveness and readiness probes.

### Configuration

Flags and environment variables, exposed through Helm values: enabled event types, auto-approve, poll interval, drain timeout and safety margin, pod grace period, delete-emptydir-data, ignore-daemonsets, webhook URL/template, metrics port, dry-run, log level.

### RBAC

RBAC mirrors NTH: get/patch nodes, list pods, create evictions, create events. It adds `get` on `machines.cluster.x-k8s.io`. It needs no Azure identity.

## Testing

- Unit tests for the event parser, the decision logic per event type, deadline math, multi-VM approval, and the uncordon-after-reboot check.
- A fake Scheduled Events server (serves scripted documents and records approvals), like NTH's `ec2-metadata-test-proxy`.
- An end-to-end test on a kind cluster, with the agent pointed at the fake server. It should cover: Redeploy → drain → approve → simulated reboot → uncordon; event withdrawn → uncordon; uncordon skipped for a Machine being deleted; Preempt; Freeze → no drain; PDB-blocked drain → approve at deadline.
- No real Azure VMs required in CI. Testing on a real CAPZ cluster is a manual step before release.

## Left to the builder

- Package layout, module path, and which NTH code to copy or adapt (license is Apache-2.0, so keep the NOTICE).
- Exact flag and metric names, the Helm chart structure, and image build/publishing.
- Retry and backoff for IMDS and Kubernetes API calls.
- Default values not specified above.
