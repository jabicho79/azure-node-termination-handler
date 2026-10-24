# azure-node-termination-handler

A Kubernetes DaemonSet that watches the Azure Instance Metadata Service (IMDS) for
[Scheduled Events](https://learn.microsoft.com/azure/virtual-machines/linux/scheduled-events)
and cordons and drains the node before Azure reboots, redeploys, preempts or
deletes its VM. It then approves the event so Azure can go ahead early, and
uncordons the node when it comes back.

It needs no Azure credentials, SDK or managed identity: it only talks to the
link-local IMDS endpoint (`169.254.169.254`) and the Kubernetes API. It is
Linux only and was built for self-managed Cluster API Provider Azure (CAPZ)
clusters, with both AzureMachinePools and MachineDeployments.

See the [design spec](docs/superpowers/specs/2026-10-22-azure-node-termination-handler-design.md)
for the full rationale.

## What the agent does

One pod runs on each Linux node and handles only its own node. Every poll
interval (5s by default) it:

1. Reads `GET /metadata/scheduledevents?api-version=2020-07-01` and keeps the
   events whose `Resources` list this VM. The VM name comes from IMDS
   (`/metadata/instance/compute/name`) unless `vmName` is set.
2. Picks one current event. A Preempt always wins; otherwise the current
   event is kept while it is listed and enabled, else the event with the
   earliest `NotBefore` is picked.
3. Cordons the node, drains it with `k8s.io/kubectl/pkg/drain` (PDBs and
   termination grace periods are respected, DaemonSet pods are ignored), and
   approves the event once the node and every other cluster node listed in the
   event have drained, or at the drain deadline at the latest.
4. Uncordons the node after the VM rebooted, or when the event has been gone
   from the document for the event-gone grace period (5 minutes by default).

The **drain deadline** is the earlier of `NotBefore - drainSafetyMargin` and
`first seen + maxDrainTime`. When the deadline is reached and pods are still
left (for example blocked by a PodDisruptionBudget), the agent logs a warning,
emits a `DrainTimeout` Kubernetes Event and metric, and **approves anyway**.
While auto-approve is on, an event that is still `Scheduled` is never left
unapproved past its deadline: other events for this VM that fall due while
one event is being handled are approved by a deadline sweep.

All state needed to survive a pod restart is stored as annotations on the
node, so a restarted agent resumes where it left off.

### Events

| Event | Action | Afterwards |
|---|---|---|
| Redeploy | cordon, drain, approve | uncordon when the VM has rebooted |
| Reboot | cordon, drain, approve | uncordon when the VM has rebooted |
| Preempt (Spot) | cordon, fast drain (pod grace 20s, safety margin 5s), approve | uncordon if the VM comes back (CAPZ Spot `Deallocate` policy) |
| Terminate | cordon, drain, approve | nothing; the VM is gone |
| Freeze | log and a Kubernetes Event only; no cordon, drain or approval | nothing |

Each type can be disabled (`enableReboot`, `enableRedeploy`, `enablePreempt`,
`enableTerminate`), and approval can be turned off with `autoApprove: false`.
Events already `Started` are handled best effort: the node is cordoned and
drained if there is time, but the event is never approved.

Azure has no "cancelled" status: an event simply disappears. When the current
event disappears and no other actionable event is listed for this VM, the
agent waits for `eventGoneGrace` and then uncordons the node (if it cordoned
it) and removes its annotations. When another actionable event is listed (the
event was rescheduled), that event takes over the node without an uncordon.

Before uncordoning, the agent checks the node's Cluster API Machine (found
through the `cluster.x-k8s.io/machine` and `cluster.x-k8s.io/cluster-namespace`
node annotations). If the Machine is being deleted, the node stays cordoned.
The agent never uncordons a node it did not cordon itself.

## Install with Helm

No public image is published yet. Build the image and make it available to
your cluster first:

```sh
# Build the image (azure-node-termination-handler:dev by default).
make docker-build IMAGE=myregistry.example.com/azure-node-termination-handler:0.1.0

# Either push it to your own registry...
docker push myregistry.example.com/azure-node-termination-handler:0.1.0

# ...or, for a local kind cluster, load it into the nodes.
kind load docker-image myregistry.example.com/azure-node-termination-handler:0.1.0 --name <cluster>
```

Then install the chart, pointing `image.repository` and `image.tag` at that image:

```sh
helm upgrade --install azure-node-termination-handler charts/azure-node-termination-handler \
  --namespace kube-system \
  --set image.repository=myregistry.example.com/azure-node-termination-handler \
  --set image.tag=0.1.0
```

The DaemonSet runs on every Linux node (`nodeSelector: kubernetes.io/os: linux`),
tolerates every taint, and uses the `system-node-critical` priority class.

## Configuration

Every setting is a flag, an environment variable (the flag name upper-cased with
`-` replaced by `_`) and a Helm value. A flag overrides its env var, which
overrides the default. The chart sets `NODE_NAME` from `spec.nodeName`.

| Helm value | Flag / env var | Default | Meaning |
|---|---|---|---|
| — | `--node-name` / `NODE_NAME` | (required) | Name of this node |
| `vmName` | `--vm-name` / `VM_NAME` | `""` (ask IMDS) | VM name as listed in event `Resources` |
| `imdsEndpoint` | `--imds-endpoint` / `IMDS_ENDPOINT` | `http://169.254.169.254` | IMDS base URL |
| `pollInterval` | `--poll-interval` / `POLL_INTERVAL` | `5s` | Scheduled Events poll interval |
| `enableReboot` | `--enable-reboot` / `ENABLE_REBOOT` | `true` | Handle Reboot events |
| `enableRedeploy` | `--enable-redeploy` / `ENABLE_REDEPLOY` | `true` | Handle Redeploy events |
| `enablePreempt` | `--enable-preempt` / `ENABLE_PREEMPT` | `true` | Handle Preempt events |
| `enableTerminate` | `--enable-terminate` / `ENABLE_TERMINATE` | `true` | Handle Terminate events |
| `autoApprove` | `--auto-approve` / `AUTO_APPROVE` | `true` | Approve events after draining |
| `drainSafetyMargin` | `--drain-safety-margin` / `DRAIN_SAFETY_MARGIN` | `60s` | Drain deadline is `NotBefore` minus this |
| `maxDrainTime` | `--max-drain-time` / `MAX_DRAIN_TIME` | `60m` | Upper bound on the drain deadline, from when the event was first seen |
| `preemptSafetyMargin` | `--preempt-safety-margin` / `PREEMPT_SAFETY_MARGIN` | `5s` | Safety margin for Preempt events |
| `podGracePeriod` | `--pod-grace-period` / `POD_GRACE_PERIOD` | `-1` | Pod termination grace in seconds; `-1` uses each pod's own |
| `preemptPodGracePeriod` | `--preempt-pod-grace-period` / `PREEMPT_POD_GRACE_PERIOD` | `20` | Pod termination grace for Preempt drains |
| `deleteEmptyDirData` | `--delete-emptydir-data` / `DELETE_EMPTYDIR_DATA` | `true` | Evict pods that use emptyDir volumes |
| `ignoreDaemonSets` | `--ignore-daemonsets` / `IGNORE_DAEMONSETS` | `true` | Skip DaemonSet pods when draining |
| `eventGoneGrace` | `--event-gone-grace` / `EVENT_GONE_GRACE` | `5m` | How long an event must stay gone before the node is released |
| `webhookURL` | `--webhook-url` / `WEBHOOK_URL` | `""` | Post Drained and Approved notifications here |
| `webhookTemplate` | `--webhook-template` / `WEBHOOK_TEMPLATE` | `""` (Slack-compatible JSON) | Go `text/template` (with Sprig) for the webhook body |
| `metricsPort` | `--metrics-port` / `METRICS_PORT` | `9092` | Port of `/metrics` |
| `probesPort` | `--probes-port` / `PROBES_PORT` | `8080` | Port of `/livez` and `/healthz` |
| `uptimeFile` | `--uptime-file` / `UPTIME_FILE` | `/proc/uptime` | Source of the node's uptime |
| `dryRun` | `--dry-run` / `DRY_RUN` | `false` | Log actions instead of performing them |
| `logLevel` | `--log-level` / `LOG_LEVEL` | `info` | zerolog level |
| `kubeApiQps` | `--kube-api-qps` / `KUBE_API_QPS` | `50` | Kubernetes client rate limit (requests per second) |
| `kubeApiBurst` | `--kube-api-burst` / `KUBE_API_BURST` | `100` | Kubernetes client burst |

Other chart values: `image.*`, `serviceAccount.*`, `hostNetwork`,
`priorityClassName`, `nodeSelector`, `tolerations`, `resources`,
`podAnnotations`, `extraEnv`, `extraVolumes` and `extraVolumeMounts`.

The webhook template receives `.Stage`, `.NodeName`, `.VMName`, `.EventID`,
`.EventType`, `.Description`, `.NotBefore` and `.Message`. Webhook posts are
sent in the background by a single worker with a queue of 64; when the queue
is full a notification is dropped and counted (see Metrics).

### Probes

- `/livez` on `probesPort` always returns 200 while the process serves HTTP.
  It is the chart's liveness probe.
- `/healthz` on `probesPort` returns 200 once startup has succeeded (the
  after-reboot release check ran) and 503 before. It is the readiness probe.
- Both servers start before the VM name is resolved.

### Dry-run

With `dryRun: true` the agent never cordons, drains, uncordons, changes node
annotations or approves events. It logs what it would have done ("Would have
cordoned node", "Would have approved event `<id>`", ...). It keeps its would-be
node changes in memory, so each event is detected, cordoned, drained and
"approved" once rather than on every poll, and the withdrawn-event and
uncordon paths behave as they would for real. Kubernetes Events and webhook
notifications for Detected, Cordoned and Drained are still sent; Approved is
not. A dry-run approval is counted as `anth_approvals_total{result="dry_run"}`.
The in-memory state is lost when the pod restarts, so a still-listed event is
reported again after a restart.

### hostNetwork

Pods can reach `169.254.169.254` on Azure by default, so `hostNetwork` is off.
Set `hostNetwork: true` when a network policy or the CNI blocks pod traffic to
IMDS; the chart then also sets `dnsPolicy: ClusterFirstWithHostNet`. With
`hostNetwork` the metrics and probes ports bind on the node, so make sure
`metricsPort` and `probesPort` are free there. The agent never sends IMDS
traffic through `HTTP(S)_PROXY`.

## RBAC

The chart creates a ServiceAccount, ClusterRole and ClusterRoleBinding:

| API group | Resource | Verbs | Why |
|---|---|---|---|
| `""` | `nodes` | get, list, patch, update | read and annotate its node, cordon and uncordon, find peer nodes of multi-VM events |
| `""` | `pods` | get, list | drain |
| `""` | `pods/eviction` | create | drain |
| `""` | `events` | create, patch | Kubernetes Events on the node |
| `apps` | `daemonsets` | get | drain (identify DaemonSet pods) |
| `cluster.x-k8s.io` | `machines` | get | skip the uncordon when the node's Machine is being deleted |

No Azure identity or credentials are needed.

## Node annotations

All annotations use the `azure-node-termination-handler/` prefix and are set
on the agent's own node.

| Annotation | Value | Meaning |
|---|---|---|
| `event-id` | event ID | The event currently being handled |
| `event-type` | e.g. `Redeploy` | Its type |
| `first-seen` | unix seconds | When the agent first saw it (anchors `maxDrainTime`) |
| `drained-event-id` | event ID | The node finished draining for this event; peers read it for multi-VM events |
| `approved-event-id` | event ID | The agent approved this event |
| `cordoned` | `true` | The agent cordoned the node (only then will it uncordon it) |
| `action` | `uncordon-after-reboot` | Uncordon once the VM has rebooted |
| `action-time` | unix seconds | When `action` was set; a reboot is an uptime shorter than the time since then |
| `action-event-id` | event ID | The event the `action` marker belongs to |
| `event-gone-since` | unix seconds | When the current event disappeared from the document |
| `completed-event-id` | event ID | Last event completed by a reboot; kept after the release so a `Started` event still listed after the reboot is not handled again |

Every annotation except `completed-event-id` is removed when the node is
released.

## Kubernetes Events

The agent emits Events on its Node with the source
`azure-node-termination-handler` and these reasons: `Detected`, `Cordoned`,
`Drained`, `DrainTimeout` (Warning), `Approved`, `Uncordoned`,
`UncordonSkipped` (Warning) and `Freeze`. The webhook only receives `Drained`
and `Approved`.

## Metrics

Served on `:9092/metrics` (Prometheus format).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `anth_events_total` | counter | `type` (`Reboot`, `Redeploy`, `Preempt`, `Terminate`) | Events detected |
| `anth_actions_total` | counter | `action` (`cordon`, `drain`, `uncordon`), `result` (`ok`, `error`) | Node actions taken |
| `anth_drain_duration_seconds` | histogram | — | Duration of successful drains |
| `anth_drain_timeouts_total` | counter | — | Drains that hit the drain deadline |
| `anth_approvals_total` | counter | `result` (`ok`, `error`, `dry_run`) | Event approvals |
| `anth_errors_total` | counter | `op` | Errors by operation, see below |

`anth_errors_total` `op` values:

| `op` | Failing operation |
|---|---|
| `imds` | reading Scheduled Events |
| `get_node` | getting the agent's node |
| `cordon` | cordoning the node |
| `uncordon` | uncordoning the node |
| `drain` | draining the node (other than a timeout) |
| `annotate` | setting or removing node annotations |
| `list_nodes` | listing nodes to check peers of a multi-VM event |
| `approve` | approving an event |
| `machine` | checking the node's Cluster API Machine |
| `webhook_dropped` | a webhook notification dropped because the queue was full |

## Development

```sh
make build        # go build ./...
make test         # unit tests; no network or cluster needed
make vet
make helm-lint
make docker-build # image azure-node-termination-handler:dev (override with IMAGE=...)
```

### End-to-end tests

`make e2e` runs `scripts/e2e.sh`, which needs **docker, kind, helm and internet
access** (to pull images). It creates a dedicated kind cluster named
`anth-e2e`, builds the image and loads it into the cluster, deploys the fake
Scheduled Events server (`cmd/fake-imds`), installs the chart pointed at it,
and runs `go test -tags e2e ./test/e2e/`. It uses its own kubeconfig file and
never your current kubectl context. The cluster is deleted afterwards unless
`KEEP_CLUSTER=1` is set.

## Known limitations

- A drain blocks the poll loop for up to the drain deadline (at most
  `maxDrainTime`). While it runs, a Preempt cannot take over and the deadline
  sweep for other events does not run. This is a tracked follow-up.
- After a crash between approving an event and recording the approval, the
  event may be approved a second time. Repeating an approval is harmless.
- Without the uncordon-after-reboot marker (for example an event that was
  never approved, or one Azure completed without rebooting the VM), the node
  is only uncordoned once the event has been gone for the 5-minute
  event-gone grace.

## License

Apache-2.0. Parts are adapted from
[aws-node-termination-handler](https://github.com/aws/aws-node-termination-handler);
see [NOTICE](NOTICE).
