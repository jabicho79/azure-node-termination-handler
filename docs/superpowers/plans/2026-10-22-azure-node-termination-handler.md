# Azure Node Termination Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A per-node Go agent (DaemonSet) that watches Azure Scheduled Events for its own VM, cordons and drains the Kubernetes node, auto-approves the event, and uncordons the node when it comes back.

**Architecture:** Pure decision logic (`pkg/decision`) is separate from I/O adapters: the IMDS client (`pkg/imds`), Kubernetes node operations (`pkg/node`), the Cluster API Machine check (`pkg/capi`) and notifiers (`pkg/observability`, `pkg/webhook`). `pkg/handler` owns the per-tick state machine and talks to the adapters only through small interfaces. All state that has to survive a restart lives in node annotations. A fake IMDS server and a kind-based e2e suite test the whole thing without Azure.

**Tech Stack:** Go 1.25, client-go/kubectl drain, zerolog, prometheus/client_golang, sprig templates, Helm 3, kind.

**Spec:** `docs/superpowers/specs/2026-10-22-azure-node-termination-handler-design.md`

## Global Constraints

- Repo root: `~/Documents/Repos/azure-node-termination-handler`. Module path: `github.com/jabicho79/azure-node-termination-handler`.
- `go 1.25`. `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go`, `k8s.io/kubectl` all on the same minor, `>= v0.33`.
- Scheduled Events: `GET|POST {endpoint}/metadata/scheduledevents?api-version=2020-07-01`, header `Metadata: true`. Approve body: `{"StartRequests":[{"EventId":"<id>"}]}`. Default endpoint `http://169.254.169.254`.
- `NotBefore` is RFC1123 (`http.TimeFormat`, e.g. `Mon, 19 Sep 2016 18:29:47 GMT`) and is empty for `Started` events.
- No Azure SDK, credentials or managed identity anywhere.
- Every annotation, label, metric prefix and Kubernetes Event source uses `azure-node-termination-handler` (annotations: `azure-node-termination-handler/<key>`; metrics: `anth_<name>`).
- Defaults: poll 5s, drain safety margin 60s, max drain time 60m, Preempt safety margin 5s, Preempt pod grace 20s, event-gone grace 5m, auto-approve on, all of Reboot/Redeploy/Preempt/Terminate enabled.
- Freeze never cordons, drains or approves.
- Never let an event's deadline pass without approving it (when auto-approve is on and the event is still `Scheduled`).
- Only uncordon a node the agent cordoned itself, and never one whose CAPI Machine is being deleted.
- Linux only. The image is `gcr.io/distroless/static:nonroot`. The repo has an Apache-2.0 `LICENSE` and a `NOTICE` crediting aws-node-termination-handler for adapted code.
- Tests use only the stdlib `testing` package (no testify). Unit tests must not need network or a cluster.

## Review Focus

1. **IMDS unreachable, slow, or returning non-2xx/invalid JSON.** Expected: log it, increment `anth_errors_total{op="imds"}`, take no node action, and keep polling. A test for this goes in Task 2 (client error) and Task 8 (tick tolerates error).
2. **`NotBefore` empty or unparseable** (always the case for `Started` events). Expected: the drain deadline is the first-seen time, so the agent cordons only and doesn't drain. It never panics. A test goes in Task 3.
3. **Events that don't list this node's VM in `Resources`** (one shared document on multi-VM hosts, or the e2e fake). Expected: ignored completely. A test goes in Task 8.
4. **A node that someone else cordoned before the event arrived.** Expected: the agent doesn't set its `cordoned` annotation and never uncordons that node. A test goes in Task 9.
5. **Agent restart mid-drain or after approval** (annotations present, in-memory state gone). Expected: the drain resumes, the event is not approved twice, and a `Started` event that is still listed after the reboot release isn't handled again. Tests go in Task 8 and Task 9.

---

### Task 1: Project scaffold and configuration

**Files:**
- Create: `go.mod`, `Makefile`, `LICENSE` (Apache-2.0), `NOTICE`, `.gitignore`, `README.md` (one paragraph + link to the spec)
- Create: `pkg/config/config.go`
- Test: `pkg/config/config_test.go`

**Interfaces:**
- Produces:
  ```go
  type Config struct {
      NodeName              string        // flag --node-name, env NODE_NAME (required)
      VMName                string        // --vm-name, VM_NAME (optional; overrides IMDS compute name)
      IMDSEndpoint          string        // --imds-endpoint, IMDS_ENDPOINT, "http://169.254.169.254"
      PollInterval          time.Duration // --poll-interval, POLL_INTERVAL, 5s
      EnableReboot          bool          // --enable-reboot, ENABLE_REBOOT, true
      EnableRedeploy        bool          // ENABLE_REDEPLOY, true
      EnablePreempt         bool          // ENABLE_PREEMPT, true
      EnableTerminate       bool          // ENABLE_TERMINATE, true
      AutoApprove           bool          // AUTO_APPROVE, true
      DrainSafetyMargin     time.Duration // DRAIN_SAFETY_MARGIN, 60s
      MaxDrainTime          time.Duration // MAX_DRAIN_TIME, 60m
      PreemptSafetyMargin   time.Duration // PREEMPT_SAFETY_MARGIN, 5s
      PodGracePeriod        int           // POD_GRACE_PERIOD seconds, -1 (= pod's own)
      PreemptPodGracePeriod int           // PREEMPT_POD_GRACE_PERIOD, 20
      DeleteEmptyDirData    bool          // DELETE_EMPTYDIR_DATA, true
      IgnoreDaemonSets      bool          // IGNORE_DAEMONSETS, true
      EventGoneGrace        time.Duration // EVENT_GONE_GRACE, 5m
      WebhookURL            string        // WEBHOOK_URL, ""
      WebhookTemplate       string        // WEBHOOK_TEMPLATE, "" (= webhook.DefaultTemplate)
      MetricsPort           int           // METRICS_PORT, 9092
      ProbesPort            int           // PROBES_PORT, 8080
      UptimeFile            string        // UPTIME_FILE, "/proc/uptime"
      DryRun                bool          // DRY_RUN, false
      LogLevel              string        // LOG_LEVEL, "info"
  }
  func Parse(args []string, getenv func(string) string) (Config, error)
  ```
  Flag names are the env var lowercased with `_` → `-`. A flag overrides its env var, and the env var overrides the default.

- [ ] **Step 1: `go mod init github.com/jabicho79/azure-node-termination-handler`**, add LICENSE/NOTICE/.gitignore/README. Makefile targets: `build` (`go build ./...`), `test` (`go test ./...`), `vet`, `docker-build`, `helm-lint`, `e2e` (bodies filled in by later tasks; leave `docker-build`/`helm-lint`/`e2e` as `@echo TODO` only until their task).
- [ ] **Step 2: Write failing tests** in `pkg/config/config_test.go`:
  - `TestParseDefaults`: `Parse(nil, env{"NODE_NAME":"n1"})` → `PollInterval==5*time.Second`, `DrainSafetyMargin==60*time.Second`, `MaxDrainTime==60*time.Minute`, `PreemptSafetyMargin==5*time.Second`, `PreemptPodGracePeriod==20`, `PodGracePeriod==-1`, `EventGoneGrace==5*time.Minute`, `AutoApprove`, all four `Enable*` true, `IMDSEndpoint=="http://169.254.169.254"`, `MetricsPort==9092`, `ProbesPort==8080`, `UptimeFile=="/proc/uptime"`.
  - `TestParseEnvOverride`: env `POLL_INTERVAL=2s`, `AUTO_APPROVE=false` → those values.
  - `TestParseFlagBeatsEnv`: env `POLL_INTERVAL=2s`, args `--poll-interval=3s` → `3s`.
  - `TestParseRequiresNodeName`: empty env → error mentioning `node-name`.
  - `TestParseRejectsNonPositiveDurations`: `POLL_INTERVAL=0s` → error.
- [ ] **Step 3: Run** `go test ./pkg/config/...` → FAIL (undefined `Parse`).
- [ ] **Step 4: Implement `Parse`** with a fresh `flag.FlagSet` (`ContinueOnError`). Each flag's default comes from `getenv` when it's set, otherwise from the constant.
- [ ] **Step 5: Run** `go test ./pkg/config/... && go vet ./...` → PASS.
- [ ] **Step 6: Commit** `feat: project scaffold and configuration`.

---

### Task 2: IMDS client

**Files:**
- Create: `pkg/imds/types.go`, `pkg/imds/client.go`
- Test: `pkg/imds/client_test.go`, `pkg/imds/types_test.go`

**Interfaces:**
- Produces:
  ```go
  type EventType string   // Reboot, Redeploy, Freeze, Preempt, Terminate
  type EventStatus string // Scheduled, Started
  const (EventReboot EventType = "Reboot"; EventRedeploy = "Redeploy"; EventFreeze = "Freeze"; EventPreempt = "Preempt"; EventTerminate = "Terminate")
  const (StatusScheduled EventStatus = "Scheduled"; StatusStarted = "Started")
  type Event struct {
      EventID           string      `json:"EventId"`
      EventType         EventType   `json:"EventType"`
      ResourceType      string      `json:"ResourceType"`
      Resources         []string    `json:"Resources"`
      EventStatus       EventStatus `json:"EventStatus"`
      NotBefore         string      `json:"NotBefore"`
      Description       string      `json:"Description"`
      EventSource       string      `json:"EventSource"`
      DurationInSeconds int         `json:"DurationInSeconds"`
  }
  func (e Event) NotBeforeTime() (time.Time, bool) // http.TimeFormat parse; false if empty/invalid
  func (e Event) Affects(vmName string) bool       // case-insensitive membership in Resources
  type Document struct { DocumentIncarnation int `json:"DocumentIncarnation"`; Events []Event `json:"Events"` }
  type Client struct { /* endpoint, *http.Client */ }
  func NewClient(endpoint string, hc *http.Client) *Client
  func (c *Client) ScheduledEvents(ctx context.Context) (Document, error)
  func (c *Client) Approve(ctx context.Context, eventID string) error
  func (c *Client) ComputeName(ctx context.Context) (string, error) // GET /metadata/instance/compute/name?api-version=2021-02-01&format=text
  ```

- [ ] **Step 1: Write failing tests** (all with `httptest.Server`):
  - `TestScheduledEventsParses`: the server asserts `r.Header.Get("Metadata")=="true"`, the path `/metadata/scheduledevents` and `r.URL.Query().Get("api-version")=="2020-07-01"`. It returns the doc `{"DocumentIncarnation":3,"Events":[{"EventId":"E1","EventType":"Redeploy","ResourceType":"VirtualMachine","Resources":["pool_3"],"EventStatus":"Scheduled","NotBefore":"Mon, 19 Sep 2016 18:29:47 GMT","Description":"degraded hardware","EventSource":"Platform","DurationInSeconds":-1}]}`. The test asserts every field.
  - `TestApprovePostsStartRequest`: the server asserts method POST, the header and the body JSON exactly `{"StartRequests":[{"EventId":"E1"}]}`.
  - `TestScheduledEventsNon2xx`: status 500 → the error is non-nil and contains `500`.
  - `TestScheduledEventsBadJSON`: body `{` → error.
  - `TestScheduledEventsTimeout`: the handler sleeps longer than the client's ctx deadline → error (Review Focus 1).
  - `TestComputeName`: returns the body `pool_3` → `"pool_3"`.
  - `TestNotBeforeTime`: valid RFC1123 → the matching `time.Time`, `true`; `""` → `false`; `"garbage"` → `false`.
  - `TestAffectsCaseInsensitive`: Resources `["Pool_3"]`, `Affects("pool_3")==true`, `Affects("pool_4")==false`.
- [ ] **Step 2: Run** `go test ./pkg/imds/...` → FAIL.
- [ ] **Step 3: Implement** the types and the client. Default `http.Client` timeout when `hc` is nil: 5s. Treat any status outside 2xx as an error.
- [ ] **Step 4: Run** `go test ./pkg/imds/...` → PASS.
- [ ] **Step 5: Commit** `feat: Azure Scheduled Events client`.

---

### Task 3: Decision logic

**Files:**
- Create: `pkg/decision/decision.go`
- Test: `pkg/decision/decision_test.go`

**Interfaces:**
- Consumes: `imds.Event`, `imds.EventType`, `imds.StatusStarted`, `Event.NotBeforeTime()`.
- Produces:
  ```go
  type Settings struct {
      Enabled               map[imds.EventType]bool
      AutoApprove           bool
      SafetyMargin          time.Duration
      PreemptSafetyMargin   time.Duration
      MaxDrainTime          time.Duration
      PodGracePeriod        int
      PreemptPodGracePeriod int
  }
  type Plan struct {
      Cordon, Drain, Approve, MarkUncordonAfterReboot bool
      DrainDeadline  time.Time // anchored on firstSeen; caller compares with its own now
      PodGracePeriod int       // seconds; -1 = pod default
  }
  func Decide(e imds.Event, s Settings, firstSeen time.Time) Plan
  ```

Rules (from the spec's event table):

| Type | Cordon | Drain | Approve | MarkUncordonAfterReboot | Margin | PodGrace |
|---|---|---|---|---|---|---|
| Reboot, Redeploy | ✓ | ✓ | AutoApprove | ✓ | SafetyMargin | PodGracePeriod |
| Preempt | ✓ | ✓ | AutoApprove | ✓ (Spot `Deallocate`) | PreemptSafetyMargin | PreemptPodGracePeriod |
| Terminate | ✓ | ✓ | AutoApprove | ✗ | SafetyMargin | PodGracePeriod |
| Freeze, disabled type, unknown type | zero `Plan{}` | | | | | |

`DrainDeadline = min(NotBefore − margin, firstSeen + MaxDrainTime)`. If `NotBefore` is missing or invalid, `DrainDeadline = firstSeen`. `EventStatus == Started` forces `Approve = false`.

- [ ] **Step 1: Write table-driven failing test `TestDecide`.** Use `firstSeen = 2026-10-22T10:00:00Z`, defaults from Global Constraints and `MaxDrainTime=60m`. Cases:
  - Redeploy, NotBefore firstSeen+7 days → `Cordon, Drain, Approve, MarkUncordonAfterReboot` true, `DrainDeadline == firstSeen+60m`, `PodGracePeriod == -1`.
  - Reboot, NotBefore firstSeen+10m → `DrainDeadline == firstSeen+9m`.
  - Preempt, NotBefore firstSeen+30s → `DrainDeadline == firstSeen+25s`, `PodGracePeriod == 20`, `MarkUncordonAfterReboot` true.
  - Terminate, NotBefore +10m → `MarkUncordonAfterReboot` false, Approve true.
  - Freeze → `Plan{}`.
  - Redeploy with `Enabled[Redeploy]=false` → `Plan{}`.
  - Redeploy with `AutoApprove=false` → `Approve` false, `Cordon/Drain` true.
  - Redeploy `EventStatus=Started`, `NotBefore=""` → `Cordon` true, `Approve` false, `DrainDeadline == firstSeen` (Review Focus 2).
  - Reboot `NotBefore="garbage"` → `DrainDeadline == firstSeen`, no panic.
- [ ] **Step 2: Run** `go test ./pkg/decision/...` → FAIL.
- [ ] **Step 3: Implement `Decide`.**
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat: per-event decision logic`.

---

### Task 4: Provider ID → VM name mapping

**Files:**
- Create: `pkg/node/providerid.go`
- Test: `pkg/node/providerid_test.go`

**Interfaces:**
- Produces:
  ```go
  // Uniform: azure:///subscriptions/s/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/<vmss>/virtualMachines/<id> → "<vmss>_<id>"
  // Flexible & standalone: .../providers/Microsoft.Compute/virtualMachines/<name> → "<name>"
  func VMNameFromProviderID(providerID string) (string, bool)
  // Keys of found are the lowercased VM names; missing keeps the original spelling.
  func NodesForVMs(nodes []corev1.Node, vmNames []string) (found map[string]*corev1.Node, missing []string)
  ```

- [ ] **Step 1: Write failing tests:**
  - `TestVMNameFromProviderID`: the Uniform example above with `vmss=pool`, `id=3` → `"pool_3", true`. Flexible `.../virtualMachines/pool_a1b2c3` → `"pool_a1b2c3", true`. Mixed-case path segments (`microsoft.compute`, `VirtualMachineScaleSets`) → still parse. `kind://docker/x/y` → `"", false`. `""` → `false`.
  - `TestNodesForVMs`: nodes with providerIDs for `pool_3` (Uniform) and `md-abc` (Flexible) plus one `kind://` node. `vmNames=["POOL_3","md-abc","other"]` → found has keys `pool_3`, `md-abc`, and `missing == ["other"]`.
- [ ] **Step 2: Run** `go test ./pkg/node/...` → FAIL.
- [ ] **Step 3: Implement** by splitting on `/` and matching segment names case-insensitively.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat: map Azure provider IDs to VM names`.

---

### Task 5: Kubernetes node operations

**Files:**
- Create: `pkg/node/annotations.go`, `pkg/node/ops.go`
- Test: `pkg/node/ops_test.go`

**Interfaces:**
- Produces:
  ```go
  const (
      Prefix                    = "azure-node-termination-handler/"
      AnnEventID                = Prefix + "event-id"
      AnnEventType              = Prefix + "event-type"
      AnnFirstSeen              = Prefix + "first-seen"         // unix seconds
      AnnDrainedEventID         = Prefix + "drained-event-id"
      AnnApprovedEventID        = Prefix + "approved-event-id"
      AnnCordoned               = Prefix + "cordoned"           // "true" only if we cordoned it
      AnnAction                 = Prefix + "action"
      AnnActionTime             = Prefix + "action-time"        // unix seconds
      AnnEventGoneSince         = Prefix + "event-gone-since"   // unix seconds
      AnnCompletedEventID       = Prefix + "completed-event-id" // survives release
      ActionUncordonAfterReboot = "uncordon-after-reboot"
  )
  // WorkingAnnotations lists every key above except AnnCompletedEventID.
  var WorkingAnnotations []string
  type DrainOptions struct { Deadline time.Time; PodGracePeriodSeconds int; DeleteEmptyDirData, IgnoreDaemonSets bool }
  type Ops struct { /* kubernetes.Interface, dryRun bool */ }
  func NewOps(client kubernetes.Interface, dryRun bool) *Ops
  func (o *Ops) Get(ctx context.Context, name string) (*corev1.Node, error)
  func (o *Ops) ListNodes(ctx context.Context) ([]corev1.Node, error)
  func (o *Ops) Cordon(ctx context.Context, name string) (alreadyCordoned bool, err error)
  func (o *Ops) Uncordon(ctx context.Context, name string) error
  func (o *Ops) Drain(ctx context.Context, name string, opts DrainOptions) error // returns context.DeadlineExceeded (wrapped) on timeout
  func (o *Ops) SetAnnotations(ctx context.Context, name string, kv map[string]string) error
  func (o *Ops) RemoveAnnotations(ctx context.Context, name string, keys ...string) error
  ```
  Drain uses `k8s.io/kubectl/pkg/drain.Helper` with `Force: true`, `Timeout: time.Until(opts.Deadline)`, `Ctx` carrying the same deadline, and zerolog writers (adapt NTH `pkg/node/node.go:743`). Annotation changes use JSON merge patches; removal sets the key to `null`. In dry-run, every mutating method logs and returns nil.

- [ ] **Step 1: Write failing tests** against `k8s.io/client-go/kubernetes/fake`:
  - `TestCordonReportsPriorState`: a schedulable node → `(false, nil)` and `Spec.Unschedulable` becomes true. Calling it again → `(true, nil)`.
  - `TestUncordon`: becomes schedulable.
  - `TestAnnotationsRoundTrip`: set `{AnnEventID:"E1", AnnFirstSeen:"1"}`, then remove `AnnFirstSeen` → only `AnnEventID` is left, and annotations the agent doesn't own are untouched.
  - `TestDrainEvictsPods`: two pods on the node, one owned by a DaemonSet. A fake reactor on `create pods/eviction` deletes the pod from the tracker. After `Drain`, only the DaemonSet pod is left.
  - `TestDrainTimesOut`: the eviction reactor returns a 429 TooManyRequests (what a PDB does). Deadline now+1s → the returned error satisfies `errors.Is(err, context.DeadlineExceeded)`.
  - `TestDryRunDoesNotMutate`: `NewOps(c, true)`, then Cordon/SetAnnotations → the node is unchanged.
- [ ] **Step 2: Run** `go test ./pkg/node/...` → FAIL.
- [ ] **Step 3: Implement.** Map the drain helper's timeout error to a wrapped `context.DeadlineExceeded`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat: node cordon, drain and annotation operations`.

---

### Task 6: Cluster API Machine check

**Files:**
- Create: `pkg/capi/machine.go`
- Test: `pkg/capi/machine_test.go`

**Interfaces:**
- Produces:
  ```go
  const (AnnMachine = "cluster.x-k8s.io/machine"; AnnClusterNamespace = "cluster.x-k8s.io/cluster-namespace")
  type MachineChecker struct { /* dynamic.Interface */ }
  func NewMachineChecker(client dynamic.Interface) *MachineChecker
  // true only if the node's Machine exists and has a deletionTimestamp.
  // Missing annotations, missing CRD, or missing Machine → (false, nil).
  func (m *MachineChecker) BeingDeleted(ctx context.Context, node *corev1.Node) (bool, error)
  ```
  The checker tries GVR `cluster.x-k8s.io/v1beta2 machines` first. On NotFound it tries `v1beta1`. If both return NotFound, it returns `(false, nil)`.

- [ ] **Step 1: Write failing tests** with `k8s.io/client-go/dynamic/fake`:
  - `TestNoAnnotations` → `false, nil`.
  - `TestMachineDeleting`: a v1beta2 Machine `m1` in `ns1` with `metadata.deletionTimestamp` set, and the node annotated with `m1`/`ns1` → `true`.
  - `TestMachineNotDeleting` → `false`.
  - `TestV1beta1Fallback`: the Machine is registered only under v1beta1 and is deleting → `true`.
  - `TestNoCRD`: no resources registered → `false, nil`.
- [ ] **Step 2: Run** `go test ./pkg/capi/...` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat: skip uncordon for Machines being deleted`.

---

### Task 7: Observability and webhook

**Files:**
- Create: `pkg/observability/metrics.go`, `pkg/observability/notifier.go`, `pkg/webhook/webhook.go`
- Test: `pkg/observability/metrics_test.go`, `pkg/observability/notifier_test.go`, `pkg/webhook/webhook_test.go`

**Interfaces:**
- Produces:
  ```go
  // observability
  type Stage string
  const (StageDetected Stage = "Detected"; StageCordoned = "Cordoned"; StageDrained = "Drained"; StageDrainTimeout = "DrainTimeout"; StageApproved = "Approved"; StageUncordoned = "Uncordoned"; StageUncordonSkipped = "UncordonSkipped"; StageFreeze = "Freeze")
  type Notification struct { Stage Stage; NodeName, VMName, EventID, EventType, Description, NotBefore, Message string }
  type Notifier interface { Notify(ctx context.Context, n Notification) }
  type Multi []Notifier                                                  // Notify fans out
  func NewEventRecorder(client kubernetes.Interface, nodeName string) Notifier // k8s Event on the Node, reason = string(Stage), Warning for DrainTimeout/UncordonSkipped, Normal otherwise, source component "azure-node-termination-handler"
  type Metrics struct{ /* ... */ }
  func NewMetrics(reg prometheus.Registerer) *Metrics
  func (m *Metrics) EventSeen(eventType string)            // anth_events_total{type}
  func (m *Metrics) Action(action, result string)          // anth_actions_total{action,result}; result "ok"|"error"
  func (m *Metrics) DrainDuration(d time.Duration)         // anth_drain_duration_seconds histogram
  func (m *Metrics) DrainTimeout()                         // anth_drain_timeouts_total
  func (m *Metrics) Approval(result string)                // anth_approvals_total{result}
  func (m *Metrics) Error(op string)                       // anth_errors_total{op}
  // webhook
  const DefaultTemplate = `{"text": {{ printf "[azure-node-termination-handler] %s: node %s (VM %s), %s event %s. %s" .Stage .NodeName .VMName .EventType .EventID .Description | toJson }}}`
  func New(url, tmpl string, hc *http.Client) (*Webhook, error) // tmpl "" → DefaultTemplate; parse with text/template + sprig.TxtFuncMap
  func (w *Webhook) Notify(ctx context.Context, n observability.Notification) // posts only for StageDrained and StageApproved; Content-Type application/json; errors are logged, never returned
  ```

- [ ] **Step 1: Write failing tests:**
  - `TestMetricsRegistered`: `NewMetrics(prometheus.NewRegistry())`. Call each method once, then `testutil.ToFloat64` on `anth_events_total{type="Redeploy"}` == 1 and on `anth_approvals_total{result="ok"}` == 1.
  - `TestEventRecorderEmitsNodeEvent`: fake clientset; `Notify(StageDrainTimeout)` → an `Event` exists with `involvedObject.kind=="Node"`, `reason=="DrainTimeout"` and `type=="Warning"`. Use a synchronous recorder, or poll for up to 2s.
  - `TestMultiFansOut`: two recording notifiers both receive the notification.
  - `TestWebhookPostsOnlyDrainedAndApproved`: an httptest server counts requests. Send Detected, Drained, Approved → count == 2.
  - `TestWebhookDefaultTemplateEscapes`: Description `say "hi"\n` → the received body passes `json.Valid` and `.text` contains `say "hi"`.
  - `TestWebhookBadTemplate`: `New(url, "{{", nil)` → error.
- [ ] **Step 2: Run** `go test ./pkg/observability/... ./pkg/webhook/...` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat: metrics, node events and webhook notifications`.

---

### Task 8: Handler — event processing and approval

**Files:**
- Create: `pkg/handler/handler.go`, `pkg/handler/fakes_test.go`
- Test: `pkg/handler/handler_test.go`

**Interfaces:**
- Consumes: `imds.Document/Event`, `decision.Decide/Settings/Plan`, `node.*` annotation constants, `node.DrainOptions`, `node.NodesForVMs`, `observability.Notifier/Notification/Metrics`.
- Produces:
  ```go
  type IMDS interface { ScheduledEvents(ctx context.Context) (imds.Document, error); Approve(ctx context.Context, eventID string) error }
  type Nodes interface { // satisfied by *node.Ops
      Get(ctx context.Context, name string) (*corev1.Node, error)
      ListNodes(ctx context.Context) ([]corev1.Node, error)
      Cordon(ctx context.Context, name string) (bool, error)
      Uncordon(ctx context.Context, name string) error
      Drain(ctx context.Context, name string, opts node.DrainOptions) error
      SetAnnotations(ctx context.Context, name string, kv map[string]string) error
      RemoveAnnotations(ctx context.Context, name string, keys ...string) error
  }
  type Machines interface { BeingDeleted(ctx context.Context, n *corev1.Node) (bool, error) } // satisfied by *capi.MachineChecker
  type Deps struct { IMDS IMDS; Nodes Nodes; Machines Machines; Notifier observability.Notifier; Metrics *observability.Metrics; Now func() time.Time; Uptime func() (time.Duration, error) }
  type Options struct { NodeName, VMName string; Decision decision.Settings; DeleteEmptyDirData, IgnoreDaemonSets, DryRun bool; EventGoneGrace time.Duration }
  func New(d Deps, o Options) *Handler
  func (h *Handler) Tick(ctx context.Context) error
  ```

`Tick` algorithm:
1. Fetch the document. On error, `Metrics.Error("imds")` and return the error without touching the node.
2. Keep only the events where `e.Affects(o.VMName)`. For a Freeze event, send `StageFreeze` once per EventID (in-memory set) and otherwise ignore it. Also drop any event whose ID equals the node's `AnnCompletedEventID`.
3. If the node has `AnnEventID` and that event is no longer in the kept set, run the gone-handling hook (Task 9; in this task it's a no-op method `h.handleGone`) and return.
4. Pick the **current** event: the one matching `AnnEventID` if it is present, else the one with the earliest `NotBefore` (events without a `NotBefore` come first).
5. If it's new (`AnnEventID` differs): set `AnnEventID`, `AnnEventType` and `AnnFirstSeen=now`, then call `EventSeen` and notify `Detected`. `firstSeen` comes from `AnnFirstSeen`.
6. `plan := decision.Decide(e, settings, firstSeen)`.
7. If `plan.Cordon` and the node is schedulable: call `Cordon`. If it was not already cordoned, set `AnnCordoned="true"`. Notify `Cordoned`.
8. If `plan.Drain`, `AnnDrainedEventID != e.EventID`, and `now < plan.DrainDeadline`: drain until `plan.DrainDeadline`. On success, set `AnnDrainedEventID`, record `DrainDuration` and notify `Drained`. On `DeadlineExceeded`, call `DrainTimeout()` and notify `DrainTimeout`. Any other error: record `Error("drain")` and return it.
9. If `plan.Approve`, `!o.DryRun` and `AnnApprovedEventID != e.EventID`: `ready := now >= plan.DrainDeadline || (selfDone && peersDrained)`. `selfDone` is true when `AnnDrainedEventID==e.EventID` or when `!plan.Drain`. `peersDrained` means every node returned by `NodesForVMs(ListNodes(), e.Resources minus own VM)` has `AnnDrainedEventID==e.EventID`; missing VMs are logged at warn and ignored. When ready: if `plan.MarkUncordonAfterReboot`, set `AnnAction` and `AnnActionTime=now`. Then call `Approve`, set `AnnApprovedEventID`, call `Approval("ok")` and notify `Approved`.

`fakes_test.go` holds in-memory fakes: `fakeIMDS` (scripted documents, records approvals, can return an error) and `fakeNodes` (map of nodes, records calls, drain can return nil, `DeadlineExceeded` or block). It also holds a `fakeMachines` and a recording notifier. Use a controllable clock.

- [ ] **Step 1: Write failing tests:**
  - `TestRedeployCordonDrainApprove`: Redeploy for `vm-a` with NotBefore +7d → the node is unschedulable, the drain was called with `Deadline == now+60m`, approvals `["E1"]`, `AnnAction == "uncordon-after-reboot"`, and notifications are Detected, Cordoned, Drained, Approved in that order.
  - `TestIgnoresEventsForOtherVMs` (Review Focus 3): event Resources `["vm-b"]`, own `vm-a` → no calls to Cordon/Drain/Approve.
  - `TestFreezeNoAction`: Freeze for `vm-a` → no cordon/drain/approve. Exactly one `StageFreeze` across two ticks.
  - `TestDrainTimeoutApprovesAtDeadline`: drain returns `DeadlineExceeded`. Advance the clock past the deadline → approved, and `DrainTimeout` was notified.
  - `TestWaitsForPeersBeforeApproving`: Resources `["vm-a","vm-b"]`. Node b doesn't yet have `AnnDrainedEventID=E1` → no approval. Annotate node b and tick → approved.
  - `TestPeersIgnoredWhenNotInCluster`: Resources `["vm-a","vm-x"]`, and no node maps to `vm-x` → approved after the own drain.
  - `TestAutoApproveOff`: drained but never approved, even after the deadline.
  - `TestStartedEventNotApproved`: Started, NotBefore `""` → cordoned, no drain, no approve.
  - `TestPreemptUsesShortGrace`: drain opts `PodGracePeriodSeconds == 20`, `Deadline == notBefore-5s`.
  - `TestIMDSErrorNoNodeAction` (Review Focus 1): fakeIMDS returns an error → Tick returns the error, there are no node calls and `anth_errors_total{op="imds"} == 1`.
  - `TestRestartResumesDrainWithoutDoubleApprove` (Review Focus 5): the node starts with `AnnEventID=E1`, `AnnFirstSeen` set and no `AnnDrainedEventID` → drain is called, followed by one approval. A second tick sees `AnnApprovedEventID=E1` → still exactly one approval.
  - `TestDryRunNeverApproves`.
- [ ] **Step 2: Run** `go test ./pkg/handler/...` → FAIL.
- [ ] **Step 3: Implement `New` and `Tick`.** Re-fetch the node after each mutation, or keep a local copy of the annotations in sync, so later steps see fresh values.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat: handle scheduled events with drain and approval`.

---

### Task 9: Handler — release paths (reboot, withdrawn event, startup)

**Files:**
- Modify: `pkg/handler/handler.go` (fill in `handleGone`, add `release`, `Startup`, `Run`)
- Create: `pkg/uptime/uptime.go`
- Test: `pkg/handler/release_test.go`, `pkg/uptime/uptime_test.go`

**Interfaces:**
- Produces:
  ```go
  func (h *Handler) Startup(ctx context.Context) error
  func (h *Handler) Run(ctx context.Context, interval time.Duration) error // Startup (retry each interval until it succeeds), then Tick each interval; Tick errors are logged; returns nil on ctx.Done()
  // pkg/uptime
  func FromFile(path string) func() (time.Duration, error) // parses the first float field of /proc/uptime format "12345.67 890.12"
  ```

Behaviour:
- `release(node, completed bool)`: if `Machines.BeingDeleted` → notify `UncordonSkipped`, remove `WorkingAnnotations` and leave the node cordoned. Else if `AnnCordoned=="true"` → `Uncordon` and notify `Uncordoned`. Always remove `WorkingAnnotations`. If `completed`, set `AnnCompletedEventID` to the event ID.
- `rebooted(node)`: `AnnAction==ActionUncordonAfterReboot` and `uptime < now − AnnActionTime`.
- `Startup`: if `rebooted(node)` → `release(node, completed=true)`.
- `handleGone(node)` (called from Tick step 3): if `AnnEventGoneSince` is absent, set it to now and return. If `now − goneSince < EventGoneGrace`, return. Otherwise `release(node, completed=false)`.
- In Tick, when the current event is present again and `AnnEventGoneSince` is set → remove `AnnEventGoneSince`.

- [ ] **Step 1: Write failing tests:**
  - `TestUptimeFromFile`: file `"5.50 1.00\n"` → `5.5s`. Missing file → error. `"abc"` → error.
  - `TestStartupUncordonsAfterReboot`: annotations: action marker with `AnnActionTime = now-10m`, `AnnCordoned=true`, `AnnEventID=E1`; uptime 2m → the node is schedulable, every working annotation is gone, and `AnnCompletedEventID == "E1"`.
  - `TestStartupNoRebootLeavesNode`: uptime 3h, same annotations → unchanged.
  - `TestStartedEventAfterRebootNotReprocessed` (Review Focus 5): after the release above, the doc still lists E1 as `Started` → Tick does nothing.
  - `TestStartupSkipsUncordonWhenMachineDeleting`: `fakeMachines` returns true → the node stays unschedulable, annotations are cleared and `UncordonSkipped` is notified.
  - `TestNeverUncordonsForeignCordon` (Review Focus 4): the node is unschedulable before the event → `AnnCordoned` is never set. After the release, the node is still unschedulable.
  - `TestWithdrawnEventUncordonsAfterGrace`: the event is processed and approved, then removed from the doc. Tick at +1m → `AnnEventGoneSince` is set and the node is still cordoned. Tick at +5m → schedulable, working annotations removed, `AnnCompletedEventID` not set.
  - `TestEventReappearsClearsGoneSince`: gone at +1m, back at +2m → `AnnEventGoneSince` removed, node still cordoned.
  - `TestRunStopsOnCancel`: `Run` with a cancelled ctx returns nil.
- [ ] **Step 2: Run** `go test ./pkg/handler/... ./pkg/uptime/...` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** `go test ./...` → PASS.
- [ ] **Step 5: Commit** `feat: uncordon after reboot or withdrawn events`.

---

### Task 10: Binary, probes and container image

**Files:**
- Create: `cmd/azure-node-termination-handler/main.go`, `Dockerfile`
- Modify: `Makefile` (`docker-build`)

**Interfaces:**
- Consumes: `config.Parse`, `imds.NewClient` / `ComputeName`, `node.NewOps`, `capi.NewMachineChecker`, `observability.NewMetrics` / `NewEventRecorder` / `Multi`, `webhook.New`, `uptime.FromFile`, `handler.New` / `Run`.

- [ ] **Step 1: Write `main.go`.** `config.Parse(os.Args[1:], os.Getenv)`. Set the zerolog global level from `LogLevel` and use JSON output. Build an in-cluster `rest.Config` → clientset plus dynamic client. Resolve `VMName`: use `cfg.VMName`, else `ComputeName`, retried every `PollInterval` until it succeeds or ctx ends. Serve `:{MetricsPort}/metrics` (promhttp on a dedicated registry) and `:{ProbesPort}/healthz` (200 once `Startup` has succeeded, 503 before). Add the webhook to the notifiers only when `WebhookURL != ""`. Use `signal.NotifyContext` for SIGINT/SIGTERM, then `handler.Run`.
- [ ] **Step 2: Dockerfile**: multi-stage. Build `golang:1.25` with `CGO_ENABLED=0` and `-trimpath -ldflags "-s -w"`, producing both `azure-node-termination-handler` and `fake-imds` (Task 11 adds `cmd/fake-imds`; until then the Dockerfile builds only the first). Runtime: `gcr.io/distroless/static:nonroot`, entrypoint `/azure-node-termination-handler`. Makefile `docker-build`: `docker build -t $(IMAGE) .` with `IMAGE ?= azure-node-termination-handler:dev`.
- [ ] **Step 3: Verify** `go build ./... && go vet ./... && make docker-build` → succeeds. `docker run --rm azure-node-termination-handler:dev` exits non-zero with an error mentioning `node-name`.
- [ ] **Step 4: Commit** `feat: agent binary and container image`.

---

### Task 11: Fake Scheduled Events server

**Files:**
- Create: `pkg/fakeimds/server.go`, `cmd/fake-imds/main.go`
- Modify: `Dockerfile` (also build `/fake-imds`)
- Test: `pkg/fakeimds/server_test.go`

**Interfaces:**
- Produces:
  ```go
  func NewServer() *Server // http.Handler
  // GET  /metadata/scheduledevents         → current Document; 400 without "Metadata: true" or api-version
  // POST /metadata/scheduledevents         → records each StartRequests EventId and sets that event's EventStatus to Started; same header rules
  // GET  /metadata/instance/compute/name   → "fake-vm"
  // PUT  /control/document                 → replaces Events from the body (imds.Document); DocumentIncarnation++
  // GET  /control/approvals                → JSON []string
  // DELETE /control/approvals              → reset
  ```
  `cmd/fake-imds` listens on `:8080`.

- [ ] **Step 1: Write failing tests** that drive `imds.Client` against `httptest.NewServer(fakeimds.NewServer())`:
  - `TestRoundTrip`: PUT a document with E1, then `ScheduledEvents` returns E1 with incarnation 1. `Approve("E1")` → GET `/control/approvals` returns `["E1"]` and E1 shows as Started.
  - `TestRequiresMetadataHeader`: a raw GET without the header → 400.
- [ ] **Step 2: Run** `go test ./pkg/fakeimds/...` → FAIL.
- [ ] **Step 3: Implement** with a mutex-guarded state.
- [ ] **Step 4: Run** → PASS. `make docker-build` → the image contains `/fake-imds`.
- [ ] **Step 5: Commit** `test: fake Scheduled Events server`.

---

### Task 12: Helm chart

**Files:**
- Create: `charts/azure-node-termination-handler/{Chart.yaml,values.yaml,templates/_helpers.tpl,templates/daemonset.yaml,templates/serviceaccount.yaml,templates/clusterrole.yaml,templates/clusterrolebinding.yaml}`
- Modify: `Makefile` (`helm-lint`)

Requirements:
- `values.yaml` has one key per `Config` field except `NodeName` (that comes from the downward API `spec.nodeName`). The keys are camelCase and the defaults are identical to Task 1. It also has `image.repository: ghcr.io/jabicho79/azure-node-termination-handler`, `image.tag: ""` (falls back to `.Chart.AppVersion`), `hostNetwork: false`, `priorityClassName: system-node-critical`, `nodeSelector: {kubernetes.io/os: linux}`, `tolerations: [{operator: Exists}]`, `resources`, `extraEnv: []` (rendered verbatim, so `valueFrom` works), and `extraVolumes` / `extraVolumeMounts: []`.
- The DaemonSet has liveness and readiness on `/healthz:{probesPort}` and a metrics container port. It runs as non-root with a read-only root filesystem.
- ClusterRole: `nodes` get/list/patch/update; `pods` get/list; `pods/eviction` create; `events` create/patch; `apps` `daemonsets` get (needed by the drain helper); `cluster.x-k8s.io` `machines` get.

- [ ] **Step 1: Write the chart.**
- [ ] **Step 2: Verify** `make helm-lint` (`helm lint charts/azure-node-termination-handler`) → 0 failures. Then check `helm template t charts/azure-node-termination-handler | grep -E 'machines|pods/eviction|AUTO_APPROVE|system-node-critical'` → all four appear. `helm template t charts/... --set hostNetwork=true | grep 'hostNetwork: true'` → matches.
- [ ] **Step 3: Commit** `feat: Helm chart`.

---

### Task 13: End-to-end tests on kind

**Files:**
- Create: `test/e2e/kind-config.yaml` (1 control-plane, 2 workers), `test/e2e/manifests/fake-imds.yaml` (Deployment + ClusterIP Service `fake-imds.anth-e2e:8080`, scheduled on the control plane, plus a NodePort `30080` mapped to host `localhost:30080` via `extraPortMappings`), `test/e2e/testdata/machine-crd.yaml` (minimal `machines.cluster.x-k8s.io` v1beta2 CRD with `x-kubernetes-preserve-unknown-fields: true`), `test/e2e/values-e2e.yaml`, `test/e2e/e2e_test.go` (`//go:build e2e`), `scripts/e2e.sh`
- Modify: `Makefile` (`e2e: ./scripts/e2e.sh`)

`values-e2e.yaml`: `imdsEndpoint: http://fake-imds.anth-e2e:8080`, `maxDrainTime: 20s`, `eventGoneGrace: 10s`, `pollInterval: 1s`, `uptimeFile: /e2e/uptime`, and `extraEnv` setting `VM_NAME` from `fieldRef spec.nodeName`. It also has a hostPath volume `/var/lib/anth-e2e` mounted at `/e2e`.

`scripts/e2e.sh`: create the kind cluster `anth-e2e`. On every node, run `docker exec <node> sh -c 'mkdir -p /var/lib/anth-e2e && echo "999999.0 0" > /var/lib/anth-e2e/uptime'`. Build the image and `kind load` it. Apply the fake-imds manifest and `helm install` with the e2e values in namespace `anth-e2e`. Run `go test -tags e2e ./test/e2e/ -v -timeout 20m`. Delete the cluster unless `KEEP_CLUSTER=1`.

Test helpers (in `e2e_test.go`): `setDocument(events...)`, `approvals()`, `resetFake()`, `setUptime(node, "5.0")`, `restartAgentOn(node)`, `waitFor(cond, timeout)`. Each test resets the fake and waits until every worker is schedulable before it starts. Workers are `anth-e2e-worker` and `anth-e2e-worker2`.

- [ ] **Step 1: Write the scenarios:**
  - `TestRedeployDrainsAndApproves`: a 1-replica nginx Deployment pinned to worker by node affinity (preferred) + Redeploy for worker, NotBefore now+15m. Within 30s the node is unschedulable. Within 60s no nginx pod is on worker and `approvals()` contains the ID.
  - `TestRebootUncordons`: continues the state above. `setUptime(worker,"5.0")` and `restartAgentOn(worker)` → within 60s the node is schedulable and `completed-event-id` is set. Restore the uptime afterwards.
  - `TestWithdrawnEventUncordons`: Reboot on worker2, wait for the approval, then `setDocument()` (empty) → within 30s worker2 is schedulable.
  - `TestFreezeDoesNothing`: Freeze on worker → it stays schedulable for 15s, there are no approvals, and a Node Event with reason `Freeze` exists.
  - `TestPDBBlockedApprovesAtDeadline`: a 1-replica Deployment on worker2 with PDB `minAvailable: 1` + Redeploy → no approval within the first 15s; approval arrives within 60s; a Node Event `DrainTimeout` exists.
  - `TestPreemptApprovesQuickly`: Preempt on worker, NotBefore now+30s → approved within 30s.
  - `TestMachineDeletingKeepsCordon`: apply the CRD and create Machine `m1` in `default` with a finalizer. Annotate worker2 with `cluster.x-k8s.io/machine=m1` and `cluster.x-k8s.io/cluster-namespace=default`. Send a Reboot and wait for the approval. Delete `m1` (it stays terminating), `setUptime(worker2,"5.0")` and restart the agent → after 30s worker2 is still unschedulable. Cleanup: remove the finalizer, uncordon, remove the annotations and restore the uptime.
- [ ] **Step 2: Run** `make e2e` → all 7 tests PASS.
- [ ] **Step 3: Run** `go test ./... && go vet ./...` (the e2e tag stays excluded) → PASS.
- [ ] **Step 4: Commit** `test: kind end-to-end suite`.
