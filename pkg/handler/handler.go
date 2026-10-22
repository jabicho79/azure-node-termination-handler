// Package handler reconciles Azure Scheduled Events with the state of this
// agent's Kubernetes node: it cordons, drains and approves.
package handler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"

	"github.com/jabicho79/azure-node-termination-handler/pkg/decision"
	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// IMDS is the Scheduled Events API.
type IMDS interface {
	ScheduledEvents(ctx context.Context) (imds.Document, error)
	Approve(ctx context.Context, eventID string) error
}

// Nodes performs Kubernetes node operations. It is satisfied by *node.Ops.
type Nodes interface {
	Get(ctx context.Context, name string) (*corev1.Node, error)
	ListNodes(ctx context.Context) ([]corev1.Node, error)
	Cordon(ctx context.Context, name string) (bool, error)
	Uncordon(ctx context.Context, name string) error
	Drain(ctx context.Context, name string, opts node.DrainOptions) error
	SetAnnotations(ctx context.Context, name string, kv map[string]string) error
	RemoveAnnotations(ctx context.Context, name string, keys ...string) error
}

// Machines reports whether a node's Cluster API Machine is being deleted.
// It is satisfied by *capi.MachineChecker.
type Machines interface {
	BeingDeleted(ctx context.Context, n *corev1.Node) (bool, error)
}

// Deps are the handler's collaborators.
type Deps struct {
	IMDS     IMDS
	Nodes    Nodes
	Machines Machines
	Notifier observability.Notifier
	Metrics  *observability.Metrics
	Now      func() time.Time
	Uptime   func() (time.Duration, error)
}

// Options configure the handler.
type Options struct {
	NodeName, VMName                             string
	Decision                                     decision.Settings
	DeleteEmptyDirData, IgnoreDaemonSets, DryRun bool
	EventGoneGrace                               time.Duration
}

// Handler is the per-node scheduled event state machine. All durable state
// lives in node annotations so a restarted agent resumes where it left off.
type Handler struct {
	d Deps
	o Options

	freezeSeen map[string]bool
	// approvedBySweep holds secondary events approved by the deadline sweep.
	approvedBySweep map[string]bool
	lastIncarnation int
	seenDocument    bool
}

// New returns a Handler.
func New(d Deps, o Options) *Handler {
	return &Handler{d: d, o: o, freezeSeen: map[string]bool{}, approvedBySweep: map[string]bool{}}
}

// Tick polls Scheduled Events once and reconciles the node with them.
func (h *Handler) Tick(ctx context.Context) error {
	doc, err := h.d.IMDS.ScheduledEvents(ctx)
	if err != nil {
		h.d.Metrics.Error("imds")
		return fmt.Errorf("fetching scheduled events: %w", err)
	}
	if !h.seenDocument || doc.DocumentIncarnation != h.lastIncarnation {
		log.Debug().Int("incarnation", doc.DocumentIncarnation).Int("events", len(doc.Events)).
			Msg("Scheduled events document changed")
		h.seenDocument = true
		h.lastIncarnation = doc.DocumentIncarnation
	}

	n, err := h.d.Nodes.Get(ctx, h.o.NodeName)
	if err != nil {
		h.d.Metrics.Error("get_node")
		return fmt.Errorf("getting node %s: %w", h.o.NodeName, err)
	}
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}

	events := h.relevantEvents(ctx, doc.Events, n.Annotations[node.AnnCompletedEventID])

	current := n.Annotations[node.AnnEventID]
	e, ok := h.pickEvent(events, current)
	takeover := ok && e.EventType == imds.EventPreempt
	if current != "" && !containsEvent(events, current) && !takeover {
		return h.handleGone(ctx, n)
	}
	if !ok {
		return nil
	}
	err = h.process(ctx, n, e)
	if ctx.Err() != nil {
		return err
	}
	return errors.Join(err, h.sweep(ctx, n, events, e.EventID))
}

// relevantEvents keeps the events that affect this VM, notifies Freeze events
// once per ID and drops them, and drops the event already completed here.
func (h *Handler) relevantEvents(ctx context.Context, all []imds.Event, completedID string) []imds.Event {
	var out []imds.Event
	for _, e := range all {
		if !e.Affects(h.o.VMName) {
			continue
		}
		if e.EventType == imds.EventFreeze {
			if !h.freezeSeen[e.EventID] {
				h.freezeSeen[e.EventID] = true
				log.Info().Str("event", e.EventID).Str("notBefore", e.NotBefore).Msg("Freeze event scheduled; no action taken")
				h.notify(ctx, observability.StageFreeze, e, "")
			}
			continue
		}
		if completedID != "" && e.EventID == completedID {
			continue
		}
		out = append(out, e)
	}
	return out
}

func containsEvent(events []imds.Event, id string) bool {
	for _, e := range events {
		if e.EventID == id {
			return true
		}
	}
	return false
}

// pickEvent returns the current event. Events whose Plan is zero (disabled or
// unknown types) are never picked. A Preempt always wins; otherwise the event
// with ID current is kept if listed, else the one with the earliest NotBefore
// is picked (events without a valid NotBefore come first).
func (h *Handler) pickEvent(events []imds.Event, current string) (imds.Event, bool) {
	now := h.d.Now()
	var actionable []imds.Event
	for _, e := range events {
		if decision.Decide(e, h.o.Decision, now) != (decision.Plan{}) {
			actionable = append(actionable, e)
		}
	}
	if len(actionable) == 0 {
		return imds.Event{}, false
	}
	sort.SliceStable(actionable, func(i, j int) bool {
		ti, oki := actionable[i].NotBeforeTime()
		tj, okj := actionable[j].NotBeforeTime()
		if !oki || !okj {
			return !oki && okj
		}
		return ti.Before(tj)
	})
	for _, e := range actionable {
		if e.EventType == imds.EventPreempt {
			return e, true
		}
	}
	for _, e := range events {
		if e.EventID == current {
			return e, true
		}
	}
	return actionable[0], true
}

// process drives the current event through detect, cordon, drain and approve.
//
// Before the drain deadline any Kubernetes error ends the tick (it is retried
// on the next poll). Once the deadline has passed, cordon, drain and
// annotation errors are recorded and approval is still attempted, so a
// failing API call never lets the deadline pass unapproved.
func (h *Handler) process(ctx context.Context, n *corev1.Node, e imds.Event) error {
	now := h.d.Now()
	logger := log.With().Str("event", e.EventID).Str("type", string(e.EventType)).Logger()

	isNew := n.Annotations[node.AnnEventID] != e.EventID
	firstSeen := time.Unix(now.Unix(), 0)
	if !isNew {
		if s, err := strconv.ParseInt(n.Annotations[node.AnnFirstSeen], 10, 64); err == nil {
			firstSeen = time.Unix(s, 0)
		}
	}
	plan := decision.Decide(e, h.o.Decision, firstSeen)

	var deferred []error
	// check returns err unless the deadline has passed, in which case it
	// records err and returns nil so processing continues to approval.
	check := func(err error) error {
		if err == nil || h.d.Now().Before(plan.DrainDeadline) {
			return err
		}
		logger.Error().Err(err).Msg("Continuing to approval because the drain deadline has passed")
		deferred = append(deferred, err)
		return nil
	}

	if isNew {
		if err := check(h.detect(ctx, n, e, firstSeen)); err != nil {
			return err
		}
	} else if unixString(firstSeen) != n.Annotations[node.AnnFirstSeen] {
		logger.Warn().Str("value", n.Annotations[node.AnnFirstSeen]).Msg("Invalid first-seen annotation; resetting it to now")
		if err := check(h.annotate(ctx, n, map[string]string{node.AnnFirstSeen: unixString(firstSeen)})); err != nil {
			return err
		}
	}

	if plan.Cordon && !n.Spec.Unschedulable {
		if err := check(h.cordon(ctx, n, e)); err != nil {
			return err
		}
	}

	if plan.Drain && n.Annotations[node.AnnDrainedEventID] != e.EventID && now.Before(plan.DrainDeadline) {
		err := h.drain(ctx, n, e, plan, now)
		if ctx.Err() != nil {
			return err
		}
		if err := check(err); err != nil {
			return err
		}
	}

	if plan.Approve && !h.o.DryRun && n.Annotations[node.AnnApprovedEventID] != e.EventID && !h.approvedBySweep[e.EventID] {
		deferred = append(deferred, h.maybeApprove(ctx, n, e, plan, check))
	}
	return errors.Join(deferred...)
}

// detect records e as the node's current event, replacing any previous one
// (a Preempt takes over), and notifies Detected.
func (h *Handler) detect(ctx context.Context, n *corev1.Node, e imds.Event, firstSeen time.Time) error {
	log.Info().Str("event", e.EventID).Str("type", string(e.EventType)).Str("status", string(e.EventStatus)).
		Str("notBefore", e.NotBefore).Str("previous", n.Annotations[node.AnnEventID]).Msg("Scheduled event detected")
	var stale []string
	for _, k := range []string{node.AnnDrainedEventID, node.AnnApprovedEventID} {
		if v, ok := n.Annotations[k]; ok && v != e.EventID {
			stale = append(stale, k)
		}
	}
	if _, ok := n.Annotations[node.AnnEventGoneSince]; ok {
		stale = append(stale, node.AnnEventGoneSince)
	}
	if err := h.unannotate(ctx, n, stale...); err != nil {
		return err
	}
	if err := h.annotate(ctx, n, map[string]string{
		node.AnnEventID:   e.EventID,
		node.AnnEventType: string(e.EventType),
		node.AnnFirstSeen: unixString(firstSeen),
	}); err != nil {
		return err
	}
	h.d.Metrics.EventSeen(string(e.EventType))
	h.notify(ctx, observability.StageDetected, e, "")
	return nil
}

// cordon cordons the node and, if it was schedulable, records that the agent
// cordoned it.
func (h *Handler) cordon(ctx context.Context, n *corev1.Node, e imds.Event) error {
	already, err := h.d.Nodes.Cordon(ctx, n.Name)
	if err != nil {
		h.d.Metrics.Action("cordon", "error")
		h.d.Metrics.Error("cordon")
		return fmt.Errorf("cordoning node %s: %w", n.Name, err)
	}
	h.d.Metrics.Action("cordon", "ok")
	n.Spec.Unschedulable = true
	log.Info().Str("event", e.EventID).Bool("alreadyCordoned", already).Msg("Node cordoned")
	if !already {
		if err := h.annotate(ctx, n, map[string]string{node.AnnCordoned: "true"}); err != nil {
			return err
		}
	}
	h.notify(ctx, observability.StageCordoned, e, "")
	return nil
}

// drain drains the node until plan.DrainDeadline. A drain that hits its
// deadline is not an error: approval still happens at the deadline.
func (h *Handler) drain(ctx context.Context, n *corev1.Node, e imds.Event, plan decision.Plan, now time.Time) error {
	logger := log.With().Str("event", e.EventID).Time("deadline", plan.DrainDeadline).Logger()
	logger.Info().Msg("Draining node")
	err := h.d.Nodes.Drain(ctx, n.Name, node.DrainOptions{
		Deadline:              plan.DrainDeadline,
		PodGracePeriodSeconds: plan.PodGracePeriod,
		DeleteEmptyDirData:    h.o.DeleteEmptyDirData,
		IgnoreDaemonSets:      h.o.IgnoreDaemonSets,
	})
	switch {
	case err == nil:
		h.d.Metrics.Action("drain", "ok")
		if err := h.annotate(ctx, n, map[string]string{node.AnnDrainedEventID: e.EventID}); err != nil {
			return err
		}
		h.d.Metrics.DrainDuration(h.d.Now().Sub(now))
		logger.Info().Msg("Node drained")
		h.notify(ctx, observability.StageDrained, e, "")
		return nil
	case ctx.Err() != nil:
		// node.Ops.Drain reports any context error as DeadlineExceeded; a
		// cancelled parent (shutdown) is not a drain timeout.
		return ctx.Err()
	case errors.Is(err, context.DeadlineExceeded):
		h.d.Metrics.DrainTimeout()
		logger.Warn().Err(err).Msg("Drain did not finish before the deadline")
		h.notify(ctx, observability.StageDrainTimeout, e,
			fmt.Sprintf("Drain for %s event %s did not finish before %s", e.EventType, e.EventID, plan.DrainDeadline.UTC().Format(time.RFC3339)))
		return nil
	default:
		h.d.Metrics.Action("drain", "error")
		h.d.Metrics.Error("drain")
		return fmt.Errorf("draining node %s: %w", n.Name, err)
	}
}

// maybeApprove approves e once the drain deadline is reached, or earlier once
// this node and every peer node affected by e have drained.
// check is process's deadline-aware error filter.
func (h *Handler) maybeApprove(ctx context.Context, n *corev1.Node, e imds.Event, plan decision.Plan, check func(error) error) error {
	now := h.d.Now()
	ready := !now.Before(plan.DrainDeadline)
	if !ready {
		selfDone := !plan.Drain || n.Annotations[node.AnnDrainedEventID] == e.EventID
		if !selfDone {
			return nil
		}
		peersDone, err := h.peersDrained(ctx, e)
		if err != nil {
			return err
		}
		ready = peersDone
	}
	if !ready {
		return nil
	}

	if plan.MarkUncordonAfterReboot {
		if err := check(h.annotate(ctx, n, map[string]string{
			node.AnnAction:     node.ActionUncordonAfterReboot,
			node.AnnActionTime: unixString(now),
		})); err != nil {
			return err
		}
	}
	if err := h.d.IMDS.Approve(ctx, e.EventID); err != nil {
		h.d.Metrics.Approval("error")
		h.d.Metrics.Error("approve")
		return fmt.Errorf("approving event %s: %w", e.EventID, err)
	}
	h.d.Metrics.Approval("ok")
	log.Info().Str("event", e.EventID).Msg("Scheduled event approved")
	if err := h.annotate(ctx, n, map[string]string{node.AnnApprovedEventID: e.EventID}); err != nil {
		return err
	}
	h.notify(ctx, observability.StageApproved, e, "")
	return nil
}

// peersDrained reports whether every other cluster node affected by e has
// drained it. VMs without a matching node are ignored.
func (h *Handler) peersDrained(ctx context.Context, e imds.Event) (bool, error) {
	var peers []string
	for _, r := range e.Resources {
		if !strings.EqualFold(r, h.o.VMName) {
			peers = append(peers, r)
		}
	}
	if len(peers) == 0 {
		return true, nil
	}
	nodes, err := h.d.Nodes.ListNodes(ctx)
	if err != nil {
		h.d.Metrics.Error("list_nodes")
		return false, fmt.Errorf("listing nodes: %w", err)
	}
	found, missing := node.NodesForVMs(nodes, peers)
	if len(missing) > 0 {
		log.Warn().Str("event", e.EventID).Strs("vms", missing).Msg("No node found for VMs in event; ignoring them")
	}
	for vm, p := range found {
		if p.Annotations[node.AnnDrainedEventID] != e.EventID {
			log.Debug().Str("event", e.EventID).Str("vm", vm).Str("node", p.Name).Msg("Waiting for peer node to drain")
			return false, nil
		}
	}
	return true, nil
}

// handleGone handles a node whose recorded event is no longer listed.
// Task 9 implements the release.
func (h *Handler) handleGone(ctx context.Context, n *corev1.Node) error {
	return nil
}

// unannotate removes keys from the node and from the local copy n.
func (h *Handler) unannotate(ctx context.Context, n *corev1.Node, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := h.d.Nodes.RemoveAnnotations(ctx, n.Name, keys...); err != nil {
		h.d.Metrics.Error("annotate")
		return fmt.Errorf("removing annotations from node %s: %w", n.Name, err)
	}
	for _, k := range keys {
		delete(n.Annotations, k)
	}
	return nil
}

// annotate sets kv on the node and mirrors them on the local copy n.
func (h *Handler) annotate(ctx context.Context, n *corev1.Node, kv map[string]string) error {
	if err := h.d.Nodes.SetAnnotations(ctx, n.Name, kv); err != nil {
		h.d.Metrics.Error("annotate")
		return fmt.Errorf("annotating node %s: %w", n.Name, err)
	}
	if n.Annotations == nil {
		n.Annotations = map[string]string{}
	}
	for k, v := range kv {
		n.Annotations[k] = v
	}
	return nil
}

func (h *Handler) notify(ctx context.Context, stage observability.Stage, e imds.Event, msg string) {
	h.d.Notifier.Notify(ctx, observability.Notification{
		Stage:       stage,
		NodeName:    h.o.NodeName,
		VMName:      h.o.VMName,
		EventID:     e.EventID,
		EventType:   string(e.EventType),
		Description: e.Description,
		NotBefore:   e.NotBefore,
		Message:     msg,
	})
}

func unixString(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }
