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

	freezeSeen      map[string]bool
	lastIncarnation int
	seenDocument    bool
}

// New returns a Handler.
func New(d Deps, o Options) *Handler {
	return &Handler{d: d, o: o, freezeSeen: map[string]bool{}}
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
	if current != "" && !containsEvent(events, current) {
		return h.handleGone(ctx, n)
	}
	if len(events) == 0 {
		return nil
	}
	e := pickEvent(events, current)
	return h.process(ctx, n, e)
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

// pickEvent returns the event with ID current if present, else the one with
// the earliest NotBefore; events without a (valid) NotBefore come first.
func pickEvent(events []imds.Event, current string) imds.Event {
	for _, e := range events {
		if e.EventID == current {
			return e
		}
	}
	sorted := append([]imds.Event(nil), events...)
	sort.SliceStable(sorted, func(i, j int) bool {
		ti, oki := sorted[i].NotBeforeTime()
		tj, okj := sorted[j].NotBeforeTime()
		if !oki || !okj {
			return !oki && okj
		}
		return ti.Before(tj)
	})
	return sorted[0]
}

// process drives the current event through detect, cordon, drain and approve.
func (h *Handler) process(ctx context.Context, n *corev1.Node, e imds.Event) error {
	now := h.d.Now()
	logger := log.With().Str("event", e.EventID).Str("type", string(e.EventType)).Logger()

	if n.Annotations[node.AnnEventID] != e.EventID {
		logger.Info().Str("status", string(e.EventStatus)).Str("notBefore", e.NotBefore).Msg("Scheduled event detected")
		if err := h.annotate(ctx, n, map[string]string{
			node.AnnEventID:   e.EventID,
			node.AnnEventType: string(e.EventType),
			node.AnnFirstSeen: unixString(now),
		}); err != nil {
			return err
		}
		h.d.Metrics.EventSeen(string(e.EventType))
		h.notify(ctx, observability.StageDetected, e, "")
	}

	firstSeen, err := h.firstSeen(ctx, n, now)
	if err != nil {
		return err
	}

	plan := decision.Decide(e, h.o.Decision, firstSeen)

	if plan.Cordon && !n.Spec.Unschedulable {
		already, err := h.d.Nodes.Cordon(ctx, n.Name)
		if err != nil {
			h.d.Metrics.Action("cordon", "error")
			h.d.Metrics.Error("cordon")
			return fmt.Errorf("cordoning node %s: %w", n.Name, err)
		}
		h.d.Metrics.Action("cordon", "ok")
		n.Spec.Unschedulable = true
		if !already {
			if err := h.annotate(ctx, n, map[string]string{node.AnnCordoned: "true"}); err != nil {
				return err
			}
		}
		logger.Info().Bool("alreadyCordoned", already).Msg("Node cordoned")
		h.notify(ctx, observability.StageCordoned, e, "")
	}

	if plan.Drain && n.Annotations[node.AnnDrainedEventID] != e.EventID && now.Before(plan.DrainDeadline) {
		if err := h.drain(ctx, n, e, plan, now); err != nil {
			return err
		}
	}

	if plan.Approve && !h.o.DryRun && n.Annotations[node.AnnApprovedEventID] != e.EventID {
		return h.maybeApprove(ctx, n, e, plan)
	}
	return nil
}

// firstSeen reads AnnFirstSeen. If it is missing or invalid it is reset to now.
func (h *Handler) firstSeen(ctx context.Context, n *corev1.Node, now time.Time) (time.Time, error) {
	if s, err := strconv.ParseInt(n.Annotations[node.AnnFirstSeen], 10, 64); err == nil {
		return time.Unix(s, 0), nil
	}
	log.Warn().Str("value", n.Annotations[node.AnnFirstSeen]).Msg("Invalid first-seen annotation; resetting it to now")
	if err := h.annotate(ctx, n, map[string]string{node.AnnFirstSeen: unixString(now)}); err != nil {
		return time.Time{}, err
	}
	return time.Unix(now.Unix(), 0), nil
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
func (h *Handler) maybeApprove(ctx context.Context, n *corev1.Node, e imds.Event, plan decision.Plan) error {
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
		if err := h.annotate(ctx, n, map[string]string{
			node.AnnAction:     node.ActionUncordonAfterReboot,
			node.AnnActionTime: unixString(now),
		}); err != nil {
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
