package handler

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"

	"github.com/jabicho79/azure-node-termination-handler/pkg/decision"
	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/node"
	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// sweep approves every non-current Scheduled event whose approval deadline
// (NotBefore minus the safety margin) has been reached, so handling events one
// at a time never lets a deadline pass unapproved. Cordon and annotation
// failures are logged and never block the approval.
func (h *Handler) sweep(ctx context.Context, n *corev1.Node, events []imds.Event, currentID string) error {
	now := h.d.Now()
	var errs []error
	for _, e := range events {
		if e.EventID == currentID || e.EventStatus != imds.StatusScheduled || h.approvedBySweep[e.EventID] {
			continue
		}
		if _, ok := e.NotBeforeTime(); !ok {
			continue
		}
		// With firstSeen=now the deadline is NotBefore minus the event's margin
		// whenever that is not after now.
		plan := decision.Decide(e, h.o.Decision, now)
		if !plan.Approve || plan.DrainDeadline.After(now) {
			continue
		}
		logger := log.With().Str("event", e.EventID).Str("type", string(e.EventType)).Logger()
		logger.Warn().Str("current", currentID).Msg("Approving overdue secondary event")

		if plan.Cordon && !n.Spec.Unschedulable {
			if err := h.cordon(ctx, n, e); err != nil {
				logger.Error().Err(err).Msg("Cordon failed; approving anyway")
			}
		}
		if plan.MarkUncordonAfterReboot {
			if err := h.annotate(ctx, n, map[string]string{
				node.AnnAction:     node.ActionUncordonAfterReboot,
				node.AnnActionTime: unixString(now),
			}); err != nil {
				logger.Error().Err(err).Msg("Setting the uncordon marker failed; approving anyway")
			}
		}
		if h.o.DryRun {
			h.approvedBySweep[e.EventID] = true
			logger.Info().Msg("Would have approved event, but dry-run was set")
			continue
		}
		if err := h.d.IMDS.Approve(ctx, e.EventID); err != nil {
			h.d.Metrics.Approval("error")
			h.d.Metrics.Error("approve")
			errs = append(errs, fmt.Errorf("approving event %s: %w", e.EventID, err))
			continue
		}
		h.approvedBySweep[e.EventID] = true
		h.d.Metrics.Approval("ok")
		logger.Info().Msg("Scheduled event approved")
		h.notify(ctx, observability.StageApproved, e, "")
	}
	return errors.Join(errs...)
}
