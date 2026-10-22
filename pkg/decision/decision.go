// Package decision maps a scheduled event to the actions the handler takes.
package decision

import (
	"time"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
)

// Settings are the inputs to Decide.
type Settings struct {
	// Enabled lists the event types to act on; absent types are disabled.
	Enabled               map[imds.EventType]bool
	AutoApprove           bool
	SafetyMargin          time.Duration
	PreemptSafetyMargin   time.Duration
	MaxDrainTime          time.Duration
	PodGracePeriod        int
	PreemptPodGracePeriod int
}

// Plan describes what to do about an event. The zero Plan means do nothing.
type Plan struct {
	Cordon, Drain, Approve, MarkUncordonAfterReboot bool
	// DrainDeadline is anchored on firstSeen; the caller compares it with its own now.
	DrainDeadline time.Time
	// PodGracePeriod is in seconds; -1 means use the pod's default.
	PodGracePeriod int
}

// Decide is pure: it performs no I/O and reads no clock.
func Decide(e imds.Event, s Settings, firstSeen time.Time) Plan {
	if !s.Enabled[e.EventType] {
		return Plan{}
	}

	margin := s.SafetyMargin
	grace := s.PodGracePeriod
	uncordon := true
	switch e.EventType {
	case imds.EventReboot, imds.EventRedeploy:
	case imds.EventPreempt:
		margin = s.PreemptSafetyMargin
		grace = s.PreemptPodGracePeriod
	case imds.EventTerminate:
		uncordon = false
	default: // Freeze and unknown types
		return Plan{}
	}

	deadline := firstSeen
	if nb, ok := e.NotBeforeTime(); ok {
		deadline = nb.Add(-margin)
		if limit := firstSeen.Add(s.MaxDrainTime); limit.Before(deadline) {
			deadline = limit
		}
	}

	return Plan{
		Cordon:                  true,
		Drain:                   true,
		Approve:                 s.AutoApprove && e.EventStatus != imds.StatusStarted,
		MarkUncordonAfterReboot: uncordon,
		DrainDeadline:           deadline,
		PodGracePeriod:          grace,
	}
}
