package decision

import (
	"net/http"
	"testing"
	"time"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
)

func testSettings() Settings {
	return Settings{
		Enabled: map[imds.EventType]bool{
			imds.EventReboot:    true,
			imds.EventRedeploy:  true,
			imds.EventPreempt:   true,
			imds.EventTerminate: true,
		},
		AutoApprove:           true,
		SafetyMargin:          60 * time.Second,
		PreemptSafetyMargin:   5 * time.Second,
		MaxDrainTime:          60 * time.Minute,
		PodGracePeriod:        -1,
		PreemptPodGracePeriod: 20,
	}
}

func TestDecide(t *testing.T) {
	firstSeen := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	nb := func(d time.Duration) string { return firstSeen.Add(d).Format(http.TimeFormat) }

	tests := []struct {
		name   string
		event  imds.Event
		mutate func(*Settings)
		want   Plan
	}{
		{
			name:  "redeploy far away is capped by max drain time",
			event: imds.Event{EventType: imds.EventRedeploy, EventStatus: imds.StatusScheduled, NotBefore: nb(7 * 24 * time.Hour)},
			want:  Plan{Cordon: true, Drain: true, Approve: true, MarkUncordonAfterReboot: true, DrainDeadline: firstSeen.Add(60 * time.Minute), PodGracePeriod: -1},
		},
		{
			name:  "reboot deadline is NotBefore minus margin",
			event: imds.Event{EventType: imds.EventReboot, EventStatus: imds.StatusScheduled, NotBefore: nb(10 * time.Minute)},
			want:  Plan{Cordon: true, Drain: true, Approve: true, MarkUncordonAfterReboot: true, DrainDeadline: firstSeen.Add(9 * time.Minute), PodGracePeriod: -1},
		},
		{
			name:  "preempt uses preempt margin and grace",
			event: imds.Event{EventType: imds.EventPreempt, EventStatus: imds.StatusScheduled, NotBefore: nb(30 * time.Second)},
			want:  Plan{Cordon: true, Drain: true, Approve: true, MarkUncordonAfterReboot: true, DrainDeadline: firstSeen.Add(25 * time.Second), PodGracePeriod: 20},
		},
		{
			name:  "terminate is not uncordoned",
			event: imds.Event{EventType: imds.EventTerminate, EventStatus: imds.StatusScheduled, NotBefore: nb(10 * time.Minute)},
			want:  Plan{Cordon: true, Drain: true, Approve: true, MarkUncordonAfterReboot: false, DrainDeadline: firstSeen.Add(9 * time.Minute), PodGracePeriod: -1},
		},
		{
			name:  "freeze does nothing",
			event: imds.Event{EventType: imds.EventFreeze, EventStatus: imds.StatusScheduled, NotBefore: nb(10 * time.Minute)},
			want:  Plan{},
		},
		{
			name:   "disabled type does nothing",
			event:  imds.Event{EventType: imds.EventRedeploy, EventStatus: imds.StatusScheduled, NotBefore: nb(10 * time.Minute)},
			mutate: func(s *Settings) { s.Enabled[imds.EventRedeploy] = false },
			want:   Plan{},
		},
		{
			name:   "absent type counts as disabled",
			event:  imds.Event{EventType: imds.EventReboot, EventStatus: imds.StatusScheduled, NotBefore: nb(10 * time.Minute)},
			mutate: func(s *Settings) { delete(s.Enabled, imds.EventReboot) },
			want:   Plan{},
		},
		{
			name:  "unknown type does nothing",
			event: imds.Event{EventType: imds.EventType("Mystery"), EventStatus: imds.StatusScheduled, NotBefore: nb(10 * time.Minute)},
			want:  Plan{},
		},
		{
			name:   "auto-approve off",
			event:  imds.Event{EventType: imds.EventRedeploy, EventStatus: imds.StatusScheduled, NotBefore: nb(7 * 24 * time.Hour)},
			mutate: func(s *Settings) { s.AutoApprove = false },
			want:   Plan{Cordon: true, Drain: true, Approve: false, MarkUncordonAfterReboot: true, DrainDeadline: firstSeen.Add(60 * time.Minute), PodGracePeriod: -1},
		},
		{
			name:  "started event with empty NotBefore never approves",
			event: imds.Event{EventType: imds.EventRedeploy, EventStatus: imds.StatusStarted, NotBefore: ""},
			want:  Plan{Cordon: true, Drain: true, Approve: false, MarkUncordonAfterReboot: true, DrainDeadline: firstSeen, PodGracePeriod: -1},
		},
		{
			name:  "garbage NotBefore falls back to firstSeen",
			event: imds.Event{EventType: imds.EventReboot, EventStatus: imds.StatusScheduled, NotBefore: "garbage"},
			want:  Plan{Cordon: true, Drain: true, Approve: true, MarkUncordonAfterReboot: true, DrainDeadline: firstSeen, PodGracePeriod: -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testSettings()
			if tt.mutate != nil {
				tt.mutate(&s)
			}
			got := Decide(tt.event, s, firstSeen)
			if got != tt.want {
				t.Errorf("Decide() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
