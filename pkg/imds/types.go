// Package imds is a client for the Azure Instance Metadata Service Scheduled Events API.
package imds

import (
	"net/http"
	"strings"
	"time"
)

// EventType is the kind of scheduled event.
type EventType string

// EventStatus is the lifecycle status of a scheduled event.
type EventStatus string

const (
	EventReboot    EventType = "Reboot"
	EventRedeploy  EventType = "Redeploy"
	EventFreeze    EventType = "Freeze"
	EventPreempt   EventType = "Preempt"
	EventTerminate EventType = "Terminate"
)

const (
	StatusScheduled EventStatus = "Scheduled"
	StatusStarted   EventStatus = "Started"
)

// Event is a single scheduled event.
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

// NotBeforeTime parses NotBefore; ok is false if it is empty or invalid.
func (e Event) NotBeforeTime() (time.Time, bool) {
	if e.NotBefore == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(http.TimeFormat, e.NotBefore)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// Affects reports whether vmName is in Resources (case-insensitive).
func (e Event) Affects(vmName string) bool {
	for _, r := range e.Resources {
		if strings.EqualFold(r, vmName) {
			return true
		}
	}
	return false
}

// Document is the Scheduled Events response.
type Document struct {
	DocumentIncarnation int     `json:"DocumentIncarnation"`
	Events              []Event `json:"Events"`
}
