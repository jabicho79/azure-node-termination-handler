package node

// Prefix is the namespace for every annotation the agent writes.
const Prefix = "azure-node-termination-handler/"

const (
	AnnEventID          = Prefix + "event-id"
	AnnEventType        = Prefix + "event-type"
	AnnFirstSeen        = Prefix + "first-seen" // unix seconds
	AnnDrainedEventID   = Prefix + "drained-event-id"
	AnnApprovedEventID  = Prefix + "approved-event-id"
	AnnCordoned         = Prefix + "cordoned" // "true" only if we cordoned it
	AnnAction           = Prefix + "action"
	AnnActionTime       = Prefix + "action-time"        // unix seconds
	AnnEventGoneSince   = Prefix + "event-gone-since"   // unix seconds
	AnnCompletedEventID = Prefix + "completed-event-id" // survives release

	ActionUncordonAfterReboot = "uncordon-after-reboot"
)

// WorkingAnnotations lists every annotation key except AnnCompletedEventID,
// which intentionally survives a release.
var WorkingAnnotations = []string{
	AnnEventID,
	AnnEventType,
	AnnFirstSeen,
	AnnDrainedEventID,
	AnnApprovedEventID,
	AnnCordoned,
	AnnAction,
	AnnActionTime,
	AnnEventGoneSince,
}
