package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the agent's Prometheus collectors.
type Metrics struct {
	events        *prometheus.CounterVec
	actions       *prometheus.CounterVec
	drainDuration prometheus.Histogram
	drainTimeouts prometheus.Counter
	approvals     *prometheus.CounterVec
	errors        *prometheus.CounterVec
}

// NewMetrics creates the collectors and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "anth_events_total", Help: "Scheduled events seen, by type."}, []string{"type"}),
		actions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "anth_actions_total", Help: "Actions taken, by action and result."}, []string{"action", "result"}),
		drainDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "anth_drain_duration_seconds", Help: "Duration of node drains.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12)}),
		drainTimeouts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "anth_drain_timeouts_total", Help: "Drains that hit their timeout."}),
		approvals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "anth_approvals_total", Help: "Event approvals, by result."}, []string{"result"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "anth_errors_total", Help: "Errors, by operation."}, []string{"op"}),
	}
	reg.MustRegister(m.events, m.actions, m.drainDuration, m.drainTimeouts, m.approvals, m.errors)
	return m
}

func (m *Metrics) EventSeen(eventType string)    { m.events.WithLabelValues(eventType).Inc() }
func (m *Metrics) Action(action, result string)  { m.actions.WithLabelValues(action, result).Inc() }
func (m *Metrics) DrainDuration(d time.Duration) { m.drainDuration.Observe(d.Seconds()) }
func (m *Metrics) DrainTimeout()                 { m.drainTimeouts.Inc() }
func (m *Metrics) Approval(result string)        { m.approvals.WithLabelValues(result).Inc() }
func (m *Metrics) Error(op string)               { m.errors.WithLabelValues(op).Inc() }
