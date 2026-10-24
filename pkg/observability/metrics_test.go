package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.EventSeen("Redeploy")
	m.Action("drain", "ok")
	m.DrainDuration(2 * time.Second)
	m.DrainTimeout()
	m.Approval("ok")
	m.Error("approve")

	if got := testutil.ToFloat64(m.events.WithLabelValues("Redeploy")); got != 1 {
		t.Errorf("anth_events_total{type=Redeploy} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.approvals.WithLabelValues("ok")); got != 1 {
		t.Errorf("anth_approvals_total{result=ok} = %v, want 1", got)
	}
	if n, err := testutil.GatherAndCount(reg); err != nil || n != 6 {
		t.Errorf("gathered %d series (err %v), want 6", n, err)
	}
}
