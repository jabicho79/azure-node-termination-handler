package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"

	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	os.Exit(m.Run())
}

func TestWebhookPostsOnlyDrainedAndApproved(t *testing.T) {
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}
	}))
	defer srv.Close()
	w, err := New(srv.URL, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []observability.Stage{observability.StageDetected, observability.StageDrained, observability.StageApproved} {
		w.Notify(context.Background(), observability.Notification{Stage: s, NodeName: "n"})
	}
	closeWebhook(t, w)
	mu.Lock()
	defer mu.Unlock()
	if count != 2 {
		t.Fatalf("requests = %d, want 2", count)
	}
}

func TestWebhookDefaultTemplateEscapes(t *testing.T) {
	var mu sync.Mutex
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		body = b
		mu.Unlock()
	}))
	defer srv.Close()
	w, err := New(srv.URL, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageDrained, Description: "say \"hi\"\n"})
	closeWebhook(t, w)
	mu.Lock()
	defer mu.Unlock()
	if !json.Valid(body) {
		t.Fatalf("invalid JSON: %s", body)
	}
	var out struct{ Text string }
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Text, `say "hi"`) {
		t.Errorf("text = %q", out.Text)
	}
}

func TestWebhookNon2xxDoesNotPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	w, _ := New(srv.URL, "", nil, nil)
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageApproved})
	closeWebhook(t, w)
}

func TestWebhookBadTemplate(t *testing.T) {
	if _, err := New("http://x", "{{", nil, nil); err == nil {
		t.Fatal("expected error")
	}
}

func closeWebhook(t *testing.T, w *Webhook) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// blockingServer holds every request until release is closed (or the
// request is cancelled) and reports each arrival on arrived.
type blockingServer struct {
	*httptest.Server
	arrived chan struct{}
	release chan struct{}
	mu      sync.Mutex
	served  int
}

func newBlockingServer(t *testing.T) *blockingServer {
	b := &blockingServer{arrived: make(chan struct{}, 128), release: make(chan struct{})}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.arrived <- struct{}{}
		select {
		case <-b.release:
		case <-r.Context().Done():
			return
		case <-time.After(2 * time.Second):
		}
		b.mu.Lock()
		b.served++
		b.mu.Unlock()
	}))
	t.Cleanup(b.Server.Close)
	return b
}

func (b *blockingServer) waitArrival(t *testing.T) {
	t.Helper()
	select {
	case <-b.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the webhook request")
	}
}

func (b *blockingServer) servedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.served
}

func TestWebhookNotifyDoesNotBlock(t *testing.T) {
	srv := newBlockingServer(t)
	w, err := New(srv.URL, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageApproved})
	if d := time.Since(start); d >= 50*time.Millisecond {
		t.Errorf("Notify took %v, want < 50ms", d)
	}
	srv.waitArrival(t)

	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		closed <- w.Close(ctx)
	}()
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v while a send was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(srv.release)
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the send finished")
	}
	if got := srv.servedCount(); got != 1 {
		t.Errorf("served = %d, want 1", got)
	}
}

func TestWebhookDropsWhenQueueFull(t *testing.T) {
	srv := newBlockingServer(t)
	reg := prometheus.NewRegistry()
	metrics := observability.NewMetrics(reg)
	w, err := New(srv.URL, "", nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	n := observability.Notification{Stage: observability.StageDrained}

	w.Notify(context.Background(), n)
	srv.waitArrival(t) // the worker is busy with the first one
	for range QueueSize {
		w.Notify(context.Background(), n)
	}
	w.Notify(context.Background(), n) // queue full: dropped

	if got := droppedCount(t, reg); got != 1 {
		t.Errorf("errors_total{op=webhook_dropped} = %v, want 1", got)
	}
	close(srv.release)
	closeWebhook(t, w)
	if got := srv.servedCount(); got != QueueSize+1 {
		t.Errorf("served = %d, want %d", got, QueueSize+1)
	}
}

func TestWebhookCloseCancelsAfterTimeout(t *testing.T) {
	srv := newBlockingServer(t)
	w, err := New(srv.URL, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageApproved})
	srv.waitArrival(t)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := w.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Close took %v, want it to cancel the in-flight send", d)
	}
	// Notify after Close is a no-op and must not panic.
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageApproved})
	close(srv.release) // let the server's handler finish before cleanup
}

func droppedCount(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "anth_errors_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "op" && lp.GetValue() == "webhook_dropped" {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
