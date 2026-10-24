// Package webhook posts drain/approval notifications to an HTTP endpoint.
// Adapted from aws-node-termination-handler (Apache-2.0).
package webhook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/rs/zerolog/log"

	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// DefaultTemplate is the payload used when no template is configured.
const DefaultTemplate = `{"text": {{ printf "[azure-node-termination-handler] %s: node %s (VM %s), %s event %s. %s" .Stage .NodeName .VMName .EventType .EventID .Description | toJson }}}`

// QueueSize is the number of notifications that may wait to be sent.
const QueueSize = 64

// Webhook is an observability.Notifier that posts to a URL. Notify only
// queues the notification; a single worker goroutine sends them in order, so
// a slow endpoint never delays the caller. Call Close to stop the worker.
type Webhook struct {
	url     string
	tmpl    *template.Template
	hc      *http.Client
	metrics *observability.Metrics

	mu     sync.Mutex // guards closed and sends on queue
	closed bool
	queue  chan observability.Notification
	// ctx is the worker's context; cancel aborts an in-flight send.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// New builds a Webhook and starts its worker. An empty tmpl selects
// DefaultTemplate; a nil hc uses a 10s-timeout client. metrics may be nil.
func New(url, tmpl string, hc *http.Client, metrics *observability.Metrics) (*Webhook, error) {
	if tmpl == "" {
		tmpl = DefaultTemplate
	}
	t, err := template.New("webhook").Funcs(sprig.TxtFuncMap()).Parse(tmpl)
	if err != nil {
		return nil, fmt.Errorf("parse webhook template: %w", err)
	}
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Webhook{
		url: url, tmpl: t, hc: hc, metrics: metrics,
		queue: make(chan observability.Notification, QueueSize),
		ctx:   ctx, cancel: cancel, done: make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// Notify queues StageDrained and StageApproved notifications and ignores the
// rest. It never blocks: when the queue is full the notification is dropped
// and counted as anth_errors_total{op="webhook_dropped"}. After Close it does
// nothing.
func (w *Webhook) Notify(_ context.Context, n observability.Notification) {
	if n.Stage != observability.StageDrained && n.Stage != observability.StageApproved {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		log.Warn().Str("stage", string(n.Stage)).Str("event", n.EventID).Msg("webhook: notifier closed; notification dropped")
		return
	}
	select {
	case w.queue <- n:
	default:
		log.Warn().Str("stage", string(n.Stage)).Str("event", n.EventID).Msg("webhook: queue full; notification dropped")
		if w.metrics != nil {
			w.metrics.Error("webhook_dropped")
		}
	}
}

// Close stops accepting notifications and waits for the worker to send the
// queued ones. If ctx ends first, the in-flight send is cancelled, the rest
// are discarded, and ctx's error is returned once the worker has exited.
func (w *Webhook) Close(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.queue)
	}
	w.mu.Unlock()

	select {
	case <-w.done:
		w.cancel()
		return nil
	case <-ctx.Done():
		w.cancel()
		<-w.done
		return ctx.Err()
	}
}

func (w *Webhook) run() {
	defer close(w.done)
	for n := range w.queue {
		if w.ctx.Err() != nil {
			continue // cancelled by Close: discard the rest
		}
		w.send(n)
	}
}

// send posts one notification. Errors are logged, never returned.
func (w *Webhook) send(n observability.Notification) {
	var buf bytes.Buffer
	if err := w.tmpl.Execute(&buf, n); err != nil {
		log.Error().Err(err).Msg("webhook: template execution failed")
		return
	}
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, w.url, &buf)
	if err != nil {
		log.Error().Err(err).Msg("webhook: building request failed")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.hc.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("webhook: request failed")
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		log.Error().Int("status", resp.StatusCode).Msg("webhook: non-2xx response")
	}
}
