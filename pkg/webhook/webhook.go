// Package webhook posts drain/approval notifications to an HTTP endpoint.
// Adapted from aws-node-termination-handler (Apache-2.0).
package webhook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/rs/zerolog/log"

	"github.com/jabicho79/azure-node-termination-handler/pkg/observability"
)

// DefaultTemplate is the payload used when no template is configured.
const DefaultTemplate = `{"text": {{ printf "[azure-node-termination-handler] %s: node %s (VM %s), %s event %s. %s" .Stage .NodeName .VMName .EventType .EventID .Description | toJson }}}`

// Webhook is an observability.Notifier that posts to a URL.
type Webhook struct {
	url  string
	tmpl *template.Template
	hc   *http.Client
}

// New builds a Webhook. An empty tmpl selects DefaultTemplate; a nil hc uses a 10s-timeout client.
func New(url, tmpl string, hc *http.Client) (*Webhook, error) {
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
	return &Webhook{url: url, tmpl: t, hc: hc}, nil
}

// Notify posts only for StageDrained and StageApproved. Errors are logged, never returned.
func (w *Webhook) Notify(ctx context.Context, n observability.Notification) {
	if n.Stage != observability.StageDrained && n.Stage != observability.StageApproved {
		return
	}
	var buf bytes.Buffer
	if err := w.tmpl.Execute(&buf, n); err != nil {
		log.Error().Err(err).Msg("webhook: template execution failed")
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, &buf)
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
