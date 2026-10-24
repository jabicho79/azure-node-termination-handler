package imds

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	scheduledEventsPath    = "/metadata/scheduledevents"
	scheduledEventsVersion = "2020-07-01"
	computeNamePath        = "/metadata/instance/compute/name"
	computeNameQuery       = "api-version=2021-02-01&format=text"
	defaultTimeout         = 5 * time.Second
)

// Client talks to the IMDS endpoint.
type Client struct {
	endpoint string
	hc       *http.Client
}

// NewClient returns a Client; a nil hc gets a 5s timeout.
func NewClient(endpoint string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}
	return &Client{endpoint: strings.TrimRight(endpoint, "/"), hc: hc}
}

func (c *Client) do(ctx context.Context, method, path, query string, body []byte) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path+"?"+query, rdr)
	if err != nil {
		return nil, fmt.Errorf("build request %s %s: %w", method, path, err)
	}
	req.Header.Set("Metadata", "true")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%s %s: unexpected status %d", method, path, resp.StatusCode)
	}
	if err != nil {
		return nil, fmt.Errorf("read %s %s: %w", method, path, err)
	}
	return data, nil
}

// ScheduledEvents fetches the current Scheduled Events document.
func (c *Client) ScheduledEvents(ctx context.Context) (Document, error) {
	data, err := c.do(ctx, http.MethodGet, scheduledEventsPath, "api-version="+scheduledEventsVersion, nil)
	if err != nil {
		return Document{}, err
	}
	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return Document{}, fmt.Errorf("decode scheduled events: %w", err)
	}
	return doc, nil
}

// Approve asks Azure to start the event immediately.
func (c *Client) Approve(ctx context.Context, eventID string) error {
	body, err := json.Marshal(map[string]any{"StartRequests": []map[string]string{{"EventId": eventID}}})
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, scheduledEventsPath, "api-version="+scheduledEventsVersion, body)
	return err
}

// ComputeName returns the VM name from instance metadata.
func (c *Client) ComputeName(ctx context.Context) (string, error) {
	data, err := c.do(ctx, http.MethodGet, computeNamePath, computeNameQuery, nil)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
