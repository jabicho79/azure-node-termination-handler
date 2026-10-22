package imds

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const sampleDoc = `{"DocumentIncarnation":3,"Events":[{"EventId":"E1","EventType":"Redeploy","ResourceType":"VirtualMachine","Resources":["pool_3"],"EventStatus":"Scheduled","NotBefore":"Mon, 19 Sep 2016 18:29:47 GMT","Description":"degraded hardware","EventSource":"Platform","DurationInSeconds":-1}]}`

func TestScheduledEventsParses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata") != "true" {
			t.Errorf("Metadata header = %q", r.Header.Get("Metadata"))
		}
		if r.URL.Path != "/metadata/scheduledevents" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if v := r.URL.Query().Get("api-version"); v != "2020-07-01" {
			t.Errorf("api-version = %q", v)
		}
		_, _ = io.WriteString(w, sampleDoc)
	}))
	defer srv.Close()

	doc, err := NewClient(srv.URL, nil).ScheduledEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if doc.DocumentIncarnation != 3 || len(doc.Events) != 1 {
		t.Fatalf("doc = %+v", doc)
	}
	e := doc.Events[0]
	if e.EventID != "E1" || e.EventType != EventRedeploy || e.ResourceType != "VirtualMachine" ||
		len(e.Resources) != 1 || e.Resources[0] != "pool_3" || e.EventStatus != StatusScheduled ||
		e.NotBefore != "Mon, 19 Sep 2016 18:29:47 GMT" || e.Description != "degraded hardware" ||
		e.EventSource != "Platform" || e.DurationInSeconds != -1 {
		t.Errorf("event = %+v", e)
	}
}

func TestApprovePostsStartRequest(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if r.Header.Get("Metadata") != "true" {
			t.Errorf("Metadata header = %q", r.Header.Get("Metadata"))
		}
		if r.URL.Path != "/metadata/scheduledevents" || r.URL.Query().Get("api-version") != "2020-07-01" {
			t.Errorf("url = %s", r.URL)
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != `{"StartRequests":[{"EventId":"E1"}]}` {
			t.Errorf("body = %s", b)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := NewClient(srv.URL, nil).Approve(context.Background(), "E1"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server not called")
	}
}

func TestApproveNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	err := NewClient(srv.URL, nil).Approve(context.Background(), "E1")
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("err = %v", err)
	}
}

func TestScheduledEventsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	_, err := NewClient(srv.URL, nil).ScheduledEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), "/metadata/scheduledevents") {
		t.Errorf("error should include path: %v", err)
	}
}

func TestScheduledEventsBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "{")
	}))
	defer srv.Close()
	if _, err := NewClient(srv.URL, nil).ScheduledEvents(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

func TestScheduledEventsTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := NewClient(srv.URL, nil).ScheduledEvents(ctx); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestComputeName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata") != "true" {
			t.Errorf("Metadata header = %q", r.Header.Get("Metadata"))
		}
		if r.URL.Path != "/metadata/instance/compute/name" {
			t.Errorf("path = %q", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("api-version") != "2021-02-01" || q.Get("format") != "text" {
			t.Errorf("query = %q", r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, "pool_3\n")
	}))
	defer srv.Close()
	got, err := NewClient(srv.URL, nil).ComputeName(context.Background())
	if err != nil || got != "pool_3" {
		t.Fatalf("got %q, %v", got, err)
	}
}
