package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

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
	w, err := New(srv.URL, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []observability.Stage{observability.StageDetected, observability.StageDrained, observability.StageApproved} {
		w.Notify(context.Background(), observability.Notification{Stage: s, NodeName: "n"})
	}
	if count != 2 {
		t.Fatalf("requests = %d, want 2", count)
	}
}

func TestWebhookDefaultTemplateEscapes(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
	}))
	defer srv.Close()
	w, err := New(srv.URL, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageDrained, Description: "say \"hi\"\n"})
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
	w, _ := New(srv.URL, "", nil)
	w.Notify(context.Background(), observability.Notification{Stage: observability.StageApproved})
}

func TestWebhookBadTemplate(t *testing.T) {
	if _, err := New("http://x", "{{", nil); err == nil {
		t.Fatal("expected error")
	}
}
