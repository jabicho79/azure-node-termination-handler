package fakeimds_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/jabicho79/azure-node-termination-handler/pkg/fakeimds"
	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
)

func TestRoundTrip(t *testing.T) {
	ts := httptest.NewServer(fakeimds.NewServer())
	defer ts.Close()
	ctx := context.Background()

	doc := imds.Document{Events: []imds.Event{{
		EventID: "E1", EventType: imds.EventReboot, EventStatus: imds.StatusScheduled,
		Resources: []string{"fake-vm"},
	}}}
	body, _ := json.Marshal(doc)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/control/document", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}

	c := imds.NewClient(ts.URL, nil)
	got, err := c.ScheduledEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.DocumentIncarnation != 1 || len(got.Events) != 1 || got.Events[0].EventID != "E1" ||
		got.Events[0].EventStatus != imds.StatusScheduled {
		t.Fatalf("unexpected document: %+v", got)
	}

	name, err := c.ComputeName(ctx)
	if err != nil || name != "fake-vm" {
		t.Fatalf("ComputeName = %q, %v", name, err)
	}

	if err := c.Approve(ctx, "E1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Approve(ctx, "unknown"); err != nil {
		t.Fatal(err)
	}

	got, err = c.ScheduledEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Events[0].EventStatus != imds.StatusStarted {
		t.Fatalf("E1 status = %q, want Started", got.Events[0].EventStatus)
	}
	if got.DocumentIncarnation != 1 {
		t.Fatalf("approval bumped incarnation to %d", got.DocumentIncarnation)
	}

	approvals := getApprovals(t, ts.URL)
	if !reflect.DeepEqual(approvals, []string{"E1", "unknown"}) {
		t.Fatalf("approvals = %v", approvals)
	}

	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/control/approvals", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if a := getApprovals(t, ts.URL); len(a) != 0 {
		t.Fatalf("approvals after reset = %v", a)
	}
}

func getApprovals(t *testing.T, base string) []string {
	t.Helper()
	resp, err := http.Get(base + "/control/approvals")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out []string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRequiresMetadataHeader(t *testing.T) {
	ts := httptest.NewServer(fakeimds.NewServer())
	defer ts.Close()

	for _, url := range []string{
		ts.URL + "/metadata/scheduledevents?api-version=2020-07-01",
		ts.URL + "/metadata/instance/compute/name?api-version=2021-02-01&format=text",
	} {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s without header: status = %d, want 400", url, resp.StatusCode)
		}
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/metadata/scheduledevents", nil)
	req.Header.Set("Metadata", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("GET without api-version: status = %d, want 400", resp.StatusCode)
	}
}
