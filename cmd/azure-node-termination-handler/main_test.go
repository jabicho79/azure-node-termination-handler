package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbes(t *testing.T) {
	for _, tc := range []struct {
		path  string
		ready bool
		want  int
	}{
		{"/livez", false, http.StatusOK},
		{"/livez", true, http.StatusOK},
		{"/healthz", false, http.StatusServiceUnavailable},
		{"/healthz", true, http.StatusOK},
	} {
		mux := newProbesMux(func() bool { return tc.ready })
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("GET %s (ready=%v) = %d, want %d", tc.path, tc.ready, rec.Code, tc.want)
		}
	}
}
