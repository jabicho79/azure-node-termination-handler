// Package fakeimds is a fake Azure IMDS Scheduled Events server for e2e tests.
package fakeimds

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/jabicho79/azure-node-termination-handler/pkg/imds"
)

const fakeVMName = "fake-vm"

// Server is an http.Handler serving a controllable Scheduled Events document.
type Server struct {
	mux       *http.ServeMux
	mu        sync.Mutex
	doc       imds.Document
	approvals []string
}

// NewServer returns an empty fake IMDS.
func NewServer() *Server {
	s := &Server{mux: http.NewServeMux(), approvals: []string{}}
	s.mux.HandleFunc("/metadata/scheduledevents", s.scheduledEvents)
	s.mux.HandleFunc("/metadata/instance/compute/name", s.computeName)
	s.mux.HandleFunc("/control/document", s.controlDocument)
	s.mux.HandleFunc("/control/approvals", s.controlApprovals)
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) scheduledEvents(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Metadata") != "true" || r.URL.Query().Get("api-version") == "" {
		http.Error(w, "Metadata header and api-version are required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		defer s.mu.Unlock()
		writeJSON(w, s.doc)
	case http.MethodPost:
		var body struct {
			StartRequests []struct {
				EventID string `json:"EventId"`
			} `json:"StartRequests"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, sr := range body.StartRequests {
			s.approvals = append(s.approvals, sr.EventID)
			for i := range s.doc.Events {
				if s.doc.Events[i].EventID == sr.EventID {
					s.doc.Events[i].EventStatus = imds.StatusStarted
				}
			}
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) computeName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Metadata") != "true" {
		http.Error(w, "Metadata header is required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(fakeVMName))
}

func (s *Server) controlDocument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in imds.Document
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doc.Events = in.Events
	s.doc.DocumentIncarnation++
	w.WriteHeader(http.StatusOK)
}

func (s *Server) controlApprovals(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, append([]string{}, s.approvals...))
	case http.MethodDelete:
		s.approvals = []string{}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
