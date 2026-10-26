package server

import (
	"encoding/json"
	"net/http"
	"sync"

	"mysql-guard/internal/guard"
	"mysql-guard/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer exposes /status and /metrics.
type HTTPServer struct {
	guard   *guard.Runtime
	metrics *metrics.Registry
	mu      sync.RWMutex
	status  guard.Status
}

func New(g *guard.Runtime, m *metrics.Registry) *HTTPServer {
	return &HTTPServer{guard: g, metrics: m}
}

func (s *HTTPServer) UpdateStatus(st guard.Status) {
	s.mu.Lock()
	s.status = st
	s.mu.Unlock()
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	st := s.status
	s.mu.RUnlock()
	if st.Node == "" && s.guard != nil {
		st = s.guard.Snapshot()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}
