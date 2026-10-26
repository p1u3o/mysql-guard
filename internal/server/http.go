package server

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"sync"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/guard"
	"mysql-guard/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer exposes UI, /status, /api/actions, and /metrics.
type HTTPServer struct {
	guard   *guard.Runtime
	store   *consul.Store
	metrics *metrics.Registry
	mu      sync.RWMutex
	status  guard.Status
}

func New(g *guard.Runtime, store *consul.Store, m *metrics.Registry) *HTTPServer {
	return &HTTPServer{guard: g, store: store, metrics: m}
}

func (s *HTTPServer) UpdateStatus(st guard.Status) {
	s.mu.Lock()
	s.status = st
	s.mu.Unlock()
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/api/actions", s.handleActions)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "ui missing", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(b)
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

func (s *HTTPServer) handleActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	list, err := s.store.GetActionLog(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"entries": list})
}
