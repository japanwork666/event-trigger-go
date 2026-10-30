package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	httpAddr         = ":8080"
	eventQueueSize   = 10000
	webhookQueueSize = 1000
	batchSize        = 1000
	batchInterval    = 5 * time.Second
	shutdownTimeout  = 30 * time.Second
)

type Event struct {
	TraceID   string  `json:"TraceID"`
	UserID    string  `json:"UserID"`
	EventName string  `json:"EventName"`
	Amount    float64 `json:"Amount"`
	Timestamp int64   `json:"Timestamp"`
}

type Service struct {
	events   chan Event
	webhooks chan Event

	mu          sync.Mutex
	seen        map[string]struct{}
	webhookSeen map[string]struct{}

	accepting atomic.Bool

	accepted   atomic.Int64
	duplicates atomic.Int64
	triggered  atomic.Int64
	stored     atomic.Int64
}

func NewService() *Service {
	s := &Service{
		events:      make(chan Event, eventQueueSize),
		webhooks:    make(chan Event, webhookQueueSize),
		seen:        make(map[string]struct{}),
		webhookSeen: make(map[string]struct{}),
	}

	s.accepting.Store(true)

	return s
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (s *Service) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.accepting.Load() {
		http.Error(w, "service shutting down", http.StatusServiceUnavailable)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var e Event

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&e); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "expected one JSON object", http.StatusBadRequest)
		return
	}

	if e.TraceID == "" ||
		e.UserID == "" ||
		e.EventName == "" ||
		e.Amount < 0 ||
		e.Timestamp <= 0 {
		http.Error(w, "invalid event fields", http.StatusBadRequest)
		return
	}

	s.mu.Lock()

	if !s.accepting.Load() {
		s.mu.Unlock()
		http.Error(w, "service shutting down", http.StatusServiceUnavailable)
		return
	}

	if _, exists := s.seen[e.TraceID]; exists {
		s.mu.Unlock()

		s.duplicates.Add(1)

		writeJSON(w, http.StatusAccepted, map[string]string{
			"status": "duplicate_ignored",
		})

		return
	}

	// Reserve TraceID before enqueueing.
	// Prevent concurrent duplicate requests.
	s.seen[e.TraceID] = struct{}{}

	select {
	case s.events <- e:
		s.accepted.Add(1)
		s.mu.Unlock()

		writeJSON(w, http.StatusAccepted, map[string]string{
			"status": "accepted",
		})

	default:
		// Queue full: release the reserved TraceID
		// so the client can retry later.
		delete(s.seen, e.TraceID)
		s.mu.Unlock()

		http.Error(
			w,
			"event queue full, retry later",
			http.StatusServiceUnavailable,
		)
	}
}

func (s *Service) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"accepting":     s.accepting.Load(),
		"accepted":      s.accepted.Load(),
		"duplicates":    s.duplicates.Load(),
		"triggered":     s.triggered.Load(),
		"stored":        s.stored.Load(),
		"event_queue":   len(s.events),
		"webhook_queue": len(s.webhooks),
	})
}

func (s *Service) batchWriter() {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	batch := make([]Event, 0, batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Demo only: simulate database batch write.
		slog.Info(
			"mock batch DB write",
			"count", len(batch),
		)

		s.stored.Add(int64(len(batch)))

		batch = batch[:0]
	}

	defer func() {
		// Flush remaining events before closing webhook queue.
		flush()

		close(s.webhooks)

		slog.Info("batch writer drained")
	}()

	for {
		select {
		case e, ok := <-s.events:
			if !ok {
				return
			}

			batch = append(batch, e)

			if e.EventName == "AddToCart" && e.Amount > 1000 {
				s.mu.Lock()

				_, exists := s.webhookSeen[e.TraceID]

				if !exists {
					s.webhookSeen[e.TraceID] = struct{}{}
				}

				s.mu.Unlock()

				if !exists {
					// Backpressure: wait rather than silently dropping.
					s.webhooks <- e

					s.triggered.Add(1)
				}
			}

			if len(batch) >= batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

func (s *Service) webhookWorker() {
	for e := range s.webhooks {
		// Demo only: simulate LINE coupon webhook.
		slog.Info(
			"mock LINE coupon webhook",
			"trace_id", e.TraceID,
			"user_id", e.UserID,
			"amount", e.Amount,
			"message", "high-value cart coupon",
		)

		time.Sleep(100 * time.Millisecond)
	}

	slog.Info("webhook worker drained")
}

func main() {
	slog.SetDefault(
		slog.New(
			slog.NewJSONHandler(os.Stdout, nil),
		),
	)

	s := NewService()

	mux := http.NewServeMux()

	mux.HandleFunc("/events", s.ingest)
	mux.HandleFunc("/health", s.health)

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Start background workers.
	var workers sync.WaitGroup

	workers.Add(2)

	go func() {
		defer workers.Done()
		s.webhookWorker()
	}()

	go func() {
		defer workers.Done()
		s.batchWriter()
	}()

	// Start HTTP server.
	serverErr := make(chan error, 1)

	go func() {
		slog.Info("server started", "addr", httpAddr)

		serverErr <- srv.ListenAndServe()
	}()

	// Listen for SIGINT and SIGTERM.
	sigCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	select {
	case <-sigCtx.Done():
		slog.Info("shutdown signal received")

	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server stopped", "error", err)
		}
	}

	// Step 1: Stop accepting new events.
	s.mu.Lock()
	s.accepting.Store(false)
	s.mu.Unlock()

	slog.Info("event ingestion disabled")

	// Step 2: Create shutdown deadline.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancel()

	// Step 3: Gracefully stop HTTP server.
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error(
			"HTTP shutdown failed",
			"error", err,
		)

		// Force close remaining HTTP connections.
		_ = srv.Close()
	}

	// Step 4: Close event queue.
	// HTTP handlers have completed before this point
	// when Shutdown succeeds.
	close(s.events)

	slog.Info("event queue closed, draining workers")

	// Step 5: Wait for Batch Writer and Webhook Worker.
	done := make(chan struct{})

	go func() {
		workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info(
			"shutdown complete",
			"accepted", s.accepted.Load(),
			"duplicates", s.duplicates.Load(),
			"triggered", s.triggered.Load(),
			"stored", s.stored.Load(),
		)

	case <-ctx.Done():
		slog.Error(
			"worker shutdown deadline exceeded",
			"accepted", s.accepted.Load(),
			"stored", s.stored.Load(),
			"triggered", s.triggered.Load(),
			"event_queue_remaining", len(s.events),
			"webhook_queue_remaining", len(s.webhooks),
		)
	}
}
