package httpq

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
)

// maxBodyBytes caps the size of a published message.
const maxBodyBytes = 1 << 20 // 1 MiB

// HTTPQ is a message broker that passes messages from producers to
// consumers over named channels. A producer blocks until a consumer
// receives its message, and a consumer blocks until a producer sends one.
type HTTPQ struct {
	mu       sync.Mutex             // guards channels
	channels map[string]chan []byte // topic name -> its channel
	timeout  time.Duration          // decides how long publish/consume wait for the other side

	rxBytes  atomic.Int64 // bytes written out to the consumers
	txBytes  atomic.Int64 // bytes handed over by the producers
	pubFails atomic.Int64 // publishes that timed out waiting for a consumer
	subFails atomic.Int64 // consumes that timed out waiting for a producer
}

// statsResponse is the JSON body returned by the stats endpoint.
type statsResponse struct {
	PublishFailures int64 `json:"publish_failures"`
	ConsumeFailures int64 `json:"consume_failures"`
	BytesPublished  int64 `json:"bytes_published"`
	BytesConsumed   int64 `json:"bytes_consumed"`
}

// New returns an HTTPQ that is ready to serve requests. Publish and consume
// wait up to timeout for the other side before giving up.
func New(timeout time.Duration) *HTTPQ {
	return &HTTPQ{
		channels: make(map[string]chan []byte),
		timeout:  timeout,
	}
}

// channel returns the channel for the named topic, creating it on first use.
// It holds h.mu only for the lookup, never while a caller waits on the channel.
func (h *HTTPQ) channel(name string) chan []byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch, ok := h.channels[name]
	if !ok {
		ch = make(chan []byte)
		h.channels[name] = ch
	}
	return ch
}

// Handler returns an http.Handler that serves the httpQ API:
// publishing to and consuming from /channels/{channel}, and /stats.
func (h *HTTPQ) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("/stats", h.Stats().ServeHTTP)
	r.Get("/channels/{channel}", h.Consume().ServeHTTP)
	r.Post("/channels/{channel}", h.Publish().ServeHTTP)

	return r
}

// Publish returns a handler that hands the request body to a consumer of the
// channel, waiting up to the broker's timeout for one to arrive.
func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, "could not read body", http.StatusBadRequest)
			return
		}

		topic := chi.URLParam(r, "channel")
		ch := h.channel(topic)

		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()

		select {
		case ch <- body:
			h.txBytes.Add(int64(len(body)))
			w.WriteHeader(http.StatusOK)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				h.pubFails.Add(1)
				log.Printf("publish to %q: timed out after %s waiting for a consumer", topic, h.timeout)
				http.Error(w, "no consumer arrived in time", http.StatusGatewayTimeout)
				return
			}
			log.Printf("publish to %q: client disconnected before a consumer arrived", topic)
		}
	})
}

// Consume returns a handler that waits up to the broker's timeout for a message
// on the channel and writes it to the caller.
func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := chi.URLParam(r, "channel")
		ch := h.channel(topic)

		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()

		select {
		case msg := <-ch:
			if _, err := w.Write(msg); err != nil {
				log.Println("write failed:", err)
				return
			}
			h.rxBytes.Add(int64(len(msg)))
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				h.subFails.Add(1)
				log.Printf("consume from %q: timed out after %s waiting for a producer", topic, h.timeout)
				http.Error(w, "no producer arrived in time", http.StatusGatewayTimeout)
				return
			}
			log.Printf("consume from %q: client disconnected before a producer arrived", topic)
		}
	})
}

// Stats returns a handler that reports the broker's statistics as JSON.
func (h *HTTPQ) Stats() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.MarshalWrite(w, statsResponse{
			PublishFailures: h.pubFails.Load(),
			ConsumeFailures: h.subFails.Load(),
			BytesPublished:  h.txBytes.Load(),
			BytesConsumed:   h.rxBytes.Load(),
		})
		if err != nil {
			log.Println("stats write failed:", err)
		}
	})
}
