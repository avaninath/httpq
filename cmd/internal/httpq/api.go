package httpq

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// maxBodyBytes caps the size of a published message.
const maxBodyBytes = 1 << 20 // 1 MiB

// statsResponse is the JSON body returned by the stats endpoint.
type statsResponse struct {
	PublishFailures int64 `json:"publish_failures"`
	ConsumeFailures int64 `json:"consume_failures"`
	BytesPublished  int64 `json:"bytes_published"`
	BytesConsumed   int64 `json:"bytes_consumed"`
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
