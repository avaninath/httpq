package httpq

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// HTTPQ is a message broker that passes messages from producers to
// consumers over named channels. A producer blocks until a consumer
// receives its message, and a consumer blocks until a producer sends one.
type HTTPQ struct {
	RxBytes  int // number of bytes (message body) consumed
	TxBytes  int // number of bytes (message body) published
	PubFails int // number of publish failures
	SubFails int // number of subscribe failures
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

// Publish returns a handler that publishes the request body to a channel.
func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channel := chi.URLParam(r, "channel")
		_, err := fmt.Fprintf(w, "publish to %s\n", channel)
		if err != nil {
			log.Println("write failed:", err)
		}
	})
}

// Consume returns a handler that delivers the next message on a channel to the caller..
func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		channel := chi.URLParam(r, "channel")
		_, err := fmt.Fprintf(w, "consume from %s\n", channel)
		if err != nil {
			log.Println("write failed:", err)
		}
	})
}

// Stats returns a handler that reports the broker's statistics.
func (h *HTTPQ) Stats() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprintln(w, "stats: coming soon")
		if err != nil {
			log.Println("stats write failed:", err)
		}
	})
}
