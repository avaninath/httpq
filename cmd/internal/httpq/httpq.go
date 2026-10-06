package httpq

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
)

// HTTPQ is a message broker that passes messages from producers to
// consumers over named channels. A producer blocks until a consumer
// receives its message, and a consumer blocks until a producer sends one.
type HTTPQ struct {
	mu       sync.Mutex             // guards channels
	channels map[string]chan []byte // topic name -> its channel

	RxBytes  int // number of bytes (message body) consumed
	TxBytes  int // number of bytes (message body) published
	PubFails int // number of publish failures
	SubFails int // number of subscribe failures
}

// New returns an HTTPQ that is ready to serve requests.
func New() *HTTPQ {
	return &HTTPQ{
		channels: make(map[string]chan []byte),
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

// Publish returns a handler that publishes the request body to a channel.
func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := chi.URLParam(r, "channel")
		ch := h.channel(topic)
		_, err := fmt.Fprintf(w, "publish to %s via %p\n", topic, ch)
		if err != nil {
			log.Println("write failed:", err)
		}
	})
}

// Consume returns a handler that delivers the next message on a channel to the caller.
func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		topic := chi.URLParam(r, "channel")
		ch := h.channel(topic)
		_, err := fmt.Fprintf(w, "consume from %s via %p\n", topic, ch)
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
