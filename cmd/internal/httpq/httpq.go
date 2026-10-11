package httpq

import (
	"sync"
	"sync/atomic"
	"time"
)

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
