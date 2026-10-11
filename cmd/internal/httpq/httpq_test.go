package httpq_test

import (
	"context"
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/avaninath/httpq/cmd/internal/httpq"
)

func TestPublisherFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())

		var wg sync.WaitGroup
		wg.Go(func() {
			if got := publish(t, srv, "orders", "hello"); got != http.StatusOK {
				t.Errorf("publish status = %d, want %d", got, http.StatusOK)
			}
		})

		synctest.Wait()

		status, body := consume(t, srv, "orders")
		if status != http.StatusOK {
			t.Errorf("consume status = %d, want %d", status, http.StatusOK)
		}
		if body != "hello" {
			t.Errorf("body = %q, want %q", body, "hello")
		}

		wg.Wait()
	})
}

func TestConsumerFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())

		var wg sync.WaitGroup
		wg.Go(func() {
			status, body := consume(t, srv, "orders")
			if status != http.StatusOK {
				t.Errorf("consume status = %d, want %d", status, http.StatusOK)
			}
			if body != "hello" {
				t.Errorf("body = %q, want %q", body, "hello")
			}
		})

		synctest.Wait()

		if got := publish(t, srv, "orders", "hello"); got != http.StatusOK {
			t.Errorf("publish status = %d, want %d", got, http.StatusOK)
		}

		wg.Wait()
	})
}

func TestPublishTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())

		start := time.Now()
		got := publish(t, srv, "orders", "hello")
		elapsed := time.Since(start)

		if got != http.StatusGatewayTimeout {
			t.Errorf("publish status = %d, want %d", got, http.StatusGatewayTimeout)
		}
		if elapsed != 30*time.Second {
			t.Errorf("timed out after %v, want %v", elapsed, 30*time.Second)
		}
	})
}

func TestConsumeTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())

		start := time.Now()
		got, _ := consume(t, srv, "orders")
		elapsed := time.Since(start)

		if got != http.StatusGatewayTimeout {
			t.Errorf("consume status = %d, want %d", got, http.StatusGatewayTimeout)
		}
		if elapsed != 30*time.Second {
			t.Errorf("timed out after %v, want %v", elapsed, 30*time.Second)
		}
	})
}

func TestFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())
		msgs := []string{"first", "second", "third"}

		var wg sync.WaitGroup
		for _, msg := range msgs {
			wg.Go(func() {
				if got := publish(t, srv, "orders", msg); got != http.StatusOK {
					t.Errorf("publish %q status = %d, want %d", msg, got, http.StatusOK)
				}
			})
			synctest.Wait()
		}

		for _, want := range msgs {
			status, body := consume(t, srv, "orders")
			if status != http.StatusOK {
				t.Errorf("consume status = %d, want %d", status, http.StatusOK)
			}
			if body != want {
				t.Errorf("body = %q, want %q", body, want)
			}
		}

		wg.Wait()
	})
}

func TestTopicIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())

		var wg sync.WaitGroup
		wg.Go(func() {
			if got := publish(t, srv, "orders", "hello"); got != http.StatusOK {
				t.Errorf("publish to orders status = %d, want %d", got, http.StatusOK)
			}
		})
		synctest.Wait()
		wg.Go(func() {
			if got := publish(t, srv, "emails", "news"); got != http.StatusOK {
				t.Errorf("publish to emails status = %d, want %d", got, http.StatusOK)
			}
		})
		synctest.Wait()

		if _, body := consume(t, srv, "emails"); body != "news" {
			t.Errorf("emails body = %q, want %q", body, "news")
		}
		if _, body := consume(t, srv, "orders"); body != "hello" {
			t.Errorf("orders body = %q, want %q", body, "hello")
		}

		wg.Wait()
	})
}

func TestGhostConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())
		client := srv.Client()
		ctx, cancel := context.WithCancel(t.Context())

		var wg sync.WaitGroup
		wg.Go(func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/channels/orders", nil)
			if err != nil {
				t.Errorf("new request: %v", err)
				return
			}
			if resp, err := client.Do(req); err == nil {
				t.Errorf("ghost got status %d, want a cancelled request", resp.StatusCode)
				_ = resp.Body.Close()
			}
		})
		synctest.Wait()
		cancel()
		synctest.Wait()

		wg.Go(func() {
			if got := publish(t, srv, "orders", "hello"); got != http.StatusOK {
				t.Errorf("publish status = %d, want %d", got, http.StatusOK)
			}
		})
		synctest.Wait()

		if _, body := consume(t, srv, "orders"); body != "hello" {
			t.Errorf("body = %q, want %q", body, "hello")
		}

		wg.Wait()
	})
}

func TestBodyLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())

		tooBig := strings.Repeat("x", 1<<20+1)
		if got := publish(t, srv, "orders", tooBig); got != http.StatusBadRequest {
			t.Errorf("publish 1 MiB + 1 byte status = %d, want %d", got, http.StatusBadRequest)
		}

		atLimit := strings.Repeat("x", 1<<20)
		var wg sync.WaitGroup
		wg.Go(func() {
			if got := publish(t, srv, "orders", atLimit); got != http.StatusOK {
				t.Errorf("publish exactly 1 MiB status = %d, want %d", got, http.StatusOK)
			}
		})
		synctest.Wait()

		if _, body := consume(t, srv, "orders"); len(body) != len(atLimit) {
			t.Errorf("consumed %d bytes, want %d", len(body), len(atLimit))
		}

		wg.Wait()
	})
}

func TestRoutes(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		want   int
		allow  []string
	}{
		{"unknown path", http.MethodGet, "/testing", http.StatusNotFound, nil},
		{"channel without a name", http.MethodGet, "/channels/", http.StatusNotFound, nil},
		{"nested channel path", http.MethodGet, "/channels/a/b/", http.StatusNotFound, nil},
		{"PUT to a channel", http.MethodPut, "/channels/orders", http.StatusMethodNotAllowed, []string{"GET", "POST"}},
		{"DELETE a channel", http.MethodDelete, "/channels/orders", http.StatusMethodNotAllowed, []string{"GET", "POST"}},
		{"POST to stats", http.MethodPost, "/stats", http.StatusMethodNotAllowed, []string{"GET"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())
			client := srv.Client()

			req, err := http.NewRequestWithContext(t.Context(), tt.method, srv.URL+tt.path, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", tt.method, tt.path, err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Errorf("close body: %v", err)
			}

			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
			if got := slices.Sorted(slices.Values(resp.Header.Values("Allow"))); !slices.Equal(got, tt.allow) {
				t.Errorf("Allow = %v, want %v", got, tt.allow)
			}
		})
	}
}

// stats mirrors the JSON served on /stats.
type stats struct {
	PublishFailures int64 `json:"publish_failures"`
	ConsumeFailures int64 `json:"consume_failures"`
	BytesPublished  int64 `json:"bytes_published"`
	BytesConsumed   int64 `json:"bytes_consumed"`
}

func TestStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := httptest.NewTestServer(t, httpq.New(30*time.Second).Handler())
		client := srv.Client()

		var wg sync.WaitGroup
		wg.Go(func() { publish(t, srv, "orders", "hello") })
		synctest.Wait()
		consume(t, srv, "orders")
		wg.Wait()

		publish(t, srv, "orders", "lost")
		consume(t, srv, "orders")

		resp, err := client.Get(srv.URL + "/stats")
		if err != nil {
			t.Fatalf("get stats: %v", err)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want %q", ct, "application/json")
		}
		var got stats
		if err := json.UnmarshalRead(resp.Body, &got); err != nil {
			t.Fatalf("decode stats: %v", err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}

		want := stats{PublishFailures: 1, ConsumeFailures: 1, BytesPublished: 5, BytesConsumed: 5}
		if got != want {
			t.Errorf("stats = %+v, want %+v", got, want)
		}
	})
}

// publish POSTs body to the topic and returns the response status code.
func publish(t *testing.T, srv *httptest.Server, topic, body string) int {
	t.Helper()
	client := srv.Client()
	resp, err := client.Post(srv.URL+"/channels/"+topic, "text/plain", strings.NewReader(body))
	if err != nil {
		t.Errorf("publish to %q: %v", topic, err)
		return 0
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("publish to %q: close body: %v", topic, err)
	}
	return resp.StatusCode
}

// consume GETs one message from the topic and returns the status code and body.
func consume(t *testing.T, srv *httptest.Server, topic string) (int, string) {
	t.Helper()
	client := srv.Client()
	resp, err := client.Get(srv.URL + "/channels/" + topic)
	if err != nil {
		t.Errorf("consume from %q: %v", topic, err)
		return 0, ""
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("consume from %q: read body: %v", topic, err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("consume from %q: close body: %v", topic, err)
	}
	return resp.StatusCode, string(body)
}
