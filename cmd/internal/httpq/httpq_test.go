package httpq_test

import (
	"io"
	"net/http"
	"net/http/httptest"
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
