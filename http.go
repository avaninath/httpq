package main

import (
	"net/http"

	"github.com/go-chi/chi"
)

type HTTPQ struct {
	RxBytes  int // number of bytes (message body) consumed
	TxBytes  int // number of bytes (message body) published
	PubFails int // number of publish failures
	SubFails int // number of subscribe failures
}

func (h *HTTPQ) Handler() http.Handler {
	r := chi.NewRouter()

	r.Get("", h.Consume().ServeHTTP)
	r.Post("", h.Publish().ServeHTTP)

	return r
}

func (h *HTTPQ) Publish() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

	})
}

func (h *HTTPQ) Consume() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

	})
}
