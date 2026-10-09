package main

import (
	"crypto/tls"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/avaninath/httpq/cmd/internal/httpq"
)

func main() {
	addr := flag.String("addr", ":24744", "address to listen on")
	timeout := flag.Duration("timeout", 30*time.Second, "how long to wait for the other side before timing out")
	flag.Parse()
	q := httpq.New(*timeout)

	cert, err := selfSignedCert()
	if err != nil {
		log.Fatal("error creating TLS certificate: ", err)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           q.Handler(),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}},
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      *timeout + 10*time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("listening on %s (HTTPS, self-signed certificate)", *addr)
	log.Fatal(srv.ListenAndServeTLS("", ""))
}
