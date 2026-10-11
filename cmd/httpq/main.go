package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("listening on %s (HTTPS, self-signed certificate)", *addr)
		if err := srv.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Println("shutdown:", err)
	}
}
