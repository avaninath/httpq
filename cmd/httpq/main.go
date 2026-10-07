package main

import (
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

	log.Printf("listening on %s", *addr)
	err := http.ListenAndServe(*addr, q.Handler())
	log.Fatal(err)
}
