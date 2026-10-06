package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/avaninath/httpq/cmd/internal/httpq"
)

func main() {
	addr := flag.String("addr", ":24744", "address to listen on")
	flag.Parse()
	q := httpq.New()

	log.Printf("listening on %s", *addr)
	err := http.ListenAndServe(*addr, q.Handler())
	log.Fatal(err)
}
