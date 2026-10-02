package main

import "net/http"

func main() {
	httpQ := &HTTPQ{}
	http.ListenAndServe(":23411", httpQ.Handler())
}
