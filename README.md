# httpQ

A simple publish/subscribe message broker over HTTPS, written in Go.

Producers publish a message to a channel with a POST request, and consumers
read it with a GET request on the same channel. Each side waits until the
other one arrives. Messages are not persisted.

The server also tracks basic stats: send failures, receive failures
(timeouts), bytes published and bytes received.