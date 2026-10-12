# httpQ

This project is a simple yet efficient implementation of a publish/subscribe message broker over HTTPS. The broker consists of named channels, producers that publish messages to a channel, and consumers that read messages from a channel. The broker hands each message directly from a producer to a consumer.

No messages are stored. A producer waits on a channel until a consumer arrives, and a consumer waits on a channel until a producer arrives. If nobody arrives on the other side in time, the request times out. The broker keeps track of the failures and the bytes passed through, and reports them as stats.

## Broker Features
  - Channel creation
    - channels are created automatically the first time they are used
    - each channel is independent, messages never cross between channels

  - Publishing a message
    - allows producer to publish a message to a channel with a `POST` request
    - the producer waits until a consumer receives the message

  - Consuming a message
    - allows consumer to read a message from a channel with a `GET` request
    - the consumer waits until a producer sends a message

  - Ordering
    - messages are delivered in the order the producers arrived

  - Timeouts
    - producers and consumers wait for 30 seconds by default, which can be configured
    - a timeout is reported back to the user and counted in the stats

  - Stats
    - reports publish failures, consume failures, bytes published and bytes consumed as JSON

  - HTTPS
    - the server is served over TLS with a self-signed certificate, generated when the server starts

  - Graceful shutdown
    - on `Ctrl+C` or `docker stop`, the server stops accepting new requests and gives the requests in progress up to 5 seconds to finish


## Channel Creation
A channel is created the first time a producer or a consumer uses it. The channel name is part of the URL, e.g - `/channels/orders`.

*Note:* *The channels are served under the `/channels/` prefix, which differs from the examples in the task. The prefix is added on purpose, so that a channel name never clashes with `/stats` and there is room for more routes in the future. The examples from the task work the same way with the prefix, e.g - `https://localhost:24744/channels/NhPvrxcJ5WfsYJ`.*

#### User Input and Validation
The following are not allowed and return `404 Not Found`:
  - URL without the `/channels/` prefix
  - Empty channel name, e.g - `/channels/`
  - Channel name containing `/`, e.g - `/channels/a/b`

Any method other than `GET` and `POST` on a channel is not allowed and returns `405 Method Not Allowed`, along with an `Allow` header listing the methods that can be used.


## Publishing a Message
The producer publishes a message by sending it as the body of a `POST` request to a channel. The request stays open until a consumer on the same channel receives the message.

#### User Input and Validation
The message body is validated to ensure that it is within the size limit.
  - A message larger than 1 MiB is not allowed and returns `400 Bad Request`

#### Responses
  - `200 OK` - the message was handed over to a consumer
  - `400 Bad Request` - the message could not be read, e.g - it is larger than 1 MiB
  - `504 Gateway Timeout` - no consumer arrived before the timeout


## Consuming a Message
The consumer reads a message by sending a `GET` request to a channel. The request stays open until a producer on the same channel sends a message, which is returned as the response body.

#### Responses
  - `200 OK` - the message is returned as the response body, with the content type `application/octet-stream`
  - `504 Gateway Timeout` - no producer arrived before the timeout


## Timeouts
The broker is programmed to stop waiting once the timeout is reached, so that no producer or consumer waits forever. The timeout is 30 seconds by default and can be changed when starting the server.

#### Cases where a timeout occurs
  - Producer is waiting and no consumer arrives in time
    - The producer gets `504 Gateway Timeout` and the publish failures in the stats go up by one

  - Consumer is waiting and no producer arrives in time
    - The consumer gets `504 Gateway Timeout` and the consume failures in the stats go up by one

#### Cases where a client disconnects
  - Producer or consumer leaves while waiting
    - It is removed from the channel and it is not counted as a failure
    - A consumer that has left never receives a message, so the next message goes to a consumer that is still waiting


## Stats
The stats are reported as JSON on `/stats`, e.g -

```
{"publish_failures":0,"consume_failures":0,"bytes_published":14,"bytes_consumed":14}
```

  - `publish_failures` - number of publishes that timed out waiting for a consumer
  - `consume_failures` - number of consumes that timed out waiting for a producer
  - `bytes_published` - number of message bytes handed over by the producers
  - `bytes_consumed` - number of message bytes returned to the consumers


## Tech Stack
**App:** Written in Go, with [chi](https://github.com/go-chi/chi) for routing

**Testing:** [Testing in-built package in Go](https://pkg.go.dev/testing), along with [synctest](https://pkg.go.dev/testing/synctest) and [httptest](https://pkg.go.dev/net/http/httptest)

**Container:** Docker, with a multi-stage build on a [distroless](https://github.com/GoogleContainerTools/distroless) image running as a non-root user


## Run App Locally
**Option 1**

*Note:* *This requires Docker to be installed on the machine*

- Clone the project from GitHub

```git clone https://github.com/avaninath/httpq.git```

- Go to the project directory

```cd httpq```

- Build the docker image. The tests run as part of the build, and the image is only created if they pass

```docker build -t httpq .```

- Start the docker container

```docker run --rm -p 24744:24744 httpq```


**Option 2**

*Note:* *This requires Go 1.27.1 or newer to be installed on the machine*

- Clone the project from GitHub and go to the project directory, same as Option 1

- Start the app locally

```go run ./cmd/httpq```


The server starts on port `24744`. The following flags can be used to change the defaults:
  - `-addr` - address to listen on, e.g - `:24744`
  - `-timeout` - how long producers and consumers wait for the other side, e.g - `30s`

*Note:* *With Docker, the flags are added after the image name, e.g - `docker run --rm -p 24744:24744 httpq -timeout 10s`*


## Using the Broker
*Note:* *The certificate is self-signed, so `curl` needs the `-k` flag*

- Publish two messages to a channel. Each request waits until its message is consumed, so run them in separate terminals

```curl -k https://localhost:24744/channels/NhPvrxcJ5WfsYJ -d "hello 1"```

```curl -k https://localhost:24744/channels/NhPvrxcJ5WfsYJ -d "hello 2"```

- Consume the messages from the same channel. They are returned in the order they were published

```curl -k https://localhost:24744/channels/NhPvrxcJ5WfsYJ```

Output: `hello 1`

```curl -k https://localhost:24744/channels/NhPvrxcJ5WfsYJ```

Output: `hello 2`

- Check the stats

```curl -k https://localhost:24744/stats```


## Running Tests
To run tests, run the following command from the project directory

```go test -race ./...```

The tests use `synctest`, which runs the timeouts on a fake clock, so the 30 second timeouts are tested without actually waiting and the whole test suite finishes within a couple of seconds. The certificate is tested with a real TLS handshake, where a client that trusts the certificate connects and a client that does not is rejected.


## Design Decisions
  - **Unbuffered channels as the meeting point**
    - Each channel name is backed by an unbuffered Go channel. A send only completes when a receive happens at the same time, which gives the required blocking in both directions without storing any message.

  - **Channel map guarded by a mutex**
    - The channels are kept in a map, protected by a mutex. The lock is held only while looking up or creating a channel, never while a producer or consumer is waiting, so one waiting request never blocks the others.

  - **Ordering**
    - Waiting producers are served in the order they arrived, so messages are consumed in the order they were published. This comes from how the Go runtime queues waiting senders on a channel, and it is verified by the tests.

  - **Timeouts and disconnects**
    - Every request waits on a context created from the request with the configured timeout. A single `select` covers both cases: the timeout is reached, or the client disconnects. A timeout is counted as a failure and returns `504`. A disconnect is not counted as a failure, but is still logged, and the request leaves the channel straight away, so a consumer that has left can never take a message meant for someone else.

  - **The `/channels/` prefix**
    - Without a prefix, `GET /stats` and `GET /{channel}` overlap. A producer could publish to a channel named `stats`, but no consumer could ever read from it, because `GET /stats` returns the stats. The prefix keeps channel names and other routes apart.

  - **Stats**
    - The counters are atomic, so concurrent requests can update them without a lock. Bytes published are counted when a message is handed over, and bytes consumed only after the message has been written back to the consumer. Only timeouts are counted as failures.

  - **Content type of consumed messages**
    - Messages are returned as `application/octet-stream`, since the broker passes raw bytes through. Without it, Go would guess the content type from the first bytes of the message.

  - **Self-signed certificate in memory**
    - The certificate and key are generated when the server starts and never written to disk, so there are no key files to leak, commit or copy into the Docker image.

  - **Server timeouts**
    - The HTTP server limits how long reading the request headers can take (5 seconds), to protect against slow clients holding connections open. The write timeout is set to the broker timeout plus 10 seconds, so that a waiting consumer still receives its `504` response.

  - **Graceful shutdown**
    - On `Ctrl+C` or `docker stop`, the server stops accepting new connections and gives the requests in progress up to 5 seconds to finish, instead of cutting them off straight away.

  - **Logging**
    - Timeouts and disconnects are logged with the channel name, but message contents are never logged.

  - **Tests on a fake clock**
    - The tests send real HTTP requests to the real handlers, using `httptest.NewTestServer` and `synctest`. The 30 second timeouts run on a fake clock, so the tests are fast and do not depend on timing.

  - **Tests in the Docker build**
    - The Docker build runs the tests before building the binary, so an image is never created from code whose tests fail.


## Known Limitations
  - A message is delivered at most once. If a consumer disconnects after receiving the message but before the response reaches it, the message is lost, while the producer has already got `200 OK`.
  - Channels are never removed once created, so a lot of unique channel names will slowly increase the memory used by the server.
  - The stats are kept in memory and reset when the server restarts.
  - During shutdown, a producer or consumer that is still waiting for the other side cannot be matched anymore, so it is cut off once the 5 seconds have passed.


## Out of Scope / Possible Extensions
  - **Removing unused channels** - each channel could keep a count of the requests using it, increased and decreased under the mutex, and be removed from the map when the count reaches zero.
  - **Answering waiting requests during shutdown** - producers and consumers still waiting when the server shuts down could get `503 Service Unavailable` instead of being cut off.
  - **Separating the broker from HTTP** - the broker logic could live in its own type with `send` and `receive` methods, with the HTTP handlers only translating requests and responses, so the broker can be unit tested without HTTP.
  - **`413 Payload Too Large`** - an oversized message currently returns `400`. It could be detected separately and return `413` instead.
  - **At-least-once delivery** - the consumer could acknowledge a message before the producer is told it was delivered, so no message is lost when a consumer disconnects.
  - **Authentication** for producers and consumers.
  - **Per-channel stats** - left out on purpose, since keeping stats for every channel name ever used would grow without limit, and `/stats` would expose the channel names.