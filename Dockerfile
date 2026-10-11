# Build stage: compile a static binary.
FROM golang:1.27.1 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /httpq ./cmd/httpq

# Run stage: just the binary, no shell, no package manager, not root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /httpq /httpq
EXPOSE 24744
ENTRYPOINT ["/httpq"]