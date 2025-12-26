# durable-go

Go implementation of the [Durable Streams](https://github.com/durable-streams/durable-streams) protocol.

## Features

- HTTP-based append-only streams with offset-based resumption
- JSON mode with array flattening
- Byte stream mode for text/binary data
- Long-poll and SSE streaming
- TTL and Expires-At stream expiration
- CDN cursor collapsing with jitter

## Storage Backends

| Backend | Description |
|---------|-------------|
| `memory` | In-memory, no persistence (default) |
| `sqlite` | SQLite with WAL mode, survives restarts |

## Usage

```bash
# Build
go build -o durable-server ./cmd/durable-server

# Run with memory storage (default)
./durable-server

# Run with SQLite
./durable-server -storage sqlite -db /path/to/durable.db

# Options
./durable-server -help
```

## API

```bash
# Create stream
curl -X PUT -H "Content-Type: application/json" http://localhost:4437/v1/mystream

# Append data
curl -X POST -H "Content-Type: application/json" -d '{"msg":"hello"}' http://localhost:4437/v1/mystream

# Read from beginning
curl http://localhost:4437/v1/mystream

# Read from offset
curl "http://localhost:4437/v1/mystream?offset=<offset>"

# Long-poll for new data
curl "http://localhost:4437/v1/mystream?offset=<offset>&live=long-poll"

# SSE stream
curl "http://localhost:4437/v1/mystream?offset=<offset>&live=sse"

# Delete stream
curl -X DELETE http://localhost:4437/v1/mystream
```

## Conformance Tests

Passes all 131 tests from `@durable-streams/server-conformance-tests`.

```bash
bun install
bun run test
```

## License

MIT
