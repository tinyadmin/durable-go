# durable-go

Go implementation of the [Durable Streams](https://github.com/durable-streams/durable-streams) protocol.

## Features

- HTTP-based append-only streams with offset-based resumption
- JSON mode with array flattening
- Byte stream mode for text/binary data
- Long-poll and SSE streaming
- TTL and Expires-At stream expiration
- CDN cursor collapsing with jitter
- Pluggable auth with multi-tenant support

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

## Authentication

The handler supports pluggable authentication via the `AuthProvider` interface:

```go
type AuthProvider interface {
    Authenticate(r *http.Request) (*AuthContext, error)
    Authorize(ctx *AuthContext, op Operation, streamURL string) error
}

type AuthContext struct {
    TenantID string         // streams scoped to /t/{TenantID}/...
    UserID   string
    Extra    map[string]any // custom claims
}
```

### Example: API Key Auth

```go
import (
    "github.com/me/durable/internal/auth"
    "github.com/me/durable/internal/handler"
)

h := handler.New(store).WithAuth(&auth.APIKeyProvider{
    Validate: func(key string) (*auth.AuthContext, error) {
        // Look up key in database
        tenant, err := db.GetTenantByAPIKey(key)
        if err != nil {
            return nil, auth.ErrInvalidCredentials
        }
        return &auth.AuthContext{
            TenantID: tenant.ID,
            UserID:   tenant.OwnerID,
        }, nil
    },
    AuthorizeFunc: func(ctx *auth.AuthContext, op auth.Operation, streamURL string) error {
        // Custom authorization logic
        if op == auth.OpDelete && !ctx.Extra["admin"].(bool) {
            return auth.ErrForbidden
        }
        return nil
    },
})
```

### Example: Bearer Token (JWT)

```go
h := handler.New(store).WithAuth(&auth.BearerProvider{
    Validate: func(token string) (*auth.AuthContext, error) {
        claims, err := jwt.Verify(token, secret)
        if err != nil {
            return nil, auth.ErrInvalidCredentials
        }
        return &auth.AuthContext{
            TenantID: claims.TenantID,
            UserID:   claims.Subject,
            Extra:    map[string]any{"roles": claims.Roles},
        }, nil
    },
})
```

### Multi-Tenant Isolation

When `TenantID` is set, streams are automatically scoped:
- Client requests `/v1/mystream`
- Stored internally as `/t/{tenantID}/v1/mystream`
- Tenants cannot access each other's streams

### Operations

| Operation | HTTP Method | Description |
|-----------|-------------|-------------|
| `OpCreate` | PUT | Create a new stream |
| `OpAppend` | POST | Append data to stream |
| `OpRead` | GET, HEAD | Read stream data or metadata |
| `OpDelete` | DELETE | Delete a stream |

## Conformance Tests

Passes all 131 tests from `@durable-streams/server-conformance-tests`.

```bash
bun install
bun run test
```

## License

MIT
