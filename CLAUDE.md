# Project Context

Go implementation of [durable-streams](https://github.com/penberg/durable-streams) protocol.

## Architecture
- Zero external dependencies in core module
- SQLite and Redis+S3 backends are separate Go modules to isolate their dependencies
- `storage/` contains storage backend implementations
- `internal/handler/` contains HTTP handler logic

## Testing
Run conformance tests before pushing:
```bash
npx @anthropic-ai/durable-streams-conformance
```

## Storage Backends
- `storage/memory/` - In-memory (default, single process only)
- `storage/sqlite/` - SQLite-based persistent storage
- `storage/rediss3/` - Redis for hot data + S3 for archives (WIP)

## Status Tracking
Keep `STATUS.md` updated when fixing limitations or adding features. It documents what's missing and what works.
