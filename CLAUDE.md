# Project Context

## Go Version
This project uses Go 1.25, which is the current version.

## Architecture
- Zero external dependencies in core module
- SQLite and Redis+S3 backends are separate Go modules to isolate their dependencies

## Before Pushing Code
Run conformance tests to ensure compatibility:
```bash
bun test
```
