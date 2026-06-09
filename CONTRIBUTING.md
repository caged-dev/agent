# Contributing to Caged

Thank you for your interest in contributing! Here's how to get started.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/caged-dev/<repo>.git
cd <repo>

# Install Go 1.23+
go version

# Run tests
go test ./...

# Run with race detector
go test -race ./...

# Build
go build ./...
```

## Pull Request Process

1. Fork the repo and create a feature branch from `main`
2. Write tests for any new functionality
3. Ensure all tests pass: `go test ./...`
4. Run the linter: `golangci-lint run` (install from https://golangci-lint.run)
5. Use conventional commit messages:
   - `feat:` new features
   - `fix:` bug fixes
   - `docs:` documentation changes
   - `test:` test additions/changes
   - `refactor:` code refactoring
   - `chore:` maintenance tasks
6. Open a PR against `main`

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Use `slog` for structured logging
- Error wrapping: `fmt.Errorf("doing X: %w", err)`
- Table-driven tests with `t.Run()`
- Interfaces defined by consumers, not providers
- No `init()` functions

## Reporting Bugs

Open an issue with:
- Go version (`go version`)
- OS and architecture
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs (with sensitive data redacted)

## Feature Requests

Open an issue describing:
- The use case
- Proposed solution
- Alternatives considered

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
