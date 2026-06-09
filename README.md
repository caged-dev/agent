# caged-agent

The **Caged Sandbox Agent** runs inside each Firecracker microVM to provide:

- Health reporting & heartbeat
- File system watching (change events)
- Process supervision
- Metrics collection (CPU, memory, disk, network)
- Init script execution
- Graceful shutdown coordination

It communicates with the Caged host over a virtio-vsock or unix socket.

## Installation

```bash
# Pre-built binary
curl -fsSL https://github.com/caged-dev/agent/releases/latest/download/caged-agent-linux-amd64 -o /usr/local/bin/caged-agent
chmod +x /usr/local/bin/caged-agent

# From source
go install github.com/caged-dev/agent@latest

# Docker
docker pull ghcr.io/caged-dev/agent:latest
```

## Usage

The agent starts automatically inside Caged sandboxes. For custom setups:

```bash
caged-agent \
  --workspace /workspace \
  --socket /run/caged/agent.sock \
  --log-level info
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CAGED_WORKSPACE` | `/workspace` | Root directory to watch |
| `CAGED_SOCKET` | `/run/caged/agent.sock` | Communication socket path |
| `CAGED_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `CAGED_HEARTBEAT_INTERVAL` | `5s` | Heartbeat reporting interval |
| `CAGED_METRICS_INTERVAL` | `10s` | Metrics collection interval |

## Architecture

```
┌─────────────────────────────────────────────────┐
│ Firecracker microVM                              │
│                                                  │
│  ┌──────────────────────────────────────┐       │
│  │         caged-agent                    │       │
│  │                                        │       │
│  │  ┌───────────┐  ┌────────────────┐   │       │
│  │  │ FS Watcher │  │ Process Mgr    │   │       │
│  │  └───────────┘  └────────────────┘   │       │
│  │  ┌───────────┐  ┌────────────────┐   │       │
│  │  │ Metrics   │  │ Health/HB      │   │       │
│  │  └───────────┘  └────────────────┘   │       │
│  └──────────────────────────────────────┘       │
│           │ unix socket / vsock                   │
└───────────┼──────────────────────────────────────┘
            │
    ┌───────┴───────┐
    │  Caged Host   │
    └───────────────┘
```

## Building

```bash
# Native
go build -o caged-agent ./cmd/agent

# Linux AMD64 (for microVMs)
GOOS=linux GOARCH=amd64 go build -o caged-agent ./cmd/agent

# Minimal static binary
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o caged-agent ./cmd/agent
```

## Docker

```bash
docker build -t caged-agent .
docker run --rm caged-agent --help
```

## Development

```bash
# Run tests
go test ./...

# Run with race detector
go test -race ./...

# Lint
golangci-lint run
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT — see [LICENSE](LICENSE).
