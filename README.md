# caged-agent

The **Caged Sandbox Agent** is the only Caged component that runs *inside* a
sandbox. Everything else observes a VM from the host; this observes it from
within, which is the only vantage point from which a file the agent changed
with `vim`, a shell redirect or `git checkout` can be seen at all.

It does four things:

- **File change observation** — a recursive, debounced, rate-limited watcher
  over the workspace, reporting *paths and operations, never contents*
- **Metrics collection** — CPU, memory and disk for the guest
- **Health reporting** — a heartbeat the host can read over the socket
- **Graceful shutdown coordination** — the host asks, the agent acknowledges
  and exits

It communicates with the Caged host over a unix socket (or a vsock, where
one is configured), and only ever answers; it never dials out. That is a
security property, not an implementation detail — see
[Why the agent has no credential](#why-the-agent-has-no-credential).

### What it does not do

Earlier versions of this README advertised **process supervision** and **init
script execution**. The agent has never implemented either, and it should
not: both moved to the host, where the trust boundary is on the right side of
them.

- **Processes** are started, stopped, enumerated and status-checked by the
  host through the runtime's process API (`StartProcess`/`StopProcess`/
  `ListProcesses`/`GetProcessStatus`), with per-process cgroup limits the
  guest cannot raise. See ADR-009 in `caged-dev/caged-api`.
- **Init scripts** run from the host at sandbox creation, from
  `SandboxConfig.init_script`, so the script and its output are recorded by
  the same pipeline as everything else.

## Installation

Inside a Caged sandbox the binary is already present and started by systemd;
the rootfs build in `caged-dev/caged-api` (`deploy/rootfs/`) installs it. For
other setups:

```bash
# From source
go install github.com/caged-dev/agent/cmd/agent@latest

# Static binary for a microVM image
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags="-s -w" -o caged-agent ./cmd/agent
```

## Usage

```bash
caged-agent \
  --workspace /workspace \
  --socket /run/caged/agent.sock \
  --log-level info
```

Relay mode, which the host uses to read the change stream over the channel it
already has into the guest:

```bash
caged-agent -subscribe        # frames from the socket to stdout
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CAGED_WORKSPACE` | `/workspace` | Root directory to watch |
| `CAGED_SOCKET` | `/run/caged/agent.sock` | Communication socket path |
| `CAGED_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `CAGED_HEARTBEAT_INTERVAL` | `5s` | Heartbeat interval |
| `CAGED_METRICS_INTERVAL` | `10s` | Metrics collection interval |
| `CAGED_WATCH_FILES` | `true` | Observe file changes under the workspace |
| `CAGED_WATCH_DEBOUNCE` | `300ms` | Coalescing window per path |
| `CAGED_WATCH_RATE_LIMIT` | `200` | Maximum reported changes per second |

## Socket protocol

Newline-delimited JSON, in both directions, in one envelope:

```json
{"type": "...", "payload": { }}
```

| Host sends | Agent answers |
|---|---|
| `ping` | `pong` |
| `metrics` | `metrics` with a `Metrics` payload |
| `watch_files` | `watch_started`, then a `file_op` stream until the connection closes |
| `shutdown` | `ack`, then exits |

A `file_op` payload is deliberately small:

```json
{"operation": "write", "path": "/workspace/.env", "size": 42,
 "ts_unix_ms": 1758240000000, "dropped_since_last": 0}
```

`operation` is one of `create`, `write`, `delete`, `rename`. **There is no
content field, and there never will be.** A path may itself be sensitive
(`/workspace/.env`) and is still reported — naming the file the agent touched
is the entire point, and it is what feeds Caged's trust scoring. The bytes in
it are never read, so they cannot leak.

`watch_started` reports whether observation is actually on, and why not if it
is off. A host reading a quiet stream must be able to tell "nothing changed"
from "this agent cannot watch".

## File watching, and what happens under a flood

Raw inotify on a workspace is not usable as an event source. `npm install`
writes tens of thousands of files in seconds; `go build` churns a cache; a
test run rewrites coverage files in a loop. Forwarded verbatim that is a
denial of service against the pipeline that is supposed to be recording the
agent's work.

Four bounds apply, in order, and each one **counts what it discards rather
than growing to hold it**:

1. **Ignore rules.** A path with an ignored directory segment
   (`node_modules`, `.git`, `target`, `dist`, `__pycache__`, `.venv`,
   `.next`, `vendor`, `site-packages`, and more) is dropped before it is
   queued, as are editor scratch files (`*~`, `*.swp`, vim's `4913`).
   Dotfiles are *not* ignored: `.env` is the most important path here.
2. **A bounded raw queue.** The inotify reader never blocks on the
   coalescer; a full queue drops and counts.
3. **Per-path coalescing.** Repeated writes to one path within the debounce
   window become one event carrying the final size, and the number of
   distinct in-flight paths is itself capped.
4. **A token-bucket rate limit** on emission, with a two-second burst.

`IN_MODIFY` is deliberately not watched — `IN_CLOSE_WRITE` is one
notification per completed write session instead of one per `write()` call,
which removes an order of magnitude of noise at the source.

Loss is never silent: the number of changes dropped since the previous frame
travels on the next frame as `dropped_since_last`, the first drop is logged
at ERROR, and a periodic summary re-states the counters while it continues.

## Why the agent has no credential

The agent runs in the VM that runs untrusted code. Anything it could
authenticate with is something that code can read out of its memory or its
environment. So it holds nothing and initiates nothing: the host connects to
the socket and reads, and the change events travel host-side over the
authenticated gRPC link that already exists between the runtime host and the
API. See ADR-034 in `caged-dev/caged-api`.

## Architecture

```
┌─────────────────────────────────────────────────┐
│ Firecracker microVM                             │
│                                                 │
│  ┌───────────────────────────────────────┐      │
│  │            caged-agent                │      │
│  │                                       │      │
│  │  ┌────────────┐  ┌─────────────────┐  │      │
│  │  │ FS watcher │  │ Metrics         │  │      │
│  │  │ (inotify,  │  │ Health/heartbeat│  │      │
│  │  │  coalesced)│  │ Shutdown ack    │  │      │
│  │  └────────────┘  └─────────────────┘  │      │
│  └───────────────────────────────────────┘      │
│           │ unix socket / vsock (inbound only)  │
└───────────┼─────────────────────────────────────┘
            │
    ┌───────┴────────┐        ┌──────────────┐
    │  Caged runtime │ ─gRPC─▶│  Caged API   │
    │  (host)        │        │  events      │
    └────────────────┘        └──────────────┘
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

The file watcher is Linux-only (inotify, via `golang.org/x/sys`). The package
builds everywhere; on a non-Linux host the watcher reports itself
unsupported and the agent starts without it rather than refusing to run.

## Development

```bash
go test ./...
go test -race ./...
golangci-lint run
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT — see [LICENSE](LICENSE).
