// Package agent implements the Caged sandbox agent that runs inside each microVM.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/caged-dev/agent/internal/watcher"
)

// Config holds agent configuration.
type Config struct {
	Workspace         string
	Socket            string
	LogLevel          string
	HeartbeatInterval time.Duration
	MetricsInterval   time.Duration

	// WatchFiles enables the workspace file change watcher. On by default
	// in cmd/agent: a sandbox whose file changes are not observed is a
	// sandbox Caged is describing incorrectly.
	WatchFiles bool
	// WatchDebounce is how long a path must be quiet before its coalesced
	// change is reported. Zero takes watcher.DefaultDebounce.
	WatchDebounce time.Duration
	// WatchRateLimit caps sustained reported changes per second. Zero
	// takes watcher.DefaultRatePerSecond.
	WatchRateLimit int
	// WatchIgnoreDirs overrides watcher.DefaultIgnoreDirs when non-nil.
	WatchIgnoreDirs []string
}

// Metrics holds system metrics collected by the agent.
type Metrics struct {
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryUsedMB  int64   `json:"memory_used_mb"`
	MemoryTotalMB int64   `json:"memory_total_mb"`
	DiskUsedMB    int64   `json:"disk_used_mb"`
	DiskTotalMB   int64   `json:"disk_total_mb"`
	NumProcesses  int     `json:"num_processes"`
	Timestamp     int64   `json:"timestamp"`
}

// Message represents a message between agent and host.
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Agent runs inside the sandbox VM, reporting health and metrics and
// observing file changes in the workspace.
type Agent struct {
	config Config
	logger *slog.Logger

	// watcher is nil when file observation is off — either disabled by
	// configuration or unsupported on this platform. Nil is a legitimate
	// state and is REPORTED to a subscriber rather than looking like a
	// workspace where nothing ever changes.
	watcher             *watcher.Watcher
	watchDisabledReason string
	hub                 *fileOpHub
}

// New creates a new agent instance.
func New(cfg Config, logger *slog.Logger) (*Agent, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Ensure workspace exists.
	if err := os.MkdirAll(cfg.Workspace, 0755); err != nil {
		return nil, fmt.Errorf("creating workspace directory: %w", err)
	}

	a := &Agent{
		config: cfg,
		logger: logger,
		hub:    newFileOpHub(logger),
	}

	if cfg.WatchFiles {
		w, err := watcher.New(watcher.Config{
			Root:          cfg.Workspace,
			Debounce:      cfg.WatchDebounce,
			RatePerSecond: cfg.WatchRateLimit,
			IgnoreDirs:    cfg.WatchIgnoreDirs,
		}, logger)
		switch {
		case err == nil:
			a.watcher = w
		default:
			// A watcher that cannot start must not stop the sandbox from
			// running the customer's workload. It is loud, and the reason
			// travels to the host on every subscription, so the gap is
			// visible instead of looking like an idle workspace.
			a.watchDisabledReason = err.Error()
			logger.Error("file change observation is OFF: the workspace watcher could not start",
				"workspace", cfg.Workspace,
				"error", err,
				"consequence", "file changes made inside this sandbox will not appear in the session timeline")
		}
	} else {
		a.watchDisabledReason = "file watching is disabled by configuration (CAGED_WATCH_FILES=false)"
	}

	return a, nil
}

// Run starts the agent's main loop. It blocks until ctx is cancelled.
//
// Every goroutine it starts is bounded by ctx, and the watcher is closed
// before Run returns, so a shutdown leaks neither the inotify descriptor
// nor the fan-out goroutine.
func (a *Agent) Run(ctx context.Context) error {
	// Start heartbeat loop.
	go a.heartbeatLoop(ctx)

	// Start metrics collection loop.
	go a.metricsLoop(ctx)

	// Start the workspace watcher and the fan-out to subscribed hosts.
	if a.watcher != nil {
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			if err := a.watcher.Run(ctx); err != nil && ctx.Err() == nil {
				a.logger.Error("workspace watcher stopped; file changes are no longer observed",
					"error", err)
			}
		}()
		hubDone := make(chan struct{})
		go func() {
			defer close(hubDone)
			a.runFileOpHub(ctx)
		}()
		defer func() {
			// Close releases the inotify descriptor, which ends
			// watcher.Run, which closes its event channel, which ends the
			// hub. Waiting on both is what makes "no leaked watcher"
			// testable rather than merely intended.
			if err := a.watcher.Close(); err != nil {
				a.logger.Warn("closing workspace watcher", "error", err)
			}
			<-watchDone
			<-hubDone
		}()
		a.logger.Info("file change observation on", "workspace", a.config.Workspace)
	}

	// Listen for host commands on the socket.
	if err := a.listenSocket(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("socket listener: %w", err)
	}

	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(a.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.logger.Debug("heartbeat", "workspace", a.config.Workspace)
		}
	}
}

func (a *Agent) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(a.config.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m := a.collectMetrics()
			a.logger.Debug("metrics",
				"cpu_percent", m.CPUPercent,
				"memory_used_mb", m.MemoryUsedMB,
				"num_processes", m.NumProcesses,
			)
		}
	}
}

func (a *Agent) collectMetrics() Metrics {
	var m Metrics
	m.Timestamp = time.Now().Unix()
	m.NumProcesses = runtime.NumGoroutine() // Proxy for now

	// Read memory info from /proc/meminfo.
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		var total, available int64
		_, _ = fmt.Sscanf(string(data), "MemTotal: %d kB", &total)
		// Find MemAvailable line.
		for _, line := range splitLines(string(data)) {
			if n, _ := fmt.Sscanf(line, "MemAvailable: %d kB", &available); n == 1 {
				break
			}
		}
		m.MemoryTotalMB = total / 1024
		m.MemoryUsedMB = (total - available) / 1024
	}

	// Disk usage for workspace.
	if stat, err := diskUsage(a.config.Workspace); err == nil {
		m.DiskTotalMB = int64(stat.Total / (1024 * 1024))
		m.DiskUsedMB = int64(stat.Used / (1024 * 1024))
	}

	return m
}

func (a *Agent) listenSocket(ctx context.Context) error {
	// Remove stale socket.
	os.Remove(a.config.Socket)

	// Ensure socket directory exists.
	if err := os.MkdirAll(filepath.Dir(a.config.Socket), 0755); err != nil {
		return fmt.Errorf("creating socket dir: %w", err)
	}

	listener, err := net.Listen("unix", a.config.Socket)
	if err != nil {
		return fmt.Errorf("listening on socket: %w", err)
	}
	defer listener.Close()

	a.logger.Info("agent listening", "socket", a.config.Socket)

	// Accept loop.
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			a.logger.Error("accept error", "error", err)
			continue
		}
		go a.handleConnection(ctx, conn)
	}
}

func (a *Agent) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			return // Connection closed.
		}

		switch msg.Type {
		case "ping":
			_ = encoder.Encode(Message{Type: "pong"})
		case "metrics":
			m := a.collectMetrics()
			payload, _ := json.Marshal(m)
			_ = encoder.Encode(Message{Type: "metrics", Payload: payload})
		case MsgWatchFiles:
			// This connection becomes a one-way stream from here on: the
			// host asked for changes, not for a request/response session.
			a.streamFileOps(ctx, conn, encoder)
			return
		case "shutdown":
			a.logger.Info("shutdown requested by host")
			_ = encoder.Encode(Message{Type: "ack"})
			return
		default:
			a.logger.Warn("unknown message type", "type", msg.Type)
		}
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := range s {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
