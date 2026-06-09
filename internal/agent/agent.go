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
	"sync"
	"time"
)

// Config holds agent configuration.
type Config struct {
	Workspace         string
	Socket            string
	LogLevel          string
	HeartbeatInterval time.Duration
	MetricsInterval   time.Duration
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

// Agent runs inside the sandbox VM, reporting health and metrics.
type Agent struct {
	config Config
	logger *slog.Logger
	mu     sync.Mutex
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

	return &Agent{
		config: cfg,
		logger: logger,
	}, nil
}

// Run starts the agent's main loop. It blocks until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	// Start heartbeat loop.
	go a.heartbeatLoop(ctx)

	// Start metrics collection loop.
	go a.metricsLoop(ctx)

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
		fmt.Sscanf(string(data), "MemTotal: %d kB", &total)
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
			encoder.Encode(Message{Type: "pong"})
		case "metrics":
			m := a.collectMetrics()
			payload, _ := json.Marshal(m)
			encoder.Encode(Message{Type: "metrics", Payload: payload})
		case "shutdown":
			a.logger.Info("shutdown requested by host")
			encoder.Encode(Message{Type: "ack"})
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
