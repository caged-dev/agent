package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/caged-dev/agent/internal/agent"
)

var (
	version = "dev"
	commit  = "none"
)

func main() {
	var cfg agent.Config
	flag.StringVar(&cfg.Workspace, "workspace", envOrDefault("CAGED_WORKSPACE", "/workspace"), "workspace root directory")
	flag.StringVar(&cfg.Socket, "socket", envOrDefault("CAGED_SOCKET", "/run/caged/agent.sock"), "communication socket path")
	flag.StringVar(&cfg.LogLevel, "log-level", envOrDefault("CAGED_LOG_LEVEL", "info"), "log level")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", envDurationOrDefault("CAGED_HEARTBEAT_INTERVAL", 5*time.Second), "heartbeat interval")
	flag.DurationVar(&cfg.MetricsInterval, "metrics-interval", envDurationOrDefault("CAGED_METRICS_INTERVAL", 10*time.Second), "metrics collection interval")
	flag.BoolVar(&cfg.WatchFiles, "watch-files", envBoolOrDefault("CAGED_WATCH_FILES", true), "observe file changes under the workspace")
	flag.DurationVar(&cfg.WatchDebounce, "watch-debounce", envDurationOrDefault("CAGED_WATCH_DEBOUNCE", 0), "coalesce repeated changes to one path within this window (0 = default)")
	flag.IntVar(&cfg.WatchRateLimit, "watch-rate-limit", envIntOrDefault("CAGED_WATCH_RATE_LIMIT", 0), "maximum reported file changes per second (0 = default)")
	// subscribe is the relay mode the runtime HOST invokes inside the
	// guest: it connects to this agent's socket, asks for the file change
	// stream and copies the frames to stdout. It exists so the host can
	// read the stream over the channel it already has into the VM, without
	// the guest ever dialing out (which would need a credential inside the
	// sandbox) and without adding socat to every sandbox image.
	subscribe := flag.Bool("subscribe", false, "connect to a running agent's socket and stream file change frames to stdout")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		slog.Info("caged-agent", "version", version, "commit", commit)
		os.Exit(0)
	}

	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *subscribe {
		// Relay mode: no watcher, no listener, no state. Frames in, frames
		// out, and a non-zero exit if the socket is not there.
		if err := agent.Subscribe(ctx, cfg.Socket, os.Stdout); err != nil {
			slog.Error("file change stream relay failed", "error", err)
			os.Exit(1)
		}
		return
	}

	slog.Info("starting caged-agent", "version", version, "workspace", cfg.Workspace, "socket", cfg.Socket)

	a, err := agent.New(cfg, logger)
	if err != nil {
		slog.Error("failed to initialize agent", "error", err)
		os.Exit(1)
	}

	if err := a.Run(ctx); err != nil {
		slog.Error("agent exited with error", "error", err)
		os.Exit(1)
	}

	slog.Info("agent shutdown complete")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBoolOrDefault(key string, def bool) bool {
	switch os.Getenv(key) {
	case "":
		return def
	case "0", "false", "FALSE", "False", "no", "off":
		return false
	default:
		return true
	}
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
