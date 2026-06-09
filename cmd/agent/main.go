package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
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

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
