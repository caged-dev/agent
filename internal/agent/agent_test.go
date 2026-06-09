package agent

import (
	"context"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Workspace:         dir,
		Socket:            dir + "/agent.sock",
		LogLevel:          "info",
		HeartbeatInterval: 1 * time.Second,
		MetricsInterval:   1 * time.Second,
	}

	a, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a == nil {
		t.Fatal("expected non-nil agent")
	}
}

func TestCollectMetrics(t *testing.T) {
	dir := t.TempDir()
	a := &Agent{
		config: Config{Workspace: dir},
	}

	m := a.collectMetrics()
	if m.Timestamp == 0 {
		t.Error("expected non-zero timestamp")
	}
}

func TestRunShutdown(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Workspace:         dir,
		Socket:            dir + "/agent.sock",
		LogLevel:          "info",
		HeartbeatInterval: 100 * time.Millisecond,
		MetricsInterval:   100 * time.Millisecond,
	}

	a, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Run should exit cleanly when context is cancelled.
	err = a.Run(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
