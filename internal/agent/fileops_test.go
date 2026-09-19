package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caged-dev/agent/internal/watcher"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startAgent runs an agent over a temp workspace and returns its socket
// path plus a stop function that asserts a clean shutdown.
func startAgent(t *testing.T, cfg Config) (*Agent, string, func()) {
	t.Helper()
	dir := t.TempDir()
	if cfg.Workspace == "" {
		cfg.Workspace = filepath.Join(dir, "workspace")
	}
	if cfg.Socket == "" {
		cfg.Socket = filepath.Join(dir, "agent.sock")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = time.Hour
	}
	if cfg.MetricsInterval == 0 {
		cfg.MetricsInterval = time.Hour
	}
	if cfg.WatchDebounce == 0 {
		cfg.WatchDebounce = 40 * time.Millisecond
	}
	a, err := New(cfg, quietLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	waitForSocket(t, cfg.Socket)
	return a, cfg.Socket, func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
}

func waitForSocket(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socket); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("socket %s never appeared", socket)
}

// TestFileOpMessageShape is the socket contract the runtime host parses.
// If this changes, caged-api's collector stops understanding the guest.
func TestFileOpMessageShape(t *testing.T) {
	a, socket, stop := startAgent(t, Config{WatchFiles: true})
	defer stop()
	if a.watcher == nil {
		t.Skip("file watching unsupported on this platform")
	}

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := json.NewEncoder(conn).Encode(Message{Type: MsgWatchFiles}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	dec := json.NewDecoder(conn)

	var ack Message
	if err := dec.Decode(&ack); err != nil {
		t.Fatalf("decoding ack: %v", err)
	}
	if ack.Type != MsgWatchStarted {
		t.Fatalf("ack type = %q, want %q", ack.Type, MsgWatchStarted)
	}
	var started WatchStartedPayload
	if err := json.Unmarshal(ack.Payload, &started); err != nil {
		t.Fatalf("unmarshaling ack: %v", err)
	}
	if !started.Enabled {
		t.Fatalf("watch not enabled: %q", started.Reason)
	}

	// A write no tool call ever saw.
	path := filepath.Join(a.config.Workspace, ".env")
	secret := "ANTHROPIC_API_KEY=sk-ant-not-real"
	if err := os.WriteFile(path, []byte(secret), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("read deadline: %v", err)
	}
	for {
		var msg Message
		if err := dec.Decode(&msg); err != nil {
			t.Fatalf("decoding file op: %v", err)
		}
		if msg.Type != MsgFileOp {
			continue
		}
		var p FileOpPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			t.Fatalf("unmarshaling file op: %v", err)
		}
		if p.Path != path {
			t.Logf("unrelated path %q", p.Path)
			continue
		}
		if p.Operation != "create" && p.Operation != "write" {
			t.Errorf("operation = %q", p.Operation)
		}
		if p.Size != int64(len(secret)) {
			t.Errorf("size = %d, want %d", p.Size, len(secret))
		}
		if p.UnixMS == 0 {
			t.Error("ts_unix_ms is zero")
		}
		// The property the whole design rests on: the frame names the
		// file and never carries a byte of it.
		if strings.Contains(string(msg.Payload), "ANTHROPIC_API_KEY") ||
			strings.Contains(string(msg.Payload), "sk-ant") {
			t.Fatalf("file contents crossed the socket: %s", msg.Payload)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(msg.Payload, &raw); err != nil {
			t.Fatalf("unmarshaling raw: %v", err)
		}
		for key := range raw {
			switch key {
			case "operation", "path", "size", "ts_unix_ms", "dropped_since_last":
			default:
				t.Errorf("unexpected field %q on a file op frame", key)
			}
		}
		return
	}
}

func TestWatchStarted_ReportsDisabledRatherThanLookingIdle(t *testing.T) {
	// A sandbox where observation is off must SAY so. Silence is the
	// failure mode this whole change exists to remove.
	_, socket, stop := startAgent(t, Config{WatchFiles: false})
	defer stop()

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := json.NewEncoder(conn).Encode(Message{Type: MsgWatchFiles}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	var ack Message
	if err := json.NewDecoder(conn).Decode(&ack); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var started WatchStartedPayload
	if err := json.Unmarshal(ack.Payload, &started); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if started.Enabled {
		t.Fatal("watch reported enabled with WatchFiles=false")
	}
	if started.Reason == "" {
		t.Error("a disabled watch must carry a reason")
	}
}

// TestSubscribe_RelaysFramesToStdout covers the mode the runtime host runs
// inside the guest.
func TestSubscribe_RelaysFramesToStdout(t *testing.T) {
	a, socket, stop := startAgent(t, Config{WatchFiles: true})
	defer stop()
	if a.watcher == nil {
		t.Skip("file watching unsupported on this platform")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out syncBuffer
	relayDone := make(chan error, 1)
	go func() { relayDone <- Subscribe(ctx, socket, &out) }()

	deadline := time.Now().Add(5 * time.Second)
	target := filepath.Join(a.config.Workspace, "notes.md")
	for time.Now().Before(deadline) {
		if err := os.WriteFile(target, []byte("hello"), 0600); err != nil {
			t.Fatalf("write: %v", err)
		}
		if strings.Contains(out.String(), "notes.md") {
			cancel()
			if err := <-relayDone; err != nil {
				t.Errorf("Subscribe: %v", err)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("relay never reported the write; got %q", out.String())
}

func TestSubscribe_MissingSocketIsAnError(t *testing.T) {
	err := Subscribe(context.Background(), filepath.Join(t.TempDir(), "nope.sock"), io.Discard)
	if err == nil {
		t.Fatal("expected an error dialing a missing socket")
	}
}

// TestShutdownDoesNotLeakTheWatcher asserts the property a long-lived VM
// agent has to have: stopping releases the inotify descriptor and the
// fan-out goroutine.
func TestShutdownDoesNotLeakTheWatcher(t *testing.T) {
	a, _, stop := startAgent(t, Config{WatchFiles: true})
	if a.watcher == nil {
		stop()
		t.Skip("file watching unsupported on this platform")
	}
	stop()

	if _, ok := <-a.watcher.Events(); ok {
		t.Error("watcher event channel still open after Run returned")
	}
	if n := a.hub.subscriberCount(); n != 0 {
		t.Errorf("%d subscribers left after shutdown", n)
	}
}

func TestFileOpHub_SlowSubscriberIsDroppedNotBuffered(t *testing.T) {
	hub := newFileOpHub(quietLogger())
	id, ch := hub.subscribe()
	defer hub.unsubscribe(id)

	for i := 0; i < subscriberBuffer*3; i++ {
		hub.broadcast(watcherEvent("/workspace/f.txt"))
	}
	if len(ch) > subscriberBuffer {
		t.Errorf("subscriber channel holds %d, capacity %d", len(ch), subscriberBuffer)
	}
	if hub.dropped.Load() == 0 {
		t.Error("a stalled subscriber must produce counted drops")
	}
}

func TestFileOpHub_UnsubscribeIsIdempotent(t *testing.T) {
	hub := newFileOpHub(quietLogger())
	id, _ := hub.subscribe()
	hub.unsubscribe(id)
	hub.unsubscribe(id)
	hub.broadcast(watcherEvent("/workspace/a"))
	if n := hub.subscriberCount(); n != 0 {
		t.Errorf("subscribers = %d", n)
	}
}

func watcherEvent(path string) watcher.Event {
	return watcher.Event{Op: watcher.OpWrite, Path: path, Size: 1, UnixMS: time.Now().UnixMilli()}
}

// syncBuffer is a bytes.Buffer safe for the relay goroutine to write while
// the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
