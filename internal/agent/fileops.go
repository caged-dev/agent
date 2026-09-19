package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caged-dev/agent/internal/watcher"
)

// Message types this file adds to the agent's socket protocol.
//
// The envelope is unchanged: file changes ride the Message{Type, Payload}
// frames the agent already speaks on the socket it already serves. There is
// deliberately no second transport — no HTTP client, no outbound
// connection, no credential. This process runs inside the VM that runs
// untrusted code; anything it could authenticate with is something that
// code can steal. The host reads from us; we never reach out. See
// caged-api's ADR-034.
const (
	// MsgWatchFiles subscribes this connection to the file change stream.
	// The agent answers with MsgWatchStarted and then streams MsgFileOp
	// frames until the connection is closed.
	MsgWatchFiles = "watch_files"
	// MsgWatchStarted acknowledges a subscription. It tells the host
	// whether observation is actually on, so a host reading a quiet stream
	// can tell "nothing changed" from "this agent cannot watch".
	MsgWatchStarted = "watch_started"
	// MsgFileOp is one coalesced file change.
	MsgFileOp = "file_op"
)

// FileOpPayload is the wire form of one observed file change.
//
// Path, operation, size, timestamp. NO CONTENTS — not a body, not a diff,
// not a line. A path may itself be sensitive (/workspace/.env) and is still
// reported, because naming the file the agent touched is the entire point;
// the bytes in it are never read, so they can never leak.
type FileOpPayload struct {
	Operation string `json:"operation"`
	Path      string `json:"path"`
	Size      int64  `json:"size,omitempty"`
	UnixMS    int64  `json:"ts_unix_ms"`
	// DroppedSince is how many changes the watcher's bounds discarded
	// since the previous frame. Non-zero means this stream is incomplete
	// and says so.
	DroppedSince int64 `json:"dropped_since_last,omitempty"`
}

// WatchStartedPayload tells a subscriber what it has actually subscribed
// to.
type WatchStartedPayload struct {
	Enabled bool   `json:"enabled"`
	Root    string `json:"root,omitempty"`
	// Reason explains a false Enabled. It is a deployment fact, not an
	// error to retry.
	Reason string `json:"reason,omitempty"`
}

// subscriberBuffer bounds one host connection's backlog. The watcher is
// already coalesced and rate-limited, so a subscriber this far behind is a
// stalled reader, and the right answer is to drop and count rather than to
// grow.
const subscriberBuffer = 256

// fileOpHub fans the single watcher stream out to the connected hosts.
//
// In practice there is one subscriber (the runtime host), but a reconnect
// overlaps two, and a hub means a slow or dead connection costs only its
// own backlog instead of stalling the watcher for everyone.
type fileOpHub struct {
	logger *slog.Logger

	mu     sync.Mutex
	nextID int64
	subs   map[int64]chan watcher.Event

	dropped atomic.Int64
}

func newFileOpHub(logger *slog.Logger) *fileOpHub {
	return &fileOpHub{logger: logger, subs: make(map[int64]chan watcher.Event)}
}

func (h *fileOpHub) subscribe() (int64, <-chan watcher.Event) {
	ch := make(chan watcher.Event, subscriberBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	id := h.nextID
	h.subs[id] = ch
	return id, ch
}

func (h *fileOpHub) unsubscribe(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(ch)
	}
}

// broadcast delivers to every subscriber without blocking on any of them.
func (h *fileOpHub) broadcast(ev watcher.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.subs {
		select {
		case ch <- ev:
		default:
			h.dropped.Add(1)
		}
	}
}

func (h *fileOpHub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		delete(h.subs, id)
		close(ch)
	}
}

// subscriberCount is used by tests and by the periodic log line.
func (h *fileOpHub) subscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// runFileOpHub pumps the watcher's events into the hub until the watcher
// stops. It is the only reader of the watcher channel.
func (a *Agent) runFileOpHub(ctx context.Context) {
	defer a.hub.closeAll()
	events := a.watcher.Events()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			a.hub.broadcast(ev)
		}
	}
}

// streamFileOps writes file change frames to one subscriber until the
// connection breaks or the agent stops.
//
// A failed write is logged and ends this stream only. It never propagates:
// the sandbox's job is to run the customer's code, and an observation
// channel that cannot deliver must not take the workload with it.
func (a *Agent) streamFileOps(ctx context.Context, conn net.Conn, encoder *json.Encoder) {
	started := WatchStartedPayload{Enabled: a.watcher != nil, Root: a.config.Workspace}
	if a.watcher == nil {
		started.Reason = a.watchDisabledReason
	}
	if err := encodeMessage(encoder, MsgWatchStarted, started); err != nil {
		a.logger.Warn("acknowledging file watch subscription failed", "error", err)
		return
	}
	if a.watcher == nil {
		return
	}

	id, ch := a.hub.subscribe()
	defer a.hub.unsubscribe(id)
	a.logger.Info("host subscribed to file change stream",
		"subscribers", a.hub.subscriberCount())

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// A write deadline keeps a host that has stopped reading — a
			// half-open connection after a runtime restart — from pinning
			// this goroutine forever.
			if err := conn.SetWriteDeadline(time.Now().Add(watchWriteTimeout)); err != nil {
				a.logger.Warn("setting file op write deadline failed", "error", err)
				return
			}
			if err := encodeMessage(encoder, MsgFileOp, FileOpPayload{
				Operation:    string(ev.Op),
				Path:         ev.Path,
				Size:         ev.Size,
				UnixMS:       ev.UnixMS,
				DroppedSince: ev.DroppedSince,
			}); err != nil {
				if !errors.Is(err, io.EOF) {
					a.logger.Info("file change stream ended", "error", err)
				}
				return
			}
		}
	}
}

// watchWriteTimeout bounds a single frame write to a subscriber.
const watchWriteTimeout = 10 * time.Second

func encodeMessage(encoder *json.Encoder, msgType string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling %s payload: %w", msgType, err)
	}
	if err := encoder.Encode(Message{Type: msgType, Payload: data}); err != nil {
		return fmt.Errorf("encoding %s message: %w", msgType, err)
	}
	return nil
}

// Subscribe connects to a running agent's socket, subscribes to the file
// change stream and copies the raw frames to out.
//
// It is the relay the runtime host runs INSIDE the guest (`caged-agent
// -subscribe`), so the host reads the change stream over the channel it
// already has into the VM without the guest ever dialing out and without
// adding socat or any other tool to the sandbox image. The transport is
// still the unix socket; this is a pipe onto it.
func Subscribe(ctx context.Context, socket string, out io.Writer) error {
	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := d.DialContext(dialCtx, "unix", socket)
	if err != nil {
		return fmt.Errorf("dialing agent socket %s: %w", socket, err)
	}
	defer func() { _ = conn.Close() }()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	if err := json.NewEncoder(conn).Encode(Message{Type: MsgWatchFiles}); err != nil {
		return fmt.Errorf("requesting file change stream: %w", err)
	}
	if _, err := io.Copy(out, conn); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("copying file change stream: %w", err)
	}
	return nil
}
