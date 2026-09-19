package watcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeNotifier is a notifier driven by the test instead of the kernel, so
// the coalescing, ignore and backpressure rules are exercised
// deterministically and without needing inotify.
type fakeNotifier struct {
	mu      sync.Mutex
	pending [][]rawEvent
	added   []string
	closed  bool
	wake    chan struct{}
	addErr  error
}

func newFakeNotifier() *fakeNotifier {
	return &fakeNotifier{wake: make(chan struct{}, 1024)}
}

func (f *fakeNotifier) Add(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		return f.addErr
	}
	f.added = append(f.added, path)
	return nil
}

func (f *fakeNotifier) feed(evs ...rawEvent) {
	f.mu.Lock()
	f.pending = append(f.pending, evs)
	f.mu.Unlock()
	f.wake <- struct{}{}
}

func (f *fakeNotifier) Next() ([]rawEvent, error) {
	for {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return nil, ErrNotifierClosed
		}
		if len(f.pending) > 0 {
			batch := f.pending[0]
			f.pending = f.pending[1:]
			f.mu.Unlock()
			return batch, nil
		}
		f.mu.Unlock()
		select {
		case <-f.wake:
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (f *fakeNotifier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeNotifier) addedDirs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.added))
	copy(out, f.added)
	return out
}

// startFake builds a watcher over a temp root with a fake notifier and
// returns it running, plus a cancel that waits for a clean stop.
func startFake(t *testing.T, cfg Config) (*Watcher, *fakeNotifier, string, func()) {
	t.Helper()
	root := t.TempDir()
	cfg.Root = root
	n := newFakeNotifier()
	w, err := newWithNotifier(cfg, testLogger(), n)
	if err != nil {
		t.Fatalf("newWithNotifier: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return w, n, root, func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
}

func TestWatcher_CoalescesRepeatedWritesToOneEvent(t *testing.T) {
	w, n, root, stop := startFake(t, Config{Debounce: 40 * time.Millisecond})
	defer stop()

	path := filepath.Join(root, "main.go")
	if err := os.WriteFile(path, []byte("package main"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i := 0; i < 50; i++ {
		n.feed(rawEvent{Path: path, Op: rawWrite})
	}

	ev := recvEvent(t, w, time.Second)
	if ev.Path != path {
		t.Fatalf("path = %q, want %q", ev.Path, path)
	}
	if ev.Op != OpWrite {
		t.Fatalf("op = %q, want %q", ev.Op, OpWrite)
	}
	if ev.Size != int64(len("package main")) {
		t.Errorf("size = %d, want %d", ev.Size, len("package main"))
	}
	if ev.UnixMS == 0 {
		t.Error("timestamp is zero")
	}
	// 50 notifications, one event: the rest were folded in.
	assertNoEvent(t, w, 150*time.Millisecond)
	if st := w.Stats(); st.Coalesced < 40 {
		t.Errorf("coalesced = %d, want >= 40 (stats: %+v)", st.Coalesced, st)
	}
}

func TestWatcher_CreateThenWriteReportsCreate(t *testing.T) {
	w, n, root, stop := startFake(t, Config{Debounce: 30 * time.Millisecond})
	defer stop()

	path := filepath.Join(root, "new.txt")
	n.feed(rawEvent{Path: path, Op: rawCreate}, rawEvent{Path: path, Op: rawWrite})

	if ev := recvEvent(t, w, time.Second); ev.Op != OpCreate {
		t.Fatalf("op = %q, want %q", ev.Op, OpCreate)
	}
}

func TestWatcher_RenameDestinationIsACreate(t *testing.T) {
	// This is how vim, `mv` and most editors actually write a file, and it
	// is the path the trust rules care about most: an agent that moves a
	// file onto /workspace/.env must be observed doing it.
	w, n, root, stop := startFake(t, Config{Debounce: 30 * time.Millisecond})
	defer stop()

	src := filepath.Join(root, "tmpfile")
	dst := filepath.Join(root, ".env")
	n.feed(rawEvent{Path: src, Op: rawMovedFrom}, rawEvent{Path: dst, Op: rawMovedTo})

	got := map[string]Op{}
	for i := 0; i < 2; i++ {
		ev := recvEvent(t, w, time.Second)
		got[ev.Path] = ev.Op
	}
	if got[src] != OpRename {
		t.Errorf("source op = %q, want %q", got[src], OpRename)
	}
	if got[dst] != OpCreate {
		t.Errorf("destination op = %q, want %q", got[dst], OpCreate)
	}
}

func TestWatcher_DeleteIsReported(t *testing.T) {
	w, n, root, stop := startFake(t, Config{Debounce: 30 * time.Millisecond})
	defer stop()

	path := filepath.Join(root, "gone.txt")
	n.feed(rawEvent{Path: path, Op: rawCreate}, rawEvent{Path: path, Op: rawDelete})

	ev := recvEvent(t, w, time.Second)
	if ev.Op != OpDelete {
		t.Fatalf("op = %q, want %q", ev.Op, OpDelete)
	}
	if ev.Size != 0 {
		t.Errorf("a delete must not report a size, got %d", ev.Size)
	}
}

func TestWatcher_IgnoreRules(t *testing.T) {
	w, n, root, stop := startFake(t, Config{Debounce: 20 * time.Millisecond})
	defer stop()

	ignored := []string{
		filepath.Join(root, "node_modules", "left-pad", "index.js"),
		filepath.Join(root, ".git", "index"),
		filepath.Join(root, "src", "__pycache__", "mod.cpython-312.pyc"),
		filepath.Join(root, "target", "debug", "app"),
		filepath.Join(root, "dist", "bundle.js"),
		filepath.Join(root, ".venv", "lib", "x.py"),
		filepath.Join(root, "notes.txt~"),
		filepath.Join(root, ".main.go.swp"),
		filepath.Join(root, "4913"),
	}
	for _, p := range ignored {
		n.feed(rawEvent{Path: p, Op: rawWrite})
	}
	assertNoEvent(t, w, 200*time.Millisecond)
	if st := w.Stats(); st.Ignored != int64(len(ignored)) {
		t.Errorf("ignored = %d, want %d", st.Ignored, len(ignored))
	}

	// A dotfile is NOT noise: .env is the single most important path this
	// watcher exists to report.
	keep := filepath.Join(root, ".env")
	n.feed(rawEvent{Path: keep, Op: rawWrite})
	if ev := recvEvent(t, w, time.Second); ev.Path != keep {
		t.Fatalf("path = %q, want %q", ev.Path, keep)
	}
}

func TestWatcher_IgnoredDirectoriesAreNotWatched(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"src", "node_modules/left-pad", ".git/objects"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	n := newFakeNotifier()
	if _, err := newWithNotifier(Config{Root: root}, testLogger(), n); err != nil {
		t.Fatalf("newWithNotifier: %v", err)
	}
	for _, dir := range n.addedDirs() {
		rel, _ := filepath.Rel(root, dir)
		for _, bad := range []string{"node_modules", ".git"} {
			if rel == bad || strings.HasPrefix(rel, bad+string(os.PathSeparator)) {
				t.Errorf("watch added on ignored tree: %s", rel)
			}
		}
	}
}

// TestWatcher_FloodIsBoundedAndCounted is the npm-install case: tens of
// thousands of distinct paths at once. Memory must stay bounded, the loss
// must be counted, and the loss must be REPORTED on the stream rather than
// leaving the host to believe it saw everything.
func TestWatcher_FloodIsBoundedAndCounted(t *testing.T) {
	const flood = 60000
	w, n, root, stop := startFake(t, Config{
		Debounce:      5 * time.Millisecond,
		QueueSize:     256,
		MaxPending:    64,
		OutSize:       32,
		RatePerSecond: 10,
	})
	defer stop()

	go func() {
		for i := 0; i < flood; i++ {
			n.feed(rawEvent{Path: filepath.Join(root, "pkg", "f"+itoa(i)+".js"), Op: rawWrite})
		}
	}()

	// Read a bounded number of events; the point is that the stream stays
	// small and self-describing, not that it delivers everything.
	deadline := time.After(3 * time.Second)
	var received int
	var sawDropReport bool
	for {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				t.Fatal("event channel closed early")
			}
			received++
			if ev.DroppedSince > 0 {
				sawDropReport = true
			}
			if received >= 20 && sawDropReport {
				goto checked
			}
		case <-deadline:
			goto checked
		}
	}
checked:
	st := w.Stats()
	if st.Dropped() == 0 {
		t.Fatalf("a %d-event flood dropped nothing; bounds are not being applied (stats %+v)", flood, st)
	}
	if !sawDropReport {
		t.Error("no emitted event carried DroppedSince: a flood was hidden from the consumer")
	}
	if st.Emitted > 200 {
		t.Errorf("emitted %d events under a rate limit of 10/s: the rate limit is not holding", st.Emitted)
	}
	// Bounded queues mean bounded memory: nothing here grew with `flood`.
	if got := len(w.raw); got > 256 {
		t.Errorf("raw queue holds %d, capacity 256", got)
	}
}

func TestWatcher_ShutdownClosesEventsAndReleasesTheNotifier(t *testing.T) {
	root := t.TempDir()
	n := newFakeNotifier()
	w, err := newWithNotifier(Config{Root: root, Debounce: 10 * time.Millisecond}, testLogger(), n)
	if err != nil {
		t.Fatalf("newWithNotifier: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	n.feed(rawEvent{Path: filepath.Join(root, "a.txt"), Op: rawWrite})
	recvEvent(t, w, time.Second)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel: the watcher leaked")
	}

	// The event channel is closed, so a consumer ranging over it stops.
	if _, ok := <-w.Events(); ok {
		t.Error("event channel still open after Run returned")
	}
	n.mu.Lock()
	closed := n.closed
	n.mu.Unlock()
	if !closed {
		t.Error("notifier was not closed: the inotify descriptor leaked")
	}
	// Close is idempotent, so an owner that also closes is not a panic.
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestWatcher_EventCarriesNoFileContents(t *testing.T) {
	// The isolation boundary property, asserted structurally: an Event has
	// exactly these fields, and none of them can hold a file's bytes.
	w, n, root, stop := startFake(t, Config{Debounce: 20 * time.Millisecond})
	defer stop()

	secret := "SUPER_SECRET_TOKEN=abc123"
	path := filepath.Join(root, ".env")
	if err := os.WriteFile(path, []byte(secret), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	n.feed(rawEvent{Path: path, Op: rawWrite})

	ev := recvEvent(t, w, time.Second)
	if ev.Size != int64(len(secret)) {
		t.Errorf("size = %d, want %d", ev.Size, len(secret))
	}
	for _, field := range []string{ev.Path, string(ev.Op)} {
		if contains(field, "SUPER_SECRET_TOKEN") {
			t.Fatalf("file contents leaked into the event: %q", field)
		}
	}
}

func TestConfig_Defaults(t *testing.T) {
	c := Config{Root: "/workspace"}.withDefaults()
	if c.Debounce != DefaultDebounce || c.QueueSize != DefaultQueueSize ||
		c.MaxPending != DefaultMaxPending || c.RatePerSecond != DefaultRatePerSecond ||
		c.MaxWatchDirs != DefaultMaxWatchDirs || c.ReportEvery != DefaultReportEvery {
		t.Errorf("defaults not applied: %+v", c)
	}
	if len(c.IgnoreDirs) == 0 {
		t.Error("ignore list is empty by default")
	}
	// MaxHold below Debounce would defeat coalescing entirely.
	c = Config{Root: "/w", Debounce: time.Second, MaxHold: time.Millisecond}.withDefaults()
	if c.MaxHold < c.Debounce {
		t.Errorf("MaxHold %v < Debounce %v", c.MaxHold, c.Debounce)
	}
}

func TestNewWithNotifier_RejectsEmptyRoot(t *testing.T) {
	if _, err := newWithNotifier(Config{}, testLogger(), newFakeNotifier()); err == nil {
		t.Fatal("expected an error for an empty root")
	}
}

func TestNewWithNotifier_PropagatesAddFailure(t *testing.T) {
	n := newFakeNotifier()
	n.addErr = errors.New("no watches left")
	if _, err := newWithNotifier(Config{Root: t.TempDir()}, testLogger(), n); err == nil {
		t.Fatal("expected the root watch failure to propagate")
	}
}

func TestWatcher_MaxWatchDirsIsEnforced(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 5; i++ {
		if err := os.MkdirAll(filepath.Join(root, "d"+itoa(i)), 0755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	n := newFakeNotifier()
	w, err := newWithNotifier(Config{Root: root, MaxWatchDirs: 2}, testLogger(), n)
	if err != nil {
		t.Fatalf("newWithNotifier: %v", err)
	}
	if st := w.Stats(); st.WatchDirs > 2 || !st.WatchLimitHit {
		t.Errorf("watch limit not enforced: %+v", st)
	}
}

func TestBucket_LimitsSustainedRate(t *testing.T) {
	b := newBucket(10) // burst 20
	now := time.Now()
	var allowed int
	for i := 0; i < 100; i++ {
		if b.take(now) {
			allowed++
		}
	}
	if allowed != 20 {
		t.Errorf("burst allowed %d, want 20", allowed)
	}
	if !b.take(now.Add(200 * time.Millisecond)) {
		t.Error("refill did not happen")
	}
}

func recvEvent(t *testing.T, w *Watcher, timeout time.Duration) Event {
	t.Helper()
	select {
	case ev, ok := <-w.Events():
		if !ok {
			t.Fatal("event channel closed")
		}
		return ev
	case <-time.After(timeout):
		t.Fatalf("no event within %v (stats %+v)", timeout, w.Stats())
		return Event{}
	}
}

func assertNoEvent(t *testing.T, w *Watcher, within time.Duration) {
	t.Helper()
	select {
	case ev := <-w.Events():
		t.Fatalf("unexpected event %+v", ev)
	case <-time.After(within):
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func itoa(n int) string { return strconv.Itoa(n) }
