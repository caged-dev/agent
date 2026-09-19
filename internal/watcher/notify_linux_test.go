//go:build linux

package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TestWatcher_SeesARealWrite is the whole point of the package against the
// real kernel: a file written by an ordinary shell redirect inside the
// workspace — no tool call, no API, nothing that passes through Caged — is
// observed.
func TestWatcher_SeesARealWrite(t *testing.T) {
	root := t.TempDir()
	w, err := New(Config{Root: root, Debounce: 50 * time.Millisecond}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	// inotify watches are registered before Run, but give the read loop a
	// moment to be blocked in the kernel rather than racing the write.
	time.Sleep(50 * time.Millisecond)

	path := filepath.Join(root, ".env")
	if err := os.WriteFile(path, []byte("OPENAI_API_KEY=sk-not-a-real-key"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	ev := waitForPath(t, w, path, 3*time.Second)
	if ev.Op != OpCreate && ev.Op != OpWrite {
		t.Errorf("op = %q, want create or write", ev.Op)
	}
	if ev.Size == 0 {
		t.Error("size not reported")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return: the inotify watcher leaked")
	}
}

// TestWatcher_SeesAWriteInANewSubdirectory covers the case a non-recursive
// watcher silently misses: a directory created after startup.
func TestWatcher_SeesAWriteInANewSubdirectory(t *testing.T) {
	root := t.TempDir()
	w, err := New(Config{Root: root, Debounce: 50 * time.Millisecond}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)

	sub := filepath.Join(root, "src", "deep")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// The watch on "deep" is added when its creation is observed, so give
	// the read loop a beat before writing into it.
	time.Sleep(200 * time.Millisecond)
	path := filepath.Join(sub, "app.go")
	if err := os.WriteFile(path, []byte("package deep"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	waitForPath(t, w, path, 3*time.Second)
}

// TestWatcher_RealIgnoreTree proves the ignore rules hold against the
// kernel, not just the fake: writes into node_modules produce nothing.
func TestWatcher_RealIgnoreTree(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "node_modules", "left-pad"), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	w, err := New(Config{Root: root, Debounce: 30 * time.Millisecond}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 200; i++ {
		p := filepath.Join(root, "node_modules", "left-pad", "f"+itoa(i)+".js")
		if err := os.WriteFile(p, []byte("x"), 0600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	real := filepath.Join(root, "index.js")
	if err := os.WriteFile(real, []byte("require('left-pad')"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	ev := waitForPath(t, w, real, 3*time.Second)
	if ev.Path != real {
		t.Fatalf("path = %q", ev.Path)
	}
}

func waitForPath(t *testing.T, w *Watcher, path string, timeout time.Duration) Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				t.Fatalf("event channel closed before %s was observed", path)
			}
			if ev.Path == path {
				return ev
			}
			// Any other path here would be a bug in the ignore rules for
			// this test's tree, so name it rather than swallowing it.
			t.Logf("unrelated event: %+v", ev)
		case <-deadline:
			t.Fatalf("%s was never observed within %v (stats %+v)", path, timeout, w.Stats())
			return Event{}
		}
	}
}

// The decode path is unit-tested directly because the interesting cases —
// a queue overflow, a watch the kernel dropped, a name with NUL padding —
// are hard to provoke on demand and easy to get wrong silently.
func TestInotifyDecode(t *testing.T) {
	n := &inotifyNotifier{watches: map[int32]string{7: "/workspace"}}

	buf := append(inotifyFrame(7, unix.IN_CREATE, "new.txt"),
		inotifyFrame(7, unix.IN_CLOSE_WRITE, "new.txt")...)
	buf = append(buf, inotifyFrame(7, unix.IN_DELETE, "old.txt")...)
	buf = append(buf, inotifyFrame(7, unix.IN_MOVED_FROM, "a")...)
	buf = append(buf, inotifyFrame(7, unix.IN_MOVED_TO, "b")...)
	buf = append(buf, inotifyFrame(7, unix.IN_CREATE|unix.IN_ISDIR, "sub")...)
	// A watch descriptor nobody registered, and one the kernel is done with.
	buf = append(buf, inotifyFrame(99, unix.IN_CREATE, "stray")...)
	buf = append(buf, inotifyFrame(7, unix.IN_Q_OVERFLOW, "")...)
	// A mask with nothing we watch for.
	buf = append(buf, inotifyFrame(7, unix.IN_ATTRIB, "chmod")...)

	got := n.decode(buf)
	want := []rawEvent{
		{Path: "/workspace/new.txt", Op: rawCreate},
		{Path: "/workspace/new.txt", Op: rawWrite},
		{Path: "/workspace/old.txt", Op: rawDelete},
		{Path: "/workspace/a", Op: rawMovedFrom},
		{Path: "/workspace/b", Op: rawMovedTo},
		{Path: "/workspace/sub", Op: rawCreate, IsDir: true},
		{Path: overflowSentinel, Op: rawWrite},
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d events, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("event %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// IN_IGNORED forgets the watch, so a later event on it is dropped
	// rather than joined onto a stale directory.
	if evs := n.decode(inotifyFrame(7, unix.IN_IGNORED, "")); len(evs) != 0 {
		t.Errorf("IN_IGNORED produced %+v", evs)
	}
	if evs := n.decode(inotifyFrame(7, unix.IN_CREATE, "after")); len(evs) != 0 {
		t.Errorf("an event on a forgotten watch produced %+v", evs)
	}

	// A truncated buffer must not panic or invent an event.
	if evs := n.decode([]byte{1, 2, 3}); len(evs) != 0 {
		t.Errorf("a truncated buffer produced %+v", evs)
	}
}

func TestInotifyDecode_TruncatedName(t *testing.T) {
	n := &inotifyNotifier{watches: map[int32]string{1: "/w"}}
	frame := inotifyFrame(1, unix.IN_CREATE, "abcdefgh")
	if evs := n.decode(frame[:len(frame)-4]); len(evs) != 0 {
		t.Errorf("a frame whose name runs past the buffer produced %+v", evs)
	}
}

func TestCString(t *testing.T) {
	if got := cString([]byte("name\x00\x00\x00")); got != "name" {
		t.Errorf("cString = %q, want %q", got, "name")
	}
	if got := cString([]byte("nopad")); got != "nopad" {
		t.Errorf("cString = %q, want %q", got, "nopad")
	}
}

func TestNotifier_CloseIsIdempotentAndUnblocksNext(t *testing.T) {
	n, err := newNotifier()
	if err != nil {
		t.Fatalf("newNotifier: %v", err)
	}
	if err := n.Add(t.TempDir()); err != nil {
		t.Fatalf("Add: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := n.Next()
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotifierClosed) {
			t.Fatalf("Next after Close = %v, want ErrNotifierClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not unblock a blocked Next: the reader would leak")
	}
	if err := n.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := n.Add(t.TempDir()); !errors.Is(err, ErrNotifierClosed) {
		t.Errorf("Add after Close = %v, want ErrNotifierClosed", err)
	}
}

func TestNotifier_AddRejectsAMissingDirectory(t *testing.T) {
	n, err := newNotifier()
	if err != nil {
		t.Fatalf("newNotifier: %v", err)
	}
	defer func() { _ = n.Close() }()
	if err := n.Add(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("expected an error adding a watch on a missing directory")
	}
}

// inotifyFrame builds one kernel-shaped inotify record.
func inotifyFrame(wd int32, mask uint32, name string) []byte {
	nameLen := 0
	if name != "" {
		// The kernel NUL-terminates and pads to a multiple of 8.
		nameLen = (len(name) + 1 + 7) / 8 * 8
	}
	buf := make([]byte, unix.SizeofInotifyEvent+nameLen)
	ev := (*unix.InotifyEvent)(unsafe.Pointer(&buf[0]))
	ev.Wd = wd
	ev.Mask = mask
	ev.Cookie = 0
	ev.Len = uint32(nameLen)
	copy(buf[unix.SizeofInotifyEvent:], name)
	return buf
}
