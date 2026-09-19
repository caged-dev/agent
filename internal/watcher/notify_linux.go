//go:build linux

package watcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Raw inotify, via golang.org/x/sys/unix, rather than a library.
//
// This binary ships inside EVERY microVM Caged starts, so its dependency
// set is part of the isolation boundary's attack surface and part of every
// VM's boot image. golang.org/x/sys is already the agent's only dependency
// and is maintained by the Go team; fsnotify would add a module (and its
// own transitive x/sys) to buy cross-platform support this process will
// never use — it runs on Linux, in a Linux VM, by construction — plus a
// recursive-watch layer we need to own anyway, because the watch limit and
// the ignore rules have to be enforced at the point a directory is added.
//
// The one thing that IS worth copying from fsnotify is how it closes:
// the inotify fd is opened non-blocking and handed to os.NewFile, so reads
// go through the Go runtime poller and Close unblocks a blocked reader
// instead of racing it into a reused descriptor.

// inotifyEvents is the set of changes worth a watch. IN_MODIFY is
// deliberately absent: IN_CLOSE_WRITE is one notification per completed
// write session instead of one per write() call, which removes an entire
// order of magnitude of flood at the source.
const inotifyEvents = unix.IN_CREATE |
	unix.IN_CLOSE_WRITE |
	unix.IN_DELETE |
	unix.IN_MOVED_FROM |
	unix.IN_MOVED_TO |
	unix.IN_DELETE_SELF |
	unix.IN_MOVE_SELF |
	unix.IN_EXCL_UNLINK

// inotifyNotifier is the Linux notifier.
type inotifyNotifier struct {
	f   *os.File
	buf []byte

	mu      sync.Mutex
	watches map[int32]string // watch descriptor -> directory
	closed  bool
}

func newNotifier() (notifier, error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("inotify_init1: %w", err)
	}
	return &inotifyNotifier{
		f:       os.NewFile(uintptr(fd), "inotify"),
		buf:     make([]byte, 64*1024),
		watches: make(map[int32]string),
	}, nil
}

func (n *inotifyNotifier) Add(dir string) error {
	n.mu.Lock()
	closed := n.closed
	n.mu.Unlock()
	if closed {
		return ErrNotifierClosed
	}
	wd, err := unix.InotifyAddWatch(int(n.f.Fd()), dir, inotifyEvents)
	if err != nil {
		return fmt.Errorf("inotify_add_watch: %w", err)
	}
	n.mu.Lock()
	n.watches[int32(wd)] = dir
	n.mu.Unlock()
	return nil
}

func (n *inotifyNotifier) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()
	// Closing the *os.File unregisters it from the runtime poller, which
	// makes a Read blocked in Next return immediately with ErrClosed.
	if err := n.f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("closing inotify fd: %w", err)
	}
	return nil
}

// Next blocks until the kernel has events, then decodes the whole batch.
func (n *inotifyNotifier) Next() ([]rawEvent, error) {
	for {
		size, err := n.f.Read(n.buf)
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return nil, ErrNotifierClosed
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return nil, fmt.Errorf("reading inotify fd: %w", err)
		}
		if size < unix.SizeofInotifyEvent {
			// A short read means the kernel had nothing coherent for us;
			// ask again rather than reporting a phantom change.
			continue
		}
		evs := n.decode(n.buf[:size])
		if len(evs) == 0 {
			continue
		}
		return evs, nil
	}
}

func (n *inotifyNotifier) decode(buf []byte) []rawEvent {
	var out []rawEvent
	for offset := 0; offset+unix.SizeofInotifyEvent <= len(buf); {
		raw := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
		nameLen := int(raw.Len)
		nameStart := offset + unix.SizeofInotifyEvent
		if nameStart+nameLen > len(buf) {
			break
		}
		name := ""
		if nameLen > 0 {
			name = cString(buf[nameStart : nameStart+nameLen])
		}
		offset = nameStart + nameLen

		if raw.Mask&unix.IN_Q_OVERFLOW != 0 {
			// The kernel's own queue overflowed, which means changes
			// happened that nothing can now describe. It is the same fact
			// as one of our own drops and is surfaced the same way.
			out = append(out, rawEvent{Path: overflowSentinel, Op: rawWrite})
			continue
		}
		dir, ok := n.dirFor(raw.Wd)
		if !ok {
			continue
		}
		if raw.Mask&(unix.IN_IGNORED|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF|unix.IN_UNMOUNT) != 0 {
			n.forget(raw.Wd)
			continue
		}
		if name == "" {
			continue
		}
		op, ok := opFromMask(raw.Mask)
		if !ok {
			continue
		}
		out = append(out, rawEvent{
			Path:  filepath.Join(dir, name),
			Op:    op,
			IsDir: raw.Mask&unix.IN_ISDIR != 0,
		})
	}
	return out
}

// overflowSentinel is a path that cannot exist in a workspace, used to
// carry an IN_Q_OVERFLOW through the ignore rules into the drop counters.
const overflowSentinel = "\x00inotify-queue-overflow"

func opFromMask(mask uint32) (rawOp, bool) {
	switch {
	case mask&unix.IN_CREATE != 0:
		return rawCreate, true
	case mask&unix.IN_CLOSE_WRITE != 0:
		return rawWrite, true
	case mask&unix.IN_DELETE != 0:
		return rawDelete, true
	case mask&unix.IN_MOVED_FROM != 0:
		return rawMovedFrom, true
	case mask&unix.IN_MOVED_TO != 0:
		return rawMovedTo, true
	default:
		return 0, false
	}
}

func (n *inotifyNotifier) dirFor(wd int32) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dir, ok := n.watches[wd]
	return dir, ok
}

func (n *inotifyNotifier) forget(wd int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.watches, wd)
}

// cString trims the NUL padding inotify puts after a name.
func cString(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}
