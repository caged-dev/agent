// Package watcher observes file changes under a sandbox's workspace and
// turns them into a bounded, coalesced stream of change events.
//
// It exists because Caged claims that every file change in a sandbox is
// recorded, and nothing outside the VM can see a file the agent changes
// directly inside it: `vim`, a shell redirect, `git checkout`, an
// `npm install`. Only a process in the guest observes those, and this is
// that process.
//
// # What a flood looks like, and what happens to it
//
// The pathological cases are ordinary developer commands.
// `npm install` on a mid-sized project writes 30k-100k files into
// node_modules in a few seconds. `go build` churns a cache. `pip install`
// unpacks wheels. A test run rewrites coverage files in a loop. Raw inotify
// reports every one of those, several times each (IN_CREATE, then one or
// more IN_MODIFY, then IN_CLOSE_WRITE), which is tens of thousands of
// events per second aimed at an event pipeline sized for human-rate agent
// activity. Forwarded verbatim it is a denial of service against Caged's
// own ClickHouse writer, paid for by the customer whose session it buries.
//
// Four bounds apply, in this order, and every one of them counts what it
// discards rather than growing to hold it:
//
//  1. Ignore rules. A path with an ignored directory segment
//     (node_modules, .git, target, dist, __pycache__, .venv, ...) is
//     dropped before it is queued. This removes the great majority of a
//     dependency install outright.
//  2. A bounded raw queue. The inotify reader never blocks on the
//     coalescer; a full queue drops the event and increments a counter.
//  3. Per-path coalescing. Repeated writes to one path inside the debounce
//     window become ONE event carrying the final size, and the number of
//     distinct in-flight paths is itself bounded — a new path arriving at
//     the ceiling is dropped and counted, not appended.
//  4. A token-bucket rate limit on emission. Sustained output is capped;
//     excess is dropped and counted.
//
// Drops are never silent. The count since the last emitted event travels
// on the event itself (DroppedSince), so the host and the API learn that
// the observation was incomplete instead of inferring completeness from a
// quiet stream, and the first drop plus a periodic summary are logged.
//
// Contents are never read. An event carries a path, an operation, a size
// and a timestamp. There is deliberately nowhere on Event to put a file's
// bytes, and nothing that could may ever be added: this stream leaves the
// isolation boundary.
package watcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Op is what happened to a path. These four values are the whole
// vocabulary; consumers match on them exactly.
type Op string

const (
	// OpCreate is a path that did not exist before, including the
	// destination of a rename (which is how vim, `mv` and most editors
	// actually write a file).
	OpCreate Op = "create"
	// OpWrite is a modification of an existing path.
	OpWrite Op = "write"
	// OpDelete is an unlinked path.
	OpDelete Op = "delete"
	// OpRename is the SOURCE of a rename: the path stopped existing here.
	// The destination is reported separately as OpCreate, because pairing
	// inotify's move cookies across a coalescing window costs state that
	// buys nothing a consumer uses.
	OpRename Op = "rename"
)

// Event is one observed change, after coalescing.
//
// Path, operation, size, timestamp. No contents, ever.
type Event struct {
	Op   Op     `json:"op"`
	Path string `json:"path"`
	// Size is the file's size in bytes at the moment it was reported, and
	// is absent for deletes and rename sources. It is metadata about the
	// file, not any part of it.
	Size int64 `json:"size,omitempty"`
	// UnixMS is when the change was observed, in Unix milliseconds UTC.
	UnixMS int64 `json:"ts_unix_ms"`
	// DroppedSince is how many observed changes were discarded by the
	// bounds above since the previous emitted event. Non-zero means this
	// stream is an incomplete picture and says so, which is the only
	// honest thing a bounded observer can do.
	DroppedSince int64 `json:"dropped_since_last,omitempty"`
}

// DefaultIgnoreDirs are directory names whose contents are never reported.
//
// Every one is a machine-written tree: a package manager's download cache,
// a build output, a virtualenv, a VCS internal. Reporting them buries the
// handful of events a human or an agent would recognise under tens of
// thousands nobody will ever read, and it is the single most effective of
// the four bounds.
//
// .git is here despite being interesting, because its internals churn on
// every command; a `git checkout` is still observed through the working
// tree files it rewrites, which is what a reviewer actually wants to see.
var DefaultIgnoreDirs = []string{
	".bundle",
	".cache",
	".git",
	".gradle",
	".idea",
	".mypy_cache",
	".next",
	".npm",
	".nuxt",
	".pnpm-store",
	".pytest_cache",
	".ruff_cache",
	".terraform",
	".tox",
	".venv",
	".vscode",
	".yarn",
	"__pycache__",
	"bower_components",
	"dist",
	"node_modules",
	"site-packages",
	"target",
	"venv",
	"vendor",
}

// ignoredSuffixes are editor and tooling scratch files. They are noise
// about a write that is separately reported on the real path.
var ignoredSuffixes = []string{"~", ".swp", ".swx", ".swo", ".tmp"}

// ignoredNames are exact basenames with the same character. "4913" is the
// probe file vim creates to test writability.
var ignoredNames = []string{"4913", ".DS_Store"}

// Defaults for Config. They are deliberately conservative: this binary
// runs inside every microVM, alongside the customer's workload.
const (
	DefaultDebounce      = 300 * time.Millisecond
	DefaultMaxHold       = 2 * time.Second
	DefaultQueueSize     = 8192
	DefaultMaxPending    = 4096
	DefaultOutSize       = 1024
	DefaultRatePerSecond = 200
	DefaultMaxWatchDirs  = 8192
	DefaultReportEvery   = time.Minute
)

// Config configures a Watcher. A zero value of any field takes the
// corresponding default above.
type Config struct {
	// Root is the directory tree to watch, normally /workspace.
	Root string
	// Debounce is how long a path must be quiet before its coalesced
	// event is emitted.
	Debounce time.Duration
	// MaxHold bounds how long coalescing may delay a path that is being
	// written continuously, so a file under a steady stream of writes is
	// still reported while it is happening.
	MaxHold time.Duration
	// QueueSize bounds the raw queue between the notifier and the
	// coalescer.
	QueueSize int
	// MaxPending bounds how many distinct paths may be coalescing at once.
	MaxPending int
	// OutSize bounds the emitted event channel.
	OutSize int
	// RatePerSecond caps sustained emission. Burst is twice this.
	RatePerSecond int
	// MaxWatchDirs bounds how many directories may hold an inotify watch,
	// because each one costs unswappable kernel memory.
	MaxWatchDirs int
	// IgnoreDirs overrides DefaultIgnoreDirs when non-nil. An explicitly
	// empty, non-nil slice means "ignore nothing", which is supported for
	// tests and is a bad idea in production.
	IgnoreDirs []string
	// ReportEvery is the interval of the periodic drop summary log.
	ReportEvery time.Duration
}

func (c Config) withDefaults() Config {
	if c.Debounce <= 0 {
		c.Debounce = DefaultDebounce
	}
	if c.MaxHold <= 0 {
		c.MaxHold = DefaultMaxHold
	}
	if c.MaxHold < c.Debounce {
		c.MaxHold = c.Debounce
	}
	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}
	if c.MaxPending <= 0 {
		c.MaxPending = DefaultMaxPending
	}
	if c.OutSize <= 0 {
		c.OutSize = DefaultOutSize
	}
	if c.RatePerSecond <= 0 {
		c.RatePerSecond = DefaultRatePerSecond
	}
	if c.MaxWatchDirs <= 0 {
		c.MaxWatchDirs = DefaultMaxWatchDirs
	}
	if c.IgnoreDirs == nil {
		c.IgnoreDirs = DefaultIgnoreDirs
	}
	if c.ReportEvery <= 0 {
		c.ReportEvery = DefaultReportEvery
	}
	return c
}

// Stats is a snapshot of a Watcher's monotonic counters.
type Stats struct {
	// Observed is raw notifications accepted from the kernel, after the
	// ignore rules.
	Observed int64
	// Ignored is notifications an ignore rule discarded.
	Ignored int64
	// Coalesced is notifications folded into an already-pending path.
	Coalesced int64
	// Emitted is events handed to the consumer.
	Emitted int64
	// DroppedQueue, DroppedPending and DroppedRate are the three bounded
	// places a change can be lost. Their sum is what DroppedSince reports.
	DroppedQueue   int64
	DroppedPending int64
	DroppedRate    int64
	DroppedOut     int64
	// WatchDirs is how many directories currently hold a watch, and
	// WatchLimitHit reports that MaxWatchDirs was reached — beyond it new
	// subdirectories are unobserved, which is a real hole and is logged.
	WatchDirs     int
	WatchLimitHit bool
}

// Dropped is the total number of observed changes this watcher discarded.
func (s Stats) Dropped() int64 {
	return s.DroppedQueue + s.DroppedPending + s.DroppedRate + s.DroppedOut
}

// rawOp is what the platform notifier saw, before coalescing.
type rawOp int

const (
	rawCreate rawOp = iota
	rawWrite
	rawDelete
	rawMovedFrom
	rawMovedTo
)

// rawEvent is one platform notification.
type rawEvent struct {
	Path  string
	Op    rawOp
	IsDir bool
}

// notifier is the platform's change-notification primitive.
//
// Declared here, in the consumer, per the repo's interface-first
// convention; inotifyNotifier on Linux implements it, and tests supply a
// fake so the coalescing, ignore and backpressure logic is exercised
// without the kernel.
//
// Next blocks until at least one event is available and returns a wrapped
// ErrNotifierClosed after Close, which is how a clean shutdown ends the
// read loop rather than by racing a file descriptor.
type notifier interface {
	Add(path string) error
	Next() ([]rawEvent, error)
	Close() error
}

// ErrNotifierClosed reports an orderly end of the notification stream.
var ErrNotifierClosed = errors.New("watcher: notifier closed")

// ErrUnsupported reports that this platform has no change notification.
// The agent treats it as "file observation is off on this build" and says
// so once, rather than failing to start: a sandbox that runs is worth more
// than one that refuses to boot because it cannot watch.
var ErrUnsupported = errors.New("watcher: file change notification is not supported on this platform")

// Watcher observes Root and emits coalesced, bounded change events.
type Watcher struct {
	cfg    Config
	logger *slog.Logger
	notify notifier

	raw chan rawEvent
	out chan Event

	// closeOnce guards Close so a shutdown path that is called from both
	// Run's defer and the owner cannot close the notifier twice.
	closeOnce sync.Once
	closeErr  error

	observed       atomic.Int64
	ignored        atomic.Int64
	coalesced      atomic.Int64
	emitted        atomic.Int64
	droppedQueue   atomic.Int64
	droppedPending atomic.Int64
	droppedRate    atomic.Int64
	droppedOut     atomic.Int64
	watchDirs      atomic.Int64
	watchLimitHit  atomic.Bool

	firstDrop sync.Once
}

// New creates a Watcher over cfg.Root using the platform notifier.
//
// It returns ErrUnsupported (wrapped) on a platform with no inotify, and
// an error if Root cannot be walked at all. Individual unreadable
// subdirectories are logged and skipped: one permission-denied directory
// must not cost the observation of the whole workspace.
func New(cfg Config, logger *slog.Logger) (*Watcher, error) {
	n, err := newNotifier()
	if err != nil {
		return nil, err
	}
	w, err := newWithNotifier(cfg, logger, n)
	if err != nil {
		_ = n.Close()
		return nil, err
	}
	return w, nil
}

// newWithNotifier is the constructor tests use to supply a fake notifier.
func newWithNotifier(cfg Config, logger *slog.Logger, n notifier) (*Watcher, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cfg = cfg.withDefaults()
	if cfg.Root == "" {
		return nil, errors.New("watcher: root is required")
	}
	root, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("resolving watch root: %w", err)
	}
	cfg.Root = root
	w := &Watcher{
		cfg:    cfg,
		logger: logger,
		notify: n,
		raw:    make(chan rawEvent, cfg.QueueSize),
		out:    make(chan Event, cfg.OutSize),
	}
	if err := w.addTree(root); err != nil {
		return nil, err
	}
	return w, nil
}

// Events is the coalesced event stream. It is closed when Run returns.
func (w *Watcher) Events() <-chan Event { return w.out }

// Stats snapshots the counters.
func (w *Watcher) Stats() Stats {
	return Stats{
		Observed:       w.observed.Load(),
		Ignored:        w.ignored.Load(),
		Coalesced:      w.coalesced.Load(),
		Emitted:        w.emitted.Load(),
		DroppedQueue:   w.droppedQueue.Load(),
		DroppedPending: w.droppedPending.Load(),
		DroppedRate:    w.droppedRate.Load(),
		DroppedOut:     w.droppedOut.Load(),
		WatchDirs:      int(w.watchDirs.Load()),
		WatchLimitHit:  w.watchLimitHit.Load(),
	}
}

// Close releases the notifier. It is idempotent and safe to call while Run
// is blocked in Next: the notifier's own Close unblocks it.
func (w *Watcher) Close() error {
	w.closeOnce.Do(func() {
		w.closeErr = w.notify.Close()
	})
	return w.closeErr
}

// Run reads notifications and emits coalesced events until ctx is done or
// the notifier closes. It closes Events before returning, so a consumer
// ranging over the channel terminates with it and nothing is left leaked.
func (w *Watcher) Run(ctx context.Context) error {
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		w.readLoop(ctx)
	}()

	// The notifier read blocks in the kernel, so cancellation has to close
	// it rather than wait for it.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = w.Close()
		case <-stop:
		}
	}()

	err := w.coalesceLoop(ctx, readDone)
	_ = w.Close()
	<-readDone
	close(w.out)
	return err
}

// readLoop drains the notifier into the bounded raw queue. It never
// blocks on the coalescer: a full queue is a counted drop, because the
// alternative is applying backpressure to the kernel, which cannot take
// it and would instead grow the inotify queue until it overflows silently.
func (w *Watcher) readLoop(ctx context.Context) {
	for {
		evs, err := w.notify.Next()
		if err != nil {
			if !errors.Is(err, ErrNotifierClosed) && ctx.Err() == nil {
				w.logger.Error("file watcher read failed; file changes are no longer observed",
					slog.String("error", err.Error()))
			}
			return
		}
		for _, ev := range evs {
			if ctx.Err() != nil {
				return
			}
			if w.ignore(ev.Path) {
				w.ignored.Add(1)
				continue
			}
			// A new directory needs its own watch, and it needs it before
			// its contents are reported, or files created inside it in the
			// same instant are missed.
			if ev.IsDir && (ev.Op == rawCreate || ev.Op == rawMovedTo) {
				if err := w.addTree(ev.Path); err != nil {
					w.logger.Warn("watching new directory failed; changes inside it are unobserved",
						slog.String("path", ev.Path), slog.String("error", err.Error()))
				}
				continue
			}
			if ev.IsDir {
				// Directory deletes and rename sources are not file
				// changes; the kernel removes their watches for us.
				continue
			}
			w.observed.Add(1)
			select {
			case w.raw <- ev:
			default:
				w.droppedQueue.Add(1)
				w.noteFirstDrop("raw queue full", ev.Path)
			}
		}
	}
}

// pending is one path's coalesced state.
type pending struct {
	op        Op
	sawCreate bool
	first     time.Time
	last      time.Time
}

// coalesceLoop folds the raw queue into one event per path per debounce
// window and emits under a rate limit.
func (w *Watcher) coalesceLoop(ctx context.Context, readDone <-chan struct{}) error {
	byPath := make(map[string]*pending)
	tick := w.cfg.Debounce / 2
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	report := time.NewTicker(w.cfg.ReportEvery)
	defer report.Stop()

	bucket := newBucket(float64(w.cfg.RatePerSecond))
	var reportedDrops int64

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-readDone:
			// The notifier ended. Drain whatever it had already queued and
			// flush what is pending, so a clean shutdown does not lose the
			// last writes, then stop.
			for {
				select {
				case ev := <-w.raw:
					w.record(byPath, ev)
					continue
				default:
				}
				break
			}
			w.flush(byPath, time.Now(), true, bucket, &reportedDrops)
			return nil
		case ev := <-w.raw:
			w.record(byPath, ev)
		case now := <-ticker.C:
			w.flush(byPath, now, false, bucket, &reportedDrops)
		case <-report.C:
			w.reportDrops(ctx)
		}
	}
}

// record folds one raw event into the pending map.
func (w *Watcher) record(byPath map[string]*pending, ev rawEvent) {
	now := time.Now()
	p, ok := byPath[ev.Path]
	if !ok {
		if len(byPath) >= w.cfg.MaxPending {
			// The ceiling is a flood: tens of thousands of distinct paths
			// in one debounce window. Growing the map here is exactly the
			// unbounded work this design refuses, so the newest path is
			// dropped and counted; the ones already pending still get
			// reported.
			w.droppedPending.Add(1)
			w.noteFirstDrop("too many distinct paths changing at once", ev.Path)
			return
		}
		p = &pending{first: now}
		byPath[ev.Path] = p
	} else {
		w.coalesced.Add(1)
	}
	p.last = now
	switch ev.Op {
	case rawCreate, rawMovedTo:
		p.sawCreate = true
		p.op = OpCreate
	case rawWrite:
		// A create followed by writes is still a create: it is the more
		// informative fact, and the size reported is the final one.
		if p.sawCreate {
			p.op = OpCreate
		} else {
			p.op = OpWrite
		}
	case rawDelete:
		p.op = OpDelete
	case rawMovedFrom:
		p.op = OpRename
	}
}

// flush emits every pending path that has gone quiet for Debounce or has
// been held for MaxHold. force emits everything regardless, for shutdown.
func (w *Watcher) flush(byPath map[string]*pending, now time.Time, force bool, bucket *bucket, reportedDrops *int64) {
	for path, p := range byPath {
		if !force && now.Sub(p.last) < w.cfg.Debounce && now.Sub(p.first) < w.cfg.MaxHold {
			continue
		}
		delete(byPath, path)
		w.emit(path, p, now, bucket, reportedDrops)
	}
}

func (w *Watcher) emit(path string, p *pending, now time.Time, bucket *bucket, reportedDrops *int64) {
	if !bucket.take(now) {
		w.droppedRate.Add(1)
		w.noteFirstDrop("emission rate limit", path)
		return
	}
	ev := Event{
		Op:     p.op,
		Path:   path,
		UnixMS: now.UTC().UnixMilli(),
	}
	if p.op == OpCreate || p.op == OpWrite {
		// Size is metadata, read from the inode. The file is never opened.
		if fi, err := os.Lstat(path); err == nil && fi.Mode().IsRegular() {
			ev.Size = fi.Size()
		}
	}
	total := w.Stats().Dropped()
	if total > *reportedDrops {
		ev.DroppedSince = total - *reportedDrops
		*reportedDrops = total
	}
	select {
	case w.out <- ev:
		w.emitted.Add(1)
	default:
		// Nothing is reading fast enough. Same rule as everywhere else:
		// bounded and counted, never buffered without limit.
		w.droppedOut.Add(1)
		w.noteFirstDrop("consumer is not keeping up", path)
		// The drops this event was going to report are still unreported.
		*reportedDrops -= ev.DroppedSince
	}
}

// noteFirstDrop makes the very first lost change visible immediately
// rather than at the next summary tick. The path is logged because a path
// is not a secret in the sense that matters here — it is the observation
// itself — but a file's CONTENTS are never read and so can never be
// logged.
func (w *Watcher) noteFirstDrop(reason, path string) {
	w.firstDrop.Do(func() {
		w.logger.Error("file change observation is INCOMPLETE: changes are being dropped",
			slog.String("reason", reason),
			slog.String("example_path", path),
			slog.String("consequence", "some file changes in this sandbox will not appear in the session timeline"),
		)
	})
}

func (w *Watcher) reportDrops(ctx context.Context) {
	st := w.Stats()
	if st.Dropped() == 0 {
		return
	}
	w.logger.LogAttrs(ctx, slog.LevelWarn, "file change observation is dropping events",
		slog.Int64("observed", st.Observed),
		slog.Int64("emitted", st.Emitted),
		slog.Int64("ignored", st.Ignored),
		slog.Int64("coalesced", st.Coalesced),
		slog.Int64("dropped_queue", st.DroppedQueue),
		slog.Int64("dropped_pending", st.DroppedPending),
		slog.Int64("dropped_rate", st.DroppedRate),
		slog.Int64("dropped_consumer", st.DroppedOut),
	)
}

// ignore reports whether a path is excluded by the ignore rules.
func (w *Watcher) ignore(path string) bool {
	rel, err := filepath.Rel(w.cfg.Root, path)
	if err != nil || rel == "." {
		return false
	}
	if strings.HasPrefix(rel, "..") {
		// Outside the watched tree: not ours to report.
		return true
	}
	base := filepath.Base(path)
	for _, name := range ignoredNames {
		if base == name {
			return true
		}
	}
	for _, suffix := range ignoredSuffixes {
		if strings.HasSuffix(base, suffix) {
			return true
		}
	}
	for _, seg := range strings.Split(rel, string(os.PathSeparator)) {
		for _, dir := range w.cfg.IgnoreDirs {
			if seg == dir {
				return true
			}
		}
	}
	return false
}

// addTree watches dir and every existing subdirectory, skipping ignored
// ones and stopping at MaxWatchDirs.
func (w *Watcher) addTree(dir string) error {
	if w.ignore(dir) {
		return nil
	}
	if err := w.addOne(dir); err != nil {
		return err
	}
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// An unreadable subdirectory costs its own observation and
			// nothing else.
			w.logger.Warn("skipping unreadable directory",
				slog.String("path", path), slog.String("error", err.Error()))
			return nil
		}
		if !d.IsDir() || path == dir {
			return nil
		}
		if w.ignore(path) {
			return filepath.SkipDir
		}
		if err := w.addOne(path); err != nil {
			if errors.Is(err, errWatchLimit) {
				return filepath.SkipAll
			}
			w.logger.Warn("watching directory failed",
				slog.String("path", path), slog.String("error", err.Error()))
		}
		return nil
	})
}

var errWatchLimit = errors.New("watcher: watch limit reached")

func (w *Watcher) addOne(dir string) error {
	if w.watchDirs.Load() >= int64(w.cfg.MaxWatchDirs) {
		if w.watchLimitHit.CompareAndSwap(false, true) {
			w.logger.Error("file watcher hit its directory limit; deeper directories are UNOBSERVED",
				slog.Int("limit", w.cfg.MaxWatchDirs),
				slog.String("consequence", "file changes below this point will not appear in the session timeline"))
		}
		return errWatchLimit
	}
	if err := w.notify.Add(dir); err != nil {
		return fmt.Errorf("adding watch on %s: %w", dir, err)
	}
	w.watchDirs.Add(1)
	return nil
}

// bucket is a token bucket, refilled lazily. Burst is two seconds' worth,
// so an ordinary editor save storm passes through and a package install
// does not.
type bucket struct {
	tokens   float64
	perSec   float64
	capacity float64
	last     time.Time
}

func newBucket(perSec float64) *bucket {
	return &bucket{tokens: perSec * 2, perSec: perSec, capacity: perSec * 2}
}

func (b *bucket) take(now time.Time) bool {
	if !b.last.IsZero() {
		b.tokens += now.Sub(b.last).Seconds() * b.perSec
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
