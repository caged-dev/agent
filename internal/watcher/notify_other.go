//go:build !linux

package watcher

// The agent only ever runs inside a Linux microVM. This stub exists so the
// package still builds on a developer's macOS machine, and it fails loudly
// rather than pretending to watch: New returns ErrUnsupported, which the
// agent reports once as "file observation off" instead of silently
// recording nothing.
func newNotifier() (notifier, error) {
	return nil, ErrUnsupported
}
