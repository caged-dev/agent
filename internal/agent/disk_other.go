//go:build !linux

package agent

type diskStat struct {
	Total uint64
	Used  uint64
}

func diskUsage(path string) (*diskStat, error) {
	// Stub for non-Linux platforms (agent only runs in Linux VMs).
	return &diskStat{Total: 0, Used: 0}, nil
}
