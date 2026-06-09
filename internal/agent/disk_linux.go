//go:build linux

package agent

import "syscall"

type diskStat struct {
	Total uint64
	Used  uint64
}

func diskUsage(path string) (*diskStat, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil, err
	}
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	return &diskStat{
		Total: total,
		Used:  total - free,
	}, nil
}
