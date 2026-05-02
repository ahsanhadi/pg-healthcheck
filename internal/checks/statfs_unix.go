//go:build !windows

package checks

import "syscall"

// diskFreeStats returns (usedPct, totalGB, freeGB, err) for the filesystem
// containing path. Only available on Unix; the Windows stub always returns an error.
func diskFreeStats(path string) (pct int, totalGB, freeGB float64, err error) {
	var st syscall.Statfs_t
	if err = syscall.Statfs(path, &st); err != nil {
		return
	}
	bsize := uint64(st.Bsize) //nolint:unconvert
	total := st.Blocks * bsize
	avail := st.Bavail * bsize
	if total > 0 {
		pct = int(float64(total-avail) / float64(total) * 100)
	}
	totalGB = float64(total) / 1024 / 1024 / 1024
	freeGB = float64(avail) / 1024 / 1024 / 1024
	return
}
