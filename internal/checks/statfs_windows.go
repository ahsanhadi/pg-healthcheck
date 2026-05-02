//go:build windows

package checks

import "errors"

func diskFreeStats(path string) (pct int, totalGB, freeGB float64, err error) {
	err = errors.New("filesystem stat not supported on Windows")
	return
}
