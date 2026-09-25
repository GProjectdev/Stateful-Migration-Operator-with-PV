//go:build linux

package artifact

import (
	"golang.org/x/sys/unix"
	"os"
	"strings"
)

// Open every component relative to an already-open directory, with no symlink
// following. NONBLOCK prevents a swapped FIFO from hanging the verifier.
func openArchive(rootPath, rel string, _ *os.Root) (*os.File, error) {
	dir, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(dir) }()
	parts := strings.Split(rel, "/")
	for _, part := range parts[:len(parts)-1] {
		next, err := unix.Openat(dir, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		_ = unix.Close(dir)
		dir = next
	}
	fd, err := unix.Openat(dir, parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), rel), nil
}
