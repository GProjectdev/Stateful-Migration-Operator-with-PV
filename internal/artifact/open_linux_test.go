//go:build linux

package artifact

import (
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

func TestLinuxOpenRejectsSymlinkComponents(t *testing.T) {
	rootPath, _ := archive(t)
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.Symlink("job.tar", filepath.Join(rootPath, "link")); err != nil {
		t.Fatal(err)
	}
	if f, err := openArchive(rootPath, "link", root); err == nil {
		f.Close()
		t.Fatal("final symlink followed")
	}
	outside, _ := archive(t)
	if err := os.Symlink(outside, filepath.Join(rootPath, "dir")); err != nil {
		t.Fatal(err)
	}
	if f, err := openArchive(rootPath, "dir/job.tar", root); err == nil {
		f.Close()
		t.Fatal("directory symlink followed")
	}
}

func TestLinuxRejectsFIFO(t *testing.T) {
	root, digest := archive(t)
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Verify(root, HostRoot+"/fifo", digest); err == nil {
		t.Fatal("FIFO accepted")
	}
}

func TestLinuxTimestampArchive(t *testing.T) {
	root, digest := archive(t)
	name := "checkpoint-2026-08-01T15:41:54+09:00.tar"
	if err := os.Rename(filepath.Join(root, "job.tar"), filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	if err := Verify(root, HostRoot+"/"+name, digest); err != nil {
		t.Fatal(err)
	}
}
