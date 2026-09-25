package artifact

import (
	"runtime"
	"testing"
)

func TestTimestampPathsAndWindowsADS(t *testing.T) {
	for _, name := range []string{"checkpoint-2026-08-01T15:41:54+09:00.tar", "job.tar:stream"} {
		_, err := RelativePath(HostRoot + "/" + name)
		if runtime.GOOS == "windows" && err == nil {
			t.Fatal("Windows alternate data stream accepted")
		}
		if runtime.GOOS != "windows" && err != nil {
			t.Fatalf("valid Linux filename rejected: %v", err)
		}
	}
}
