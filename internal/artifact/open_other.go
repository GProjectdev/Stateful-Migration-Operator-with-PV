//go:build !linux

package artifact

import (
	"os"
	"path/filepath"
)

func openArchive(_ string, rel string, root *os.Root) (*os.File, error) {
	return root.Open(filepath.FromSlash(rel))
}
