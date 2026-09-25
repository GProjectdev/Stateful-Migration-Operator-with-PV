// SPDX-License-Identifier: Apache-2.0
package server

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const restoreAnnotationPrefix = "checkpoint-restore.crio.io/"
const restoreArchiveRoot = "/var/lib/kubelet/checkpoints"

func annotationRestoreArchive(enabled bool, name string, labels, sandbox, container map[string]string) (string, error) {
	return annotationRestoreArchiveAt(restoreArchiveRoot, enabled, name, labels, sandbox, container)
}
func annotationRestoreArchiveAt(root string, enabled bool, name string, labels, sandbox, container map[string]string) (string, error) {
	if name == "" {
		for _, annotations := range []map[string]string{sandbox, container} {
			for key := range annotations {
				if strings.HasPrefix(key, restoreAnnotationPrefix) {
					return "", fmt.Errorf("restore annotation requires container metadata name")
				}
			}
		}
		return "", nil
	}
	key := restoreAnnotationPrefix + name
	archive, selected := sandbox[key]
	local, localSelected := container[key]
	if !selected {
		if localSelected {
			return "", fmt.Errorf("container restore annotation requires matching sandbox annotation")
		}
		return "", nil
	}
	if !enabled {
		return "", fmt.Errorf("annotation restore requires enable_criu_support")
	}
	generation, err := strconv.ParseInt(sandbox["migration.dcnlab.com/restore-plan-generation"], 10, 64)
	if labels["migration.dcnlab.com/restore-plan"] == "" || sandbox["migration.dcnlab.com/restore-plan-uid"] == "" || err != nil || generation <= 0 {
		return "", fmt.Errorf("annotation restore requires admission-bound plan label, UID and generation")
	}
	if localSelected && local != archive {
		return "", fmt.Errorf("conflicting container and sandbox restore annotations")
	}
	if archive == "" || !path.IsAbs(archive) || path.Clean(archive) != archive || strings.ContainsAny(archive, "\\\x00\r\n") || !strings.HasPrefix(archive, restoreArchiveRoot+"/") {
		return "", fmt.Errorf("restore archive must be a canonical path below %s", restoreArchiveRoot)
	}
	relative := strings.TrimPrefix(archive, restoreArchiveRoot+"/")
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	for current := absoluteRoot; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return "", fmt.Errorf("restore root: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", fmt.Errorf("restore root contains a link or non-directory")
		}
		if filepath.Dir(current) == current {
			break
		}
	}
	confined, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("open restore root: %w", err)
	}
	defer confined.Close()
	parts := strings.Split(relative, "/")
	for i := range parts {
		info, err := confined.Lstat(filepath.Join(parts[:i+1]...))
		if err != nil {
			return "", fmt.Errorf("restore archive: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("restore archive contains a symbolic link")
		}
		if i < len(parts)-1 && !info.IsDir() {
			return "", fmt.Errorf("restore ancestor is not a directory")
		}
		if i == len(parts)-1 && !info.Mode().IsRegular() {
			return "", fmt.Errorf("restore archive is not a regular file")
		}
	}
	file, err := confined.Open(filepath.FromSlash(relative))
	if err != nil {
		return "", fmt.Errorf("open restore archive: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return "", fmt.Errorf("restore archive must be a nonempty regular file")
	}
	return archive, nil
}
