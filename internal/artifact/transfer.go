package artifact

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
)

const DefaultStoreRoot = "/artifact-store"

func ObjectKey(plan *api.RestorePlan, archive api.Archive) (string, error) {
	if _, err := RelativePath(archive.TargetPath); err != nil {
		return "", err
	}
	if plan == nil {
		return "", fmt.Errorf("RestorePlan is required for artifact key")
	}
	return DigestKey(plan.Namespace, archive.SHA256)
}

func Upload(storeRoot, sourceRoot string, plan *api.RestorePlan, archive api.Archive) error {
	if storeRoot == "" {
		return fmt.Errorf("artifact store root is not configured")
	}
	if sourceRoot == "" {
		sourceRoot = DefaultRoot
	}
	if err := Verify(sourceRoot, archive.SourcePath, archive.SHA256); err != nil {
		return fmt.Errorf("source archive verification failed: %w", err)
	}
	key, err := ObjectKey(plan, archive)
	if err != nil {
		return err
	}
	if err := copyAtomic(sourceRoot, archive.SourcePath, storeRoot, key, archive.SHA256); err != nil {
		return err
	}
	return Verify(storeRoot, HostRoot+"/"+key, archive.SHA256)
}

func Download(storeRoot, targetRoot string, plan *api.RestorePlan, archive api.Archive) error {
	if storeRoot == "" {
		return fmt.Errorf("artifact store root is not configured")
	}
	key, err := ObjectKey(plan, archive)
	if err != nil {
		return err
	}
	if err := Verify(storeRoot, HostRoot+"/"+key, archive.SHA256); err != nil {
		return fmt.Errorf("stored archive verification failed: %w", err)
	}
	if targetRoot == "" {
		targetRoot = DefaultRoot
	}
	return copyAtomic(storeRoot, HostRoot+"/"+key, targetRoot, strings.TrimPrefix(archive.TargetPath, HostRoot+"/"), archive.SHA256)
}

func copyAtomic(srcRoot, srcTarget, dstRoot, dstRel, digest string) error {
	srcRel, err := RelativePath(srcTarget)
	if err != nil {
		return err
	}
	dstRel = path.Clean(dstRel)
	if dstRel == "." || strings.HasPrefix(dstRel, "../") || strings.HasPrefix(dstRel, "/") || strings.ContainsAny(dstRel, "\\\x00\r\n") {
		return fmt.Errorf("invalid destination path")
	}
	dstHandle, err := os.OpenRoot(dstRoot)
	if err != nil {
		return err
	}
	defer dstHandle.Close()
	if err = ensureDirNoSymlink(dstHandle, path.Dir(dstRel)); err != nil {
		return err
	}
	if err = Verify(dstRoot, HostRoot+"/"+dstRel, digest); err == nil {
		return nil
	}
	srcRootHandle, err := os.OpenRoot(srcRoot)
	if err != nil {
		return err
	}
	defer srcRootHandle.Close()
	src, err := openArchive(srcRoot, srcRel, srcRootHandle)
	if err != nil {
		return err
	}
	defer src.Close()
	dir := path.Dir(dstRel)
	tmpRel, tmp, err := createRandomTemp(dstHandle, dir)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = dstHandle.Remove(tmpRel)
		}
	}()
	h := sha256.New()
	if _, err = io.Copy(io.MultiWriter(tmp, h), src); err != nil {
		_ = tmp.Close()
		return err
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), digest) {
		_ = tmp.Close()
		return fmt.Errorf("SHA256 mismatch")
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = ensureDirNoSymlink(dstHandle, dir); err != nil {
		return err
	}
	if err = dstHandle.Rename(tmpRel, dstRel); err != nil {
		return err
	}
	published = true
	if err = syncRootDir(dstHandle, dir); err != nil {
		return err
	}
	return Verify(dstRoot, HostRoot+"/"+dstRel, digest)
}

func createRandomTemp(root *os.Root, dir string) (string, *os.File, error) {
	for i := 0; i < 16; i++ {
		suffix := make([]byte, 16)
		if _, err := rand.Read(suffix); err != nil {
			return "", nil, err
		}
		tmpRel := path.Join(dir, ".artifact-"+hex.EncodeToString(suffix)+".tmp")
		tmp, err := root.OpenFile(tmpRel, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			return tmpRel, tmp, nil
		}
		if !os.IsExist(err) {
			return "", nil, err
		}
	}
	return "", nil, fmt.Errorf("unable to allocate artifact temp file")
}
func ensureDirNoSymlink(root *os.Root, rel string) error {
	rel = path.Clean(rel)
	if rel == "." {
		return nil
	}
	current := ""
	for _, component := range strings.Split(rel, "/") {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("invalid destination directory")
		}
		if current == "" {
			current = component
		} else {
			current = path.Join(current, component)
		}
		info, err := root.Lstat(current)
		if os.IsNotExist(err) {
			if err = root.Mkdir(current, 0700); err != nil && !os.IsExist(err) {
				return err
			}
			info, err = root.Lstat(current)
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("destination directory contains symlink or non-directory")
		}
	}
	return nil
}

func syncRootDir(root *os.Root, rel string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := root.Open(path.Clean(rel))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
