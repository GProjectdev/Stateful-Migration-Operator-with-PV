package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
	"time"
)

const DefaultRoot = "/host-checkpoints"
const HostRoot = "/var/lib/kubelet/checkpoints"
const MaxAge = 2 * time.Minute

func RelativePath(target string) (string, error) {
	if strings.ContainsAny(target, "\\\x00") || (runtime.GOOS == "windows" && strings.Contains(target, ":")) || path.Clean(target) != target || !strings.HasPrefix(target, HostRoot+"/") {
		return "", fmt.Errorf("invalid checkpoint target path %q", target)
	}
	rel := strings.TrimPrefix(target, HostRoot+"/")
	if rel == "" || rel == "." {
		return "", fmt.Errorf("empty archive path")
	}
	return rel, nil
}

func Verify(rootPath, target, digest string) error {
	rel, err := RelativePath(target)
	if err != nil {
		return err
	}
	expected, err := hex.DecodeString(digest)
	if err != nil || len(expected) != sha256.Size {
		return fmt.Errorf("invalid SHA256")
	}
	info, err := os.Lstat(rootPath)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("archive root must be a real directory")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer root.Close()
	current := ""
	for _, component := range strings.Split(rel, "/") {
		current = filepath.Join(current, component)
		info, e := root.Lstat(current)
		if e != nil {
			return e
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink archive component")
		}
	}
	before, err := root.Lstat(filepath.FromSlash(rel))
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() {
		return fmt.Errorf("archive is not a regular file")
	}
	// Root confines resolution even if a directory changes between check and open.
	f, err := openArchive(rootPath, rel, root)
	if err != nil {
		return err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return fmt.Errorf("archive changed while opening")
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return err
	}
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return fmt.Errorf("archive changed while hashing")
	}
	if !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), digest) {
		return fmt.Errorf("SHA256 mismatch")
	}
	return nil
}

func Fresh(p *api.RestorePlan, node string, now time.Time) bool {
	count := 0
	for _, report := range p.Status.Artifacts {
		if report.NodeName != node {
			continue
		}
		count++
		age := now.Sub(report.CheckedAt.Time)
		if !report.Verified || report.ObservedGeneration != p.Generation || report.CheckedAt.IsZero() || age < 0 || age > MaxAge {
			return false
		}
	}
	return count == 1
}

type Verifier struct {
	Client      client.Client
	Reader      client.Reader
	NodeName    string
	ClusterName string
	Root        string
	Interval    time.Duration
}

func NewVerifier(c client.Client, reader client.Reader, clusterName, root string) *Verifier {
	if root == "" {
		root = DefaultRoot
	}
	return &Verifier{Client: c, Reader: reader, NodeName: os.Getenv("NODE_NAME"), ClusterName: clusterName, Root: root, Interval: 30 * time.Second}
}
func (v *Verifier) SetupWithManager(mgr manager.Manager) error { return mgr.Add(v) }
func (v *Verifier) NeedLeaderElection() bool                   { return false }
func (v *Verifier) Start(ctx context.Context) error {
	if v.NodeName == "" || v.ClusterName == "" || v.Reader == nil || v.Client == nil {
		return fmt.Errorf("node, cluster and local clients are required")
	}
	interval := v.Interval
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	if interval > 30*time.Second {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := v.Poll(ctx); err != nil && ctx.Err() == nil {
			log.FromContext(ctx).Error(err, "checkpoint verification poll failed")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (v *Verifier) Poll(ctx context.Context) error {
	var plans api.RestorePlanList
	if err := v.Reader.List(ctx, &plans); err != nil {
		return err
	}
	var firstErr error
	for i := range plans.Items {
		p := &plans.Items[i]
		if p.Spec.TargetCluster != v.ClusterName || !p.DeletionTimestamp.IsZero() {
			continue
		}
		found, verified, message := false, true, "SHA256 verified"
		checkedAt := metav1.Now()
		for _, pod := range p.Spec.Pods {
			if pod.TargetNode != v.NodeName {
				continue
			}
			found = true
			if len(pod.Archives) == 0 {
				verified, message = false, "no target archives"
			}
			for _, archive := range pod.Archives {
				if err := Verify(v.Root, archive.TargetPath, archive.SHA256); err != nil {
					verified, message = false, err.Error()
				}
			}
		}
		if !found {
			continue
		}
		report := api.ArtifactStatus{NodeName: v.NodeName, ObservedGeneration: p.Generation, Verified: verified, Message: message, CheckedAt: checkedAt}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var latest api.RestorePlan
			if err := v.Reader.Get(ctx, client.ObjectKeyFromObject(p), &latest); err != nil {
				return err
			}
			if latest.UID != p.UID || latest.Generation != p.Generation {
				return nil
			}
			reports := make([]api.ArtifactStatus, 0, len(latest.Status.Artifacts)+1)
			for _, old := range latest.Status.Artifacts {
				if old.NodeName != v.NodeName {
					reports = append(reports, old)
				}
			}
			latest.Status.Artifacts = append(reports, report)
			return v.Client.Status().Update(ctx, &latest)
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
