package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func DigestKey(namespace, digest string) (string, error) {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(digest) != digest || len(validation.IsDNS1123Label(namespace)) != 0 || namespace == "" {
		return "", fmt.Errorf("invalid namespace or SHA256 store identity")
	}
	return namespace + "/sha256/" + digest, nil
}

// Exporter persists completed checkpoint archives before any restore operation exists.
type Exporter struct {
	Client     client.Client
	Reader     client.Reader
	NodeName   string
	SourceRoot string
	StoreRoot  string
}

func (*Exporter) NeedLeaderElection() bool { return false }

func (e *Exporter) Start(ctx context.Context) error {
	if e.NodeName == "" || e.SourceRoot == "" || e.StoreRoot == "" {
		return fmt.Errorf("exporter requires node, source root and durable store root")
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := e.Poll(ctx); err != nil && ctx.Err() == nil {
			log.FromContext(ctx).Error(err, "checkpoint archive export failed")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (e *Exporter) Poll(ctx context.Context) error {
	var list fluidcr.FluidCRMigrationList
	if err := e.Reader.List(ctx, &list); err != nil {
		return err
	}
	var firstErr error
	for i := range list.Items {
		m := &list.Items[i]
		if m.Status.Phase != fluidcr.PhaseCompleted || m.Generation <= 0 || m.Status.ObservedGeneration != m.Generation || !m.DeletionTimestamp.IsZero() {
			continue
		}
		for _, pod := range m.Status.Pods {
			if pod.NodeName != e.NodeName || pod.PodUID == "" {
				continue
			}
			for _, archive := range pod.CheckpointFiles {
				if archive.DurableRef != "" {
					continue
				}
				digest, err := archiveDigest(e.SourceRoot, archive.FilePath)
				if err == nil {
					var key string
					key, err = DigestKey(m.Namespace, digest)
					if err == nil {
						err = copyAtomic(e.SourceRoot, archive.FilePath, e.StoreRoot, key, digest)
					}
					if err == nil {
						err = Verify(e.StoreRoot, HostRoot+"/"+key, digest)
					}
					if err == nil {
						err = e.publish(ctx, m, pod, archive, digest, "file-store:"+key)
					}
				}
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("export %s/%s pod %s: %w", m.Namespace, m.Name, pod.PodName, err)
				}
			}
		}
	}
	return firstErr
}

func archiveDigest(rootPath, hostPath string) (string, error) {
	rel, err := RelativePath(hostPath)
	if err != nil {
		return "", err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return "", err
	}
	defer root.Close()
	f, err := openArchive(rootPath, rel, root)
	if err != nil {
		return "", err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		return "", fmt.Errorf("checkpoint must be a regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if err := Verify(rootPath, hostPath, digest); err != nil {
		return "", err
	}
	return digest, nil
}

func (e *Exporter) publish(ctx context.Context, source *fluidcr.FluidCRMigration, pod fluidcr.PodMigrationStatus, archive fluidcr.CheckpointFile, digest, ref string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current fluidcr.FluidCRMigration
		if err := e.Reader.Get(ctx, client.ObjectKeyFromObject(source), &current); err != nil {
			return err
		}
		if current.UID != source.UID || current.Generation != source.Generation || current.Status.ObservedGeneration != current.Generation || current.Status.Phase != fluidcr.PhaseCompleted || !current.DeletionTimestamp.IsZero() {
			return fmt.Errorf("checkpoint identity changed during export")
		}
		base := current.DeepCopy()
		for i := range current.Status.Pods {
			p := &current.Status.Pods[i]
			if p.PodName != pod.PodName || p.PodUID != pod.PodUID || p.NodeName != e.NodeName {
				continue
			}
			for j := range p.CheckpointFiles {
				f := &p.CheckpointFiles[j]
				if f.ContainerName != archive.ContainerName || f.FilePath != archive.FilePath {
					continue
				}
				if f.DurableRef != "" {
					if f.DurableRef != ref || f.SHA256 != digest {
						return fmt.Errorf("conflicting published archive")
					}
					return nil
				}
				f.SHA256, f.DurableRef, f.ExportedAt = digest, ref, time.Now().UTC().Format(time.RFC3339)
				return e.Client.Status().Patch(ctx, &current, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
			}
		}
		return fmt.Errorf("checkpoint archive identity changed during export")
	})
}
