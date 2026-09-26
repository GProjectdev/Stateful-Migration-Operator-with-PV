package artifact

import (
	"crypto/sha256"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestUploadDownloadUsesDurableStoreAndAtomicTarget(t *testing.T) {
	source := t.TempDir()
	store := t.TempDir()
	target := t.TempDir()
	data := []byte("checkpoint archive")
	if err := os.WriteFile(filepath.Join(source, "src.tar"), data, 0600); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: "ns", Generation: 3}, Spec: api.RestorePlanSpec{RequestUID: "request-uid", CheckpointRef: api.CheckpointReference{CheckpointID: "round-001"}}}
	archive := api.Archive{SourcePath: HostRoot + "/src.tar", TargetPath: HostRoot + "/dst/checkpoint.tar", SHA256: digest}
	if err := Upload(store, source, plan, archive); err != nil {
		t.Fatal(err)
	}
	key, err := ObjectKey(plan, archive)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(store, HostRoot+"/"+key, digest); err != nil {
		t.Fatal(err)
	}
	if err := Download(store, target, plan, archive); err != nil {
		t.Fatal(err)
	}
	if err := Verify(target, archive.TargetPath, digest); err != nil {
		t.Fatal(err)
	}
}

func TestTransferRejectsUnsafeOrCorruptArtifacts(t *testing.T) {
	source := t.TempDir()
	store := t.TempDir()
	data := []byte("checkpoint archive")
	if err := os.WriteFile(filepath.Join(source, "src.tar"), data, 0600); err != nil {
		t.Fatal(err)
	}
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: "ns", Generation: 3}, Spec: api.RestorePlanSpec{RequestUID: "request-uid", CheckpointRef: api.CheckpointReference{CheckpointID: "round-001"}}}
	badDigest := fmt.Sprintf("%x", sha256.Sum256([]byte("other")))
	archive := api.Archive{SourcePath: HostRoot + "/src.tar", TargetPath: HostRoot + "/dst/checkpoint.tar", SHA256: badDigest}
	if err := Upload(store, source, plan, archive); err == nil {
		t.Fatal("corrupt source accepted")
	}
	archive.SHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
	archive.TargetPath = HostRoot + "/../escape.tar"
	if err := Upload(store, source, plan, archive); err == nil {
		t.Fatal("unsafe target accepted")
	}
}

func TestConcurrentIdenticalDigestWritersDoNotClobberTempFiles(t *testing.T) {
	source := t.TempDir()
	store := t.TempDir()
	data := []byte("shared checkpoint archive")
	if err := os.WriteFile(filepath.Join(source, "src.tar"), data, 0600); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: "ns", Generation: 3}}
	archive := api.Archive{SourcePath: HostRoot + "/src.tar", TargetPath: HostRoot + "/dst/checkpoint.tar", SHA256: digest}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- Upload(store, source, plan, archive)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	key, err := ObjectKey(plan, archive)
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(store, HostRoot+"/"+key, digest); err != nil {
		t.Fatal(err)
	}
}
