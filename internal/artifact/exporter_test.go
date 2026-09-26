package artifact

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	fluidcr "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/fluidcr/v1alpha1"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func exportFixture() *fluidcr.FluidCRMigration {
	return &fluidcr.FluidCRMigration{
		ObjectMeta: metav1.ObjectMeta{Name: "round-1", Namespace: "default", UID: "checkpoint-uid", Generation: 1},
		Status:     fluidcr.FluidCRMigrationStatus{ObservedGeneration: 1, Phase: fluidcr.PhaseCompleted, Pods: []fluidcr.PodMigrationStatus{{PodName: "train-0", PodUID: "pod-uid", NodeName: "source-node", Phase: fluidcr.PodPhaseResumed, CheckpointFiles: []fluidcr.CheckpointFile{{ContainerName: "trainer", FilePath: HostRoot + "/checkpoint.tar"}}}}},
	}
}

func TestExportBeforeRestoreAllowsDownloadAfterSourceLoss(t *testing.T) {
	ctx := context.Background()
	source, store, target := t.TempDir(), t.TempDir(), t.TempDir()
	content := []byte("immutable checkpoint archive")
	if err := os.WriteFile(filepath.Join(source, "checkpoint.tar"), content, 0600); err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	if err := fluidcr.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	m := exportFixture()
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(m).WithObjects(m).Build()
	exporter := &Exporter{Client: c, Reader: c, NodeName: "source-node", SourceRoot: source, StoreRoot: store}
	if err := exporter.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(m), m); err != nil {
		t.Fatal(err)
	}
	archive := m.Status.Pods[0].CheckpointFiles[0]
	key, err := DigestKey(m.Namespace, archive.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if archive.DurableRef != "file-store:"+key || archive.ExportedAt == "" {
		t.Fatalf("missing durable evidence: %#v", archive)
	}
	if err := os.Remove(filepath.Join(source, "checkpoint.tar")); err != nil {
		t.Fatal(err)
	}
	if err := exporter.Poll(ctx); err != nil {
		t.Fatalf("published evidence depends on live source: %v", err)
	}
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}
	if err := Download(store, target, plan, api.Archive{ContainerName: "trainer", SourcePath: HostRoot + "/checkpoint.tar", TargetPath: HostRoot + "/restored.tar", SHA256: archive.SHA256}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(target, "restored.tar"))
	if err != nil || string(got) != string(content) {
		t.Fatalf("download after source loss: %q %v", got, err)
	}
}

func TestExporterSkipsForeignNodeAndStaleGeneration(t *testing.T) {
	for _, tc := range []struct {
		name, node string
		generation int64
	}{{"foreign", "other-node", 1}, {"stale", "source-node", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = fluidcr.AddToScheme(scheme)
			m := exportFixture()
			m.Generation = tc.generation
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(m).WithObjects(m).Build()
			e := &Exporter{Client: c, Reader: c, NodeName: tc.node, SourceRoot: t.TempDir(), StoreRoot: t.TempDir()}
			if err := e.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			_ = c.Get(context.Background(), client.ObjectKeyFromObject(m), m)
			if m.Status.Pods[0].CheckpointFiles[0].DurableRef != "" {
				t.Fatal("exported foreign/stale checkpoint")
			}
		})
	}
}

func TestExporterRejectsRecreatedCheckpointAtPublish(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = fluidcr.AddToScheme(scheme)
	source := exportFixture()
	current := source.DeepCopy()
	current.UID = "replacement-uid"
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(current).WithObjects(current).Build()
	e := &Exporter{Client: c, Reader: c, NodeName: "source-node"}
	if err := e.publish(context.Background(), source, source.Status.Pods[0], source.Status.Pods[0].CheckpointFiles[0], "digest", "ref"); err == nil {
		t.Fatal("recreated checkpoint accepted")
	}
}

func TestTwoNodeExportersPreserveBothRankArchives(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = fluidcr.AddToScheme(scheme)
	m := exportFixture()
	second := m.Status.Pods[0]
	second.PodName, second.PodUID, second.NodeName = "train-1", "pod-uid-2", "source-node-2"
	second.CheckpointFiles = []fluidcr.CheckpointFile{{ContainerName: "trainer", FilePath: HostRoot + "/checkpoint.tar"}}
	m.Status.Pods = append(m.Status.Pods, second)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(m).WithObjects(m).Build()
	store := t.TempDir()
	for i, node := range []string{"source-node", "source-node-2"} {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, "checkpoint.tar"), []byte{byte(i)}, 0600); err != nil {
			t.Fatal(err)
		}
		e := &Exporter{Client: c, Reader: c, NodeName: node, SourceRoot: root, StoreRoot: store}
		if err := e.Poll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(m), m); err != nil {
		t.Fatal(err)
	}
	if m.Status.Phase != fluidcr.PhaseCompleted {
		t.Fatal("export changed checkpoint phase")
	}
	for _, pod := range m.Status.Pods {
		if pod.CheckpointFiles[0].DurableRef == "" || pod.Phase != fluidcr.PodPhaseResumed {
			t.Fatalf("lost rank evidence: %#v", pod)
		}
	}
	if m.Status.Pods[0].CheckpointFiles[0].SHA256 == m.Status.Pods[1].CheckpointFiles[0].SHA256 {
		t.Fatal("different rank archives collided")
	}
}
