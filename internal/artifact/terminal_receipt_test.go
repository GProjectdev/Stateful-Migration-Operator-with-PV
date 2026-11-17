package artifact

import (
	"context"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	"github.com/GProjectdev/Stateful-Migration-Operator-with-PV/internal/groupcontract"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
)

func TestVerifiedPartialReceiptStopsHistoricalIOOnlyForBoundGeneration(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "new-generation"}[stale], func(t *testing.T) {
			p := &api.RestorePlan{ObjectMeta: meta.ObjectMeta{Name: "plan", Namespace: "jobs", UID: "plan-uid", Generation: 1}, Spec: api.RestorePlanSpec{RequestUID: "request", CheckpointRef: api.CheckpointReference{CheckpointID: "checkpoint"}, PartialRestore: &api.PartialRestoreSpec{}, TargetCluster: "target", Pods: []api.RestorePod{{TargetNode: "local", Archives: []api.Archive{{TargetPath: HostRoot + "/missing.tar", SHA256: strings.Repeat("a", 64)}}}}}}
			receipt, err := groupcontract.EncodePartialTargetVerified(p, "operation")
			if err != nil {
				t.Fatal(err)
			}
			p.Annotations = map[string]string{groupcontract.PartialTargetVerifiedAnnotation: receipt}
			if stale {
				p.Generation = 2
			}
			scheme := runtime.NewScheme()
			_ = api.AddToScheme(scheme)
			c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}).WithObjects(p).Build()
			v := NewVerifier(c, c, "target", t.TempDir())
			v.NodeName = "local"
			v.StoreRoot = t.TempDir()
			if err := v.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
			var got api.RestorePlan
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(p), &got); err != nil {
				t.Fatal(err)
			}
			if !stale && len(got.Status.Artifacts) != 0 {
				t.Fatal("completed history was hashed again")
			}
			if stale && (len(got.Status.Artifacts) != 1 || got.Status.Artifacts[0].Verified) {
				t.Fatalf("new generation bypassed missing archive validation: %+v", got.Status.Artifacts)
			}
		})
	}
}
