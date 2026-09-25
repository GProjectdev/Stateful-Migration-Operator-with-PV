package artifact

import (
	"context"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

type concurrentClient struct {
	client.Client
	collided bool
}
type concurrentStatus struct {
	client.SubResourceWriter
	owner *concurrentClient
}

func (c *concurrentClient) Status() client.SubResourceWriter {
	return &concurrentStatus{SubResourceWriter: c.Client.Status(), owner: c}
}
func (s *concurrentStatus) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if !s.owner.collided {
		s.owner.collided = true
		var latest api.RestorePlan
		if err := s.owner.Client.Get(ctx, client.ObjectKeyFromObject(obj), &latest); err != nil {
			return err
		}
		latest.Status.Phase = "Running"
		latest.Status.Pods = []api.PodStatus{{Name: "job", Phase: "Running"}}
		latest.Status.Artifacts = append(latest.Status.Artifacts, api.ArtifactStatus{NodeName: "other", ObservedGeneration: 2, Verified: true, CheckedAt: metav1.Now()})
		if err := s.owner.Client.Status().Update(ctx, &latest); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Group: api.GroupVersion.Group, Resource: "restoreplans"}, obj.GetName(), fmt.Errorf("concurrent status"))
	}
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func TestArtifactConflictRetryPreservesMemberAndOtherNode(t *testing.T) {
	root, digest := archive(t)
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: "ns", UID: "p", Generation: 2}, Spec: api.RestorePlanSpec{TargetCluster: "target", Pods: []api.RestorePod{{TargetNode: "local", Archives: []api.Archive{{TargetPath: HostRoot + "/job.tar", SHA256: digest}}}}}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}).WithObjects(plan).Build()
	c := &concurrentClient{Client: base}
	v := NewVerifier(c, base, "target", root)
	v.NodeName = "local"
	if err := v.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := base.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if !c.collided || got.Status.Phase != "Running" || len(got.Status.Pods) != 1 || len(got.Status.Artifacts) != 2 || got.Status.Artifacts[0].NodeName != "other" || !got.Status.Artifacts[1].Verified {
		t.Fatalf("concurrent status lost: %+v", got.Status)
	}
}

type changedGenerationReader struct{ client.Reader }

func (r changedGenerationReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if p, ok := obj.(*api.RestorePlan); ok {
		p.Generation++
	}
	return nil
}
func TestDropsVerificationWhenGenerationChanges(t *testing.T) {
	root, digest := archive(t)
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: "ns", UID: "p", Generation: 2}, Spec: api.RestorePlanSpec{TargetCluster: "target", Pods: []api.RestorePod{{TargetNode: "local", Archives: []api.Archive{{TargetPath: HostRoot + "/job.tar", SHA256: digest}}}}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}).WithObjects(plan).Build()
	v := NewVerifier(c, changedGenerationReader{Reader: c}, "target", root)
	v.NodeName = "local"
	if err := v.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Artifacts) != 0 {
		t.Fatal("published report for superseded generation")
	}
}
