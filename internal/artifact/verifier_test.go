package artifact

import (
	"context"
	"crypto/sha256"
	"fmt"
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"os"
	"path/filepath"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
	"time"
)

func archive(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	data := []byte("checkpoint data")
	if err := os.WriteFile(filepath.Join(root, "job.tar"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return root, fmt.Sprintf("%x", sha256.Sum256(data))
}

func TestVerifyPathsAndDigest(t *testing.T) {
	root, digest := archive(t)
	if err := Verify(root, HostRoot+"/job.tar", digest); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"../job.tar", "/etc/shadow", HostRoot + "/../secret", HostRoot + "/sub/../../secret", HostRoot + "/sub/../job.tar", HostRoot + "//job.tar", HostRoot + "/job.tar/", HostRoot + "/..\\secret", HostRoot + "/job.tar:stream", HostRoot + "/job.tar\x00", HostRoot, HostRoot + "-other/job.tar"} {
		t.Run(target, func(t *testing.T) {
			if err := Verify(root, target, digest); err == nil {
				t.Fatal("unsafe path accepted")
			}
		})
	}
	if err := Verify(root, HostRoot+"/job.tar", strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong digest accepted")
	}
	if err := Verify(root, HostRoot+"/job.tar", "bad"); err == nil {
		t.Fatal("malformed digest accepted")
	}
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := Verify(root, HostRoot+"/directory", digest); err == nil {
		t.Fatal("directory accepted")
	}
	if err := Verify(root, HostRoot+"/missing", digest); err == nil {
		t.Fatal("missing archive accepted")
	}
}

func TestVerifyRejectsSymlinks(t *testing.T) {
	root, digest := archive(t)
	outside, _ := archive(t)
	for _, tc := range []struct{ name, target, archive string }{{"inside", "job.tar", "inside"}, {"outside", filepath.Join(outside, "job.tar"), "outside"}, {"directory", outside, "directory/job.tar"}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.Symlink(tc.target, filepath.Join(root, tc.name)); err != nil {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			if err := Verify(root, HostRoot+"/"+tc.archive, digest); err == nil {
				t.Fatal("symlink accepted")
			}
		})
	}
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(root, link); err != nil {
		t.Skipf("root symlink creation unavailable: %v", err)
	}
	if err := Verify(link, HostRoot+"/job.tar", digest); err == nil {
		t.Fatal("symlink root accepted")
	}
}

func TestFreshness(t *testing.T) {
	now := time.Now()
	base := api.ArtifactStatus{NodeName: "n", ObservedGeneration: 4, Verified: true, CheckedAt: metav1.NewTime(now.Add(-30 * time.Second))}
	for _, tc := range []struct {
		name    string
		reports []api.ArtifactStatus
		want    bool
	}{{"fresh", []api.ArtifactStatus{base}, true}, {"missing", nil, false}, {"duplicate", []api.ArtifactStatus{base, base}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			p := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Generation: 4}, Status: api.RestoreStatus{Artifacts: tc.reports}}
			if Fresh(p, "n", now) != tc.want {
				t.Fatal("unexpected freshness")
			}
		})
	}
	for _, change := range []func(*api.ArtifactStatus){func(r *api.ArtifactStatus) { r.Verified = false }, func(r *api.ArtifactStatus) { r.ObservedGeneration-- }, func(r *api.ArtifactStatus) { r.CheckedAt = metav1.NewTime(now.Add(-MaxAge - time.Second)) }, func(r *api.ArtifactStatus) { r.CheckedAt = metav1.NewTime(now.Add(time.Second)) }, func(r *api.ArtifactStatus) { r.CheckedAt = metav1.Time{} }} {
		report := base
		change(&report)
		p := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Generation: 4}, Status: api.RestoreStatus{Artifacts: []api.ArtifactStatus{report}}}
		if Fresh(p, "n", now) {
			t.Fatal("unsafe freshness accepted")
		}
	}
}

func TestPollOwnNodeOnlyPreservesStatus(t *testing.T) {
	root, digest := archive(t)
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	other := api.ArtifactStatus{NodeName: "other", ObservedGeneration: 2, Verified: true, CheckedAt: metav1.Now()}
	plan := &api.RestorePlan{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: "ns", UID: "p", Generation: 2}, Spec: api.RestorePlanSpec{TargetCluster: "target", Pods: []api.RestorePod{{TargetNode: "local", Archives: []api.Archive{{TargetPath: HostRoot + "/job.tar", SHA256: digest}}}, {TargetNode: "other", Archives: []api.Archive{{TargetPath: "/etc/shadow", SHA256: digest}}}}}, Status: api.RestoreStatus{Phase: "Prepared", Message: "member owns this", Artifacts: []api.ArtifactStatus{other}, Pods: []api.PodStatus{{Name: "job", Phase: "Pending"}}}}
	foreign := plan.DeepCopy()
	foreign.Name = "foreign"
	foreign.UID = "foreign"
	foreign.Spec.TargetCluster = "elsewhere"
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.RestorePlan{}).WithObjects(plan, foreign).Build()
	v := NewVerifier(c, c, "target", root)
	v.NodeName = "local"
	var storedPlan api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &storedPlan); err != nil {
		t.Fatal(err)
	}
	other = storedPlan.Status.Artifacts[0]
	if err := v.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	var got api.RestorePlan
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Artifacts) != 2 || !Fresh(&got, "local", time.Now()) || !reflect.DeepEqual(got.Status.Artifacts[0], other) || got.Status.Phase != "Prepared" || got.Status.Message != plan.Status.Message || !reflect.DeepEqual(got.Status.Pods, plan.Status.Pods) {
		t.Fatalf("bad status merge: %+v", got.Status)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(foreign), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Artifacts) != 1 {
		t.Fatal("foreign cluster modified")
	}
	if err := os.WriteFile(filepath.Join(root, "job.tar"), []byte("corrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := v.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(plan), &got); err != nil {
		t.Fatal(err)
	}
	if Fresh(&got, "local", time.Now()) || got.Status.Artifacts[1].Verified {
		t.Fatal("corruption remained verified")
	}
}

func TestVerifierDefaultsAndMissingIdentity(t *testing.T) {
	t.Setenv("NODE_NAME", "node-a")
	v := NewVerifier(nil, nil, "target", "")
	if v.NodeName != "node-a" || v.Root != DefaultRoot || v.Interval != 30*time.Second || v.NeedLeaderElection() {
		t.Fatal("invalid daemon defaults")
	}
	if err := v.Start(context.Background()); err == nil {
		t.Fatal("missing clients accepted")
	}
}
