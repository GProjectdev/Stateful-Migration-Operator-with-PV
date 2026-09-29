package management

import (
	"testing"

	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRuntimeVerificationUsesReflectedMemberGeneration(t *testing.T) {
	for _, mode := range []string{"nested", "stale", "missing", "conflicting", "duplicate"} {
		t.Run(mode, func(t *testing.T) {
			req, cp := fixture()
			plan := mustDesiredPlan(req)
			plan.Generation = 1
			plan.Status.Clusters = []api.ClusterStatus{{ClusterName: "target", ObservedGeneration: 1, Phase: "Running", Pods: []api.PodStatus{{Name: "db-0", UID: "target-pod-uid", Phase: "Running"}}}}
			tr := trainingRuntime(req, "target-pod-uid", metav1.Now())
			clusters := tr.Object["status"].(map[string]interface{})["clusters"].([]interface{})
			cluster := clusters[0].(map[string]interface{})
			delete(cluster, "observedGeneration")
			status := cluster["status"].(map[string]interface{})
			status["observedGeneration"] = int64(1)
			switch mode {
			case "stale":
				status["observedGeneration"] = int64(0)
			case "missing":
				delete(status, "observedGeneration")
			case "conflicting":
				cluster["observedGeneration"] = int64(2)
			case "duplicate":
				tr.Object["status"].(map[string]interface{})["clusters"] = append(clusters, cluster)
			}
			got := reconcile(t, testClient(t, req, cp, plan, tr), req)
			if (got.Status.Phase == "Verified") != (mode == "nested") {
				t.Fatalf("unexpected verification: %+v", got.Status)
			}
		})
	}
}
