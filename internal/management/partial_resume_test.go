package management

import (
	api "github.com/GProjectdev/Stateful-Migration-Operator-with-PV/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestPartialResumeRequiresBoundReceiptAndProgress(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "uid", "generation", "checkpoint", "time", "progress", "target-checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			req, cp := fixture()
			makeSameClusterPartial(req, cp)
			plan, err := desiredPlan(req, cp)
			if err != nil {
				t.Fatal(err)
			}
			now := metav1.Now()
			tr := partialTrainingRuntime(req, "target-pod-uid", now)
			status := tr.Object["status"].(map[string]interface{})["clusters"].([]interface{})[0].(map[string]interface{})["status"].(map[string]interface{})
			pods := status["pods"].([]interface{})
			survivor := pods[1].(map[string]interface{})
			receipt := survivor["survivorResume"].(map[string]interface{})
			switch mode {
			case "missing":
				delete(survivor, "survivorResume")
			case "uid":
				receipt["podUID"] = "another-pod"
			case "generation":
				receipt["generation"] = int64(999)
			case "checkpoint":
				receipt["checkpointID"] = "another-round"
			case "time":
				receipt["resumedAt"] = now.Time.Add(time.Minute).Format(time.RFC3339)
			case "progress":
				survivor["previousGlobalStep"] = survivor["globalStep"]
			case "target-checkpoint":
				pods[0].(map[string]interface{})["checkpointID"] = "prior-round"
			}
			err = validateRuntimeStatus(req, plan, []api.PodStatus{{Name: "db-0", UID: "target-pod-uid"}}, status, now.Time)
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode %s: %v", mode, err)
			}
		})
	}
}
