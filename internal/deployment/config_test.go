package deployment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"strings"
	"testing"
)

func load(t *testing.T, name string) map[string]interface{} {
	t.Helper()
	b, e := os.ReadFile(filepath.Join("..", "..", "config", name))
	if e != nil {
		t.Fatal(e)
	}
	var o map[string]interface{}
	if e = yaml.Unmarshal(b, &o); e != nil {
		t.Fatal(e)
	}
	return o
}
func TestManagementPermissionsNeverReachMember(t *testing.T) {
	role := load(t, "karmada/role.yaml")
	for _, entry := range role["rules"].([]interface{}) {
		rule := entry.(map[string]interface{})
		for _, v := range rule["resources"].([]interface{}) {
			name := v.(string)
			if name == "*" || strings.Contains(name, "proxy") || name == "secrets" || name == "pods" || name == "nodes" || strings.Contains(name, "checkpoint") {
				t.Fatalf("unsafe MGMT resource %s", name)
			}
		}
	}
	d := load(t, "management/deployment.yaml")
	spec := d["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
	c := spec["containers"].([]interface{})[0].(map[string]interface{})
	b, _ := json.Marshal(c["args"])
	if !strings.Contains(string(b), "--kubeconfig=/etc/karmada/kubeconfig") {
		t.Fatal("explicit Karmada config missing")
	}
}
func TestAdmissionIsOptInAndFailClosed(t *testing.T) {
	for _, name := range []string{"member/mutate-webhook.yaml", "member/validate-webhook.yaml"} {
		obj := load(t, name)
		wh := obj["webhooks"].([]interface{})[0].(map[string]interface{})
		if wh["failurePolicy"] != "Fail" || wh["sideEffects"] != "None" {
			t.Fatal("unsafe webhook policy")
		}
		s := wh["objectSelector"].(map[string]interface{})["matchExpressions"].([]interface{})[0].(map[string]interface{})
		if s["key"] != "migration.dcnlab.com/restore-plan" || s["operator"] != "Exists" {
			t.Fatal("wrong scope")
		}
	}
}

func TestSuspensionPermissionsAreControlPlaneOnly(t *testing.T) {
	role := load(t, "karmada/role.yaml")
	want := map[string]string{
		"work.karmada.io/resourcebindings":  "get,list,watch,patch",
		"apps/statefulsets":                 "get",
		"migration.dcnlab.com/pvmigrations": "get",
		"migration.dcnlab.com/pvmetadata":   "get",
	}
	for _, entry := range role["rules"].([]interface{}) {
		rule := entry.(map[string]interface{})
		verbs := []string{}
		for _, v := range rule["verbs"].([]interface{}) {
			verbs = append(verbs, v.(string))
		}
		for _, group := range rule["apiGroups"].([]interface{}) {
			for _, resource := range rule["resources"].([]interface{}) {
				key := group.(string) + "/" + resource.(string)
				if expected, exists := want[key]; exists {
					if strings.Join(verbs, ",") != expected {
						t.Fatalf("unexpected permissions for %s: %v", key, verbs)
					}
					delete(want, key)
				}
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing suspension permissions: %v", want)
	}
}
func TestArtifactHostMountReadOnly(t *testing.T) {
	d := load(t, "member/artifact-daemonset.yaml")
	spec := d["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
	c := spec["containers"].([]interface{})[0].(map[string]interface{})
	mounts := c["volumeMounts"].([]interface{})
	if len(mounts) != 1 || mounts[0].(map[string]interface{})["readOnly"] != true {
		t.Fatal("archive mount must be read only")
	}
	volumes := spec["volumes"].([]interface{})
	if len(volumes) != 1 || volumes[0].(map[string]interface{})["hostPath"].(map[string]interface{})["path"] != "/var/lib/kubelet/checkpoints" {
		t.Fatal("host mount scope")
	}
}
