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
		"work.karmada.io/resourcebindings":     "get,list,watch,patch",
		"apps/statefulsets":                    "get",
		"migration.dcnlab.com/pvmigrations":    "get",
		"migration.dcnlab.com/pvmetadata":      "get",
		"training.dcnlab.com/trainingruntimes": "get",
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
func TestArtifactDaemonMountsOnlyCheckpointAndStoreWritable(t *testing.T) {
	d := load(t, "member/artifact-daemonset.yaml")
	spec := d["spec"].(map[string]interface{})["template"].(map[string]interface{})["spec"].(map[string]interface{})
	c := spec["containers"].([]interface{})[0].(map[string]interface{})
	security := c["securityContext"].(map[string]interface{})
	if security["readOnlyRootFilesystem"] != true || security["allowPrivilegeEscalation"] != false {
		t.Fatal("artifact daemon must keep a read-only root filesystem and no privilege escalation")
	}
	mounts := map[string]map[string]interface{}{}
	for _, raw := range c["volumeMounts"].([]interface{}) {
		m := raw.(map[string]interface{})
		mounts[m["name"].(string)] = m
	}
	if len(mounts) != 2 || mounts["archives"]["mountPath"] != "/host-checkpoints" || mounts["archives"]["readOnly"] != false || mounts["artifact-store"]["mountPath"] != "/artifact-store" || mounts["artifact-store"]["readOnly"] != false {
		t.Fatal("only checkpoint root and durable store should be writable")
	}
	volumes := map[string]map[string]interface{}{}
	for _, raw := range spec["volumes"].([]interface{}) {
		v := raw.(map[string]interface{})
		volumes[v["name"].(string)] = v
	}
	if len(volumes) != 2 || volumes["archives"]["hostPath"].(map[string]interface{})["path"] != "/var/lib/kubelet/checkpoints" || volumes["artifact-store"]["persistentVolumeClaim"].(map[string]interface{})["claimName"] != "stateful-migration-artifacts" {
		t.Fatal("artifact daemon mount scope")
	}
}
