// SPDX-License-Identifier: Apache-2.0
package server

import (
	"os"
	"path/filepath"
	"testing"
)

func boundAnnotations(value string) map[string]string {
	return map[string]string{restoreAnnotationPrefix + "trainer": value, "migration.dcnlab.com/restore-plan-uid": "plan-uid", "migration.dcnlab.com/restore-plan-generation": "1"}
}
func TestAnnotationAdapter(t *testing.T) {
	root := t.TempDir()
	for name, data := range map[string]string{"valid.tar": "archive fixture", "empty.tar": ""} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"migration.dcnlab.com/restore-plan": "plan"}
	valid := restoreArchiveRoot + "/valid.tar"
	cases := []struct {
		name, path string
		bad        bool
	}{
		{"valid", valid, false}, {"empty", "", true}, {"missing", restoreArchiveRoot + "/missing.tar", true},
		{"outside", "/etc/passwd", true}, {"prefix", restoreArchiveRoot + "-other/file", true},
		{"traversal", restoreArchiveRoot + "/../file", true}, {"double-slash", restoreArchiveRoot + "//valid.tar", true},
		{"relative", "valid.tar", true}, {"directory", restoreArchiveRoot + "/directory", true},
		{"empty-file", restoreArchiveRoot + "/empty.tar", true}, {"newline", restoreArchiveRoot + "/bad\nfile", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := annotationRestoreArchiveAt(root, true, "trainer", labels, boundAnnotations(tc.path), nil)
			if (err != nil) != tc.bad || (!tc.bad && got != valid) {
				t.Fatalf("got %q err=%v", got, err)
			}
		})
	}
	t.Run("normal", func(t *testing.T) {
		got, err := annotationRestoreArchiveAt(root, false, "trainer", nil, nil, nil)
		if got != "" || err != nil {
			t.Fatal(got, err)
		}
	})
	for _, name := range []string{"fluidcr-inject", "sidecar"} {
		t.Run(name, func(t *testing.T) {
			got, err := annotationRestoreArchiveAt(root, false, name, nil, boundAnnotations(valid), nil)
			if got != "" || err != nil {
				t.Fatal("changed nonselected container", got, err)
			}
		})
	}
	t.Run("disabled", func(t *testing.T) {
		if _, err := annotationRestoreArchiveAt(root, false, "trainer", labels, boundAnnotations(valid), nil); err == nil {
			t.Fatal("accepted disabled checkpoint support")
		}
	})
	t.Run("missing-name", func(t *testing.T) {
		if _, err := annotationRestoreArchiveAt(root, true, "", labels, boundAnnotations(valid), nil); err == nil {
			t.Fatal("accepted missing name")
		}
	})
	t.Run("missing-label", func(t *testing.T) {
		if _, err := annotationRestoreArchiveAt(root, true, "trainer", nil, boundAnnotations(valid), nil); err == nil {
			t.Fatal("bypassed admission selector")
		}
	})
	for _, key := range []string{"migration.dcnlab.com/restore-plan-uid", "migration.dcnlab.com/restore-plan-generation"} {
		t.Run(key, func(t *testing.T) {
			a := boundAnnotations(valid)
			delete(a, key)
			if _, err := annotationRestoreArchiveAt(root, true, "trainer", labels, a, nil); err == nil {
				t.Fatal("accepted missing binding")
			}
		})
	}
	t.Run("bad-generation", func(t *testing.T) {
		a := boundAnnotations(valid)
		a["migration.dcnlab.com/restore-plan-generation"] = "0"
		if _, err := annotationRestoreArchiveAt(root, true, "trainer", labels, a, nil); err == nil {
			t.Fatal("accepted invalid generation")
		}
	})
	t.Run("conflict", func(t *testing.T) {
		if _, err := annotationRestoreArchiveAt(root, true, "trainer", labels, boundAnnotations(valid), boundAnnotations("/other")); err == nil {
			t.Fatal("accepted conflict")
		}
	})
	t.Run("container-only", func(t *testing.T) {
		if _, err := annotationRestoreArchiveAt(root, true, "trainer", labels, nil, boundAnnotations(valid)); err == nil {
			t.Fatal("accepted container-only annotation")
		}
	})
}
func TestAnnotationAdapterRejectsSymlink(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "file.tar"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	labels := map[string]string{"migration.dcnlab.com/restore-plan": "plan"}
	if _, err := annotationRestoreArchiveAt(root, true, "trainer", labels, boundAnnotations(restoreArchiveRoot+"/link/file.tar"), nil); err == nil {
		t.Fatal("accepted symlink")
	}
}
