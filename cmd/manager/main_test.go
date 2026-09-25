package main

import "testing"

func TestCredentialBoundaries(t *testing.T) {
	for _, tc := range []struct {
		mode, path, cluster, node string
		valid                     bool
	}{
		{"management", "", "", "", false}, {"management", "karmada.yaml", "", "", true},
		{"member", "member.yaml", "aws", "", false}, {"member", "", "aws", "", true},
		{"checkpoint", "anything", "", "", false}, {"checkpoint", "", "", "", true},
		{"artifact", "", "aws", "", false}, {"artifact", "", "aws", "node1", true}, {"unknown", "", "", "", false},
	} {
		if got := validateOptions(tc.mode, tc.path, tc.cluster, tc.node) == nil; got != tc.valid {
			t.Errorf("%+v: valid=%v", tc, got)
		}
	}
}
