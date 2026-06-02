// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

func TestLoadApacheListsCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "loadApacheListsCmd(") {
		t.Error("main.go must register loadApacheListsCmd so it's discoverable via `aveloxis --help`")
	}
}

func TestLoadApacheListsCmdShape(t *testing.T) {
	data, err := os.ReadFile("load_apache_lists.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"load-apache-lists"`) {
		t.Error(`command Use must be "load-apache-lists"`)
	}
	// It must use the PMC-slug fetch, list enumeration (Phase 2.6), the §5
	// collect policy, and the per-PMC group + list registration store methods.
	for _, needle := range []string{
		"apache.FetchPMCs(",
		"UpsertRepoGroup(",
		"SetRepoGroup(",
		"RegisterMailingList(",
		"apache_ponymail",
		"EnumerateLists(",
		"shouldCollectList(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("load_apache_lists.go must use %q", needle)
		}
	}
}

// TestShouldCollectListPolicy pins the §5 collect/skip decisions.
func TestShouldCollectListPolicy(t *testing.T) {
	cases := []struct {
		name, bugDB string
		want        bool
	}{
		{"dev", "", true},
		{"users", "", true},
		{"common-dev", "", true}, // §4 naming drift
		{"commits", "", false},
		{"cvs", "", false},
		{"notifications", "", false},
		{"jira", "https://issues.apache.org/jira/browse/KAFKA", true}, // Jira-primary
		{"jira", "https://github.com/apache/x/issues", false},         // GitHub-native → skip mirror
		{"bugs", "https://bz.apache.org/bugzilla/", true},             // Bugzilla-primary
	}
	for _, c := range cases {
		if got := shouldCollectList(c.name, c.bugDB); got != c.want {
			t.Errorf("shouldCollectList(%q, %q) = %v, want %v", c.name, c.bugDB, got, c.want)
		}
	}
}
