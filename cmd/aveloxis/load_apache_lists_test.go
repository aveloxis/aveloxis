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
	// It must use the PMC-slug fetch (lists are derived from the slug) and the
	// per-PMC group + list registration store methods.
	for _, needle := range []string{
		"apache.FetchPMCs(",
		"UpsertRepoGroup(",
		"SetRepoGroup(",
		"RegisterMailingList(",
		"apache_ponymail",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("load_apache_lists.go must use %q", needle)
		}
	}
	// dev@ and users@ are the always-collected human lists (§5).
	if !strings.Contains(src, `"dev"`) || !strings.Contains(src, `"users"`) {
		t.Error("must register the dev@ and users@ lists")
	}
}
