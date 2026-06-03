// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

func TestRegisterMailingListCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "registerMailingListCmd(") {
		t.Error("main.go must register registerMailingListCmd")
	}
}

func TestRegisterMailingListCmdShape(t *testing.T) {
	data, err := os.ReadFile("register_mailing_list.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{`"register-mailing-list"`, `"system"`, `"list"`, `"repo"`,
		"RegisterMailingList(", "SetRepoGroup("} {
		if !strings.Contains(src, needle) {
			t.Errorf("register-mailing-list must contain %q", needle)
		}
	}
	// The repo_group must be named after the repo, not the list, so multiple
	// lists for one repo share a group instead of orphaning (Phase 4 bug,
	// 2026-06-02). A per-list group name would re-create the bug.
	if !strings.Contains(src, `UpsertRepoGroup(ctx, "ML: "+repoURL`) {
		t.Error(`register-mailing-list must name the repo_group "ML: "+repoURL (per-repo), not per-list, so multi-list repos don't orphan`)
	}
	if strings.Contains(src, `UpsertRepoGroup(ctx, "ML: "+list`) {
		t.Error(`register-mailing-list must NOT name the repo_group after the list — that orphans earlier lists for multi-list repos`)
	}
}
