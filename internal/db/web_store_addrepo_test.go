// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the v0.25.32 AddRepoToGroup refactor. The
// pre-v0.25.32 implementation hand-rolled its own exact-match SELECT,
// inline platform detection, inline owner/name parsing, and a raw INSERT
// INTO aveloxis_data.repos — a parallel repo-creation path that (a)
// missed case variants of already-tracked repos (the source of the
// production duplicate cohort: bulk-pasted lowercase URLs) and (b) left
// data_source empty. The refactor routes through the shared store
// helpers so there is exactly ONE way a repo row comes into existence.

package db

import (
	"strings"
	"testing"
)

func TestAddRepoToGroupUsesSharedResolvers(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddRepoToGroup")

	for _, needle := range []string{"s.FindRepoByURL(", "s.UpsertRepo(", "s.EnqueueRepo("} {
		if !strings.Contains(body, needle) {
			t.Errorf("AddRepoToGroup must call %s — the shared resolver/creator path is "+
				"what makes web bulk-paste case-variant-safe and cross-user repo sharing "+
				"work (a case variant of an already-collected repo links the SHARED "+
				"repo_id into the new user's group).", needle)
		}
	}

	if strings.Contains(body, "INSERT INTO aveloxis_data.repos") {
		t.Error("AddRepoToGroup must NOT hand-roll its own INSERT INTO aveloxis_data.repos — " +
			"that parallel creation path bypassed case-variant resolution and left " +
			"data_source empty. Route through UpsertRepo.")
	}
}

// The v0.19.0 group-approval gate must survive the refactor: pending
// groups defer enqueue to ApproveGroup; rejected groups refuse the add.
func TestAddRepoToGroupKeepsApprovalGate(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddRepoToGroup")

	for _, needle := range []string{`"rejected"`, `"approved"`, "user_repos"} {
		if !strings.Contains(body, needle) {
			t.Errorf("AddRepoToGroup must keep the v0.19.0 approval gate and the "+
				"user_repos link; expected %q in the function body.", needle)
		}
	}
}

// Existing repos must NOT be routed through UpsertRepo — its ON CONFLICT
// DO UPDATE would clobber populated columns (repo_description,
// primary_language, ...) with the web path's empty struct. UpsertRepo is
// only for the not-found branch.
func TestAddRepoToGroupOnlyUpsertsWhenNotFound(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddRepoToGroup")

	findIdx := strings.Index(body, "s.FindRepoByURL(")
	upsertIdx := strings.Index(body, "s.UpsertRepo(")
	if findIdx < 0 || upsertIdx < 0 {
		t.Skip("resolver calls missing — covered by TestAddRepoToGroupUsesSharedResolvers")
	}
	if findIdx > upsertIdx {
		t.Error("AddRepoToGroup must look up via FindRepoByURL FIRST and only call " +
			"UpsertRepo when the repo is not found — routing existing repos through " +
			"UpsertRepo's DO UPDATE clobbers collected metadata with empty values.")
	}
}
