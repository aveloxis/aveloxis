// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the repo-add path. Originally written for
// the v0.25.32 AddRepoToGroup refactor (shared resolver/creator path);
// re-anchored at v0.27.20 when the flow moved into AddReposToGroup +
// ensureRepoCollectedInGroup (add_requests.go) under the per-add
// approval rule. The invariants are unchanged: exactly ONE way a repo
// row comes into existence (FindRepoByURL first, UpsertRepo only on
// the not-found branch — its DO UPDATE would clobber collected
// metadata), and the group-status abuse lever survives.

package db

import (
	"strings"
	"testing"
)

func TestAddReposToGroupUsesSharedResolvers(t *testing.T) {
	body := extractFunctionBody(t, "add_requests.go", "ensureRepoCollectedInGroup") +
		extractFunctionBody(t, "add_requests.go", "AddReposToGroup")

	for _, needle := range []string{"s.FindRepoByURL(", "s.UpsertRepo(", "s.EnqueueRepo("} {
		if !strings.Contains(body, needle) {
			t.Errorf("the AddReposToGroup/ensureRepoCollectedInGroup path must call %s — the shared "+
				"resolver/creator path is what makes web bulk-paste case-variant-safe and "+
				"cross-user repo sharing work (a case variant of an already-collected repo "+
				"links the SHARED repo_id into the new user's group).", needle)
		}
	}

	if strings.Contains(body, "INSERT INTO aveloxis_data.repos") {
		t.Error("the repo-add path must NOT hand-roll its own INSERT INTO aveloxis_data.repos — " +
			"that parallel creation path bypassed case-variant resolution and left " +
			"data_source empty. Route through UpsertRepo.")
	}
}

// AddRepoToGroup must stay a thin wrapper over AddReposToGroup so the
// CLI importers and the batch path can never diverge on approval
// semantics.
func TestAddRepoToGroupDelegatesToBatch(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddRepoToGroup")
	if !strings.Contains(body, "s.AddReposToGroup(") {
		t.Error("AddRepoToGroup must delegate to AddReposToGroup — two parallel add " +
			"implementations WILL diverge on the per-add approval rule.")
	}
}

// The v0.27.20 per-add approval gate: rejected groups refuse the add;
// non-admin additions of not-yet-tracked URLs pend on an add-request
// instead of enqueueing; tracked repos link into user_repos instantly.
func TestAddReposToGroupKeepsApprovalGate(t *testing.T) {
	body := extractFunctionBody(t, "add_requests.go", "AddReposToGroup")

	for _, needle := range []string{`"rejected"`, "createAddRequest", "repoTracked", "AddRepoToGroupByID"} {
		if !strings.Contains(body, needle) {
			t.Errorf("AddReposToGroup must keep the v0.27.20 per-add approval gate; "+
				"expected %q in the function body.", needle)
		}
	}
	// The non-admin new-URL branch must NOT enqueue directly — enqueue
	// happens via the admin path (ensureRepoCollectedInGroup) or after
	// approval (ProcessApprovedAddRequest). A direct EnqueueRepo call
	// in AddReposToGroup itself would bypass the approval rule.
	if strings.Contains(body, "s.EnqueueRepo(") {
		t.Error("AddReposToGroup must not call EnqueueRepo directly — enqueue is owned by " +
			"ensureRepoCollectedInGroup (admin/approved paths only), so a non-admin's new " +
			"URLs can never reach the queue without an approval.")
	}
}

// Existing repos must NOT be routed through UpsertRepo — its ON CONFLICT
// DO UPDATE would clobber populated columns (repo_description,
// primary_language, ...) with the web path's empty struct. UpsertRepo is
// only for the not-found branch.
func TestEnsureRepoCollectedOnlyUpsertsWhenNotFound(t *testing.T) {
	body := extractFunctionBody(t, "add_requests.go", "ensureRepoCollectedInGroup")

	findIdx := strings.Index(body, "s.FindRepoByURL(")
	upsertIdx := strings.Index(body, "s.UpsertRepo(")
	if findIdx < 0 || upsertIdx < 0 {
		t.Skip("resolver calls missing — covered by TestAddReposToGroupUsesSharedResolvers")
	}
	if findIdx > upsertIdx {
		t.Error("ensureRepoCollectedInGroup must look up via FindRepoByURL FIRST and only call " +
			"UpsertRepo when the repo is not found — routing existing repos through " +
			"UpsertRepo's DO UPDATE clobbers collected metadata with empty values.")
	}
}
