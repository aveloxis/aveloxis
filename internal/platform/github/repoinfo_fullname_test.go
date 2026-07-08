// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.32: RepoInfo carries the forge's canonical "owner/name" spelling
// (FullName) so the Phase 0 case self-heal can correct case-drifted
// repo_git values. GitHub GraphQL calls it nameWithOwner; REST full_name.

package github

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestGraphQLRepoInfoQuerySelectsNameWithOwner(t *testing.T) {
	q := repoInfoGraphQL("chaoss", "augur")
	if !strings.Contains(q, "nameWithOwner") {
		t.Error("repoInfoGraphQL must select nameWithOwner — it is the canonical-case " +
			"source for the Phase 0 case self-heal (HealRepoCaseDrift). The API is " +
			"case-insensitive on lookup but always RETURNS the canonical spelling.")
	}
}

func TestGhRepoInfoDecodesFullName(t *testing.T) {
	var raw ghRepoInfo
	if err := json.Unmarshal([]byte(`{"full_name": "Azure/azure-sdk-tools"}`), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw.FullName != "Azure/azure-sdk-tools" {
		t.Errorf("ghRepoInfo.FullName = %q, want the decoded full_name — the REST "+
			"fallback path must carry the canonical spelling too.", raw.FullName)
	}
}

func TestFetchRepoInfoMapsFullName(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("read client.go: %v", err)
	}
	// Both the GraphQL path and the REST fallback must populate
	// model.RepoInfo.FullName.
	if strings.Count(string(src), "FullName:") < 2 {
		t.Error("client.go must map FullName into model.RepoInfo on BOTH the GraphQL " +
			"path (r.NameWithOwner) and the REST fallback (raw.FullName) — a canonical " +
			"name that only arrives on one path makes the self-heal availability " +
			"depend on which transport happened to serve Phase 0.")
	}
}
