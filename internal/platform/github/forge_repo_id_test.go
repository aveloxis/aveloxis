// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.102 — forge numeric repo ID capture (rename-dedup enabler).
// The v0.27.78 fork-capture lesson applies verbatim: BOTH transports
// (GraphQL primary + REST fallback) must select AND map the field, or
// the fleet's fill rate silently splits by transport.
package github

import (
	"os"
	"strings"
	"testing"
)

func TestRepoInfoQuerySelectsDatabaseID(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, "func repoInfoGraphQL")
	if i < 0 {
		t.Fatal("repoInfoGraphQL not found")
	}
	block := s[i:]
	if j := strings.Index(block, "\nfunc "); j > 0 {
		block = block[:j]
	}
	if !strings.Contains(block, "databaseId") {
		t.Error("repoInfoGraphQL must select databaseId — the rename-proof repo identity")
	}
}

func TestBothRepoInfoTransportsMapPlatformRepoID(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if strings.Count(s, "PlatformRepoID:") < 2 {
		t.Error("both FetchRepoInfo mappings (GraphQL + REST fallback) must populate model.RepoInfo.PlatformRepoID")
	}
	types, err := os.ReadFile("types.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(types), "`json:\"id\"`") {
		t.Error("ghRepoInfo must decode the REST numeric `id` field for the fallback transport")
	}
}
