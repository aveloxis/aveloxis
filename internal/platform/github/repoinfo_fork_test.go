// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.78: fork capture. GitHub delivers fork status on both
// transports (GraphQL isFork/parent, REST fork/parent) and the REST
// struct even parsed it already — but both mappings dropped it before
// model.RepoInfo, which is why 0 of 94,104 production repos had
// forked_from populated. These tests pin the capture on BOTH paths so
// fork data availability never depends on which transport served
// Phase 0 (the FullName lesson, repeated).

package github

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestGraphQLRepoInfoQuerySelectsForkFields(t *testing.T) {
	q := repoInfoGraphQL("chaoss", "augur")
	if !strings.Contains(q, "isFork") {
		t.Error("repoInfoGraphQL must select isFork — repos.forked_from is populated from Phase 0")
	}
	if !strings.Contains(q, "parent { nameWithOwner }") {
		t.Error("repoInfoGraphQL must select parent { nameWithOwner } — the upstream full name")
	}
}

func TestGhRepoInfoDecodesForkAndParent(t *testing.T) {
	var raw ghRepoInfo
	err := json.Unmarshal([]byte(`{"fork": true, "parent": {"full_name": "NixOS/nixpkgs"}}`), &raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !raw.Fork || raw.Parent == nil || raw.Parent.FullName != "NixOS/nixpkgs" {
		t.Errorf("ghRepoInfo must decode fork+parent, got Fork=%v Parent=%+v", raw.Fork, raw.Parent)
	}
}

func TestFetchRepoInfoMapsForkFieldsOnBothPaths(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("read client.go: %v", err)
	}
	s := string(src)
	// GraphQL path: graphQLRepo → model.RepoInfo.
	if strings.Count(s, "IsFork:") < 2 || strings.Count(s, "ForkParent:") < 2 {
		t.Error("client.go must map IsFork + ForkParent into model.RepoInfo on BOTH the " +
			"GraphQL path and the REST fallback — fork capture that only arrives on one " +
			"transport silently defeats the showcase fork filter for repos whose Phase 0 " +
			"happened to use the other one.")
	}
}
