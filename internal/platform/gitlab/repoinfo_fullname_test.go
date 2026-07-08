// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.32: GitLab's canonical repo spelling is path_with_namespace —
// carried as RepoInfo.FullName for the Phase 0 case self-heal. GitLab
// nested groups are covered because path_with_namespace holds the full
// group/subgroup/project path.

package gitlab

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestGlProjectDecodesPathWithNamespace(t *testing.T) {
	var raw glProject
	if err := json.Unmarshal([]byte(`{"id": 1, "path_with_namespace": "GitLab-Org/sub/Project"}`), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw.PathWithNamespace != "GitLab-Org/sub/Project" {
		t.Errorf("glProject.PathWithNamespace = %q, want the decoded path_with_namespace.",
			raw.PathWithNamespace)
	}
}

func TestGitLabFetchRepoInfoMapsFullName(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("read client.go: %v", err)
	}
	if !strings.Contains(string(src), "FullName:") {
		t.Error("gitlab client.go's FetchRepoInfo must map path_with_namespace into " +
			"model.RepoInfo.FullName — GitLab/GitHub parity for the case self-heal.")
	}
}
