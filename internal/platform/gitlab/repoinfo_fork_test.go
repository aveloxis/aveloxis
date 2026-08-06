// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.78: fork capture, GitLab side. glProject has parsed
// forked_from_project.path_with_namespace since the early days — the
// FetchRepoInfo mapping just dropped it. Platform parity: GitHub and
// GitLab both populate model.RepoInfo's fork fields.

package gitlab

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestGlProjectDecodesForkedFromProject(t *testing.T) {
	var raw glProject
	err := json.Unmarshal([]byte(`{"forked_from_project": {"path_with_namespace": "gitlab-org/gitlab"}}`), &raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw.ForkedFromProject == nil || raw.ForkedFromProject.PathWithNamespace != "gitlab-org/gitlab" {
		t.Errorf("glProject must decode forked_from_project, got %+v", raw.ForkedFromProject)
	}
}

func TestFetchRepoInfoMapsForkParent(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatalf("read client.go: %v", err)
	}
	s := string(src)
	if !strings.Contains(s, "ForkedFromProject") || !strings.Contains(s, "ForkParent") {
		t.Error("gitlab FetchRepoInfo must map forked_from_project.path_with_namespace " +
			"into model.RepoInfo.ForkParent — GitLab parity with the GitHub fork capture " +
			"(a fork relationship GitLab reports must not be dropped at the model layer).")
	}
}
