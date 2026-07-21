// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

// v0.27.30 — live canary (audit G6): the ENTIRE GitLab client was
// mock-only, zero canaries — every GitLab-hosted repo's collection
// rides parse assumptions never checked against the real API.

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestLiveGitLabRepoInfoForGitLabRunner(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITLAB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITLAB_TOKEN to run the GitLab canary")
	}
	logger := slog.Default()
	keys := platform.NewKeyPool([]string{tok}, logger)
	client := New("https://gitlab.com/api/v4", keys, logger)
	info, err := client.FetchRepoInfo(context.Background(), "gitlab-org", "gitlab-runner")
	if err != nil {
		t.Fatalf("live GitLab FetchRepoInfo failed: %v", err)
	}
	if info.FullName == "" || info.DefaultBranch == "" {
		t.Errorf("live GitLab shape drift: full_name=%q default_branch=%q", info.FullName, info.DefaultBranch)
	}
	if info.CommitCount == 0 && info.StarCount == 0 && info.IssuesCount == 0 {
		t.Error("all counts zero for a very active project — the statistics shape drifted")
	}
}
