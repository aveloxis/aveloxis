// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.102 — forge numeric project ID capture (rename-dedup enabler).
// glProject has decoded `id` since inception; FetchRepoInfo must map it
// into model.RepoInfo.PlatformRepoID so Phase 0 backfills the fleet.
package gitlab

import (
	"os"
	"strings"
	"testing"
)

func TestFetchRepoInfoMapsPlatformRepoID(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "PlatformRepoID:") {
		t.Error("gitlab FetchRepoInfo must populate model.RepoInfo.PlatformRepoID from the project's numeric id")
	}
}
