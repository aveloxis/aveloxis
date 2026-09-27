// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestGitHubArmsStopAfterTheFirstNonAnswer pins worklist item 26: a GitHub
// source non-answer fails the whole scan (v0.29.55), so the later GitHub
// calls of that scan were spent on a scan already lost. The second and third
// arms run only while no non-answer is recorded, and each arm's error is its
// own (the previous arm's error must not be re-counted when a call is
// skipped). The GitHub source is the concrete client, so this pins the
// control flow.
func TestGitHubArmsStopAfterTheFirstNonAnswer(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/distribution/scanner.go"), "func (s *CompositeScanner) Scan("))
	for _, call := range []string{"s.GitHub.ListRepoPackages(", "s.GitHub.ListRootManifests("} {
		i := strings.Index(body, call)
		if i < 0 {
			t.Fatalf("%s not found", call)
		}
		before := body[:i]
		guard := strings.LastIndex(before, "if len(githubNonAnswers) == 0 {")
		reset := strings.LastIndex(before, "err = nil")
		arm := strings.LastIndex(before, "enabledSources++")
		if guard < 0 || guard < arm {
			t.Errorf("%s must run only while no GitHub non-answer is recorded (guard `if len(githubNonAnswers) == 0`)", call)
		}
		if reset < 0 || reset < arm {
			t.Errorf("%s must clear err before its guarded call, or a skipped call re-counts the previous arm's error", call)
		}
	}
	// The manifest-content loop stops too (review round 1).
	loop := strings.Index(body, "for _, m := range rawManifests {")
	fetch := strings.Index(body, "s.GitHub.FetchManifestContent(")
	if loop < 0 || fetch < loop || !strings.Contains(body[loop:fetch], "len(githubNonAnswers) > 0") || !strings.Contains(body[loop:fetch], "break") {
		t.Error("the manifest-content loop must break once a GitHub non-answer is recorded, before the next fetch")
	}
}
