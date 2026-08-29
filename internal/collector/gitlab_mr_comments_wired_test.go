// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 31 (v0.28.18): GitLab MR conversation notes are collected on the
// main path — ListPRComments had no production caller, so merged and
// closed MRs' threads were never stored (only the open-item refresher
// and gap fill read per-MR notes). Gated on GitLab: GitHub's repo-wide
// /issues/comments already covers PR conversations and its
// ListPRComments delegates to it.
func TestGitLabMRConversationNotesAreCollectedOnTheMainPath(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/staged.go"), "func (sc *StagedCollector) collectMessages("))
	gate := strings.Index(body, "if sc.platID == int16(model.PlatformGitLab) {")
	call := strings.Index(body, "sc.client.ListPRComments(ctx, owner, repo, since)")
	if gate < 0 || call < 0 || call < gate {
		t.Fatalf("collectMessages must walk ListPRComments under the GitLab gate (gate=%d call=%d)", gate, call)
	}
	if !strings.Contains(body[call:], `fmt.Errorf("mr comments: %w", err)`) {
		t.Error("a non-skippable MR comments error must enter result.Errors")
	}
	gl := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/platform/gitlab/client.go"), "func (c *Client) ListPRComments("))
	if !strings.Contains(gl, "note.System || note.Position != nil") {
		t.Error("GitLab ListPRComments must skip diff-positioned notes — they are review comments (the kinds stay disjoint)")
	}
}
