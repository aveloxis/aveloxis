// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestHistoryTooExpensiveGatesOnSentinel (code-review round 2026-09-06,
// finding 12): the lossy floor-skip must gate the global resource-limit
// shape on errors.Is(err, platform.ErrResourceLimits) — graphql.go's own
// contract — never a raw message substring (a rewording would flip every
// global-RLE floor hit from lossy-skip to infinite bubble). The string
// check exists ONLY for the truncated-body decode shape and must be
// scoped to it.
func TestHistoryTooExpensiveGatesOnSentinel(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/platform/github/contributor_history.go"),
		"func (c *Client) fetchHistoryWindow("))
	if !strings.Contains(body, "errors.Is(err, platform.ErrResourceLimits)") {
		t.Error("the global RLE shape must gate on the ErrResourceLimits sentinel, not the message text (finding 12)")
	}
	// The string check may exist only alongside the decode-shape scope.
	if strings.Contains(body, `"RESOURCE_LIMITS_EXCEEDED"`) &&
		!strings.Contains(body, `"decode graphql envelope"`) {
		t.Error("the RESOURCE_LIMITS_EXCEEDED string check must be scoped to the truncated-body decode shape (finding 12)")
	}
}
