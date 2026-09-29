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
// global-RLE floor hit from lossy-skip to infinite bubble). Since PR #218
// review E1 the truncated-body shape arrives as the same sentinel, so the
// decode-error string match it used to need must not come back.
func TestHistoryTooExpensiveGatesOnSentinel(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/platform/github/contributor_history.go"),
		"func (c *Client) fetchHistoryWindow("))
	if !strings.Contains(body, "errors.Is(err, platform.ErrResourceLimits)") {
		t.Error("the global RLE shape must gate on the ErrResourceLimits sentinel, not the message text (finding 12)")
	}
	for _, banned := range []string{`"decode graphql envelope"`, "CarriesResourceLimitsError", `"RESOURCE_LIMITS_EXCEEDED"`} {
		if strings.Contains(body, banned) {
			t.Errorf("fetchHistoryWindow matches %s; the truncated resource-limit answer arrives as ErrResourceLimits (PR #218 review E1)", banned)
		}
	}
}
