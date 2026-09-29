// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestOrgApprovalRefusalAdviceCoversApprovedRequests — PR #218 fix review r2
// F6 and r3 F3: an org approved before a check existed (the index bound, the
// credentials refusal, v0.29.57's host gate) that lost its registration
// reaches these refusals on re-approve, and rejecting an approved row is a
// silent no-op. Every arm says what happens to an already-approved request.
func TestOrgApprovalRefusalAdviceCoversApprovedRequests(t *testing.T) {
	for _, err := range []error{
		ErrURLTooLong,
		platform.ErrURLUserinfo,
		ErrOrgOffGitHubHost,
		fmt.Errorf("register: %w", ErrOrgOffGitHubHost),
	} {
		advice := OrgApprovalRefusalAdvice(err, "https://api.github.com")
		if !strings.Contains(advice, "reject") || !strings.Contains(advice, "already approved") {
			t.Errorf("%v: advice %q must say to reject a pending request and what happens to an approved one", err, advice)
		}
	}
}
