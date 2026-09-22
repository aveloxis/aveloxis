// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.57 — the scorecard API-spend probe sent a POOL TOKEN to a hardcoded
// https://api.github.com/rate_limit. The subprocess half of scorecard really
// does resolve its own host (worklist 34's GH_HOST), but this half is an
// in-process request this code makes, and it had a seam for the URL all
// along — so the exemption that covered the subprocess was covering a live
// site of exactly the class the release set out to close.

import "testing"

func TestScorecardRateLimitProbeFollowsTheConfiguredHost(t *testing.T) {
	for _, tc := range []struct{ base, want string }{
		{"", "https://api.github.com/rate_limit"},
		{"https://api.github.com", "https://api.github.com/rate_limit"},
		{"https://ghe.example.invalid/api/v3", "https://ghe.example.invalid/api/v3/rate_limit"},
		{"https://ghe.example.invalid/api/v3/", "https://ghe.example.invalid/api/v3/rate_limit"},
	} {
		if got := scorecardRateLimitURLFor(tc.base); got != tc.want {
			t.Errorf("scorecardRateLimitURLFor(%q) = %q, want %q", tc.base, got, tc.want)
		}
	}
}
