// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestScorecardAttemptNamesItsLentTokens — worklist item 79 (Stage 1,
// observation only): a scorecard attempt that timed out waiting on GitHub's
// rate limit could not be tied to the keys it was lent, so the limit could
// not be matched against the key pool's refusals. The attempt line names
// the lent tokens by their 8-character prefix (the key pool's
// token_prefix), never in full.
func TestScorecardAttemptNamesItsLentTokens(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	const a, b = "ghp_AAAAAAAAsecretpartone", "ghp_BBBBBBBBsecretparttwo"
	_, _ = invokeScorecard(context.Background(), "/nonexistent/scorecard-binary", 7, "https://github.com/o/r", "", time.Second, a+","+b, logger)
	l := logs.String()
	if !strings.Contains(l, `msg="scorecard attempt"`) || !strings.Contains(l, "ghp_AAAA...") || !strings.Contains(l, "ghp_BBBB...") {
		t.Errorf("the attempt line must name the lent tokens by prefix:\n%s", l)
	}
	if strings.Contains(l, "secretpart") {
		t.Errorf("a token leaked in full:\n%s", l)
	}
}
