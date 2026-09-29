// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestScorecardAttemptNamesItsLentTokens — worklist item 79 (Stage 1,
// observation only): a scorecard attempt that timed out waiting on GitHub's
// rate limit could not be tied to the keys it was lent, so the limit could
// not be matched against the key pool's refusals. The attempt line names
// the lent tokens by the key pool's token_hash (platform.TokenHash), which
// carries no character of the token (CodeQL alert 201, PR #220).
func TestScorecardAttemptNamesItsLentTokens(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	const a, b = "ghp_AAAAAAAAsecretpartone", "ghp_BBBBBBBBsecretparttwo"
	_, _ = invokeScorecard(context.Background(), "/nonexistent/scorecard-binary", 7, "https://github.com/o/r", "", time.Second, a+","+b, logger)
	l := logs.String()
	if !strings.Contains(l, `msg="scorecard attempt"`) || !strings.Contains(l, platform.TokenHash(a)) || !strings.Contains(l, platform.TokenHash(b)) {
		t.Errorf("the attempt line must name the lent tokens by their token_hash:\n%s", l)
	}
	if strings.Contains(l, "AAAA") || strings.Contains(l, "BBBB") || strings.Contains(l, "secretpart") {
		t.Errorf("a token leaked in full:\n%s", l)
	}
}
