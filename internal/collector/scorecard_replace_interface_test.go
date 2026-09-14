// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// Copilot review on PR #203: the persist half must not be able to
// interleave a mode check with another writer's rotation. The store
// interface therefore exposes ONE fused operation and nothing the
// collector could sequence itself (SR-18).
func TestScorecardStoreIsOneFusedOperation(t *testing.T) {
	b, err := os.ReadFile("scorecard.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	i := strings.Index(src, "type scorecardStore interface {")
	if i < 0 {
		t.Fatal("scorecardStore interface missing")
	}
	body := src[i:]
	body = body[:strings.Index(body, "\n}")]
	if !strings.Contains(body, "ReplaceScorecard(") {
		t.Error("scorecardStore must expose ReplaceScorecard (check + rotate + insert in one store transaction)")
	}
	for _, banned := range []string{"CurrentScorecardMode(", "RotateScorecardToHistory(", "InsertScorecardResult("} {
		if strings.Contains(body, banned) {
			t.Errorf("scorecardStore must not expose %s — a collector that can call it can interleave it with another writer", banned)
		}
	}
}
