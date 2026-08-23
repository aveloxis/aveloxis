// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.28.1 (A8) — heal-messages stamps as it goes. The 2026-08
// full-batch run was killed ~20% done after 6 days with ZERO rows
// stamped (verified live: all 546,081 worklist rows still pending),
// because healed_at stamped once at run end. These pins hold the
// pass-loop contract.

func TestHealMessagesLoopsInBoundedPasses(t *testing.T) {
	data, err := os.ReadFile("message_heal.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "const healPassSize = 25000") {
		t.Error("healPassSize must exist (the operator-proven 25K chunk from the v0.27.67 recovery)")
	}
	i := strings.Index(src, "func HealMessages(")
	j := strings.Index(src, "func runMessageHealPass(")
	if i < 0 || j < 0 {
		t.Fatal("HealMessages loop + runMessageHealPass split missing")
	}
	loop := src[i:j]
	if !strings.Contains(loop, "runMessageHealPass(") {
		t.Error("HealMessages must drive runMessageHealPass in a loop")
	}
	// Zero-progress passes must STOP the loop (a fully-failing batch
	// re-claims the same rows forever otherwise).
	if !strings.Contains(loop, "res.Healed == 0") {
		t.Error("a zero-progress pass must terminate the loop")
	}
	// The STAMP lives inside the per-pass function — that's the whole
	// point: progress persists per pass, not per run.
	passBody := src[j:]
	if !strings.Contains(passBody, "MarkMessagesHealed(") {
		t.Error("runMessageHealPass must stamp its own healed rows (per-pass persistence)")
	}
	if strings.Contains(loop, "MarkMessagesHealed(") {
		t.Error("the outer loop must NOT stamp — stamping is per-pass")
	}
}
