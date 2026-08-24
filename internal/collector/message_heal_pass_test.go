// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
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
	// v0.28.8 (Copilot round 4): the loop walks by a strictly
	// increasing msg_id CURSOR. The old zero-progress termination
	// starved: failed rows stay pending, each pass reselected the
	// same lowest 25K ids, and once failures filled the first batch
	// the run exited "successfully" without visiting higher ids.
	if strings.Contains(loop, "res.Healed == 0") {
		t.Error("the zero-progress termination must be GONE — it permanently starved every row above a fully-failing first batch")
	}
	if !strings.Contains(loop, "cursor = res.MaxMsgID") {
		t.Error("the loop must advance the cursor to the batch's MaxMsgID (strictly increasing — termination guaranteed)")
	}
	if !strings.Contains(loop, "res.Batch == 0") {
		t.Error("the loop must terminate when a pass claims ZERO rows above the cursor (the walk is done; failed rows stay pending for the next RUN)")
	}
	storeSrc := srctest.Read(t, "internal/db/message_heal_store.go")
	if !strings.Contains(storeSrc, "AND w.msg_id > $1") {
		t.Error("GetMessageHealBatch must select above the cursor (w.msg_id > $1)")
	}
	// v0.28.8 (Copilot round 5): --limit bounds rows CONSIDERED, not
	// rows healed — decrementing by healed-only let a failure-heavy
	// canary run traverse the whole worklist (refetching parents all
	// the way), exactly the cohort a canary probes.
	if !strings.Contains(loop, "remaining -= res.Batch") {
		t.Error("the limit must decrement by res.Batch (rows considered)")
	}
	if strings.Contains(loop, "remaining -= res.Healed") {
		t.Error("decrementing the limit by healed-only must be gone — it breaks the canary cap under failures")
	}
	// And the CLI exits nonzero on ANY claimed-but-unstamped row —
	// Batch > Healed covers the failure paths that never touch
	// ParentErrors (staging flush, ProcessRepo, stale-link cleanup).
	cmdSrc := srctest.Read(t, "cmd/aveloxis/heal_messages.go")
	if !strings.Contains(cmdSrc, "if res.Batch > res.Healed {") {
		t.Error("heal-messages must exit nonzero when any claimed row failed to heal (Batch > Healed)")
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
