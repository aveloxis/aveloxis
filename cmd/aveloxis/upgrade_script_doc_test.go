// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestUpgradeScriptUsesThePendingChecklist — PR #218 fix review r2 F2 and
// r3 F2: the upgrade page's script computed the range in shell (a
// deploy_ack query, a schema_meta fallback, --inclusive), and each round
// found another shape where it disagreed with the start gate — an empty
// stamp, then an ack ahead of the stamp. The page now runs
// `deploy-checklist --pending`, which is the gate's computation
// (TestPendingChecklistIsTheGatesRange); no hand-rolled range query may
// come back.
func TestUpgradeScriptUsesThePendingChecklist(t *testing.T) {
	page := srctest.Read(t, "docs/getting-started/upgrading.md")
	start := strings.Index(page, "```bash")
	if start < 0 {
		t.Fatal("the upgrade steps block was not found")
	}
	block := page[start:]
	block = block[:strings.Index(block[3:], "```")+3]
	if !strings.Contains(block, "aveloxis deploy-checklist --pending") {
		t.Errorf("the upgrade steps must print the range with `aveloxis deploy-checklist --pending`:\n%s", block)
	}
	for _, banned := range []string{"deploy_ack", "schema_meta", "--since", "--inclusive"} {
		if strings.Contains(block, banned) {
			t.Errorf("the upgrade steps compute the range by hand again (%q) — a second spelling of deployRange (SR-17):\n%s", banned, block)
		}
	}
}
