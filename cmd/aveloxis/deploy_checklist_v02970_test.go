// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"
)

// TestV02970ChecklistNamesItsOperatorVisibleChanges pins what an operator
// must learn from `aveloxis deploy-checklist` for 0.29.70: the four columns
// and the function marking the migrate applies, and at start the changes a
// log reader or a dashboard would otherwise misread — the scancode pacing
// unit, the vulnerability counts that drop, the GUI deploy order, and the
// key name in every key-pool log line (token_prefix → token_hash, CodeQL
// alert 201 on PR #220).
func TestV02970ChecklistNamesItsOperatorVisibleChanges(t *testing.T) {
	steps, ok := deployChecklistFor("0.29.70")
	if !ok || len(steps) == 0 {
		t.Fatal("0.29.70 has no deploy checklist")
	}
	var migrate, start string
	for _, s := range steps {
		switch {
		case strings.HasPrefix(s.cmd, "aveloxis migrate"):
			migrate = s.desc
		case s.cmd == "aveloxis start all":
			start = s.desc
		}
	}
	if steps[0].cmd != "aveloxis stop all" || steps[len(steps)-1].cmd != "aveloxis start all" {
		t.Errorf("the ladder must start with stop all and end with start all: %v", steps)
	}
	for _, want := range []string{"repo_unavailable_reason", "repo_unavailable_url", "first_commit_at", "last_commit_at", "PARALLEL SAFE"} {
		if !strings.Contains(migrate, want) {
			t.Errorf("the migrate step must name %q", want)
		}
	}
	for _, want := range []string{"scancode_start_interval_s", "version unknown", "aveloxis-gui", "token_hash", "token_prefix", "shasum -a 256"} {
		if !strings.Contains(start, want) {
			t.Errorf("the start step must name %q", want)
		}
	}
}
