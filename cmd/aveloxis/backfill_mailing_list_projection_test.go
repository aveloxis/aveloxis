// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestBackfillProjectionCmdRegisteredAndOrdered pins Phase 5: the command is
// registered, runs the three steps in order (keyed → thread → mark), and does
// NOT migrate (v0.21.5 contract).
func TestBackfillProjectionCmdRegisteredAndOrdered(t *testing.T) {
	main, _ := os.ReadFile("main.go")
	if !strings.Contains(string(main), "backfillMailingListProjectionCmd(&cfgPath)") {
		t.Error("backfillMailingListProjectionCmd must be registered in main.go")
	}
	src, err := os.ReadFile("backfill_mailing_list_projection.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	keyed := strings.Index(s, "BackfillKeyedIssueProjection(")
	thread := strings.Index(s, "BackfillThreadInheritance(")
	mark := strings.Index(s, "BackfillMarkRemainingProjected(")
	if keyed < 0 || thread < 0 || mark < 0 {
		t.Fatal("command must call all three backfill steps")
	}
	if !(keyed < thread && thread < mark) {
		t.Error("steps must run keyed → thread → mark (so thread issues exist before inheritance)")
	}
	if strings.Contains(s, "store.Migrate(") {
		t.Error("backfill command must NOT migrate (v0.21.5 contract)")
	}
}
