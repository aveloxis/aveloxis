// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// strip_quoted_history_test.go — Part B's history walker CLI. The
// v0.27.105 whitespace-walker precedent: heavy Go-side walks live in a
// resumable CLI, never inside migrate (the F13 class — serve runs full
// migrations inline on every version bump).
package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestStripQuotedHistoryCommandRegistered(t *testing.T) {
	if !strings.Contains(srctest.Read(t, "cmd/aveloxis/main.go"), "stripQuotedHistoryCmd(") {
		t.Error("main.go must register stripQuotedHistoryCmd")
	}
	src := srctest.Read(t, "cmd/aveloxis/strip_quoted_history.go")
	for _, needle := range []string{
		`"strip-quoted-history"`,
		`"limit"`,
		`"rule-rerun"`,
		`ConnectionStringWithAppName("aveloxis-strip-quotes")`,
		"GetMailingListBodiesForStrip(",
		"UpdateMessageCleanBatch(",
		"StripQuotedHistory(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("strip_quoted_history.go must contain %s", needle)
		}
	}
}

func TestStripQuotedHistoryDoesNotMigrate(t *testing.T) {
	code := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/strip_quoted_history.go"))
	if strings.Contains(code, ".Migrate(") {
		t.Error("strip-quoted-history must NOT call store.Migrate (v0.21.5)")
	}
}
