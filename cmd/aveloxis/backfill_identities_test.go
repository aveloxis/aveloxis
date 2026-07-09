// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.26.5 — `aveloxis backfill-identities` command pins. See
// summary/identity-attribution-audit-2026-07-09.md.

import (
	"os"
	"strings"
	"testing"
)

func TestBackfillIdentitiesCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("backfill_identities.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needle := range []string{
		`"backfill-identities`,
		`"dry-run"`,
		`"batch-size"`,
		`"limit"`,
		`"phase"`,
		"BackfillAssignmentIdentities(",
		"BackfillPRMetaOwners(",
		"BackfillClosedByFromEvents(",
		// phase 3 timeline sweep
		"ClosedBySweep",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("backfill_identities.go must contain %q", needle)
		}
	}
	if strings.Contains(stripLineComments(code), "store.Migrate(") {
		t.Error("backfill-identities must NOT call store.Migrate — v0.21.5 contract: " +
			"only serve and migrate run migrations")
	}

	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainSrc), "backfillIdentitiesCmd(") {
		t.Error("main.go must register backfillIdentitiesCmd")
	}
}
