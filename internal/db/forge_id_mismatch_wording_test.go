// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestForgeIDMismatchHasOneWording — review round 1 on v0.29.66: the org
// scan's and Phase 0's forge-ID mismatch ERRORs had drifted into two
// remediation texts. Both log the shared constants; the wording is spelled
// in exactly one declaration (SR-17). The denominator is every non-test
// source examined, and both detectors must be among the users.
func TestForgeIDMismatchHasOneWording(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	examined, spelled := 0, 0
	users := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		examined++
		src := srctest.StripGoComments(string(b))
		spelled += strings.Count(src, "likely upstream delete-and-recreate")
		if strings.Contains(src, `"remediation", forgeIDMismatchRemediation`) && strings.Contains(src, "s.logger.Error(forgeIDMismatchMsg,") {
			users[f] = true
		}
	}
	if examined == 0 {
		t.Fatal("examined no sources")
	}
	if spelled != 1 {
		t.Errorf("the mismatch wording is spelled %d times across %d sources; want 1 (the shared constant)", spelled, examined)
	}
	for _, f := range []string{"repo_forge_id.go", "repo_metadata.go"} {
		if !users[f] {
			t.Errorf("%s must log forgeIDMismatchMsg with forgeIDMismatchRemediation", f)
		}
	}
}
