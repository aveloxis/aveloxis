// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The 0.29.61 checklist quotes three log fragments the operator greps for.
// Each must be a substring of a log string in the code that emits it
// (round 3 on v0.29.61: a wording edit would otherwise orphan the
// instruction silently).
func TestV02961ChecklistQuotesRealLogLines(t *testing.T) {
	var step string
	for _, s := range v02961DeployChecklist {
		if strings.Contains(s.cmd, "pg_matviews") {
			step = s.desc
		}
	}
	if step == "" {
		t.Fatal("the 0.29.61 checklist has no pg_matviews verification step")
	}
	var code strings.Builder
	for _, f := range []string{"../../internal/db/supply_chain_views.go", "../../internal/db/migrate.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		code.WriteString(srctest.StripGoComments(string(src)))
	}
	quoted := regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(step, -1)
	checked := 0
	for _, m := range quoted {
		frag := m[1]
		if !strings.Contains(frag, "supply-chain view") && !strings.Contains(frag, "failed") {
			continue // a command, not a log fragment
		}
		checked++
		if !strings.Contains(code.String(), frag) {
			t.Errorf("the checklist quotes %q but no log string in supply_chain_views.go or migrate.go contains it", frag)
		}
	}
	if checked < 4 {
		t.Errorf("expected the ERROR gate and three fragments to be checked, got %d", checked)
	}
}
