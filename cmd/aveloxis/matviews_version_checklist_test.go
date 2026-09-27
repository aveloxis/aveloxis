// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestMatviewsChangeIsDeployedByAPlainMigrate is the checklist half of
// worklist item 36 (option 2): the release that last changed a materialized
// view's definition (db.MatviewsDefinitionVersion, pinned to matviews.sql by
// digest in internal/db) must have a deploy checklist whose migrate step is
// the plain `aveloxis migrate` — the only step that re-creates a view from
// its definition; `--skip-views` and `refresh-views` never do.
func TestMatviewsChangeIsDeployedByAPlainMigrate(t *testing.T) {
	steps, ok := deployChecklistFor(db.MatviewsDefinitionVersion)
	if !ok {
		t.Fatalf("db.MatviewsDefinitionVersion = %q names a release with no deploy checklist; the release that changes matviews.sql must have one with a plain `aveloxis migrate`", db.MatviewsDefinitionVersion)
	}
	plain := false
	for _, s := range steps {
		if s.cmd == "aveloxis migrate" {
			plain = true
		}
		if strings.HasPrefix(s.cmd, "aveloxis migrate") && strings.Contains(s.cmd, "--skip-views") {
			t.Errorf("%s: %q — --skip-views skips the view definitions this release changed", db.MatviewsDefinitionVersion, s.cmd)
		}
	}
	if !plain {
		t.Errorf("%s's checklist has no plain `aveloxis migrate` step, so the view definitions it changed never reach a fleet deployed by it", db.MatviewsDefinitionVersion)
	}
}
