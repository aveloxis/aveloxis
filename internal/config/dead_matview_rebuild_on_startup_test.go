// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.57 removed `collection.matview_rebuild_on_startup`. It never did
// anything: `serve` handed it to the store AFTER its startup migration had
// run (main.go called SetMatviewOnStartup below store.Migrate, and the
// migration is the only reader), and since v0.27.131 a current schema stamp
// skips that migration outright. The docs said `serve` rebuilt the views at
// startup when it was true; an operator deploying a changed view definition
// who relied on that kept the old definition. A plain `aveloxis migrate` is
// what re-creates the views.
//
// This negative tripwire keeps the field from coming back unwired. If a
// real need for startup view creation ever arises, give it a name that says
// what it does and set it BEFORE the migration that reads it — with an
// end-to-end test (SR-10) from the JSON value to a re-created view.
func TestDeadMatviewRebuildOnStartupNotReintroduced(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/config/config.go"))
	if strings.Contains(src, `json:"matview_rebuild_on_startup"`) {
		t.Error(`config.go declares json:"matview_rebuild_on_startup" — removed in v0.29.57 because serve applied it after the migration that reads it, so it never did anything`)
	}
	body := srctest.TypeBody(t, src, "CollectionConfig")
	for _, line := range strings.Split(body, "\n") {
		if f := strings.Fields(line); len(f) > 0 && f[0] == "MatviewRebuildOnStartup" {
			t.Errorf("CollectionConfig declares MatviewRebuildOnStartup again: %q", strings.TrimSpace(line))
		}
	}
	main := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	if strings.Contains(main, "MatviewRebuildOnStartup") {
		t.Error("cmd/aveloxis/main.go reads MatviewRebuildOnStartup again — it was removed in v0.29.57")
	}
}
