// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The Sphinx site under docs/_build/html is tracked, and docs/conf.py reads
// its version from internal/db/version.go at build time — so a version bump
// without a rebuild publishes pages that name the previous release (PR #226
// review 5448678338: 0.29.74 shipped with 0.29.73 in every page title).
// Rebuild: cd docs && sphinx-build -W -b html . _build/html (the pinned
// toolchain in docs/requirements.txt).
func TestTrackedDocsBuildCarriesTheCurrentVersion(t *testing.T) {
	root := srctest.Root(t)
	js, err := os.ReadFile(filepath.Join(root, "docs", "_build", "html", "_static", "documentation_options.js"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`VERSION:\s*'([^']*)'`).FindSubmatch(js)
	if m == nil {
		t.Fatal("docs/_build/html/_static/documentation_options.js has no VERSION line")
	}
	if got := string(m[1]); got != db.ToolVersion {
		t.Errorf("the tracked docs build is version %s, internal/db/version.go is %s — rebuild it: cd docs && sphinx-build -W -b html . _build/html", got, db.ToolVersion)
	}
}
