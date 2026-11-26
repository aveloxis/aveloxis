// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The class of findings whose dependency declares no version is "version
// not determined" (summary/70, 8.2). Its earlier descriptions said more than
// is known ("exposure unknown") or borrowed a word that means someone is
// looking into it (VEX's under_investigation); both are gone from the
// source, the comments and the docs, and this keeps them gone.
//
// Found by search, not listed: every text file of the repository but the
// ones named here, each with its reason.
func TestTheClassIsNotDescribedAsExposureUnknown(t *testing.T) {
	root := srctest.Root(t)
	needles := []string{"exposure unknown", "exposure is unknown", "under_investigation"}
	skipDirs := map[string]bool{
		".git": true, "node_modules": true, "vendor": true,
		"summary":  true, // design documents and the ledger record the old wording
		"_build":   true, // built docs follow their sources
		"testdata": true, // goldens of frozen ladders; schemas
	}
	// By base name: the deploy ladders are frozen once a host has run them
	// (0.29.70's holds the phrase), and this file holds the needles.
	skipFiles := map[string]string{
		"deploy_checklist.go":       "frozen deploy ladders",
		"vuln_wording_bans_test.go": "this test's own needles",
	}
	found := map[string]bool{}
	examined := 0
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		switch filepath.Ext(p) {
		case ".go", ".html", ".md", ".js", ".css", ".yaml", ".yml", ".sql", ".json":
		default:
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if _, skip := skipFiles[d.Name()]; skip {
			found[d.Name()] = true
			return nil
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil
		}
		examined++
		low := strings.ToLower(string(data))
		for _, n := range needles {
			if i := strings.Index(low, n); i >= 0 {
				line := 1 + strings.Count(low[:i], "\n")
				t.Errorf("%s:%d says %q: the class is \"version not determined\"", rel, line, n)
			}
		}
		return nil
	})
	srctest.MinCount(t, "text files examined", examined, 1000)
	for f := range skipFiles {
		if !found[f] {
			t.Errorf("exempt file %s is gone: remove its exemption", f)
		}
	}
}
