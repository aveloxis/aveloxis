// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// import_grouping_test.go — v0.27.135 (Copilot round 19): repo-wide
// tripwire for the stdlib-after-module import misgrouping class.
// gofmt sorts WITHIN groups but never regroups, so a stdlib import
// appended into the project-import group (the round-19 findings:
// gitlab/client.go's "sync", fill_audit_b_test.go's "sync/atomic",
// plus collector/vulnerability.go's "net/http" which the review
// missed) passes every formatter gate while violating the house
// stdlib-first layout. The rule enforced here: within a file's
// import block, every stdlib path (first segment has no dot) must
// appear before every module path (first segment has a dot).

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestImportGroupsKeepStdlibFirst(t *testing.T) {
	root := srctest.Root(t)
	importBlockRe := regexp.MustCompile(`(?ms)^import \(\n(.*?)^\)`)
	pathRe := regexp.MustCompile(`"([^"]+)"`)

	scanned := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", ".claude", "testdata", "vendor", "node_modules", "docs":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		scanned++
		m := importBlockRe.FindStringSubmatch(string(data))
		if m == nil {
			return nil // single-import or import-free file
		}
		seenModule := ""
		for _, line := range strings.Split(m[1], "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "//") {
				continue
			}
			pm := pathRe.FindStringSubmatch(line)
			if pm == nil {
				continue
			}
			first, _, _ := strings.Cut(pm[1], "/")
			if strings.Contains(first, ".") {
				seenModule = pm[1]
			} else if seenModule != "" {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s: stdlib import %q appears after module import %q — move it into the stdlib group (round-19 class)", rel, pm[1], seenModule)
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// Self-check: the walk must actually cover the tree — a broken
	// root/filter that scans nothing would pass vacuously.
	if scanned < 500 {
		t.Fatalf("only %d .go files scanned — the walk lost the tree (self-check floor 500)", scanned)
	}
}
