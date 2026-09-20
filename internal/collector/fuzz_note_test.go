// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// fuzzNoteAbsentStems are the parser-name stems FuzzManifestParsers' doc
// comment says the target does NOT call. The comment's first version listed
// XML and setup.cfg readers as absent after both had been added (v0.29.57,
// Copilot review 5261384568); a claim about what a test covers is pinned or
// it drifts.
var fuzzNoteAbsentStems = []string{
	"BuildGradle", "MixExs", "Pubspec", "PackageYaml", "ComposerJSON",
	"SetupCfgDeps", "SetupCfgVersions", "Lock", "PackageResolved",
}

func TestFuzzManifestParsersNoteIsCurrent(t *testing.T) {
	src := srctest.Read(t, "internal/collector/fuzz_test.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func FuzzManifestParsers("))
	called := regexp.MustCompile(`\b(parse[A-Za-z0-9]+)\(`).FindAllStringSubmatch(body, -1)
	srctest.MinCount(t, "parsers called by FuzzManifestParsers", len(called), 20)

	declared := map[string]bool{}
	decl := regexp.MustCompile(`(?m)^func (parse[A-Za-z0-9]+)\(`)
	for _, content := range srctest.PackageFiles(t, "internal/collector", 50) {
		for _, m := range decl.FindAllStringSubmatch(content, -1) {
			declared[m[1]] = true
		}
	}

	// The note names each stem (the list here and the prose agree) …
	note := src[:strings.Index(src, "func FuzzManifestParsers(")]
	note = note[strings.LastIndex(note, "\n\n")+1:]
	for _, stem := range fuzzNoteAbsentStems {
		if !strings.Contains(note, stem) {
			t.Errorf("the FuzzManifestParsers note does not name %q, which this test treats as absent", stem)
		}
		// … each stem still names at least one declared parser (a rename
		// makes the claim vacuous) …
		found := false
		for name := range declared {
			if strings.Contains(name, stem) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no declared parser name contains %q — the note's absent list names a reader that no longer exists", stem)
		}
		// … and the target calls none of them.
		for _, m := range called {
			if strings.Contains(m[1], stem) {
				t.Errorf("the note says the %s readers are absent from FuzzManifestParsers, but it calls %s", stem, m[1])
			}
		}
	}
	// The one exception the note states.
	if !strings.Contains(body, "parseSetupCfgExtrasVersions(") {
		t.Error("the note says the SetupCfgExtras reader IS fuzzed; it is not called")
	}
}
