// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// deployChecklistCommand is the command's two words, any whitespace (a
// wrapped line included) between them; flaggedTail is one of the command's
// range flags after it on the same line (r8 F3: any `--` also passed a
// prose dash and --help).
var (
	deployChecklistCommand = regexp.MustCompile(`aveloxis\s+deploy-checklist\b`)
	flaggedTail            = regexp.MustCompile(`^[ \t]+--(?:pending|since|inclusive)\b`)
)

// flaglessDeployChecklist returns the spans of every mention of the command
// that is NOT followed by a flag on the same line. Inverted in PR #218 fix
// review r7 F3: a list of terminators (quote, shell operator, line end)
// missed a Go "\n" escape, prose punctuation, emphasis and HTML — the
// escape class rounds 5 and 6 closed one member at a time.
func flaglessDeployChecklist(src string) [][]int {
	var out [][]int
	for _, m := range deployChecklistCommand.FindAllStringIndex(src, -1) {
		if !flaggedTail.MatchString(src[m[1]:]) {
			out = append(out, m)
		}
	}
	return out
}

func TestFlaglessDeployChecklistCorpus(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"per pair. Run this binary's deploy steps first: 'aveloxis\ndeploy-checklist' prints them", true}, // the r4 original, wrapped
		{"the steps `aveloxis deploy-checklist` prints", true},
		{"run `aveloxis\ndeploy-checklist` and follow it", true},
		{"aveloxis deploy-checklist | less\n", true},
		{"aveloxis deploy-checklist\t# first\n", true},
		{"x=$(aveloxis deploy-checklist)", true},
		{"aveloxis deploy-checklist && aveloxis stop all", true},
		{"aveloxis deploy-checklist\n", true},
		{"the steps `aveloxis deploy-checklist --pending` prints", false},
		{"aveloxis deploy-checklist --since 0.29.64\n", false},
		{"aveloxis deploy-checklist\n--pending wrapped flag", true}, // a flag on the next line is not the same command
		{"`deploy-checklist --pending`", false},
		// r7 F3: terminators the first list missed.
		{`fmt.Println("Run aveloxis deploy-checklist\n")`, true},
		{"run aveloxis deploy-checklist, then ack", true},
		{"Run aveloxis deploy-checklist.", true},
		{"*aveloxis deploy-checklist*", true},
		{"<code>aveloxis deploy-checklist</code>", true},
		{"aveloxis deploy-checklist\t--pending\n", false},
		// r8 F3: a prose dash or a flag that prints nothing is not a range.
		{"Run aveloxis deploy-checklist -- it prints the steps.", true},
		{"aveloxis deploy-checklist  -- see below", true},
		{"aveloxis deploy-checklist --", true},
		{"aveloxis deploy-checklist --help", true},
		{"aveloxis deploy-checklist --inclusive --since 0.29.1", false},
	} {
		if got := len(flaglessDeployChecklist(tc.text)) > 0; got != tc.want {
			t.Errorf("flaglessDeployChecklist(%q) = %v; want %v", tc.text, got, tc.want)
		}
	}
}

// TestDeployAdviceNamesThePendingRange — PR #218 fix review r4 F2 and r5 F2
// (the L11 sweep of r3 F2): advice that sends an operator to the flag-less
// `aveloxis deploy-checklist` names the binary's own list, which misses a
// skipped release's plain migrate; the range the gate enforces is
// `deploy-checklist --pending`. Round 4 banned seven phrasings and round 5
// escaped it with an eighth, so the OPERATION is banned instead: the
// command spelled with no flag — in quotes or backquotes, or alone on a
// line (a shell fence) — in non-test Go (comments stripped: history there
// is not advice), the public docs, the root Markdown files and .github.
// The command's own section in commands.md describes the flag-less form on
// purpose and is the reviewed allowance.
//
// r6 F2/F4: matching line by line missed the very wording round 4 removed
// ('aveloxis\ndeploy-checklist' wrapped inside a raw string) and a fence
// line ending in `| less`; the allowance was keyed by line text, so the
// same line passed anywhere in commands.md. flaglessDeployChecklist now
// runs over whole files with any whitespace between the words, and the
// allowance is the command's own section only.
func TestDeployAdviceNamesThePendingRange(t *testing.T) {
	root := srctest.Root(t)
	var files []string
	for _, dir := range []string{"cmd", "internal", "scripts", "docs", ".github"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "_build" || d.Name() == "testdata") {
				return filepath.SkipDir
			}
			ext := filepath.Ext(path)
			if !d.IsDir() && !strings.HasSuffix(path, "_test.go") && (ext == ".go" || ext == ".md" || ext == ".yml" || ext == ".yaml") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	rootMD, err := filepath.Glob(filepath.Join(root, "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, rootMD...)
	allowedSection := 0
	for _, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		if strings.HasSuffix(path, ".go") {
			src = srctest.StripGoComments(src)
		}
		rel, _ := filepath.Rel(root, path)
		lo, hi := -1, -1
		if filepath.ToSlash(rel) == "docs/guide/commands.md" {
			const heading = "\n## `aveloxis deploy-checklist`\n"
			if lo = strings.Index(src, heading); lo < 0 {
				t.Fatalf("%s: the deploy-checklist section (%q) was not found; the allowance is scoped to it", rel, strings.TrimSpace(heading))
			}
			hi = len(src)
			if next := strings.Index(src[lo+len(heading):], "\n## "); next >= 0 {
				hi = lo + len(heading) + next
			}
		}
		for _, m := range flaglessDeployChecklist(src) {
			if m[0] >= lo && m[1] <= hi {
				allowedSection++
				continue
			}
			line := strings.Count(src[:m[0]], "\n") + 1
			t.Errorf("%s:%d: the flag-less `aveloxis deploy-checklist` is the binary's own steps; advice names `aveloxis deploy-checklist --pending` (the gate's range): %q", rel, line, src[m[0]:m[1]])
		}
	}
	srctest.MinCount(t, "sources examined for flag-less deploy-checklist advice", len(files), 100)
	// The section's heading and usage line: fewer means the scoping broke.
	srctest.MinCount(t, "flag-less mentions inside the command's own section", allowedSection, 2)
}
