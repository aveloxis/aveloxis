// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// shellFenceOpenRe matches the opening of a bash/sh/shell/console
// fence. Only those fences are judged: a `<placeholder>` inside a
// SQL or JSON block is not a shell redirection.
//
// Round 17 (Copilot round 7, finding 3): CommonMark permits a fenced
// code block to be indented by up to three spaces, and the docs use
// exactly that shape for every fence inside a numbered list
// (`3. Copy the token ...` followed by a three-space-indented
// ```bash). The column-zero anchor skipped all fourteen of them, so
// api.md's `https://<your-site>/api/v1/...` passed the guard. The
// CLOSER must accept the same indentation: an opener that is matched
// without a matching closer never ends, and every line to the end of
// the file is judged as shell.
var shellFenceOpenRe = regexp.MustCompile("^ {0,3}```(?:bash|sh|shell|zsh|console)\\s*$")

// shellFenceCloseRe matches any fence line (opener of a non-shell
// block or a closer) at CommonMark's permitted indentation.
var shellFenceCloseRe = regexp.MustCompile("^ {0,3}```")

// TestShellFenceRegexesAcceptCommonMarkIndentation pins the fence
// grammar the tripwire walks: a fence indented by one to three spaces
// is a fence (round 17), four spaces is an indented code block and
// not a fence, and the closer accepts the same range so an indented
// block actually closes.
func TestShellFenceRegexesAcceptCommonMarkIndentation(t *testing.T) {
	opens := []string{"```bash", " ```bash", "   ```sh", "   ```console  "}
	for _, line := range opens {
		if !shellFenceOpenRe.MatchString(line) {
			t.Errorf("must recognize the shell fence opener %q", line)
		}
	}
	notOpens := []string{"    ```bash", "```json", "   ```sql", "\t```bash", "text ```bash"}
	for _, line := range notOpens {
		if shellFenceOpenRe.MatchString(line) {
			t.Errorf("must NOT treat %q as a shell fence opener", line)
		}
	}
	closes := []string{"```", "   ```", " ```json"}
	for _, line := range closes {
		if !shellFenceCloseRe.MatchString(line) {
			t.Errorf("must recognize the fence closer %q", line)
		}
	}
	if shellFenceCloseRe.MatchString("    ```") {
		t.Error("four spaces is an indented code block, not a closer")
	}
}

// TestDocsShellBlocksCarryNoAnglePlaceholders — PR #197 round 15
// (Copilot round 5). dedicated-scancode-host.md told operators to run
// `go install ...@v<primary version>` verbatim: the shell parses the
// angle brackets as redirections, the install fails on a file named
// "primary", and the page's own next sentence says the WRONG version
// on this host reaches production. A placeholder that needs editing
// belongs in a variable assignment above the command, not inside it.
func TestDocsShellBlocksCarryNoAnglePlaceholders(t *testing.T) {
	root := srctest.Root(t)
	files := docsMarkdownCorpus(t)

	blocks := 0
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		inShell := false
		for i, line := range strings.Split(string(src), "\n") {
			switch {
			case !inShell && shellFenceOpenRe.MatchString(line):
				inShell = true
				blocks++
			case inShell && shellFenceCloseRe.MatchString(line):
				inShell = false
			case inShell:
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "#") {
					continue // a comment may describe a placeholder
				}
				if m := srctest.AnglePlaceholder.FindString(line); m != "" {
					rel, _ := filepath.Rel(root, path)
					t.Errorf("%s:%d: shell block carries the placeholder %q inside a command — "+
						"a shell reads <...> as redirections, so the line fails when pasted; "+
						"put the value in a variable above the command instead", rel, i+1, m)
				}
			}
		}
	}
	if blocks < 50 {
		t.Fatalf("only %d shell fences examined — the fence regex no longer matches the corpus", blocks)
	}
}

// docsMarkdownCorpus is the docs corpus every prose/shell tripwire in
// this package walks: docs/**/*.md (skipping the generated _build
// tree, __pycache__ and logos) plus the root README and CONTRIBUTING.
// One walker, so the corpus guard and the skip list cannot drift
// between the placeholder tripwire and its siblings.
func docsMarkdownCorpus(t *testing.T) []string {
	t.Helper()
	root := srctest.Root(t)
	var files []string
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "_build", "__pycache__", "logos":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, filepath.Join(root, "README.md"), filepath.Join(root, "CONTRIBUTING.md"))
	if len(files) < 30 {
		t.Fatalf("docs walk found only %d markdown files — the corpus guard tripped", len(files))
	}

	return files
}
