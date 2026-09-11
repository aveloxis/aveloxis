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
var shellFenceOpenRe = regexp.MustCompile("^```(?:bash|sh|shell|zsh|console)\\s*$")

// anglePlaceholderRe matches a `<like this>` placeholder glued to a
// command argument. In a shell, an unquoted `<x>` is an input
// redirection from a file named x plus an output redirection — a
// copy-pasted line either fails on a missing file or silently
// truncates one. The space inside the angle brackets is what
// distinguishes a placeholder from a legitimate `<file` redirection
// or a `<<EOF` heredoc.
var anglePlaceholderRe = regexp.MustCompile(`\S<[A-Za-z][^<>\n]* [^<>\n]*>`)

// TestDocsShellBlocksCarryNoAnglePlaceholders — PR #197 round 15
// (Copilot round 5). dedicated-scancode-host.md told operators to run
// `go install ...@v<primary version>` verbatim: the shell parses the
// angle brackets as redirections, the install fails on a file named
// "primary", and the page's own next sentence says the WRONG version
// on this host reaches production. A placeholder that needs editing
// belongs in a variable assignment above the command, not inside it.
func TestDocsShellBlocksCarryNoAnglePlaceholders(t *testing.T) {
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
			case inShell && strings.HasPrefix(line, "```"):
				inShell = false
			case inShell:
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "#") {
					continue // a comment may describe a placeholder
				}
				if m := anglePlaceholderRe.FindString(line); m != "" {
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
