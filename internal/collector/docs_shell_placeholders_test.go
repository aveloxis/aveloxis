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

// anglePlaceholderRe matches a `<like this>` placeholder anywhere on a
// command line. In a shell, an unquoted `<x>` is an input redirection
// from a file named x plus an output redirection — a copy-pasted line
// either fails on a missing file or silently truncates one.
//
// Round 16 (Copilot round 6, finding 2): the first draft required a
// non-space character glued to the `<` AND a space inside the
// brackets, so it caught only multi-word placeholders attached to a
// command (`@v<primary version>`) and walked past the ordinary
// one-word shape — `psql -h <host>`, `perf-<previous>.txt`,
// `aveloxis sbom <repo-id>` — which breaks a pasted line just the
// same. Nineteen such sites were in the corpus. What separates a
// placeholder from real shell is now the CONTENT, not the glue: a
// placeholder is words (letters, digits, `_`, `-`) separated by
// single spaces, closed by `>` directly after a word. `cmd <in.txt
// >out.txt` has a dot and a space before `>`, `cat <<EOF` has a
// second `<`, `diff <(a) <(b)` has parens, `2<&1` an ampersand — none
// match. Both arms are pinned by TestAnglePlaceholderRegexShape.
var anglePlaceholderRe = regexp.MustCompile(`<[A-Za-z][A-Za-z0-9_-]*(?: [A-Za-z0-9_-]+)*>`)

// TestAnglePlaceholderRegexShape is the tripwire's own contract, both
// arms: every shape that broke a pasted line in the corpus must fire,
// and every legitimate use of `<` in a shell block must not — a
// broader regex that flags real redirections would be "fixed" by
// rewording working commands.
func TestAnglePlaceholderRegexShape(t *testing.T) {
	fires := []string{
		"psql -h <host> -p 5434 -U aveloxis",
		"diff perf-<previous>.txt perf-$(date +%Y%m%d).txt",
		"aveloxis sbom <repo-id>       # generate SBOMs",
		"go install github.com/aveloxis/aveloxis/cmd/aveloxis@v<primary version>",
		"  SELECT * FROM aveloxis_data.<flagged_table>",
		"  WHERE <pk> NOT IN (SELECT <pk> FROM x)",
		"scancode -clpi --json <output-file> --quiet <path>",
		"cd <checkout> && go install ./cmd/aveloxis",
		"aveloxis add-key [flags] [<token>]",
	}
	for _, line := range fires {
		if anglePlaceholderRe.FindString(line) == "" {
			t.Errorf("must flag the placeholder in %q", line)
		}
	}
	quiet := []string{
		"cmd <in.txt >out.txt",
		"sort <input.txt > sorted.txt",
		"cat <<EOF > aveloxis.json",
		"cat <<'EOF'",
		"diff <(aveloxis version) <(cat expected)",
		"some-command 2<&1",
		"echo $(<file)",
		"psql -c \"SELECT 1 <> 2\"",
		"go test ./... -run 'TestX' >/dev/null",
	}
	for _, line := range quiet {
		if m := anglePlaceholderRe.FindString(line); m != "" {
			t.Errorf("must NOT flag %q in the legitimate shell line %q", m, line)
		}
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
