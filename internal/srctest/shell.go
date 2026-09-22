// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import "regexp"

// AnglePlaceholder matches a `<like this>` placeholder anywhere on a
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
// Shared (v0.29.57, Copilot on PR #210) so the docs' shell fences and the
// commands `aveloxis deploy-checklist` prints are judged by one rule.
var AnglePlaceholder = regexp.MustCompile(`<[A-Za-z][A-Za-z0-9_-]*(?: [A-Za-z0-9_-]+)*>`)
