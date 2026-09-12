// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import "strings"

// StripGoComments removes // line comments and /* */ block comments,
// aware of string, raw-string, and rune literals — a "//" inside a
// string survives. Used by pins that must not false-match on comments
// mentioning the very pattern they check for (the v0.21.5 lesson).
// Newlines inside removed comments are preserved so line-oriented
// scans keep their geometry.
func StripGoComments(src string) string {
	var b strings.Builder
	b.Grow(len(src))
	i := 0
	for i < len(src) {
		c := src[i]
		switch c {
		case '/':
			if i+1 < len(src) && src[i+1] == '/' {
				if nl := strings.IndexByte(src[i:], '\n'); nl >= 0 {
					i += nl
					continue
				}
				return b.String()
			}
			if i+1 < len(src) && src[i+1] == '*' {
				end := strings.Index(src[i+2:], "*/")
				if end < 0 {
					return b.String()
				}
				// v0.27.125: a newline-free inline block comment leaves one
				// space so adjacent tokens stay separated (func/*x*/f must
				// not become funcf); embedded newlines keep being emitted.
				wroteNL := false
				for _, r := range src[i : i+2+end+2] {
					if r == '\n' {
						b.WriteByte('\n')
						wroteNL = true
					}
				}
				if !wroteNL {
					b.WriteByte(' ')
				}
				i += 2 + end + 2
				continue
			}
			b.WriteByte(c)
			i++
		case '"', '\'':
			q := c
			b.WriteByte(c)
			i++
			for i < len(src) {
				b.WriteByte(src[i])
				if src[i] == '\\' && i+1 < len(src) {
					i++
					b.WriteByte(src[i])
					i++
					continue
				}
				if src[i] == q || src[i] == '\n' {
					i++
					break
				}
				i++
			}
		case '`':
			end := strings.IndexByte(src[i+1:], '`')
			if end < 0 {
				b.WriteString(src[i:])
				return b.String()
			}
			b.WriteString(src[i : i+1+end+1])
			i += 1 + end + 1
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// StripSQLComments removes -- line comments and /* */ block comments,
// aware of single-quoted SQL strings (a '--' inside a string
// survives). This is the single, fixture-tested home for the
// operation whose scattered ad-hoc copies caused the v0.27.89
// incident: a `);` inside a SQL comment truncating naive block
// extraction. Strip FIRST, extract structure SECOND.
func StripSQLComments(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			if nl := strings.IndexByte(sql[i:], '\n'); nl >= 0 {
				i += nl // keep the newline
			} else {
				return b.String()
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return b.String()
			}
			// v0.27.125: same token-separation contract as StripGoComments
			// (SELECT/*x*/FROM must not become SELECTFROM).
			wroteNL := false
			for _, r := range sql[i : i+2+end+2] {
				if r == '\n' {
					b.WriteByte('\n')
					wroteNL = true
				}
			}
			if !wroteNL {
				b.WriteByte(' ')
			}
			i += 2 + end + 2
		case c == '\'':
			b.WriteByte(c)
			i++
			for i < len(sql) {
				b.WriteByte(sql[i])
				if sql[i] == '\'' {
					// '' is an escaped quote inside the string.
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i++
						b.WriteByte(sql[i])
						i++
						continue
					}
					i++
					break
				}
				i++
			}
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// BacktickLiterals returns every backtick-delimited literal in src,
// backticks included — the SQL-in-Go extraction the flagship
// column-writer tripwire pioneered. DOCUMENTED BLIND SPOT: SQL built
// by string concatenation or fmt.Sprintf is invisible here (and to
// every consumer of this helper).
func BacktickLiterals(src string) []string {
	var out []string
	for {
		start := strings.IndexByte(src, '`')
		if start < 0 {
			return out
		}
		end := strings.IndexByte(src[start+1:], '`')
		if end < 0 {
			return out
		}
		out = append(out, src[start:start+1+end+1])
		src = src[start+1+end+1:]
	}
}

// StripShellComment drops an unquoted `#…` tail from ONE physical
// shell line and returns what precedes it. The shell ends a command at
// an unquoted `#` that starts a WORD — at line start, after a blank,
// or after a control operator — and a backslash inside that comment is
// not a line continuation, so callers judging docs shell fences run
// this before the continuation check. Single and double quotes, and a
// backslash escape outside single quotes, are tracked; nothing else is
// parsed (no `$(…)`, no heredocs). A `#` glued to a preceding word
// (`a#b`) is literal, as in the shell.
//
// Added for the facade-fetch docs tripwire (round 17 L10 pass 6). The
// operator half came from Copilot's PR #198 and closes a real bypass:
// `cmd;# note \` kept its trailing backslash, so the caller joined the
// NEXT line onto it — and once that caller split the joined command on
// `;`, the PIECE carrying the fetch began with `#` and was skipped, so
// a `git fetch --all` on that next line was never judged at all.
// Measured: the bypass existed for `;` and `|` (the separators the
// caller splits on) and not for `&`, `>` or `)`, which already fired.
// Reproduced end-to-end against the docs corpus before the fix.
func StripShellComment(line string) string {
	inSingle, inDouble := false, false
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '\\' && !inSingle:
			i++ // the escaped character is literal
		case c == '\'' && !inDouble:
			inSingle = !inSingle
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case c == '#' && !inSingle && !inDouble && (i == 0 || isShellWordBoundary(line[i-1])):
			return line[:i]
		}
	}
	return line
}

// isShellWordBoundary reports whether b can precede the `#` that opens
// a comment. POSIX (Shell Command Language, 2.3 Token Recognition)
// appends the current character to a word already in progress (rule 8)
// and starts a comment only where no word is in progress (rule 9) — so
// a comment can open at line start, after a blank, or after an
// operator. Every member below was checked against bash, and every one
// has a row in TestStripShellComment, so dropping any of them fails
// that table.
//
// Two characters that a naive reading would include are deliberately
// OUT, both erring toward under-stripping (the safe direction for a
// caller that judges docs commands — see StripShellComment's own "no
// `$(…)`" disclaimer):
//
//   - `)` ends a word only when it closes a SUBSHELL. Closing `$(…)`,
//     `$((…))` or `<(…)` it is part of the word, so bash keeps a
//     following `#` literal (`echo $(echo A)#c` prints `A#c`). Telling
//     the two apart needs `$(`/`<(` nesting depth, which this helper
//     does not track. Copilot's PR #198 included `)`; with it, a
//     CORRECT fetch line containing `$(…)#` was truncated and the
//     facade tripwire reported its refspecs as missing.
//   - an opening backquote does start a comment in bash (`echo X`+
//     "`#c`"+`Y` prints `XY`), but tracking backquote state is out of
//     scope here, and no facade doc spells one.
func isShellWordBoundary(b byte) bool {
	switch b {
	case ' ', '\t', ';', '&', '|', '(', '<', '>':
		return true
	default:
		return false
	}
}
