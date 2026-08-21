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
				for _, r := range src[i : i+2+end+2] {
					if r == '\n' {
						b.WriteByte('\n')
					}
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
			for _, r := range sql[i : i+2+end+2] {
				if r == '\n' {
					b.WriteByte('\n')
				}
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
