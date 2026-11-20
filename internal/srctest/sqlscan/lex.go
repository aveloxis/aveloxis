// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// A tokeniser for PostgreSQL's lexical grammar (0.29.212; the manual's
// "Lexical Structure"). The write-policy check reads statements through it
// and through nothing else, so that a name is a name however it is spelled
// and a word inside a string or a comment is not a word.

type tokKind int

const (
	tWord   tokKind = iota // an unquoted identifier or key word; val is lower case
	tQuoted                // "a quoted identifier"; val is the name it names
	tString                // a string constant of any spelling; val is its value
	tNumber
	tParam // $1
	tOp    // an operator, :: and :=
	tPunct // ( ) [ ] , ; . :
)

type sqlTok struct {
	kind tokKind
	raw  string
	val  string
	pos  int
	end  int
	// depth is the nesting of ( and [ the token stands at; a bracket has
	// the depth of what is outside it.
	depth int
	// dollar: a dollar-quoted string, whose text may be a body of SQL.
	dollar bool
}

// is reports whether the token is the unquoted word w (lower case).
func (t sqlTok) is(w string) bool { return t.kind == tWord && t.val == w }

// isName reports whether the token is an identifier, quoted or not.
func (t sqlTok) isName() bool { return t.kind == tWord || t.kind == tQuoted }

func (t sqlTok) isPunct(p string) bool { return t.kind == tPunct && t.raw == p }

func identStart(c byte) bool {
	return c == '_' || c >= 0x80 || (c|0x20 >= 'a' && c|0x20 <= 'z')
}

func identPart(c byte) bool { return identStart(c) || c == '$' || (c >= '0' && c <= '9') }

const opChars = "+-*/<>=~!@#%^&|`?"

// lexSQL is sql as tokens, comments and white space dropped. It fails on
// text that does not end where it should (an open string, comment, quoted
// name or bracket) and on a character PostgreSQL has no token for.
func lexSQL(sql string) ([]sqlTok, error) {
	var out []sqlTok
	depth := 0
	n := len(sql)
	emit := func(k tokKind, from, to int, val string) *sqlTok {
		out = append(out, sqlTok{kind: k, raw: sql[from:to], val: val, pos: from, end: to, depth: depth})
		return &out[len(out)-1]
	}
	// quoted reads a body closed by q, where q doubled stands for itself
	// and, when backslash is set, a backslash escapes the next character.
	quoted := func(i int, q byte, backslash bool) (string, int, error) {
		var b strings.Builder
		for i < n {
			c := sql[i]
			switch {
			case backslash && c == '\\' && i+1 < n:
				b.WriteByte(c)
				b.WriteByte(sql[i+1])
				i += 2
			case c == q && i+1 < n && sql[i+1] == q:
				b.WriteByte(q)
				i += 2
			case c == q:
				return b.String(), i + 1, nil
			default:
				b.WriteByte(c)
				i++
			}
		}
		return "", i, fmt.Errorf("a %c is not closed", q)
	}
	for i := 0; i < n; {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			level, j := 1, i+2
			for j < n && level > 0 {
				switch {
				case sql[j] == '/' && j+1 < n && sql[j+1] == '*':
					level++
					j += 2
				case sql[j] == '*' && j+1 < n && sql[j+1] == '/':
					level--
					j += 2
				default:
					j++
				}
			}
			if level > 0 {
				return nil, fmt.Errorf("a /* comment is not closed")
			}
			i = j
		case c == '\'':
			v, j, err := quoted(i+1, '\'', false)
			if err != nil {
				return nil, err
			}
			emit(tString, i, j, v)
			i = j
		case c == '"':
			v, j, err := quoted(i+1, '"', false)
			if err != nil {
				return nil, err
			}
			if v == "" {
				return nil, fmt.Errorf("an empty quoted name")
			}
			emit(tQuoted, i, j, v)
			i = j
		case (c|0x20 == 'e' || c|0x20 == 'b' || c|0x20 == 'x' || c|0x20 == 'n') && i+1 < n && sql[i+1] == '\'':
			v, j, err := quoted(i+2, '\'', c|0x20 == 'e')
			if err != nil {
				return nil, err
			}
			emit(tString, i, j, v)
			i = j
		case c|0x20 == 'u' && i+2 < n && sql[i+1] == '&' && (sql[i+2] == '\'' || sql[i+2] == '"'):
			q := sql[i+2]
			v, j, err := quoted(i+3, q, false)
			if err != nil {
				return nil, err
			}
			// UESCAPE names another escape character. Nothing here needs
			// it, so text that uses it is not read.
			k := j
			for k < n && (sql[k] == ' ' || sql[k] == '\n' || sql[k] == '\t' || sql[k] == '\r') {
				k++
			}
			if k+7 <= n && strings.EqualFold(sql[k:k+7], "uescape") {
				return nil, fmt.Errorf("a Unicode-escaped name or string with UESCAPE is not read")
			}
			v, err = unicodeEscapes(v)
			if err != nil {
				return nil, err
			}
			if q == '"' {
				emit(tQuoted, i, j, v)
			} else {
				emit(tString, i, j, v)
			}
			i = j
		case c == '$' && i+1 < n && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i + 1
			for j < n && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			emit(tParam, i, j, sql[i:j])
			i = j
		case c == '$':
			// $tag$ ... $tag$, the tag empty or an identifier without $.
			j := i + 1
			for j < n && identPart(sql[j]) && sql[j] != '$' {
				j++
			}
			if j >= n || sql[j] != '$' || (j > i+1 && !identStart(sql[i+1])) {
				return nil, fmt.Errorf("a $ that begins neither a parameter nor a dollar-quoted string")
			}
			tag := sql[i : j+1]
			closeAt := strings.Index(sql[j+1:], tag)
			if closeAt < 0 {
				return nil, fmt.Errorf("the dollar-quoted string %s is not closed", tag)
			}
			body := sql[j+1 : j+1+closeAt]
			end := j + 1 + closeAt + len(tag)
			emit(tString, i, end, body).dollar = true
			i = end
		case identStart(c):
			j := i + 1
			for j < n && identPart(sql[j]) {
				j++
			}
			emit(tWord, i, j, strings.ToLower(sql[i:j]))
			i = j
		case c >= '0' && c <= '9', c == '.' && i+1 < n && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i + 1
			for j < n {
				d := sql[j]
				if (d >= '0' && d <= '9') || d == '_' || (d == '.' && !(j+1 < n && sql[j+1] == '.')) ||
					((d|0x20 == 'e') && j+1 < n && (sql[j+1] == '+' || sql[j+1] == '-' || (sql[j+1] >= '0' && sql[j+1] <= '9'))) ||
					((sql[j-1]|0x20 == 'e') && (d == '+' || d == '-')) {
					j++
					continue
				}
				break
			}
			emit(tNumber, i, j, sql[i:j])
			i = j
		case c == '(' || c == '[':
			emit(tPunct, i, i+1, "")
			depth++
			i++
		case c == ')' || c == ']':
			if depth == 0 {
				return nil, fmt.Errorf("a %c closes nothing", c)
			}
			depth--
			emit(tPunct, i, i+1, "")
			i++
		case c == ',' || c == ';' || c == '.':
			emit(tPunct, i, i+1, "")
			i++
		case c == ':':
			if i+1 < n && (sql[i+1] == ':' || sql[i+1] == '=') {
				emit(tOp, i, i+2, "")
				i += 2
			} else {
				emit(tPunct, i, i+1, "")
				i++
			}
		case strings.IndexByte(opChars, c) >= 0:
			j := i + 1
			for j < n && strings.IndexByte(opChars, sql[j]) >= 0 {
				// An operator ends where a comment begins.
				if (sql[j] == '-' && j+1 < n && sql[j+1] == '-') || (sql[j] == '/' && j+1 < n && sql[j+1] == '*') {
					break
				}
				j++
			}
			emit(tOp, i, j, "")
			i = j
		default:
			r, _ := utf8.DecodeRuneInString(sql[i:])
			return nil, fmt.Errorf("the character %q is not SQL", r)
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("a bracket is not closed")
	}
	return out, nil
}

// unicodeEscapes resolves \XXXX, \+XXXXXX and \\ in the body of a U& name
// or string.
func unicodeEscapes(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			i++
			continue
		}
		switch {
		case i+1 < len(s) && s[i+1] == '\\':
			b.WriteByte('\\')
			i += 2
		case i+1 < len(s) && s[i+1] == '+' && i+8 <= len(s):
			r, err := strconv.ParseUint(s[i+2:i+8], 16, 32)
			if err != nil {
				return "", fmt.Errorf("a Unicode escape is not six hexadecimal digits")
			}
			b.WriteRune(rune(r))
			i += 8
		case i+5 <= len(s):
			r, err := strconv.ParseUint(s[i+1:i+5], 16, 32)
			if err != nil {
				return "", fmt.Errorf("a Unicode escape is not four hexadecimal digits")
			}
			b.WriteRune(rune(r))
			i += 5
		default:
			return "", fmt.Errorf("a Unicode escape is cut short")
		}
	}
	return b.String(), nil
}
