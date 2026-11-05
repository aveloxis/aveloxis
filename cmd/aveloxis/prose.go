// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Prose the CLI prints for an operator to read — the deploy checklist's
// step notes (the longest is over four thousand characters), the
// accumulated range's once-only header, the start gate's refusals — wraps
// at the column width (0.29.136; `deploy-checklist --pending` on kate
// scrolled as one line per note with no line returns). Commands are never
// wrapped: a step's command line is copied into a shell, and a backticked
// command inside prose stays on one line.

package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// defaultProseWidth is the column count assumed when COLUMNS does not say:
// the width every terminal has at least, and the one fold(1) and the
// POSIX terminal default share.
const defaultProseWidth = 80

// proseWidth is the column width the prose wraps at: COLUMNS when it is a
// positive integer (the shell variable, exported, or set for the one
// command: `COLUMNS=120 aveloxis deploy-checklist --pending`), else the
// default. Anything else in COLUMNS — empty, text, zero, negative,
// padded — is the default; the value is read as the shell sets it, not
// trimmed.
func proseWidth() int {
	if n, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && n > 0 {
		return n
	}
	return defaultProseWidth
}

// prose prints one formatted paragraph wrapped at the column width.
func prose(out io.Writer, format string, args ...any) {
	wrapProse(out, "", proseWidth(), fmt.Sprintf(format, args...))
}

// wrapProse prints text wrapped at width columns, every line prefixed by
// indent (which counts toward the width). Words are separated by runs of
// spaces, which fold to one; a backtick span (`aveloxis migrate
// --skip-views`) is one word, so a command is never split across lines.
// A word longer than the width stands alone on its line, never cut. A
// newline in the text is a paragraph break and is kept; an empty text
// prints one empty line. Width is measured in characters (runes), not
// bytes: an em-dash is three bytes and one column (L10 round 1 F4).
func wrapProse(out io.Writer, indent string, width int, text string) {
	for _, para := range strings.Split(text, "\n") {
		words := proseWords(para)
		if len(words) == 0 {
			fmt.Fprintln(out)
			continue
		}
		line := indent + words[0]
		for _, w := range words[1:] {
			if utf8.RuneCountInString(line)+1+utf8.RuneCountInString(w) > width {
				fmt.Fprintln(out, line)
				line = indent + w
				continue
			}
			line += " " + w
		}
		fmt.Fprintln(out, line)
	}
}

// proseWords splits a paragraph into the units wrapProse places: its
// space-separated fields, with a field that opens a backtick span (an odd
// number of backticks) joined to the fields up to the one that closes it
// (the next with an odd number). A span nothing closes is ordinary words.
func proseWords(para string) []string {
	fields := strings.Fields(para)
	var words []string
	for i := 0; i < len(fields); i++ {
		if j := spanCloseAt(fields, i); j > i {
			words = append(words, strings.Join(fields[i:j+1], " "))
			i = j
			continue
		}
		words = append(words, fields[i])
	}
	return words
}

// spanCloseAt is the index of the field closing a backtick span that
// fields[i] opens, or -1 when fields[i] opens none or nothing closes it.
func spanCloseAt(fields []string, i int) int {
	if strings.Count(fields[i], "`")%2 == 0 {
		return -1
	}
	for j := i + 1; j < len(fields); j++ {
		if strings.Count(fields[j], "`")%2 == 1 {
			return j
		}
	}
	return -1
}
