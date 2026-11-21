// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// gemLogicalLinesByPatterns is the round-6 splitter, kept as the statement
// of what a split is: at each `;`, the three patterns are asked about the
// whole head, and the rest is read again. It is quadratic (round 7) and is
// used by nothing but this comparison.
func gemLogicalLinesByPatterns(line string) []string {
	literals := quotedLiteral.FindAllString(line, -1)
	n := -1
	masked := quotedLiteral.ReplaceAllStringFunc(line, func(string) string { n++; return `"` + quotedMark + strconv.Itoa(n) + `"` })
	if i := strings.Index(masked, "#"); i >= 0 {
		masked = masked[:i]
	}
	restore := func(piece string) string {
		return maskedLiteral.ReplaceAllStringFunc(piece, func(mark string) string {
			k, err := strconv.Atoi(mark[1+len(quotedMark) : len(mark)-1])
			if err != nil || k >= len(literals) {
				return mark
			}
			return literals[k]
		})
	}
	for i := 0; i < len(masked); i++ {
		if masked[i] != ';' {
			continue
		}
		head, rest := strings.TrimSpace(masked[:i]), strings.TrimSpace(masked[i+1:])
		if head == "" || rest == "" {
			continue
		}
		if gemOneLine.MatchString(head) || gemOneLineBraces.MatchString(head) || gemPlainDeclaration.MatchString(head) {
			return append([]string{restore(head)}, gemLogicalLinesByPatterns(restore(rest))...)
		}
	}
	return []string{line}
}

// The one-pass splitter gives what the patterns give, on lines built from
// every word the patterns look for.
func TestGemLogicalLinesAnswersAsThePatternsDo(t *testing.T) {
	words := []string{
		"gem", "gem 'a'", "gem 'b', '1.0'", "gem(", "gems", "end", "end", "ends", "do", "do", "then", "dot", "if", "unless",
		"case", "when", "else", "elsif", "while", "until", "group", "group :test", "platforms", "platform", "path 'x'", "git",
		"github", "source", "install_if", "env", "x", "y", ";", ";", ";", ";", " ", " ", "  ", "\t", "|", "|", "|a|", "{", "}",
		"{", "}", "'", "\"", "'q;q'", "\"z # z\"", "#", ":test", ",", "(", ")", "\u00a0", "\u00e9", "\v", "\x00",
	}
	seps := []string{" ", " ", " ", "", ";", "; ", " ;"}
	rng := rand.New(rand.NewSource(7))
	cases := 0
	for ; cases < gemLineCases; cases++ {
		var b strings.Builder
		for k := rng.Intn(14); k >= 0; k-- {
			b.WriteString(words[rng.Intn(len(words))])
			b.WriteString(seps[rng.Intn(len(seps))])
		}
		line := strings.TrimSpace(b.String())
		got, want := gemLogicalLines(line), gemLogicalLinesByPatterns(line)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("line %q:\n one pass: %q\n patterns: %q", line, got, want)
		}
	}
	for _, line := range []string{
		"", ";", ";;", "gem 'a';", "gem 'a'; gem 'b'", "gem 'a' ; gem 'b' ;gem 'c'", "group :test do gem 'ee', '1.0' end; gem 'ff', '2.0'",
		"group :x do; gem 'a'; end", "group :x do; gem 'a'; end; gem 'b'", "group(:t) { gem 'a'; gem 'b' }; gem 'c'",
		"if x then gem 'a' end if y; gem 'b'", "group :x do |g| gem 'a' end; gem 'b'", "do |x do gem y| gem z end; gem 'q'",
		"group do |a| x; gem 'a' end; gem 'b'", "gem 'a', b: {c: 1}; gem 'd'", "gem 'a' # c; gem 'b'", "gem 'a;b'; gem \"c;d\"",
		"env x { |v| gem 'a' }; gem 'b'", "if a then gem 'x' end end; gem 'y'", "case X when 'a' then gem 'x' when 'b' then gem 'y' end; gem 'z'",
		// An opener inside another's |...| has its gem first, and the
		// block ends between the two gems.
		"group do |x do gem y end if| gem z; gem 'q'", "group do |x do gem y end| gem z; gem 'q'", "group do |x then gem y end unless a| z; gem 'q'",
		// The `end` stands directly after the gem.
		"group do gem end if y; gem 'b'", "group do gem end; gem 'b'", "if a then gem;end if b; gem 'c'", "group do gem  end until x; gem 'b'",
	} {
		if got, want := gemLogicalLines(line), gemLogicalLinesByPatterns(line); !reflect.DeepEqual(got, want) {
			t.Errorf("line %q:\n one pass: %q\n patterns: %q", line, got, want)
		}
	}
}

// A Gemfile line of many `;` is read in time that grows with its length
// (round 7: 100 KB on one line took over a minute, in the square).
func TestAGemfileLineOfManyStatementsIsReadInLinearTime(t *testing.T) {
	for _, unit := range []string{
		"gem 'a';", "git+https://x/y;-r  -", "if do gem end q;", "group :t do gem 'a' end;", "group do x end if y;", "env { |z| gem 'a' };",
		"group :t do gem 'a' end ;q;", "do |", "x;", "gem a {;", "if then gem a end if b;",
	} {
		read := func(n int) time.Duration {
			path := filepath.Join(t.TempDir(), "Gemfile")
			if err := os.WriteFile(path, []byte(strings.Repeat(unit, n/len(unit))+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			done := make(chan time.Duration, 1)
			go func() {
				start := time.Now()
				ParseGemfileVersions(path)
				done <- time.Since(start)
			}()
			select {
			case d := <-done:
				return d
			case <-time.After(20 * time.Second):
				t.Fatalf("%q repeated to %d bytes on one line was not read in 20 s", unit, n)
				return 0
			}
		}
		if raceEnabled {
			// Time says nothing under the detector (scaling_test.go), and
			// the megabyte read costs it a minute over these units: the
			// unit is read once, for what the detector is there to see.
			read(64 << 10)
			continue
		}
		small, large := read(256<<10), read(1<<20)
		t.Logf("%-28q 256 KB %v, 1 MB %v", unit, small, large)
		if small > 50*time.Millisecond && large > 8*small {
			t.Errorf("%q: 1 MB took %v, more than 8 times the %v of 256 KB", unit, large, small)
		}
	}
}

// `gem 'a'; gem 'b'` on one line is two declarations, as it is in Ruby.
func TestTwoGemsOnOneLineAreTwoRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Gemfile")
	if err := os.WriteFile(path, []byte("gem 'a', '1.0'; gem 'b', '2.0'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, d := range ParseGemfileVersions(path) {
		got[d.Name] = d.Version
	}
	if want := map[string]string{"a": "1.0", "b": "2.0"}; !reflect.DeepEqual(got, want) {
		t.Errorf("read %v, want %v", got, want)
	}
}
