// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// oneLine folds a wrapped block back to the single line it was wrapped
// from: a newline and the indent that follows it read as one space.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// The deploy-checklist's step notes were printed as one line each — the
// longest is over four thousand characters — so `deploy-checklist
// --pending` on kate scrolled as a wall with no line returns (operator,
// 2026-11-03). Every prose line now wraps at the column width; a step's
// COMMAND line never wraps (it is copied into a shell), and the text is
// unchanged word for word.
func TestDeployChecklistProseWrapsAtTheColumnWidth(t *testing.T) {
	t.Setenv("COLUMNS", "")
	steps, ok := deployChecklistFor("0.29.89")
	if !ok {
		t.Fatal("0.29.89's checklist missing")
	}
	var longest int
	for _, s := range steps {
		longest = max(longest, len(s.desc))
	}
	if longest < 4*defaultProseWidth {
		t.Fatalf("the fixture must hold a note many times the width (the operator's wall); longest is %d", longest)
	}
	var out bytes.Buffer
	printChecklist(&out, "0.29.89", steps)
	assertWrapped(t, out.String(), defaultProseWidth, steps)
}

// COLUMNS is the one knob (SR-10: end to end, not an accessor): a wider
// terminal gets wider lines; an unusable value falls back to the default.
func TestDeployChecklistProseHonoursCOLUMNS(t *testing.T) {
	steps, _ := deployChecklistFor("0.29.89")
	for _, tc := range []struct {
		columns string
		width   int
	}{
		{"120", 120},
		{"abc", defaultProseWidth},
		{"0", defaultProseWidth},
		{"-5", defaultProseWidth},
		{" 100 ", defaultProseWidth},
	} {
		t.Run("COLUMNS="+tc.columns, func(t *testing.T) {
			t.Setenv("COLUMNS", tc.columns)
			var out bytes.Buffer
			printChecklist(&out, "0.29.89", steps)
			assertWrapped(t, out.String(), tc.width, steps)
			if tc.width > defaultProseWidth && !hasLineLongerThan(out.String(), defaultProseWidth) {
				t.Errorf("COLUMNS=%s must widen the prose past %d columns", tc.columns, defaultProseWidth)
			}
		})
	}
}

// assertWrapped: no prose line over width, every command line intact on
// one line, every note present word for word, a blank line between steps.
func assertWrapped(t *testing.T, out string, width int, steps []deployStep) {
	t.Helper()
	cmds := map[string]bool{}
	for _, s := range steps {
		cmds[s.cmd] = true
	}
	numbered := regexp.MustCompile(`^  \d+\. (.*)$`)
	var sawSteps int
	for _, line := range strings.Split(out, "\n") {
		if m := numbered.FindStringSubmatch(line); m != nil {
			sawSteps++
			if !cmds[m[1]] {
				t.Errorf("a step's command line must be the whole command, unwrapped: %q", line)
			}
			continue
		}
		if strings.HasPrefix(line, "===") {
			continue
		}
		// A line over the width holds exactly one word — a backticked
		// command, or a token with no space — which is never cut.
		if n := utf8.RuneCountInString(line); n > width && len(proseWords(line)) != 1 {
			t.Errorf("prose line of %d characters exceeds the width %d: %q", n, width, line)
		}
	}
	if sawSteps != len(steps) {
		t.Errorf("%d numbered command lines, want %d", sawSteps, len(steps))
	}
	folded := oneLine(out)
	for i, s := range steps {
		if !strings.Contains(folded, oneLine(s.desc)) {
			t.Errorf("step %d's note is not printed word for word", i+1)
		}
	}
	for i, s := range steps[:len(steps)-1] {
		// The next command line must be preceded by a blank line, so the
		// steps read as blocks rather than one run of text.
		next := "\n\n  " + strconv.Itoa(i+2) + ". " + steps[i+1].cmd + "\n"
		if !strings.Contains(out, next) {
			t.Errorf("step %d (%s) must be followed by a blank line before step %d", i+1, s.cmd, i+2)
		}
	}
}

// hasLineLongerThan reports a wrapped prose line (several words, so the
// wrapper chose its length) past width; a single long word does not count.
func hasLineLongerThan(s string, width int) bool {
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "===") || regexp.MustCompile(`^  \d+\. `).MatchString(line) {
			continue
		}
		if utf8.RuneCountInString(line) > width && len(proseWords(line)) > 1 {
			return true
		}
	}
	return false
}

// The once-only header of an accumulated range (several releases pending)
// is prose too, and wraps the same way; the range's migrate, which it
// names in backticks, is never split across lines — a backtick span is
// one word to the wrapper.
func TestAccumulatedHeaderWrapsAndKeepsCommandsWhole(t *testing.T) {
	t.Setenv("COLUMNS", "")
	list := deployStepsFrom("0.29.64", false, "0.29.69")
	if len(list) < 2 {
		t.Fatalf("the fixture needs several pending releases; got %d", len(list))
	}
	var out bytes.Buffer
	printStepBlocks(&out, list, "since the last acknowledged deploy (0.29.64)")
	s := out.String()
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(line, "  ") || strings.HasPrefix(line, "===") {
			continue
		}
		if n := utf8.RuneCountInString(line); n > defaultProseWidth {
			t.Errorf("header line of %d characters exceeds %d: %q", n, defaultProseWidth, line)
		}
		if strings.Count(line, "`")%2 == 1 {
			t.Errorf("a backticked command is split across lines: %q", line)
		}
	}
	if !strings.Contains(oneLine(s), "`aveloxis migrate --skip-views` (the migrate this range needs)") {
		t.Errorf("the header's words are unchanged:\n%s", s)
	}
}

// wrapProse's edges: an empty text prints nothing but the newline; a word
// longer than the width stands on its own line, never cut; a backtick span
// longer than the width likewise; an unclosed backtick is an ordinary
// character; runs of spaces fold to one; a newline in the text is a
// paragraph break that is kept.
func TestWrapProseEdges(t *testing.T) {
	t.Setenv("COLUMNS", "")
	for _, tc := range []struct {
		name, text string
		indent     string
		width      int
		want       string
	}{
		{"empty", "", "", 20, "\n"},
		{"fits", "one two three", "", 20, "one two three\n"},
		{"wraps on a word boundary", "one two three four five", "", 13, "one two three\nfour five\n"},
		{"indent counts toward the width", "one two three four five", "  ", 15, "  one two three\n  four five\n"},
		{"long word stands alone", "a bbbbbbbbbbbbbbbbbbbbbbbbb c", "", 10, "a\nbbbbbbbbbbbbbbbbbbbbbbbbb\nc\n"},
		{"backtick span is one word", "run `aveloxis migrate --skip-views` now", "", 20, "run\n`aveloxis migrate --skip-views`\nnow\n"},
		{"unclosed backtick is a character", "run `aveloxis migrate now", "", 12, "run\n`aveloxis\nmigrate now\n"},
		{"space runs fold", "one   two  three", "", 20, "one two three\n"},
		{"newline keeps a paragraph", "one two\n\nthree four", "", 20, "one two\n\nthree four\n"},
		{"width smaller than the indent still prints", "one two", "      ", 3, "      one\n      two\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			wrapProse(&out, tc.indent, tc.width, tc.text)
			if out.String() != tc.want {
				t.Errorf("wrapProse(%q, %d, %q):\n got %q\nwant %q", tc.indent, tc.width, tc.text, out.String(), tc.want)
			}
		})
	}
}

// The suite's width is pinned by the package (TestMain unsets COLUMNS):
// with it exported by a developer's shell or tmux, the printers rendered
// at that width and eleven asserts failed at COLUMNS=23 without naming
// COLUMNS (L10 round 1 F1).
func TestTheSuiteRendersProseAtTheDefaultWidth(t *testing.T) {
	if v, set := os.LookupEnv("COLUMNS"); set {
		t.Fatalf("COLUMNS=%q reached a test: TestMain must unset it", v)
	}
	if proseWidth() != defaultProseWidth {
		t.Fatalf("proseWidth() = %d under no COLUMNS, want %d", proseWidth(), defaultProseWidth)
	}
}

// Width is counted in characters: an em-dash is three bytes and one
// column, so a byte count wrapped lines with one early (L10 round 1 F4).
func TestWrapProseCountsCharactersNotBytes(t *testing.T) {
	var out bytes.Buffer
	wrapProse(&out, "", 5, "a — b")
	if out.String() != "a — b\n" {
		t.Errorf("a five-character line with an em-dash must fit a width of 5; got %q", out.String())
	}
}

// A command a note carries without backticks wraps like prose and no
// longer copies whole (L10 round 1 F2): the historical notes are frozen,
// so the rule is a ratchet on NEW text — a shell fragment in what a
// ladder ADDS from 0.29.136 on must sit in backticks. A ladder inherits
// its predecessors' steps verbatim and its start note prepended to the
// predecessor's (withStartNote; a ladder may be built from an older base,
// so every older ladder is a candidate predecessor), so the text a
// release adds is its steps no older ladder has, and its start note with
// the longest older start note stripped. The predicate is proven on the
// frozen notes: the hits it must still find are pinned (round 2 F3), so a
// weakened predicate fails here, not silently.
func TestNewLadderNotesKeepCommandsInBackticks(t *testing.T) {
	const ratchetFrom = "0.29.136"
	versions := make([]string, 0, len(deployChecklists))
	for v := range deployChecklists {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return !db.SchemaVersionAtLeast(versions[i], versions[j]) })
	bare := bareShellFragmentFinder(t)
	var examined int
	olderHits := map[string][]string{} // version → every fragment found in text it added
	for i, version := range versions {
		for _, s := range deployChecklists[version] {
			examined++
			fragments := bare(textAdded(s, versions[:i]))
			switch {
			case len(fragments) == 0:
			case db.SchemaVersionAtLeast(version, ratchetFrom):
				t.Errorf("ladder %s adds the shell fragment %q outside backticks (step %q): a new note keeps every command in backticks so the wrapper keeps it on one line", version, fragments[0], s.cmd)
			default:
				olderHits[version] = append(olderHits[version], fragments...)
			}
		}
	}
	srctest.MinCount(t, "ladder steps examined", examined, 100)
	// The frozen notes the predicate must find, one hit per arm it has
	// (round 3 R3-1: a dropped arm the corpus never exercised passed): the
	// subcommand oracle (0.29.77's "rerun aveloxis migrate --skip-views",
	// 0.29.89's signup-escrow), the expansions (0.29.88's "${PGHOST:?}",
	// 0.29.70's "$TOKEN"), the pipe (0.29.70), the conjunction (0.29.125's
	// "chmod 700 … && chmod 600"), the `>` arm (0.29.69's bare SQL
	// "HAVING count(*) > 1" — the only `>` the frozen notes carry outside
	// backticks; shell redirections are proven synthetically below), and
	// the programs (0.29.88's and 0.29.89's psql, 0.29.125's chmod,
	// 0.29.70's printf and shasum).
	for version, wants := range map[string][]string{
		"0.29.77":  {"aveloxis migrate"},
		"0.29.89":  {"aveloxis signup-escrow", "psql -", "${"},
		"0.29.88":  {"psql -", "${"},
		"0.29.70":  {"$TOKEN", "|", "printf '", "shasum -"},
		"0.29.125": {"chmod 7", "&&"},
		"0.29.69":  {">"},
	} {
		for _, want := range wants {
			if !slices.ContainsFunc(olderHits[version], func(f string) bool { return strings.HasPrefix(f, want) }) {
				t.Errorf("the predicate must find %s's frozen bare fragment %q; found %q — a weakened predicate proves nothing", version, want, olderHits[version])
			}
		}
	}
	if !db.SchemaVersionAtLeast(db.ToolVersion, ratchetFrom) {
		t.Fatalf("the ratchet starts at %s, after the binary's %s", ratchetFrom, db.ToolVersion)
	}
}

// textAdded is the part of step s a ladder did not inherit from any older
// ladder (round 2 F2: 0.29.109 was built from 0.29.102, not 0.29.108):
// nothing when an older ladder has the identical step, the description
// with the longest older same-command description stripped from its end
// when one is its suffix, else the whole description.
func textAdded(s deployStep, olderVersions []string) string {
	text := s.desc
	for _, v := range olderVersions {
		for _, p := range deployChecklists[v] {
			if p.cmd != s.cmd {
				continue
			}
			if p.desc == s.desc {
				return ""
			}
			if strings.HasSuffix(s.desc, p.desc) && len(s.desc)-len(p.desc) < len(text) {
				text = s.desc[:len(s.desc)-len(p.desc)]
			}
		}
	}
	return text
}

// bareShellFragmentFinder returns the predicate: every shell fragment
// outside backtick spans, in order. Two of its three parts are oracles
// (round 2 F1): an invocation of THIS binary is `aveloxis <subcommand>` for
// the subcommands and aliases the root command registers; shell grammar
// is the closed set of an expansion ($(, ${, $NAME), a pipe, a conjunction
// and a redirection. The third is the external programs the frozen notes
// have invoked bare — a list, because no oracle names a host's programs;
// the pinned hits above and TestBareShellFragmentFinderHasEveryArm keep it
// honest. A program whose name is also an English word (git, go, nginx,
// psql) counts only when a flag, path, quote, variable or number follows
// — the frozen notes say "git clones", "an nginx location" and "whose go
// line" sixteen times against seven real commands (round 3 R3-2); nohup,
// sudo, systemctl and chown have no English reading and count before any
// word;
// git and go also count before their own verbs (install, pull, clone …:
// the upgrade commands a new note is likeliest to carry bare; round 4 F3)
// but never before "log", which 0.29.71's note uses as English.
// A flag on its own (--since) is not a command. A subcommand named as a
// noun in prose ("aveloxis api refuses", six times in the frozen notes)
// fires too: a new note puts the binary's name in backticks.
func bareShellFragmentFinder(t *testing.T) func(string) []string {
	t.Helper()
	var subcommands []string
	for _, c := range newRootCmd().Commands() {
		for _, name := range append([]string{c.Name()}, c.Aliases...) {
			subcommands = append(subcommands, regexp.QuoteMeta(name))
		}
	}
	if len(subcommands) < 10 {
		t.Fatalf("the root command registers %d subcommands; the oracle is empty", len(subcommands))
	}
	spans := regexp.MustCompile("`[^`]*`")
	fragment := regexp.MustCompile(`\baveloxis (` + strings.Join(subcommands, "|") + `)\b` +
		`|\$\(|\$\{|\$[A-Za-z_]\w*|\s\|\s|\s&&\s|\s2?>>?\s|2>&1` +
		`|\b(psql|chmod|git|go|nginx|printf|shasum)\s+[-/~$"'0-9.]` +
		`|\b(nohup|sudo|systemctl|chown)\s+[-A-Za-z]` +
		`|\b(git|go)\s+(clone|pull|fetch|checkout|install|build|test|mod|get|run|generate|env)\b`)
	return func(desc string) []string {
		var out []string
		for _, f := range fragment.FindAllString(spans.ReplaceAllString(desc, " "), -1) {
			out = append(out, strings.TrimSpace(f))
		}
		return out
	}
}

// Every arm of the predicate, and every option inside an arm (round 3
// R3-1, round 4 F1: six in-arm mutants survived a note with one case per
// alternation), on a synthetic note: the frozen corpus exercises some
// arms only in command lines, which the ratchet does not scan, so a
// dropped arm passed it. Each fragment comes back in order; the English
// readings of the program names do not fire; a command alias counts;
// backticked commands are invisible.
func TestBareShellFragmentFinderHasEveryArm(t *testing.T) {
	bare := bareShellFragmentFinder(t)
	note := "run aveloxis migrate --skip-views, then aveloxis import-foundations once; set X=$(id -u) and ${PGHOST:?} and $TOKEN; " +
		"a | b; c && d; e > f; h >> i; j 2> k; g 2>&1; psql -h x; chmod 700 ~/.aveloxis; chown u f; git /tmp/r; go 1.25; nginx -t; " +
		"printf '%s'; shasum -a 256; git ~/r; psql \"$X\"; go ./x; git .; " +
		"nohup aveloxis serve; sudo -n true; systemctl reload nginx; go install ./cmd/aveloxis; git pull; git clone https://x"
	want := []string{"aveloxis migrate", "aveloxis import-foundations", "$(", "${", "$TOKEN", "|", "&&", ">", ">>", "2>", "2>&1",
		"psql -", "chmod 7", "chown u", "git /", "go 1", "nginx -", "printf '", "shasum -", "git ~", "psql \"", "$X", "go .", "git .",
		"nohup a", "sudo -", "systemctl r", "go install", "git pull", "git clone"}
	if got := bare(note); !slices.Equal(got, want) {
		t.Errorf("fragments of the synthetic note:\n got %q\nwant %q", got, want)
	}
	prose := "git clones the repository, the git endpoint, whose go line is newer, an nginx location, a bare psql connects, " +
		"the walk's git log, the rest go through, `aveloxis migrate --skip-views` and `psql -h x` in backticks, --since on its own"
	if got := bare(prose); len(got) != 0 {
		t.Errorf("prose with the program names as English words and backticked commands must yield nothing; got %q", got)
	}
}
