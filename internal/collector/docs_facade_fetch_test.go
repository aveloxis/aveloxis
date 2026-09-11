// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// facadeFetchRefspecs derives the refspecs the facade actually
// passes to `git fetch` from ensureClone's body — never a hand list,
// so a refspec change in the code moves the docs contract with it.
func facadeFetchRefspecs(t *testing.T) []string {
	t.Helper()
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/collector/facade.go"),
		"func (f *FacadeCollector) ensureClone("))
	re := regexp.MustCompile(`"(\+refs/[^"]+)"`)
	var specs []string
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		specs = append(specs, m[1])
	}
	if len(specs) < 2 {
		t.Fatalf("ensureClone names %d fetch refspecs; expected the heads and tags pair — "+
			"if the fetch was rewritten, this derivation needs the new shape", len(specs))
	}
	if !strings.Contains(body, `"--prune"`) {
		t.Fatal("ensureClone's fetch no longer passes --prune; the docs contract below assumes it")
	}
	return specs
}

// pruneFlagRe and allFlagRe match the flag as a whole token. A
// substring test let `--prune-tags` (a real git flag) satisfy the
// --prune requirement (round 17 L10 pass 6). The right edge accepts any
// non-word, non-hyphen byte, so a subshell `(cd … && git fetch --all)`
// still fires on `--all)`. The left edge is whitespace only: the
// command splitter has already cut at `&&`, so a flag never follows `(`.
var (
	pruneFlagRe = regexp.MustCompile(`(?:^|\s)--prune(?:$|[^\w-])`)
	allFlagRe   = regexp.MustCompile(`(?:^|\s)--all(?:$|[^\w-])`)
)

// shellCommandSepRe splits one logical shell command into the commands
// it chains. `||` precedes `|` so the alternation takes the longer
// operator first.
var shellCommandSepRe = regexp.MustCompile(`&&|\|\||;|\|`)

// fetchLineRe matches a `git fetch` command ANYWHERE on a shell line —
// after `cd … &&`, after `sudo -u …`, with or without `-C <path>`. The
// first draft anchored at line start and let all three of those escape
// (round 17, L10 pass 2).
var fetchLineRe = regexp.MustCompile(`\bgit\b(?:\s+(?:-C\s+\S+|--git-dir[= ]\S+|-c\s+\S+))*\s+fetch\b`)

// bareContextRe marks a shell block as describing the facade's bare
// clone: the clone flag, the doc's clone variable, the config knob, or
// the fleet's clone directory by name. A block walking an ordinary
// checkout (a contributor recipe) is a different contract.
var bareContextRe = regexp.MustCompile(`--bare|\$\{?CLONE_PATH\}?|repo_clone_dir|aveloxis-repos`)

// TestFetchPinRegexShapes pins both regexes against the escapes the
// second L10 pass planted — each must be judged — and against the
// contributor recipe that must not be.
func TestFetchPinRegexShapes(t *testing.T) {
	judged := []string{
		`git -C "$CLONE_PATH" fetch origin '+refs/heads/*:refs/heads/*' --prune`,
		`cd "$CLONE_PATH" && git fetch --all`,
		`sudo -u aveloxis git -C "$CLONE_PATH" fetch --all`,
		`cd /data/aveloxis-repos/augurlabs-augur.git && git fetch --all`,
		`git --git-dir="$CLONE_PATH" fetch --all`,
		`git --git-dir=$CLONE_PATH fetch --all`,
		`git -C "$CLONE_PATH" -c core.x=y fetch --all`,
		`cd "${CLONE_PATH}" && git fetch --all`,
	}
	for _, line := range judged {
		if !fetchLineRe.MatchString(line) {
			t.Errorf("fetchLineRe must match %q", line)
		}
		if !bareContextRe.MatchString(line) {
			t.Errorf("bareContextRe must mark %q as bare-clone context", line)
		}
	}
	for _, line := range []string{"git fetch upstream main", "git fetch --tags"} {
		if !fetchLineRe.MatchString(line) {
			t.Errorf("fetchLineRe must match %q (it is a fetch; the CONTEXT is what exempts it)", line)
		}
		if bareContextRe.MatchString(line) {
			t.Errorf("a contributor recipe line %q must not read as bare-clone context", line)
		}
	}
	for _, line := range []string{"git-lfs fetch", "gitfetch", "git log --all"} {
		if fetchLineRe.MatchString(line) {
			t.Errorf("fetchLineRe must not match %q", line)
		}
	}
}

// TestDocsFetchCommandMatchesTheFacade — PR #197 round 17 (Copilot
// round 7, findings 4+5). collection-pipeline.md told operators that
// subsequent runs do `git fetch --all`, and facade-commits.md said
// the same three times. The facade has never run that: `git clone
// --bare` writes no fetch refspec, so a plain `git fetch` (--all or
// not) downloads objects and never advances refs/heads/* — the clone
// stays permanently stale, which is exactly why ensureClone passes
// explicit refspecs. A doc that prescribes the stale form is worse
// than no doc. The contract, derived from the code:
//
//  1. every `git fetch` command inside a docs shell fence that describes
//     the BARE clone (any line of the block matches bareContextRe: the
//     clone flag, the CLONE_PATH variable in either spelling, the
//     repo_clone_dir knob, or the fleet's aveloxis-repos directory)
//     carries each refspec the facade passes plus --prune, and none
//     spells `--all` — a fence walking an ordinary checkout (`git fetch
//     upstream main` in a contributor recipe) is a different contract
//     and is deliberately not judged (round 17 L10 finding 3);
//  2. the facade architecture page names each refspec in prose.
func TestDocsFetchCommandMatchesTheFacade(t *testing.T) {
	root := srctest.Root(t)
	specs := facadeFetchRefspecs(t)

	fetchLines := 0
	for _, path := range docsMarkdownCorpus(t) {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, path)
		judged, problems := judgeBareFetch(string(src), specs)
		fetchLines += judged
		for _, p := range problems {
			t.Errorf("%s:%s", rel, p)
		}
	}
	if fetchLines < 1 {
		t.Fatal("no `git fetch` line found in any bare-clone docs shell fence — the corpus guard tripped")
	}

	arch := srctest.Read(t, "docs/architecture/facade-commits.md")
	for _, spec := range specs {
		if !strings.Contains(arch, spec) {
			t.Errorf("docs/architecture/facade-commits.md never names the refspec %q the facade fetches with", spec)
		}
	}
}

// judgeBareFetch applies the facade-fetch contract to one markdown
// source and returns how many fetch commands it judged plus one
// "<line>: <problem>" string per violation.
//
// Round 17 L10 pass 4: judging PHYSICAL lines was wrong for this
// corpus, which wraps long commands with a trailing backslash (59
// continuation lines across the shell fences) — and the facade's
// 101-character fetch is the most wrap-likely command in it. Wrapped
// after `fetch origin`, a CORRECT doc false-fired three times (the
// refspecs and --prune sat on the continuation); wrapped before
// `fetch`, a `--all` dropped out of judgment entirely. Continuations
// are therefore joined into one logical command, inside shell blocks
// only (a markdown hard break outside a fence must never swallow the
// fence opener that follows it), and a problem is reported at the
// command's FIRST physical line.
//
// RESIDUAL, pinned rather than described (round 17, the L16 exit). This
// is textual judgment, not a shell tokenizer, and this paragraph no
// longer tries to say in prose which spellings fall through it. FOUR
// drafts tried; every one was wrong, and every one was caught the same
// way — by running the real judge over the spellings it described:
// (1) "every gap can only fire loudly, never pass a stale-clone
// recipe" — false, several pass silently; (2) an operator inside `$(…)`
// filed as loud, when the placement ahead of `fetch` is silent;
// (3) "a separation between `git` and its `fetch` is never judged" —
// false, a `.git` path token directly before `fetch` re-matches
// `\bgit\b` and IS judged; (4) "quoting and escaped newlines are not
// parsed" — false of both, and refuted by this file's own fixtures
// (continuations ARE joined, and the comment strip DOES track quotes).
//
// So: the parsing steps are `judgeBareFetch` below, the regexes
// declared above it, the fence pair shellFenceOpenRe /
// shellFenceCloseRe in docs_shell_placeholders_test.go (which decides
// what is a shell block AT ALL, and carries its own CommonMark
// indentation contract), and the quote-aware comment strip
// srctest.StripShellComment in internal/srctest/strip.go. Draft (4)
// was wrong about one mechanism in each of two of those: continuations
// are joined HERE, quotes are tracked in strip.go. Read all four, not
// just the function under this comment. The shapes that fall through
// them are executable fact in
// TestFetchPinResidualShapes — some mis-parses fire, some drop a
// command out of judgment entirely, a trailing `--all` included. That
// test is an INVENTORY of current behaviour, not a wish list; a change
// that moves a row is a contract change to be reviewed, not a fixture
// to re-baseline.
//
// The tripwire guards the spellings the facade docs actually use;
// widening it is a contract change, not a fix.
func judgeBareFetch(src string, specs []string) (int, []string) {
	lines := strings.Split(src, "\n")
	// Pass 1: which shell blocks describe the bare clone. A block is
	// judged as a whole, so the clone line above the fetch line is
	// what marks the context.
	bareBlock := map[int]bool{} // opener line index → bare context
	inShell, opener := false, -1
	for i, line := range lines {
		switch {
		case !inShell && shellFenceOpenRe.MatchString(line):
			inShell, opener = true, i
		case inShell && shellFenceCloseRe.MatchString(line):
			inShell = false
		case inShell && bareContextRe.MatchString(line):
			bareBlock[opener] = true
		}
	}

	judged := 0
	var problems []string
	judgeOne := func(first int, cmd string) {
		if strings.HasPrefix(cmd, "#") || !fetchLineRe.MatchString(cmd) {
			return
		}
		judged++
		if allFlagRe.MatchString(cmd) {
			problems = append(problems, fmt.Sprintf("%d: shell block runs `git fetch --all` — a bare clone has no "+
				"fetch refspec, so that never advances refs/heads/*; mirror the facade's explicit refspecs instead", first))
		}
		for _, spec := range specs {
			if !strings.Contains(cmd, spec) {
				problems = append(problems, fmt.Sprintf("%d: shell block's fetch lacks the refspec %q the facade passes", first, spec))
			}
		}
		if !pruneFlagRe.MatchString(cmd) {
			problems = append(problems, fmt.Sprintf("%d: shell block's fetch lacks --prune, which the facade passes", first))
		}
	}

	// A logical command may chain several (`fetch … && git log --all`);
	// each piece is judged on its own, so a neighbour's `--all` cannot
	// fire on a correct fetch and two fetches cannot satisfy each
	// other's refspecs (round 17 L10 pass 5). Quoting is not parsed: an
	// operator inside quotes can only split a fetch from flags that sit
	// AFTER it in the same quotes — no doc spells a refspec that way.
	judge := func(first int, cmd string) {
		for _, piece := range shellCommandSepRe.Split(cmd, -1) {
			judgeOne(first, strings.TrimSpace(piece))
		}
	}

	// Pass 2: judge logical commands in bare-clone blocks.
	inShell, opener = false, -1
	var pending strings.Builder
	pendingFirst := 0
	flush := func() {
		if pending.Len() > 0 {
			judge(pendingFirst, strings.TrimSpace(pending.String()))
			pending.Reset()
		}
	}
	for i, line := range lines {
		switch {
		case !inShell && shellFenceOpenRe.MatchString(line):
			inShell, opener = true, i
		case inShell && shellFenceCloseRe.MatchString(line):
			flush()
			inShell = false
		case inShell && bareBlock[opener]:
			trimmed := strings.TrimSpace(line)
			// A comment line ends the command in progress, and its own
			// trailing backslash is NOT a continuation — the shell's
			// comment runs to end of line (round 17 L10 pass 5: joining
			// `# … \` onto the next line hid a `git fetch --all`).
			if strings.HasPrefix(trimmed, "#") {
				flush()
				continue
			}
			trimmed = strings.TrimSpace(srctest.StripShellComment(trimmed))
			if pending.Len() == 0 {
				pendingFirst = i + 1
			}
			if strings.HasSuffix(trimmed, "\\") {
				pending.WriteString(strings.TrimSuffix(trimmed, "\\"))
				pending.WriteString(" ")
				continue
			}
			pending.WriteString(trimmed)
			flush()
		}
	}
	return judged, problems
}

// TestFetchPinResidualShapes is the executable half of judgeBareFetch's
// RESIDUAL note: an INVENTORY of what the heuristic actually does with
// the spellings its gaps touch. FOUR prose drafts were each wrong in a
// new way (round 17 L10 passes 7-10 — every gap "fires loudly"; an
// operator inside `$(…)` filed as loud; "a separation is never judged";
// and a bare "what it parses" list that was wrong about both quoting
// and escaped newlines), so the claims live here, where `go test`
// checks them against the real judge instead of against a maintainer's
// model of it.
//
// A row that MOVES is a contract change — the tripwire's reach grew or
// shrank — and is reviewed as one (L16). Do not re-baseline a row to
// make a run green. Rows that read "silent" are known gaps, kept
// deliberately: this tripwire guards the spellings the facade docs use,
// and a doc that spells a fetch this way is not a shape any facade page
// has ever carried.
func TestFetchPinResidualShapes(t *testing.T) {
	specs := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}
	fence := "```"

	for _, c := range []struct {
		name   string
		cmd    string
		judged int
		fires  bool
	}{
		// Controls: the contract the tripwire exists to enforce.
		{"the facade's own command passes",
			`git -C "$CLONE_PATH" fetch origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune`, 1, false},
		{"a plain stale fetch fires",
			`git -C "$CLONE_PATH" fetch --all`, 1, true},

		// Judged, and fires — the `\bgit\b` rescue. fetchLineRe is not
		// anchored, so when an unrecognized global option breaks the match
		// at the first `git`, a `<name>.git` path token directly before
		// `fetch` re-matches and the command IS judged. Bare clones are
		// conventionally named that way, and the corpus spells them so.
		{"a .git path token before fetch re-matches",
			`git --no-pager -C /data/aveloxis-repos/augurlabs-augur.git fetch --all`, 1, true},

		// Judged, but SILENT — the refspec check is strings.Contains, so
		// refspec TEXT that is not a real argument satisfies it while the
		// fetch passes no refspec at all.
		{"refspecs inside a quoted option value satisfy the substring check",
			`git -C "$CLONE_PATH" fetch origin --upload-pack='+refs/heads/*:refs/heads/* +refs/tags/*:refs/tags/*' --prune`, 1, false},

		// Not judged at all — the `git` token is separated from its
		// `fetch` and no later token rescues the match. Each of these is a
		// stale-clone recipe that passes.
		{"an unrecognized global option drops the command",
			`git --bare fetch --all`, 0, false},
		{"an unrecognized global option before a quoted path drops it",
			`git --no-pager -C "$CLONE_PATH" fetch --all`, 0, false},
		{"a space inside a quoted -C path drops it",
			`git -C "$HOME/my repos/clone.git" fetch --all`, 0, false},
		{"a command separator inside $() ahead of fetch drops it",
			`git -C "$(cd "$X" && pwd)" fetch --all`, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			src := fence + "bash\n" +
				"git clone --bare \"$URL\" \"$CLONE_PATH\"\n" +
				c.cmd + "\n" +
				fence + "\n"
			judged, problems := judgeBareFetch(src, specs)
			if judged != c.judged {
				t.Errorf("judged=%d, want %d — the tripwire's REACH moved; that is a contract change, not a fixture to update\ncmd: %s\nproblems: %v",
					judged, c.judged, c.cmd, problems)
			}
			if fires := len(problems) > 0; fires != c.fires {
				t.Errorf("fires=%v, want %v — the tripwire's VERDICT moved; that is a contract change, not a fixture to update\ncmd: %s\nproblems: %v",
					fires, c.fires, c.cmd, problems)
			}
		})
	}
}

// TestJudgeBareFetchJoinsContinuations pins the two wrap shapes the
// pass-4 reviewer planted: a correct command wrapped after `fetch
// origin` must pass, and a `--all` wrapped before `fetch` must be
// judged and fire. A third fixture keeps the fence-boundary rule: a
// hard break in prose must not swallow the fence opener after it.
func TestJudgeBareFetchJoinsContinuations(t *testing.T) {
	specs := []string{"+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"}
	fence := "```"

	correctWrapped := fence + "bash\n" +
		"git clone --bare \"$URL\" \"$CLONE_PATH\"\n" +
		"git -C \"$CLONE_PATH\" fetch origin \\\n" +
		"    '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune\n" +
		fence + "\n"
	if judged, problems := judgeBareFetch(correctWrapped, specs); judged != 1 || len(problems) != 0 {
		t.Errorf("a correct fetch wrapped after `fetch origin` must be judged once and pass: judged=%d problems=%v", judged, problems)
	}

	allWrapped := fence + "bash\n" +
		"git clone --bare \"$URL\" \"$CLONE_PATH\"\n" +
		"git -C \"$CLONE_PATH\" \\\n" +
		"    fetch --all\n" +
		fence + "\n"
	judged, problems := judgeBareFetch(allWrapped, specs)
	if judged != 1 {
		t.Fatalf("a fetch wrapped before `fetch` must still be judged, judged=%d", judged)
	}
	if !strings.Contains(strings.Join(problems, "\n"), "3: shell block runs `git fetch --all`") {
		t.Errorf("the wrapped --all must fire at the command's first physical line (3), got: %v", problems)
	}

	// Pass 5 fixtures. Each is a shape the pass-4 judge got wrong or
	// left unprotected.
	block := func(body string) string {
		return fence + "bash\n" + "git clone --bare \"$URL\" \"$CLONE_PATH\"\n" + body + fence + "\n"
	}
	for _, tc := range []struct {
		name     string
		body     string
		judged   int
		mustFire string // "" = must pass clean
	}{
		{"wrapped-missing-prune",
			"git -C \"$CLONE_PATH\" fetch origin \\\n    '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*'\n",
			1, "3: shell block's fetch lacks --prune"},
		{"dangling-continuation-at-closer",
			"git -C \"$CLONE_PATH\" fetch --all \\\n",
			1, "3: shell block runs `git fetch --all`"},
		{"comment-with-trailing-backslash-does-not-hide-the-next-command",
			"# refresh (Windows: C:\\) \\\ngit -C \"$CLONE_PATH\" fetch --all\n",
			1, "4: shell block runs `git fetch --all`"},
		{"comment-mid-continuation-ends-the-command",
			"git -C \"$CLONE_PATH\" fetch origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune \\\n# note \\\n--all\n",
			1, ""},
		{"neighbouring-log-all-does-not-fire-on-a-correct-fetch",
			"git -C \"$CLONE_PATH\" fetch origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune && \\\n    git -C \"$CLONE_PATH\" log --all --oneline\n",
			1, ""},
		{"trailing-comment-mentioning-all-does-not-fire",
			"git -C \"$CLONE_PATH\" fetch origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune  # never fetch --all here\n",
			1, ""},
		{"trailing-comment-backslash-does-not-swallow-a-stale-fetch",
			"git -C \"$CLONE_PATH\" fetch origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune  # note \\\ngit -C \"$CLONE_PATH\" fetch --all\n",
			2, "4: shell block runs `git fetch --all`"},
		{"prune-tags-is-not-prune",
			"git -C \"$CLONE_PATH\" fetch origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune-tags\n",
			1, "3: shell block's fetch lacks --prune"},
		{"subshell-all-still-fires",
			"(cd \"$CLONE_PATH\" && git fetch --all)\n",
			1, "3: shell block runs `git fetch --all`"},
		// The quoted # sits BEFORE the refspecs, so treating it as a
		// comment would drop them and fire; only quote tracking keeps
		// these clean. One fixture per quote kind.
		{"single-quoted-hash-is-not-a-comment",
			"git -C \"$CLONE_PATH\" fetch --upload-pack='git-upload-pack # x' origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune\n",
			1, ""},
		{"double-quoted-hash-is-not-a-comment",
			"git -C \"$CLONE_PATH\" fetch --upload-pack=\"git-upload-pack # x\" origin '+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --prune\n",
			1, ""},
		{"two-fetches-cannot-satisfy-each-other",
			"git -C \"$CLONE_PATH\" fetch origin '+refs/heads/*:refs/heads/*' && git -C \"$CLONE_PATH\" fetch origin '+refs/tags/*:refs/tags/*' --prune\n",
			2, "3: shell block's fetch lacks --prune"},
	} {
		judged, problems := judgeBareFetch(block(tc.body), specs)
		joined := strings.Join(problems, "\n")
		if judged != tc.judged {
			t.Errorf("%s: judged=%d, want %d (problems: %v)", tc.name, judged, tc.judged, problems)
		}
		if tc.mustFire == "" && len(problems) != 0 {
			t.Errorf("%s: must pass clean, got: %v", tc.name, problems)
		}
		if tc.mustFire != "" && !strings.Contains(joined, tc.mustFire) {
			t.Errorf("%s: must report %q, got: %v", tc.name, tc.mustFire, problems)
		}
	}

	proseBreak := "A bare clone is refreshed like this: \\\n" +
		fence + "bash\n" +
		"git clone --bare \"$URL\" \"$CLONE_PATH\"\n" +
		"git -C \"$CLONE_PATH\" fetch --all\n" +
		fence + "\n"
	if judged, problems := judgeBareFetch(proseBreak, specs); judged != 1 || len(problems) == 0 {
		t.Errorf("a prose hard break must not swallow the fence opener after it: judged=%d problems=%v", judged, problems)
	}
}
