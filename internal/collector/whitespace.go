// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.105 — whitespace measurement (Workstream C of the 2026-08-19
// fill audit). commits.cmt_whitespace had been 0% populated for the
// product's life while SUM(cmt_whitespace) feeds six live aggregate
// queries (internal/db/aggregates.go) — a surfaced metric reporting a
// structural zero. Operator decision 2026-08-19: implement the
// measurement (Augur-parity semantics) with a SEPARATE bulk-rewalk
// command for history plus an incremental per-cycle facade phase.
//
// ALGORITHM (Augur parity — pinned from analyzecommit.py in the
// operator's Augur checkout): parse unified-diff patch text per file;
//   - an added line whose content strips to empty → whitespace++
//     (NOT counted as added);
//   - an added line whose stripped content equals a JUST-REMOVED
//     line's stripped content AND is longer than 8 chars → a
//     whitespace-only reformat: removed--, whitespace++ (not added);
//   - otherwise added++.
//
// Consequence: cmt_added/cmt_removed on updated rows become the
// whitespace-ADJUSTED counts (Augur's numbers), superseding the raw
// numstat counts the facade's first pass stored. Dashboard commit
// counts (COUNT DISTINCT hash) are unaffected; dm_ sums shift at the
// next aggregate rebuild — expected and correct.
//
// FILENAME JOIN (the load-bearing detail): cmt_filename stores the RAW
// numstat path — for renames that is git's arrow/brace form
// ("dir/{old => new}/file"), which no patch-derived path reproduces.
// The walker therefore runs `git log --numstat -p` and pairs each
// patch section POSITIONALLY with the commit's numstat block (both
// follow the same diff ordering), keying every stat by the numstat
// name so the UPDATE join always hits the stored row.

package collector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
)

// whitespaceFileStat is one file's adjusted counters within a commit.
type whitespaceFileStat struct {
	Filename   string
	Added      int
	Removed    int
	Whitespace int
}

// whitespaceCommit is one commit's worth of per-file stats.
type whitespaceCommit struct {
	Hash  string
	Files []whitespaceFileStat
}

// whitespaceLineCap bounds how much of a single line is retained for
// the reformat-match comparison. Minified/mega lines beyond the cap
// compare by prefix only — a theoretical false reformat-match needs
// two >64KB lines identical in their first 64KB, which is noise.
// Bounding here (instead of a Scanner buffer) means arbitrarily long
// lines can never abort the walk with "token too long".
const whitespaceLineCap = 64 * 1024

// readCappedLine reads one \n-terminated line, retaining at most
// whitespaceLineCap bytes. v0.27.106 (PR #184 review): built on
// ReadSlice so the retained prefix is the ONLY allocation — ReadString
// would materialize the whole physical line first, letting one huge
// generated/minified line allocate hundreds of MB per walker (kate is
// an OOM-sensitive host). Overflow fragments are consumed and dropped.
func readCappedLine(r *bufio.Reader) (string, error) {
	var kept []byte
	for {
		frag, err := r.ReadSlice('\n')
		if len(frag) > 0 && len(kept) < whitespaceLineCap {
			take := frag
			if len(kept)+len(take) > whitespaceLineCap {
				take = take[:whitespaceLineCap-len(kept)]
			}
			kept = append(kept, take...)
		}
		switch err {
		case nil:
			return strings.TrimRight(string(kept), "\n"), nil
		case bufio.ErrBufferFull:
			continue // long line — keep consuming fragments
		default:
			if len(kept) > 0 || err == io.EOF {
				return strings.TrimRight(string(kept), "\n"), err
			}
			return "", err
		}
	}
}

// parseWhitespaceLog parses `git log --format=%x1e%H --numstat -p`
// output, emitting one whitespaceCommit per commit. Merge commits emit
// no numstat/patch by default and naturally produce zero files —
// matching the numstat pass, which stores no rows for them either.
func parseWhitespaceLog(r io.Reader, emit func(whitespaceCommit) error) error {
	br := bufio.NewReaderSize(r, 1<<20)

	var (
		cur          *whitespaceCommit
		numstatNames []string
		fileIdx      = -1
		curFile      *whitespaceFileStat
		inHunk       bool
		resetRemoval bool
	)
	// wsCheck is an occurrence-count MULTISET of the current removal
	// run's stripped lines (v0.27.113, Copilot round 9): the original
	// []string linear scan made a replacement block O(N×M) — a
	// 500K-line generated-file rewrite cost ~10^11 string comparisons
	// on one fleet worker. Entries are only ever matched by exact
	// content equality, so counts are semantically identical to the
	// list (consuming "any one" occurrence ≡ consuming "the first").
	wsCheck := make(map[string]int)

	flushFile := func() {
		if cur != nil && curFile != nil {
			cur.Files = append(cur.Files, *curFile)
		}
		curFile = nil
		inHunk = false
		clear(wsCheck)
		resetRemoval = true
	}
	flushCommit := func() error {
		flushFile()
		if cur == nil {
			return nil
		}
		c := *cur
		cur = nil
		return emit(c)
	}

	for {
		line, readErr := readCappedLine(br)
		if readErr != nil && line == "" {
			if readErr == io.EOF {
				return flushCommit()
			}
			return readErr
		}

		switch {
		case strings.HasPrefix(line, "\x1e"):
			if err := flushCommit(); err != nil {
				return err
			}
			cur = &whitespaceCommit{Hash: strings.TrimSpace(line[1:])}
			numstatNames = numstatNames[:0]
			fileIdx = -1

		case cur == nil:
			// Preamble noise before the first commit — skip.

		case strings.HasPrefix(line, "diff --git "):
			flushFile()
			fileIdx++
			name := ""
			if fileIdx < len(numstatNames) {
				name = numstatNames[fileIdx]
			} else if i := strings.Index(line, " b/"); i >= 0 {
				// Positional pairing should always hold (numstat and
				// patch follow the same diff order); the b-side path is
				// a last-resort fallback so a surprise never drops data
				// silently.
				name = line[i+3:]
			}
			curFile = &whitespaceFileStat{Filename: name}

		case curFile == nil:
			// Between the commit header and the first diff section:
			// the numstat block. Lines are "N\tN\tpath" or "-\t-\tpath".
			if parts := strings.SplitN(line, "\t", 3); len(parts) == 3 {
				numstatNames = append(numstatNames, parts[2])
			}

		case strings.HasPrefix(line, "@@"):
			inHunk = true

		case !inHunk:
			// Per-file headers (---/+++/index/rename/mode/Binary...).

		case strings.HasPrefix(line, "+"):
			content := strings.TrimSpace(line[1:])
			switch {
			case content == "":
				curFile.Whitespace++
			default:
				// v0.27.117: RUNE count, not byte length — Augur's
				// Python len() counts code points, so a short
				// non-ASCII line (e.g. 3 CJK chars = 9 bytes) must
				// NOT pass the >8 guard here when it wouldn't there.
				if utf8.RuneCountInString(content) > 8 && wsCheck[content] > 0 {
					curFile.Removed--
					curFile.Whitespace++
					wsCheck[content]--
				} else {
					curFile.Added++
				}
			}
			resetRemoval = true

		case strings.HasPrefix(line, "-"):
			curFile.Removed++
			if resetRemoval {
				clear(wsCheck)
				resetRemoval = false
			}
			wsCheck[strings.TrimSpace(line[1:])]++

			// DELIBERATE Augur-parity quirk (PR #184 review finding 2,
			// declined): wsCheck is NOT cleared on context lines or
			// hunk ("@@") boundaries, so an addition later in the same
			// file that equals an earlier non-adjacent removal is
			// counted as a whitespace reformat. Augur's analyzecommit.py
			// has exactly this behavior (context lines and @@ fall
			// through its +/- dispatch untouched); "fixing" it would
			// diverge our cmt_whitespace numbers from Augur's. Column
			// parity wins over abstract correctness here.
		}

		if readErr == io.EOF {
			return flushCommit()
		}
	}
}

// whitespaceFlushEvery bounds how many file stats accumulate before a
// batched DB flush (the v0.27.97 multi-row pattern; only rows whose
// values actually change are touched — IS DISTINCT guard in the store).
var whitespaceFlushEvery = 5000

// runWhitespaceWalk streams git log over rangeSpec ("" = the default
// branch's full history; "old..branch" = incremental) and applies the
// adjusted counters to the repo's commits rows. On success it stamps
// repos.whitespace_head_hash = the branch head, which is what makes
// subsequent facade cycles incremental. Returns rows updated + head.
func (f *FacadeCollector) runWhitespaceWalk(ctx context.Context, repoID int64, clonePath, rangeSpec string) (int64, string, error) {
	branch := resolveDefaultBranch(ctx, clonePath)
	headCmd := exec.CommandContext(ctx, "git", "-C", clonePath, "rev-parse", branch)
	headOut, err := headCmd.Output()
	if err != nil {
		return 0, "", fmt.Errorf("rev-parse %s: %w", branch, execErr(ctx, err))
	}
	head := strings.TrimSpace(string(headOut))

	target := branch
	if rangeSpec != "" {
		target = rangeSpec
	}
	// Derived context (ultrareview 2026-08-19, bug_001): when the parse
	// loop bails early (a DB flush error), git is still writing and the
	// pipe fills — calling cmd.Wait() with an undrained StdoutPipe then
	// blocks FOREVER (git wedged in write(), Wait wedged on git's exit;
	// the exact pattern the exec.StdoutPipe docs warn about). Cancelling
	// this context on the error path SIGKILLs git so Wait() returns.
	walkCtx, cancelWalk := context.WithCancel(ctx)
	defer cancelWalk()
	cmd := exec.CommandContext(walkCtx, "git", "-C", clonePath, "log",
		target, "--numstat", "-p", "--format=%x1e%H")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, "", err
	}
	if err := cmd.Start(); err != nil {
		return 0, "", fmt.Errorf("starting git log -p: %w", err)
	}

	var (
		updated int64
		matched int64
		total   int64
		pending []db.CommitWhitespaceStat
	)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		n, m, ferr := f.store.UpdateCommitWhitespaceBatch(ctx, repoID, pending)
		pending = pending[:0]
		updated += n
		matched += m
		return ferr
	}
	parseErr := parseWhitespaceLog(stdout, func(c whitespaceCommit) error {
		for _, fs := range c.Files {
			if fs.Filename == "" {
				continue
			}
			total++
			pending = append(pending, db.CommitWhitespaceStat{
				Hash: c.Hash, Filename: fs.Filename,
				Added: fs.Added, Removed: fs.Removed, Whitespace: fs.Whitespace,
			})
		}
		if len(pending) >= whitespaceFlushEvery {
			return flush()
		}
		return nil
	})
	if parseErr != nil {
		// Kill git before Wait — see the walkCtx comment above.
		cancelWalk()
	}
	waitErr := cmd.Wait()
	if parseErr != nil {
		return updated, head, fmt.Errorf("parse whitespace log: %w", parseErr)
	}
	if waitErr != nil {
		return updated, head, fmt.Errorf("git log -p exited: %w", execErr(ctx, waitErr))
	}
	if err := flush(); err != nil {
		return updated, head, err
	}
	// v0.27.116 (Copilot round 10, active): refuse the stamp when any
	// emitted stat matched NO stored commit row. Historical per-row
	// write failures (swallowed by the pre-v0.27.107 facade while
	// last_collected still advanced) leave gaps the numstat pass will
	// re-insert on the repo's NEXT facade cycle — stamping over them
	// now would exclude their whitespace from every future incremental
	// walk (the marker-over-missing-rows class, third path). Updates
	// already applied stay (idempotent); a rerun after the next facade
	// cycle finds full coverage and stamps.
	if matched < total {
		return updated, head, fmt.Errorf(
			"%d of %d whitespace stats matched no stored commit row — refusing to stamp the marker; "+
				"the repo's next facade numstat pass re-inserts the missing rows, rerun after it",
			total-matched, total)
	}
	if err := f.store.SetWhitespaceHead(ctx, repoID, head); err != nil {
		return updated, head, fmt.Errorf("stamp whitespace head: %w", err)
	}
	return updated, head, nil
}

// markerResolves reports whether the stamped marker still names a
// commit in the clone. A vanished marker (force-push + gc, fresh
// re-clone with pruned history) makes the ranged walk's revision spec
// invalid; the caller falls back to a full-history walk for THAT case
// only.
func markerResolves(ctx context.Context, clonePath, marker string) bool {
	cmd := exec.CommandContext(ctx, "git", "-C", clonePath,
		"rev-parse", "--verify", "--quiet", marker+"^{commit}")
	return cmd.Run() == nil
}

// runWhitespacePhase is the per-cycle facade phase. A stamped marker
// makes the walk incremental (only commits past the marker); an empty
// marker triggers the full-history walk (brand-new repos bootstrap
// inline; for the pre-existing fleet the operator-controlled
// `aveloxis rewalk-whitespace` command is the recommended bulk
// bootstrap — without it each repo self-bootstraps on its next cycle).
//
// v0.27.117 (Copilot round 11, suppressed — real): marker validity is
// checked UP FRONT instead of falling back to a full walk on ANY
// incremental-walk error. The old shape turned every transient DB
// failure into a full multi-hour `git log -p` that then hit the same
// DB failure — an outage amplifier. Now only a marker that no longer
// resolves (force-push + gc, pruned re-clone) triggers the full walk;
// every other failure warns and retries next cycle.
func (f *FacadeCollector) runWhitespacePhase(ctx context.Context, repoID int64, clonePath string) {
	if ctx.Err() != nil {
		return // shutdown: the phase is warn-don't-fail, so it must not misdiagnose a dead ctx (pass 36)
	}
	marker, err := f.store.GetWhitespaceHead(ctx, repoID)
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		f.logger.Warn("whitespace phase: marker read failed", "repo_id", repoID, "error", err)
		return
	}
	rangeSpec := ""
	if marker != "" {
		if markerResolves(ctx, clonePath, marker) {
			rangeSpec = marker + ".." + resolveDefaultBranch(ctx, clonePath)
		} else if ctx.Err() != nil {
			return // a killed rev-parse is not a vanished marker
		} else {
			f.logger.Warn("whitespace phase: marker no longer resolves — full-history walk",
				"repo_id", repoID, "marker", marker)
		}
	}
	updated, head, werr := f.runWhitespaceWalk(ctx, repoID, clonePath, rangeSpec)
	if errors.Is(werr, context.Canceled) {
		return // shutdown mid-walk: nothing stamped, the next cycle re-walks
	}
	if werr != nil {
		f.logger.Warn("whitespace phase failed", "repo_id", repoID, "error", werr)
		return
	}
	if updated > 0 {
		f.logger.Info("whitespace phase complete", "repo_id", repoID,
			"rows_updated", updated, "head", head, "incremental", rangeSpec != "")
	}
}

// RewalkWhitespace is the exported per-repo unit for the
// `aveloxis rewalk-whitespace` command: ensure the bare clone exists
// (reusing the facade's persistent clone), walk the FULL history, and
// stamp the marker so subsequent facade cycles stay incremental.
func (f *FacadeCollector) RewalkWhitespace(ctx context.Context, repoID int64, gitURL string) (int64, error) {
	clonePath := f.clonePath(repoID)
	if err := f.ensureClone(ctx, gitURL, clonePath); err != nil {
		return 0, fmt.Errorf("clone/fetch: %w", err)
	}
	updated, _, err := f.runWhitespaceWalk(ctx, repoID, clonePath, "")
	return updated, err
}
