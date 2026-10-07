// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// summary/49: the facade folds the commits it PROVES written into daily
// per-author counts (UTC day of the author timestamp × author email) and
// replaces the repository's rows in repo_commit_daily after a completed
// walk. The fold is per commit, not per file row (the v0.19.11 distinct
// contract), and a NULL author timestamp buckets nowhere, as the readers'
// `cmt_author_timestamp IS NOT NULL` excludes such rows.

func TestFacadeResultFoldsCommitsPerDayAndAuthor(t *testing.T) {
	var r FacadeResult
	ny, _ := time.LoadLocation("America/New_York")
	late := time.Date(2026, 10, 6, 23, 30, 0, 0, ny) // 2026-10-07 03:30 UTC
	early := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC)
	r.noteCommitDaily(&late, "a@x")
	r.noteCommitDaily(&early, "a@x")
	r.noteCommitDaily(&early, "a@x")
	r.noteCommitDaily(&early, "b@x")
	r.noteCommitDaily(nil, "a@x") // no timestamp: no bucket

	rows := r.commitDailyRows()
	got := map[string]int{}
	for _, row := range rows {
		if row.Day.Location() != time.UTC || row.Day.Hour() != 0 {
			t.Errorf("a day is a UTC midnight, got %v", row.Day)
		}
		got[row.Day.Format("2006-01-02")+" "+row.AuthorEmail] = row.Commits
	}
	want := map[string]int{"2026-10-07 a@x": 1, "2026-10-06 a@x": 2, "2026-10-06 b@x": 1}
	if len(got) != len(want) {
		t.Fatalf("rows %v want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: %d want %d", k, got[k], v)
		}
	}
	if (&FacadeResult{}).commitDailyRows() != nil {
		t.Error("no commits folded yields nil rows (an empty walk clears the table through the store, with no allocation here)")
	}
}

// The fold sites, by source (comments stripped): every path that proves a
// commit written also folds it — the batch success loop (once per built
// commit) and the per-row fallback (once per hash, where insertedByHash
// is first set) — and CollectRepo replaces the table only after the walk
// completed.
func TestCommitDailyIsFoldedWhereCommitsAreProvenWritten(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/collector/facade.go"))
	batch := srctest.FuncBody(t, src, "func (f *FacadeCollector) insertCommitBatch(")
	if !regexp.MustCompile(`for _, pc := range built \{\s*result\.Commits\+\+\s*result\.noteCommitDaily\(parseTimestamp\(pc\.AuthorDate\), pc\.AuthorEmail\)`).MatchString(batch) {
		t.Error("the batch success loop must fold each built commit once, beside result.Commits++")
	}
	fallback := srctest.FuncBody(t, src, "func (f *FacadeCollector) upsertCommitRowsFallback(")
	if !regexp.MustCompile(`result\.noteFallbackRowWritten\(insertedByHash, commit\)`).MatchString(fallback) || strings.Contains(fallback, "insertedByHash[commit.Hash] = true") {
		t.Error("the per-row fallback must fold through noteFallbackRowWritten (once per commit, where its hash is first proven written), not per file row")
	}
	collect := srctest.FuncBody(t, src, "func (f *FacadeCollector) CollectRepo(")
	// Round 2 F2: a walk that swallowed commit writes still records what it
	// saw (freshness) but does not TRIM rows it may have missed, and says so.
	if !regexp.MustCompile(`parseErr := f\.parseGitLog\(ctx, repoID, clonePath, result\)\s*f\.recordCommitBounds\(ctx, repoID, result\)\s*if parseErr != nil \{[^}]*\}\s*f\.recordCommitDaily\(ctx, repoID, result\)`).MatchString(collect) {
		t.Error("CollectRepo must record the daily fold right after a COMPLETED walk (after the git-log error return), never after a partial one")
	}
	record := srctest.FuncBody(t, src, "func (f *FacadeCollector) recordCommitDaily(")
	if !strings.Contains(record, "trim := result.CommitWriteFailures == 0") || !strings.Contains(record, "f.store.ReplaceRepoCommitDaily(ctx, repoID, result.commitDailyRows(), trim)") {
		t.Error("recordCommitDaily trims only when every walked commit was proven written, and always upserts what it saw")
	}
	if !regexp.MustCompile(`if !trim \{\s*f\.logger\.Warn\(`).MatchString(record) {
		t.Error("a walk that swallowed commit writes must WARN that the daily rows were not trimmed")
	}
	if strings.Count(collect, "f.recordCommitDaily(") != 1 {
		t.Error("exactly one replace site: a partial walk (clone failure, git-log error) must leave the previous rows in place")
	}
}

// ─── Review round 1 (summary/49) ────────────────────────────────

// F1: the per-row fallback holds one row per FILE; a commit must fold once,
// where its hash is first proven written.
func TestFallbackFoldsOncePerCommit(t *testing.T) {
	var r FacadeResult
	seen := map[string]bool{}
	ts := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	for _, file := range []string{"a.go", "b.go", "c.go"} {
		r.noteFallbackRowWritten(seen, &model.Commit{Hash: "abc", Filename: file, AuthorEmail: "a@x", AuthorTimestamp: &ts})
	}
	r.noteFallbackRowWritten(seen, &model.Commit{Hash: "def", Filename: "d.go", AuthorEmail: "a@x", AuthorTimestamp: &ts})
	rows := r.commitDailyRows()
	if len(rows) != 1 || rows[0].Commits != 2 {
		t.Fatalf("two commits (one with three files) fold to 2, got %+v", rows)
	}
	if len(seen) != 2 {
		t.Fatalf("seen tracks hashes, got %d", len(seen))
	}
}

// F2 (round 2 F1 corrected it): a GitHub noreply address names the author's
// LOGIN, which the resolver's strategy 1 uses and which maps to the
// contributor row through the backfill's own rule (LOWER(gh_login)) at read
// time. The fold stores the login — never an id: PlatformUUID(1, id) is the
// id the resolver WANTS, not the one it keeps when a legacy row already
// holds that login (UpsertContributorFull), and a guessed id overwrote the
// backfill's correct one. The facade's rows carry no cntrb_id at all.
func TestFoldStoresTheNoreplyLoginNeverAnID(t *testing.T) {
	var r FacadeResult
	ts := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	r.noteCommitDaily(&ts, "777+Web@users.noreply.github.com")
	r.noteCommitDaily(&ts, "legacy@users.noreply.github.com")
	r.noteCommitDaily(&ts, "plain@x")
	logins := map[string]string{}
	uids := map[string]int64{}
	for _, row := range r.commitDailyRows() {
		logins[row.AuthorEmail] = row.AuthorLogin
		uids[row.AuthorEmail] = row.AuthorGhUserID
		if row.CntrbID != "" {
			t.Errorf("the facade never stores an identity, got %q for %s", row.CntrbID, row.AuthorEmail)
		}
	}
	if logins["777+Web@users.noreply.github.com"] != "Web" || uids["777+Web@users.noreply.github.com"] != 777 {
		t.Errorf("noreply with an id carries its login AND GitHub's numeric user id (round 3 F2: the login can be renamed, the id cannot): got %q / %d", logins["777+Web@users.noreply.github.com"], uids["777+Web@users.noreply.github.com"])
	}
	if uids["legacy@users.noreply.github.com"] != 0 || uids["plain@x"] != 0 {
		t.Errorf("no numeric id without one in the address: %v", uids)
	}
	if logins["legacy@users.noreply.github.com"] != "legacy" {
		t.Errorf("an ID-less noreply address still names a login: got %q", logins["legacy@users.noreply.github.com"])
	}
	if logins["plain@x"] != "" {
		t.Errorf("a plain email names no login: %v", logins)
	}
}

// F3: the fold key is the scrubbed email, the same string the commits rows
// carry, so the alias join matches and two raw spellings that scrub alike
// land in one bucket.
func TestFoldKeyIsTheScrubbedEmail(t *testing.T) {
	var r FacadeResult
	ts := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	r.noteCommitDaily(&ts, "j\xf6rg@x")
	r.noteCommitDaily(&ts, "j\xfdrg@x") // a different invalid byte, the same scrubbed string
	rows := r.commitDailyRows()
	if len(rows) != 1 || rows[0].AuthorEmail != db.SafeUTF8("j\xf6rg@x") || rows[0].Commits != 2 {
		t.Fatalf("got %+v", rows)
	}
}
