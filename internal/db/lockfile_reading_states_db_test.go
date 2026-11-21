// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// THE STATE TABLE of the lockfile reading (0.29.215, review round 8).
//
// What is stored before a reading is recorded:
//
//	record   no coverage record | a record with neither column | with the
//	         date (D1) | with a reason (R0) | with both
//	rows     lockfile rows stored for the repository (their snapshot dated
//	         D0, earlier than D1), or none
//
// The reading recorded:
//
//	CL  completed, the clone has lockfiles
//	CN  completed, the clone has none
//	NP  not completed (reason R), the clone is KNOWN to hold a lockfile
//	NU  not completed (reason R), nothing known of what the clone holds
//
// The columns after, and what a page says:
//
//	CL  date = now, reason cleared                              no caution
//	CN  date cleared, reason cleared                            no caution
//	NP  date kept, reason = R
//	NU  date kept, reason = R when rows are stored, or the record had a date
//	    or a reason; otherwise nothing is claimed (the repository may have
//	    no lockfile)
//
//	and for NP / NU with a reason: rows stored    -> "Lockfiles last read
//	    <date>; ..." with the record's date when it has one, else the stored
//	    snapshot's (D0: rows from before the column existed);
//	    no rows stored -> "Lockfiles could not be read ...", whatever date
//	    the record holds (the lockfiles read then are gone).
//
// Every cell is one row of lockfileReadingCells, driven through the real
// store and vulnclass.
var (
	lockfileStateD0 = time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	lockfileStateD1 = time.Date(2026, 11, 2, 23, 30, 0, 0, time.UTC)
)

const (
	lockfileStateR0 = model.InventoryAttemptReadCPULimit
	lockfileStateR  = model.InventoryAttemptReadMemoryLimit
)

type lockfileReadingCell struct {
	record string // "none", "empty", "date", "reason", "both"
	rows   bool
	input  string // "CL", "CN", "NP", "NU"
	// After: the date column ("", "now", "D1"), the reason column ("", "R"),
	// and the caution ("", "behind D0", "behind D1", "never").
	date, reason, caution string
}

var lockfileReadingCells = []lockfileReadingCell{
	// Completed with lockfiles: read now, nothing to say.
	{"none", false, "CL", "now", "", ""}, {"none", true, "CL", "now", "", ""},
	{"empty", false, "CL", "now", "", ""}, {"empty", true, "CL", "now", "", ""},
	{"date", false, "CL", "now", "", ""}, {"date", true, "CL", "now", "", ""},
	{"reason", false, "CL", "now", "", ""}, {"reason", true, "CL", "now", "", ""},
	{"both", false, "CL", "now", "", ""}, {"both", true, "CL", "now", "", ""},
	// Completed with none: both cleared.
	{"none", false, "CN", "", "", ""}, {"none", true, "CN", "", "", ""},
	{"empty", false, "CN", "", "", ""}, {"empty", true, "CN", "", "", ""},
	{"date", false, "CN", "", "", ""}, {"date", true, "CN", "", "", ""},
	{"reason", false, "CN", "", "", ""}, {"reason", true, "CN", "", "", ""},
	{"both", false, "CN", "", "", ""}, {"both", true, "CN", "", "", ""},
	// Not completed, lockfiles known present: always claimed.
	{"none", false, "NP", "", "R", "never"}, {"none", true, "NP", "", "R", "behind D0"},
	{"empty", false, "NP", "", "R", "never"}, {"empty", true, "NP", "", "R", "behind D0"},
	{"date", false, "NP", "D1", "R", "never"}, {"date", true, "NP", "D1", "R", "behind D1"},
	{"reason", false, "NP", "", "R", "never"}, {"reason", true, "NP", "", "R", "behind D0"},
	{"both", false, "NP", "D1", "R", "never"}, {"both", true, "NP", "D1", "R", "behind D1"},
	// Not completed, nothing known: claimed when there is something stored
	// or recorded to be behind on.
	{"none", false, "NU", "", "", ""}, {"none", true, "NU", "", "R", "behind D0"},
	{"empty", false, "NU", "", "", ""}, {"empty", true, "NU", "", "R", "behind D0"},
	{"date", false, "NU", "D1", "R", "never"}, {"date", true, "NU", "D1", "R", "behind D1"},
	{"reason", false, "NU", "", "R", "never"}, {"reason", true, "NU", "", "R", "behind D0"},
	{"both", false, "NU", "D1", "R", "never"}, {"both", true, "NU", "D1", "R", "behind D1"},
}

func TestTheLockfileReadingStateTable(t *testing.T) {
	if len(lockfileReadingCells) != 5*2*4 {
		t.Fatalf("%d cells; the table is 5 records x 2 x 4 readings", len(lockfileReadingCells))
	}
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-state-table")
	clear := func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID)
	}
	t.Cleanup(clear)
	inputs := map[string]model.LockfileReading{
		"CL": {Completed: true, Lockfiles: true},
		"CN": {Completed: true},
		"NP": {NotCompleted: lockfileStateR, Lockfiles: true},
		"NU": {NotCompleted: lockfileStateR},
	}
	reasonWords := vulnclass.LockfileUnreadReasonWording(lockfileStateR)
	cautions := map[string]string{
		"":          "",
		"behind D0": "Lockfiles last read 2026-10-01; the latest read did not finish (" + reasonWords + "). Versions and transitive dependencies may be out of date.",
		"behind D1": "Lockfiles last read 2026-11-02; the latest read did not finish (" + reasonWords + "). Versions and transitive dependencies may be out of date.",
		"never":     "Lockfiles could not be read (" + reasonWords + "). Transitive dependencies are not checked.",
	}
	seen := map[string]bool{}
	for _, c := range lockfileReadingCells {
		name := fmt.Sprintf("%s/rows=%v/%s", c.record, c.rows, c.input)
		if seen[name] {
			t.Fatalf("cell %s is in the table twice", name)
		}
		seen[name] = true
		t.Run(name, func(t *testing.T) {
			clear()
			switch c.record {
			case "none":
			case "empty":
				mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id) VALUES ($1)`, repoID)
			case "date":
				mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, lockfiles_read_at) VALUES ($1, $2)`, repoID, lockfileStateD1)
			case "reason":
				mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, lockfiles_unread_reason) VALUES ($1, $2)`, repoID, lockfileStateR0)
			case "both":
				mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_dependency_coverage (repo_id, lockfiles_read_at, lockfiles_unread_reason) VALUES ($1, $2, $3)`, repoID, lockfileStateD1, lockfileStateR0)
			default:
				t.Fatalf("record state %q", c.record)
			}
			if c.rows {
				// Two lockfiles of one snapshot; the later date is the
				// snapshot's.
				mustExecRetry(ctx, t, store, `
					INSERT INTO aveloxis_data.repo_lockfiles (repo_id, ecosystem, lockfile_path, lockfile_kind, data_collection_date)
					VALUES ($1, 'npm', 'package-lock.json', 'package-lock.json', $2), ($1, 'npm', 'web/yarn.lock', 'yarn.lock', $3)`,
					repoID, lockfileStateD0, lockfileStateD0.Add(-time.Hour))
			}
			before := time.Now().Add(-2 * time.Second)
			if err := store.RecordLockfileReading(ctx, repoID, inputs[c.input]); err != nil {
				t.Fatal(err)
			}
			cov, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
			if err != nil || !found {
				t.Fatalf("found %v err %v: every reading leaves a record", found, err)
			}
			switch c.date {
			case "":
				if cov.LockfilesReadAt != nil {
					t.Errorf("date column = %v, want none", cov.LockfilesReadAt)
				}
			case "now":
				if cov.LockfilesReadAt == nil || cov.LockfilesReadAt.Before(before) {
					t.Errorf("date column = %v, want now", cov.LockfilesReadAt)
				}
			case "D1":
				if cov.LockfilesReadAt == nil || !cov.LockfilesReadAt.Equal(lockfileStateD1) {
					t.Errorf("date column = %v, want %v kept", cov.LockfilesReadAt, lockfileStateD1)
				}
			}
			wantReason := ""
			if c.reason == "R" {
				wantReason = lockfileStateR
			}
			if cov.LockfilesUnreadReason != wantReason {
				t.Errorf("reason column = %q, want %q", cov.LockfilesUnreadReason, wantReason)
			}
			// What the stored rows date, as the store decides it.
			switch {
			case !c.rows && cov.LockfilesLastRead != nil:
				t.Errorf("no lockfile row is stored and the record dates them %v", cov.LockfilesLastRead)
			case c.rows && cov.LockfilesLastRead == nil:
				t.Error("lockfile rows are stored and nothing dates them")
			}
			// And exactly which date: the record's when it has one, else
			// the stored snapshot's latest.
			if want, has := map[string]time.Time{"behind D0": lockfileStateD0, "behind D1": lockfileStateD1}[c.caution]; has &&
				(cov.LockfilesLastRead == nil || !cov.LockfilesLastRead.Equal(want)) {
				t.Errorf("the stored rows are dated %v, want %v", cov.LockfilesLastRead, want)
			}
			scanned := time.Now()
			view := vulnclass.Coverage(vulnclass.CoverageInputs{HasRow: true, ScanStamp: &scanned, Coverage: cov})
			if got := vulnclass.CoverageLockfileCaution(&view); got != cautions[c.caution] {
				t.Errorf("caution:\n got %q\nwant %q", got, cautions[c.caution])
			}
			// The batch read decides alike.
			in, err := store.GetRepoCoverageInputs(ctx, []int64{repoID})
			if err != nil {
				t.Fatal(err)
			}
			batch := vulnclass.Coverage(in[repoID])
			if got := vulnclass.CoverageLockfileCaution(&batch); got != cautions[c.caution] {
				t.Errorf("caution from the batch read:\n got %q\nwant %q", got, cautions[c.caution])
			}
		})
	}
}

// The reason shown is the LATEST reading's: a stopped reading replaces the
// reason an earlier one left, also when nothing is known of what the clone
// holds and no row is stored (round 8: the page named the wrong setting).
func TestALaterUnfinishedLockfileReadingReplacesTheReason(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-latest-reason")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID)
	})
	for _, r := range []model.LockfileReading{
		{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true},
		{NotCompleted: model.InventoryAttemptRootUnreadable},
		{NotCompleted: model.LockfileUnreadNotReached},
	} {
		if err := store.RecordLockfileReading(ctx, repoID, r); err != nil {
			t.Fatal(err)
		}
		cov, _, err := store.GetRepoDependencyCoverage(ctx, repoID)
		if err != nil {
			t.Fatal(err)
		}
		if cov.LockfilesUnreadReason != r.NotCompleted {
			t.Errorf("after a reading that ended %q the record says %q", r.NotCompleted, cov.LockfilesUnreadReason)
		}
	}
}

// The reading can be written inside a transaction its caller owns, by the
// same statement: committed with it, and gone with it when it rolls back.
func TestTheLockfileReadingIsWrittenInTheCallersTransaction(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "lockfile-reading-tx")
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID)
	})
	in := func(commit bool, r model.LockfileReading) error {
		tx, err := store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err := recordLockfileReadingIn(ctx, tx, repoID, r); err != nil {
			return err
		}
		if commit {
			return tx.Commit(ctx)
		}
		return nil
	}
	if err := in(false, model.LockfileReading{Completed: true, Lockfiles: true}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetRepoDependencyCoverage(ctx, repoID); err != nil || found {
		t.Fatalf("a rolled-back reading left a record: found %v err %v", found, err)
	}
	if err := in(true, model.LockfileReading{Completed: true, Lockfiles: true}); err != nil {
		t.Fatal(err)
	}
	cov, found, err := store.GetRepoDependencyCoverage(ctx, repoID)
	if err != nil || !found || cov.LockfilesReadAt == nil || cov.LockfilesUnreadReason != "" {
		t.Fatalf("a committed reading: found %v err %v record %+v", found, err, cov)
	}
	// The same refusals as on the pool, before anything is sent.
	if err := in(true, model.LockfileReading{NotCompleted: "made up"}); err == nil {
		t.Error("a reason that is not one of the list was accepted in a transaction")
	}
	var _ pgx.Tx // the transaction type the snapshot replace hands in
}

// lockfiles_read_at and lockfiles_unread_reason have ONE writer in the
// module: recordLockfileReadingOn's statement. Their discipline (date kept on a failed
// reading, reason cleared on a completed one) lives in that statement; a
// second statement assigning either would not have it.
func TestTheLockfileReadingColumnsHaveOneWriter(t *testing.T) {
	stmts := columnWriteReading(t).Statements
	if len(stmts) < 1000 {
		t.Fatalf("%d statements read; the store has more", len(stmts))
	}
	guarded := map[string]bool{"lockfiles_read_at": true, "lockfiles_unread_reason": true}
	writers := map[string]string{}
	for _, st := range stmts {
		ws, err := sqlscan.ParseWrites(st.SQL)
		if err != nil {
			continue // the policy test reports what does not parse
		}
		for _, w := range ws {
			if w.Table != "repo_dependency_coverage" {
				continue
			}
			touches := w.InsertAll || w.CopyAll
			for _, c := range append(append([]string{}, w.InsertColumns...), w.CopyColumns...) {
				touches = touches || guarded[c]
			}
			for _, set := range w.Sets {
				touches = touches || guarded[set.Column]
			}
			if touches {
				writers[st.SQL] = st.File + " " + st.Decl
			}
		}
	}
	if len(writers) != 1 {
		t.Fatalf("%d statements write the lockfile reading columns, want one: %v", len(writers), writers)
	}
	for _, where := range writers {
		if where != "internal/db/history.go recordLockfileReadingOn" {
			t.Errorf("the one writer is in %q, want recordLockfileReadingOn of internal/db/history.go", where)
		}
	}
}
