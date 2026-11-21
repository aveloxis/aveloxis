// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package testdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/testdb/writejudge"
)

// Every DB-tier package's run installs the write-policy judge, fails on
// what it holds against a statement production code sent while the tests
// ran, and, when the closing suite asks, leaves a record of the store's
// library calls it reached (0.29.213).
func TestRunInstallsTheJudgeAndFailsOnItsVerdict(t *testing.T) {
	base := baseDSN(t)
	t.Setenv(EnvVar, base)
	records := t.TempDir()
	t.Setenv(writejudge.SitesDirEnv, records)
	at := []writejudge.Frame{{File: "internal/db/zz.go", Line: 7, Decl: "zzWriter"}}

	var outer = writejudge.Installed()
	// Green tests that sent a read from the store: the run passes, and its
	// record names the line.
	code := run(func() int {
		j := writejudge.Installed()
		if j == nil || j == outer {
			t.Error("run did not install a judge of its own before the tests")
			return 0
		}
		j.RecordFrom(at, "SELECT 1", false, nil)
		return 0
	}, base, nil, nil)
	if code != 0 {
		t.Fatalf("a run whose tests sent only a read: exit %d", code)
	}
	entries, err := os.ReadDir(records)
	if err != nil || len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".sites") {
		t.Fatalf("the run left %v in the records folder (%v); want its one record", entries, err)
	}
	b, err := os.ReadFile(filepath.Join(records, entries[0].Name()))
	if err != nil || !strings.Contains(string(b), "L\tinternal/db/zz.go:7\n") {
		t.Errorf("the record %s holds %q (%v); want the line the statement was sent from", entries[0].Name(), b, err)
	}
	if entries[0].Name() != "internal_testdb.sites" {
		t.Errorf("the record is named %s; want it named for this package, internal_testdb.sites", entries[0].Name())
	}

	// Green tests during which production sent a write the judge does not
	// accept (here, of a relation the run's empty database does not have):
	// the run fails.
	code = run(func() int {
		writejudge.Installed().RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
		return 0
	}, base, nil, nil)
	if code != 1 {
		t.Errorf("a run during which production sent a forbidden write: exit %d, want 1", code)
	}
	// The judge is installed BEFORE the preparation (round 7: installed
	// after it, every test passed): what the preparation sends through the
	// store, its migrate included, is judged like anything else.
	before, sawItsOwnJudge := writejudge.Installed(), false
	code = run(func() int { return 0 }, base, func(context.Context, string) error {
		j := writejudge.Installed()
		sawItsOwnJudge = j != nil && j != before
		if j != nil {
			// What the preparation sends goes to whichever judge is in
			// place: this run's, when it is installed first.
			j.RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
		}
		return nil
	}, nil)
	if !sawItsOwnJudge || code != 1 {
		t.Errorf("a run whose PREPARATION sent a forbidden write from production: its own judge in place %v, exit %d; want it judged by this run's judge (exit 1)", sawItsOwnJudge, code)
	}
	// Failed tests keep their own exit code: there is no verdict to add.
	code = run(func() int {
		writejudge.Installed().RecordFrom(at, "UPDATE aveloxis_data.repos SET platform_repo_id = 0", false, nil)
		return 3
	}, base, nil, nil)
	if code != 3 {
		t.Errorf("a run whose tests failed: exit %d, want the tests' own 3", code)
	}
}
