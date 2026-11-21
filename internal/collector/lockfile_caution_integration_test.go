// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/scantarget"
	"github.com/aveloxis/aveloxis/internal/versionuse"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// lockfilesNotRead decides the reading by what the name pass learned of the
// clone before the reader stopped.
func TestLockfilesNotReadSaysWhatIsKnownOfTheClone(t *testing.T) {
	const why = model.InventoryAttemptReadCPULimit
	for name, c := range map[string]struct {
		locks lockfileSet
		want  LockfileReading
	}{
		"lockfiles listed, some there": {lockfileSet{PresentKnown: true, Present: map[string]bool{"yarn.lock": true}}, LockfileReading{NotCompleted: why, Lockfiles: true}},
		"lockfiles listed, none there": {lockfileSet{PresentKnown: true, Present: map[string]bool{}}, LockfileReading{Completed: true}},
		"listed, nil map":              {lockfileSet{PresentKnown: true}, LockfileReading{Completed: true}},
		"not listed":                   {lockfileSet{}, LockfileReading{NotCompleted: why}},
		"not listed, a stale map":      {lockfileSet{Present: map[string]bool{"yarn.lock": true}}, LockfileReading{NotCompleted: why}},
	} {
		if got := lockfilesNotRead(why, c.locks); got != c.want {
			t.Errorf("%s: %+v, want %+v", name, got, c.want)
		}
	}
}

// The whole of it, with the real reader, the real store and nothing stood in
// (0.29.214; review round 7's express scenario and the operator's caution):
// a repository deletes a lockfile and moves its manifest on, and another
// lockfile then takes more memory to read than the limit allows. After that
// analysis the deleted lockfile's rows are gone from all three tables, the
// inventory reports what the manifest declares, the scan does not target the
// version the deleted lockfile locked, the coverage record says the lockfiles
// went unread and since when, and the pages' caution says it in words. The
// next analysis that reads them clears it.
func TestUnreadLockfilesAreRecordedCautionedAndDoNotLockWhatIsGone(t *testing.T) {
	f := lockFixture(t, "locks-caution")
	ctx := context.Background()
	if _, ok := any(f.ac.store).(LockfileReadingRecorder); !ok {
		t.Fatal("the store does not keep lockfile readings")
	}
	if _, ok := any(f.ac.store).(LockfileRowDeleter); !ok {
		t.Fatal("the store does not remove the rows of lockfiles that are gone")
	}
	caution := func() (model.DependencyCoverage, string) {
		t.Helper()
		cov, found := f.coverage(t)
		if !found {
			t.Fatal("no coverage record")
		}
		v := vulnclass.Coverage(vulnclass.CoverageInputs{HasRow: true, Coverage: cov})
		return cov, vulnclass.CoverageLockfileCaution(&v)
	}
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	f.write(t, "package-lock.json", goodNpmLock)
	before := time.Now().Add(-time.Second)
	f.analyse(t)
	cov, said := caution()
	if cov.LockfilesReadAt == nil || cov.LockfilesReadAt.Before(before) || cov.LockfilesUnreadReason != "" || said != "" {
		t.Fatalf("after a reading that finished: at %v reason %q caution %q; want now, no reason, no caution", cov.LockfilesReadAt, cov.LockfilesUnreadReason, said)
	}
	firstRead := *cov.LockfilesReadAt
	if n := f.count(t, "repo_lockfile_edges"); n == 0 {
		t.Fatal("the fixture stored no lockfile edge: the removal below would not be seen in that table")
	}

	// The lockfile is deleted, the manifest moves on, and a lockfile too
	// large for the limit appears.
	if err := os.Remove(filepath.Join(f.dir, "package-lock.json")); err != nil {
		t.Fatal(err)
	}
	f.write(t, "package.json", `{"dependencies": {"express": "^5.0.0"}}`)
	f.write(t, "big/yarn.lock", lockfileThatOutgrowsItsCost(outgrowingLockfileBytes))
	SetManifestLimits(limitsFromConfig(t, outgrownMemorySetting))
	t.Cleanup(func() { installedManifestLimits.Store(nil) })

	read := f.ac.readClone(ctx, f.repoID, f.dir)
	locks := read.lockfiles()
	var child *errManifestChild
	if !errors.As(locks.WalkErr, &child) || child.Attempt != model.InventoryAttemptReadMemoryLimit {
		read.close()
		t.Fatalf("the lockfiles: %v; want them unread at the memory limit", locks.WalkErr)
	}
	if err := f.ac.scanLibyear(ctx, f.repoID, f.dir, read, f.ac.lockfilesInUse(ctx, f.repoID, locks, read.declaredKeys()), &AnalysisResult{}); err != nil {
		t.Fatal(err)
	}
	if err := f.ac.scanLockfiles(ctx, f.repoID, f.dir, locks, &AnalysisResult{}); err == nil {
		t.Fatal("unread lockfiles were not an error")
	}
	read.close()

	for _, table := range []string{"repo_lockfiles", "repo_lockfile_packages", "repo_lockfile_edges"} {
		if n := f.count(t, table); n != 0 {
			t.Errorf("%s holds %d rows of this repository; its one stored lockfile is gone from the clone, so none should stand", table, n)
		}
	}
	if e := inventoryRows(t, f)["package.json:express"]; e.Version == "4.18.2" || e.Source == versionuse.SourceLocked || e.Requirement != "^5.0.0" {
		t.Errorf("express = %+v; want what the manifest declares", e)
	}
	set, err := scantarget.Build(ctx, f.store, f.repoID, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range set.Purls {
		if strings.Contains(p, "4.18.2") || strings.Contains(p, "body-parser") {
			t.Errorf("the scan targets %s, which only the deleted lockfile gave", p)
		}
	}
	cov, said = caution()
	if cov.LockfilesReadAt == nil || !cov.LockfilesReadAt.Equal(firstRead) || cov.LockfilesUnreadReason != model.InventoryAttemptReadMemoryLimit {
		t.Errorf("the record: at %v reason %q; want %v kept and the memory limit", cov.LockfilesReadAt, cov.LockfilesUnreadReason, firstRead)
	}
	// Round 8 (D8-4): every lockfile read on the kept date is gone and so
	// are its rows, so there is nothing that date dates. The true sentence
	// is the never-read one; the store decides it from what is stored.
	if cov.LockfilesLastRead != nil {
		t.Errorf("no lockfile row is stored and the record dates them %v", cov.LockfilesLastRead)
	}
	want := "Lockfiles could not be read (reading them took more memory than allowed). Transitive dependencies are not checked."
	if said != want {
		t.Errorf("the caution:\n got %q\nwant %q", said, want)
	}
	if cov.LastAttemptResult != "" {
		t.Errorf("the inventory was replaced, and an inventory attempt is recorded: %q", cov.LastAttemptResult)
	}

	// The limit is raised: the lockfiles are read, and there is nothing to
	// caution about.
	installedManifestLimits.Store(nil)
	if manifestRaceEnabled {
		// Under the race detector the same reading, of a lockfile that is
		// no longer large: storing the large one's 150,000 packages costs
		// the detector fifteen seconds, and what is judged here is that a
		// reading which completes clears the caution, whatever its size.
		// (That the SAME clone is read once there is room is
		// TestAnAnalysisWhoseLockfilesWereNotReadKeepsThemAndSaysWhy's.)
		f.write(t, "big/yarn.lock", "# yarn lockfile v1\n\nleft-pad@1.0.0:\n  version \"1.0.0\"\n")
	}
	f.analyse(t)
	cov, said = caution()
	if cov.LockfilesUnreadReason != "" || said != "" || cov.LockfilesReadAt == nil || !cov.LockfilesReadAt.After(firstRead) {
		t.Errorf("after the lockfiles were read again: at %v reason %q caution %q", cov.LockfilesReadAt, cov.LockfilesUnreadReason, said)
	}
}

// A repository with no lockfile whose reader is stopped (here in the
// lockfile pass's own walk, by a memory limit no clone fits) carries no
// caution: there is nothing to be behind on, and one left by an earlier
// reading is cleared.
func TestARepositoryWithNoLockfileIsNeverCautioned(t *testing.T) {
	f := lockFixture(t, "locks-none")
	ctx := context.Background()
	f.write(t, "package.json", `{"dependencies": {"express": "^4.0.0"}}`)
	if err := f.store.RecordLockfileReading(ctx, f.repoID, model.LockfileReading{NotCompleted: model.InventoryAttemptReadFailed, Lockfiles: true}); err != nil {
		t.Fatal(err)
	}
	locks := lockfileSet{WalkErr: &errManifestChild{Attempt: model.InventoryAttemptReadMemoryLimit, detail: "stopped"}, PresentKnown: true, Present: map[string]bool{}}
	if err := f.ac.scanLockfiles(ctx, f.repoID, f.dir, locks, &AnalysisResult{}); err == nil {
		t.Fatal("unread lockfiles were not an error")
	}
	cov, found := f.coverage(t)
	if !found || cov.LockfilesUnreadReason != "" || cov.LockfilesReadAt != nil {
		t.Errorf("a clone known to hold no lockfile: %+v (found %v); want no reason and no date", cov, found)
	}
}
