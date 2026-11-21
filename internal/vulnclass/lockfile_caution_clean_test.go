// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Operator, 2026-11-21, on whether the lockfile caution withdraws the clean
// claim: "Withdraw, keep the count". A repository whose lockfiles went
// unread has not been shown to be clean: clean_result is false while the
// caution is up, in every state, and what a sentence says in place of "no
// vulnerabilities" is the state's own wording (the caution is beside it and
// is not repeated). The COUNT is a true count of what was checked and does
// not change: CountBlank is the same with the caution as without.
func TestTheLockfileCautionWithdrawsTheCleanClaimAndKeepsTheCount(t *testing.T) {
	read := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	cleanSeen := 0
	for _, c := range []covCase{
		{outcome: model.ScanSent, partial: true, stamp: true, analysed: true},
		{outcome: model.ScanSent, stamp: true, analysed: true},
		{outcome: model.ScanNothingToSend, stamp: true, analysed: true},
		{outcome: model.ScanUnsendable, analysed: true},
		{outcome: model.ScanUnanswered, analysed: true},
		{outcome: model.ScanFailed, stamp: true, findings: true, analysed: true},
		{outcome: model.ScanInventoryIncomplete, analysed: true},
		{outcome: model.ScanInventoryIncomplete, stamp: true, analysed: true},
		{stamp: true, analysed: true},
		{analysed: true},
		{outcome: "an outcome this release does not know", stamp: true, analysed: true},
	} {
		without := Coverage(c.inputs())
		in := c.inputs()
		in.Coverage.LockfilesUnreadReason, in.Coverage.LockfilesLastRead = model.InventoryAttemptReadMemoryLimit, &read
		with := Coverage(in)
		if with.LockfilesUnread == nil {
			t.Fatalf("%+v: the view carries no caution", c)
		}
		if without.CleanResult {
			cleanSeen++
		}
		if with.CleanResult {
			t.Errorf("%+v (%s): clean_result is true while the lockfiles are unread", c, with.State)
		}
		if with.CountBlank != without.CountBlank {
			t.Errorf("%+v (%s): the count is blank %v with the caution and %v without: the caution does not change the count", c, with.State, with.CountBlank, without.CountBlank)
		}
		if with.State != without.State || with.SecondLine != without.SecondLine {
			t.Errorf("%+v: the caution changed the state or its second line", c)
		}
		if got, want := CoverageAbsenceClause(&with), CoverageAbsenceClause(&without); got != want || got == "" {
			t.Errorf("%+v: in place of the clean sentence %q, without the caution %q", c, got, want)
		}
	}
	// Guard the denominator: the table holds states that ARE clean without
	// the caution, or the assertion above withdraws nothing.
	if cleanSeen < 2 {
		t.Fatalf("%d of the states were clean without the caution: the table does not test the withdrawal", cleanSeen)
	}
	// A repository scanned with no exposure and its lockfiles unread says
	// "scanned", not "no open vulnerabilities".
	in := covCase{outcome: model.ScanSent, stamp: true, analysed: true}.inputs()
	in.Coverage.LockfilesUnreadReason = model.InventoryAttemptReadCPULimit
	v := Coverage(in)
	if got := CoverageAbsenceClause(&v); got != CoverageStateWording(CoverageScanned) {
		t.Errorf("in place of the clean sentence: %q", got)
	}
}

// A lockfile left unread for its size is recorded by a reading that DID
// replace the snapshot: every other lockfile was read then (0.29.215). The
// two dated and undated sentences are about a reading that replaced
// nothing, and said "Lockfiles last read <an old day>" over rows read today
// (review round 9). This case has its own sentence, with no date in it.
func TestSomeLockfilesTooLargeIsItsOwnSentenceWithNoDate(t *testing.T) {
	read := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	const want = "Some lockfiles could not be read (reading them would take more memory than allowed). Their versions and transitive dependencies may be out of date."
	for _, at := range []*time.Time{&read, nil} {
		in := covCase{outcome: model.ScanSent, stamp: true, analysed: true}.inputs()
		in.Coverage.LockfilesUnreadReason, in.Coverage.LockfilesLastRead = model.LockfileUnreadTooLarge, at
		v := Coverage(in)
		if got := CoverageLockfileCaution(&v); got != want {
			t.Errorf("stored rows dated %v:\n got %q\nwant %q", at, got, want)
		}
		if v.CleanResult {
			t.Error("clean with a lockfile unread")
		}
	}
	// The other reasons keep their two sentences.
	in := covCase{outcome: model.ScanSent, stamp: true, analysed: true}.inputs()
	in.Coverage.LockfilesUnreadReason, in.Coverage.LockfilesLastRead = model.InventoryAttemptReadMemoryLimit, &read
	v := Coverage(in)
	if got := CoverageLockfileCaution(&v); got != "Lockfiles last read 2026-11-02; the latest read did not finish (reading them took more memory than allowed). Versions and transitive dependencies may be out of date." {
		t.Errorf("a reading that replaced nothing: %q", got)
	}
}
