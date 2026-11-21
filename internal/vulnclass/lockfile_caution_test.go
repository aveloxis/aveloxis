// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The lockfile caution (0.29.214; operator, 2026-11-20): a repository whose
// lockfiles went unread at the last analysis says so on its pages, in one of
// two sentences; every other repository says nothing.
func TestTheLockfileCautionIsSaidInTwoCasesAndNoOther(t *testing.T) {
	read := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	view := func(at *time.Time, reason string, hasRow bool) *CoverageView {
		// Analysed and scanned later than the lockfiles were read: neither
		// date is the lockfiles'.
		later := read.Add(48 * time.Hour)
		v := Coverage(CoverageInputs{HasRow: hasRow, ScanStamp: &later,
			Coverage: model.DependencyCoverage{AnalysedAt: &later, ScanAttemptAt: &later, LockfilesLastRead: at, LockfilesUnreadReason: reason}})
		return &v
	}
	// Behind: lockfile rows are stored, read on that date.
	v := view(&read, model.InventoryAttemptReadMemoryLimit, true)
	if v.LockfilesUnread == nil || v.LockfilesUnread.Reason != model.InventoryAttemptReadMemoryLimit || v.LockfilesUnread.LastReadAt == nil || !v.LockfilesUnread.LastReadAt.Equal(read) {
		t.Fatalf("behind: %+v", v.LockfilesUnread)
	}
	if got, want := CoverageLockfileCaution(v), "Lockfiles last read 2026-11-02; the latest read did not finish (reading them took more memory than allowed). Versions and transitive dependencies may be out of date."; got != want {
		t.Errorf("behind:\n got %q\nwant %q", got, want)
	}
	// Never read.
	v = view(nil, model.InventoryAttemptReadCPULimit, true)
	if got, want := CoverageLockfileCaution(v), "Lockfiles could not be read (reading them took more processor time than allowed). Transitive dependencies are not checked."; got != want {
		t.Errorf("never read:\n got %q\nwant %q", got, want)
	}
	if v.LockfilesUnread == nil || v.LockfilesUnread.LastReadAt != nil {
		t.Errorf("never read: %+v; want a reason and no date", v.LockfilesUnread)
	}
	// Nothing otherwise: read at the last analysis; no record at all; no
	// lockfiles; a nil view; a reason on a row the reader says it has not.
	for name, v := range map[string]*CoverageView{
		"read":            view(&read, "", true),
		"nothing known":   view(nil, "", true),
		"no record":       view(&read, model.InventoryAttemptReadFailed, false),
		"nil":             nil,
		"an older answer": {},
	} {
		if got := CoverageLockfileCaution(v); got != "" {
			t.Errorf("%s: a caution where none applies: %q", name, got)
		}
		if v != nil && v.LockfilesUnread != nil {
			t.Errorf("%s: the view carries %+v", name, v.LockfilesUnread)
		}
	}
}

// Every reason the store accepts has words of its own, about lockfiles
// ("them"), and none is shown as its stored value. A reason this release
// does not know is shown as written.
func TestEveryLockfileUnreadReasonHasItsWording(t *testing.T) {
	if len(model.LockfileUnreadReasons()) != 6 {
		t.Fatalf("model lists %d reasons", len(model.LockfileUnreadReasons()))
	}
	seen := map[string]bool{}
	for _, r := range model.LockfileUnreadReasons() {
		w := LockfileUnreadReasonWording(r)
		if w == "" || w == r || seen[w] {
			t.Errorf("reason %q has no wording of its own: %q", r, w)
		}
		if strings.Contains(w, "manifest") {
			t.Errorf("reason %q is worded for manifests: %q", r, w)
		}
		seen[w] = true
	}
	// Each reason reads as a sentence inside both cautions, and neither
	// caution says the same thing twice (round 8: "Lockfiles could not be
	// read (they could not be read).").
	read := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for _, r := range model.LockfileUnreadReasons() {
		for _, at := range []*time.Time{nil, &read} {
			got := CoverageLockfileCaution(&CoverageView{LockfilesUnread: &LockfilesUnread{Reason: r, LastReadAt: at}})
			inside := got[strings.Index(got, "(")+1 : strings.Index(got, ")")]
			if inside != LockfileUnreadReasonWording(r) || strings.Contains(strings.ToLower(got[:strings.Index(got, "(")]), strings.ToLower(inside)) {
				t.Errorf("reason %q: the caution repeats itself or lost its reason: %q", r, got)
			}
		}
	}
	if got, want := CoverageLockfileCaution(&CoverageView{LockfilesUnread: &LockfilesUnread{Reason: model.InventoryAttemptReadFailed}}), "Lockfiles could not be read (the reader failed). Transitive dependencies are not checked."; got != want {
		t.Errorf("never read, the reader failed:\n got %q\nwant %q", got, want)
	}
	if got, want := CoverageLockfileCaution(&CoverageView{LockfilesUnread: &LockfilesUnread{Reason: model.LockfileUnreadNotReached, LastReadAt: &read}}), "Lockfiles last read 2026-11-02; the latest read did not finish (the reader was stopped before it reached them). Versions and transitive dependencies may be out of date."; got != want {
		t.Errorf("behind, not reached:\n got %q\nwant %q", got, want)
	}
	if LockfileUnreadReasonWording("later_reason") != "later_reason" {
		t.Error("an unknown reason must be shown as written")
	}
	if got := CoverageLockfileCaution(&CoverageView{LockfilesUnread: &LockfilesUnread{Reason: "later_reason"}}); got != "Lockfiles could not be read (later_reason). Transitive dependencies are not checked." {
		t.Errorf("unknown reason: %q", got)
	}
}

// The date is the day in UTC, on the page and in the API, whatever zone the
// time arrived in (the database driver hands times in the local zone): 23:30
// in New York on the 1st is the 2nd.
func TestTheLockfileCautionDatesInUTC(t *testing.T) {
	ny := time.FixedZone("EST", -5*3600)
	local := time.Date(2026, 11, 1, 23, 30, 0, 0, ny)
	later := local.Add(48 * time.Hour)
	v := Coverage(CoverageInputs{HasRow: true, ScanStamp: &later,
		Coverage: model.DependencyCoverage{AnalysedAt: &later, ScanAttemptAt: &later, LockfilesLastRead: &local, LockfilesUnreadReason: model.InventoryAttemptReadTimedOut}})
	if got := CoverageLockfileCaution(&v); !strings.HasPrefix(got, "Lockfiles last read 2026-11-02;") {
		t.Errorf("the date is not the UTC day: %q", got)
	}
	// And a view built elsewhere (an API client's, a test's) with a local
	// time: the sentence still prints the UTC day.
	if got := CoverageLockfileCaution(&CoverageView{LockfilesUnread: &LockfilesUnread{Reason: "x", LastReadAt: &local}}); !strings.HasPrefix(got, "Lockfiles last read 2026-11-02;") {
		t.Errorf("the sentence's date is not the UTC day: %q", got)
	}
	body, err := json.Marshal(v.LockfilesUnread)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(body), `{"reason":"manifest_read_timed_out","last_read_at":"2026-11-02T04:30:00Z"}`; got != want {
		t.Errorf("the API object:\n got %s\nwant %s", got, want)
	}
}

// last_read_at is ALWAYS sent: null when no lockfile row is stored. A
// client tells the two cautions apart by it, so an absent key and a null
// must not be two things.
func TestTheLockfilesUnreadObjectAlwaysCarriesItsDate(t *testing.T) {
	body, err := json.Marshal(LockfilesUnread{Reason: model.InventoryAttemptReadFailed})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(body), `{"reason":"manifest_read_failed","last_read_at":null}`; got != want {
		t.Errorf("never read:\n got %s\nwant %s", got, want)
	}
	// And the view always carries the key, null when there is no caution.
	whole, err := json.Marshal(CoverageView{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(whole), `"lockfiles_unread":null`) {
		t.Errorf("a view with no caution does not send lockfiles_unread: %s", whole)
	}
}
