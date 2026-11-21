// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
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
			Coverage: model.DependencyCoverage{AnalysedAt: &later, ScanAttemptAt: &later, LockfilesReadAt: at, LockfilesUnreadReason: reason}})
		return &v
	}
	// Behind: an earlier reading exists.
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
	if len(model.LockfileUnreadReasons()) != 5 {
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
	if LockfileUnreadReasonWording("later_reason") != "later_reason" {
		t.Error("an unknown reason must be shown as written")
	}
	if got := CoverageLockfileCaution(&CoverageView{LockfilesUnread: &LockfilesUnread{Reason: "later_reason"}}); got != "Lockfiles could not be read (later_reason). Transitive dependencies are not checked." {
		t.Errorf("unknown reason: %q", got)
	}
}
