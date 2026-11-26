// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The coverage state (summary/70, 4.8): what is known about whether a
// repository's dependencies were read and checked. One function decides it
// for the API, the built-in tile and the showcase.

var (
	covStamp    = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	covAnalysed = time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	covAttempt  = time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
)

type covCase struct {
	outcome  string // "" = no scan half
	partial  bool
	analysed bool
	stamp    bool
	findings bool
	noRow    bool
}

func (c covCase) inputs() CoverageInputs {
	in := CoverageInputs{HasRow: !c.noRow, HasFindings: c.findings}
	if c.stamp {
		in.ScanStamp = &covStamp
	}
	if c.analysed {
		in.Coverage.AnalysedAt = &covAnalysed
	}
	if c.outcome != "" {
		in.Coverage.ScanAttemptAt = &covAttempt
		in.Coverage.Scan.Outcome, in.Coverage.Scan.Partial = c.outcome, c.partial
	}
	return in
}

// The plan's table, row by row: the first that applies wins.
func TestCoverageState(t *testing.T) {
	for _, c := range []struct {
		what string
		in   covCase
		want string
	}{
		{"sent, partial", covCase{outcome: model.ScanSent, partial: true, stamp: true}, CoverageScannedPartial},
		{"sent", covCase{outcome: model.ScanSent, stamp: true, analysed: true}, CoverageScanned},
		{"nothing to send", covCase{outcome: model.ScanNothingToSend, stamp: true}, CoverageNothingToScan},
		{"unsendable", covCase{outcome: model.ScanUnsendable}, CoverageNothingSendable},
		{"sent, nothing answered", covCase{outcome: model.ScanUnanswered, analysed: true}, CoverageNoAnswer},
		{"sent, nothing answered, with an older stamp", covCase{outcome: model.ScanUnanswered, stamp: true, findings: true, analysed: true}, CoverageNoAnswer},
		{"unsendable, with an older stamp", covCase{outcome: model.ScanUnsendable, stamp: true, findings: true}, CoverageNothingSendable},
		{"failed", covCase{outcome: model.ScanFailed, stamp: true, findings: true}, CoverageScanFailed},
		{"inventory incomplete, never analysed", covCase{outcome: model.ScanInventoryIncomplete}, CoverageNotAnalysed},
		{"inventory incomplete, analysed", covCase{outcome: model.ScanInventoryIncomplete, analysed: true}, CoverageAnalysedNotScanned},
		// A completed scan whose latest attempt was inventory_incomplete
		// still reads scanned, with its date.
		{"inventory incomplete, with a stamp", covCase{outcome: model.ScanInventoryIncomplete, stamp: true}, CoverageScanned},
		{"a stamp and no scan half", covCase{stamp: true, findings: true}, CoverageScanned},
		{"a stamp and no coverage row at all (a legacy repository)", covCase{stamp: true, noRow: true}, CoverageScanned},
		{"findings, no stamp, no scan half", covCase{findings: true}, CoverageScanHistoryUndated},
		{"findings, no stamp, inventory incomplete", covCase{outcome: model.ScanInventoryIncomplete, findings: true}, CoverageScanHistoryUndated},
		{"findings, no stamp, no row", covCase{findings: true, noRow: true}, CoverageScanHistoryUndated},
		{"analysed, no scan half", covCase{analysed: true}, CoverageAnalysedNotScanned},
		{"nothing at all", covCase{}, CoverageNotScanned},
		{"no row", covCase{noRow: true}, CoverageNotScanned},
		// An outcome a later release wrote is not a completed scan here.
		{"an outcome this release does not know, with a stamp", covCase{outcome: "something-new", stamp: true}, CoverageScanned},
		{"an outcome this release does not know, nothing else", covCase{outcome: "something-new"}, CoverageNotScanned},
	} {
		got := Coverage(c.in.inputs())
		if got.State != c.want {
			t.Errorf("%s: state %q, want %q", c.what, got.State, c.want)
		}
		if got.Partial != (c.want == CoverageScannedPartial) {
			t.Errorf("%s: partial = %v; it is true exactly for scanned_partial", c.what, got.Partial)
		}
		if CoverageStateWording(got.State) == "" {
			t.Errorf("%s: state %q has no wording", c.what, got.State)
		}
	}
}

// Every state has one wording, and each wording is its own.
func TestEveryCoverageStateHasItsOwnWording(t *testing.T) {
	seen := map[string]string{}
	for _, s := range CoverageStates {
		w := CoverageStateWording(s)
		if w == "" {
			t.Errorf("state %q has no wording", s)
		}
		if other, dup := seen[w]; dup {
			t.Errorf("states %q and %q share the wording %q", s, other, w)
		}
		seen[w] = s
	}
	// The plan's ten (summary/70, 4.8) and `no_answer`, added by review
	// round 1 (0.29.208): a scan whose every query went unanswered read
	// "no dependency could be sent" about dependencies that were all sent.
	if len(CoverageStates) != 11 {
		t.Errorf("%d states; want the plan's ten and no_answer", len(CoverageStates))
	}
	if CoverageStateWording("a-state-from-a-later-release") != CoverageNotKnownWording {
		t.Error("a state this release does not know must read as not known")
	}
	for _, id := range []string{SecondLineAttemptFailed, SecondLineNoInventory, SecondLineInventoryOlder} {
		if CoverageSecondLineWording(id) == "" {
			t.Errorf("second line %q has no wording", id)
		}
	}
}

// The three facts a surface needs, decided here so that no surface works
// them out again.
func TestCoverageFacts(t *testing.T) {
	yes := true
	no := false
	type want struct{ blank, clean, unknown bool }
	for _, c := range []struct {
		what    string
		in      covCase
		current int
		depsKey *bool
		want    want
	}{
		{"scanned, nothing found", covCase{outcome: model.ScanSent, stamp: true, analysed: true}, 0, &no, want{clean: true}},
		{"scanned, exposure", covCase{outcome: model.ScanSent, stamp: true, analysed: true, findings: true}, 3, &no, want{}},
		{"scanned, only own-package findings", covCase{outcome: model.ScanSent, stamp: true, analysed: true, findings: true}, 0, &no, want{clean: true}},
		{"nothing to scan", covCase{outcome: model.ScanNothingToSend, stamp: true, analysed: true}, 0, &no, want{clean: true}},
		// Partial is never a clean result: some packages were not checked.
		{"scanned partial, nothing found", covCase{outcome: model.ScanSent, partial: true, stamp: true, analysed: true}, 0, &no, want{}},
		// Nothing has ever said what it depends on: not clean, and said so.
		// And the count is blank (0.29.208): a 0 beside "scanned" would say
		// its dependencies were checked.
		{"scanned, dependencies unknown", covCase{outcome: model.ScanSent, stamp: true}, 0, &yes, want{blank: true, unknown: true}},
		{"scanned, dependencies unknown, exposure from own-package checks aside", covCase{outcome: model.ScanSent, stamp: true, findings: true}, 2, &yes, want{unknown: true}},
		// The last attempt was not a completed scan, or is one this release
		// does not know: an older stamp does not make it a clean result.
		{"inventory incomplete since, an earlier stamp", covCase{outcome: model.ScanInventoryIncomplete, stamp: true, analysed: true}, 0, &no, want{}},
		{"an outcome this release does not know, a stamp", covCase{outcome: "decided_later", stamp: true, analysed: true}, 0, &no, want{}},
		// No scan half at all (a row written before the scan half existed,
		// or no row): the stamp is all there is, and it stands.
		{"a stamp and no scan half", covCase{stamp: true, analysed: true}, 0, &no, want{clean: true}},
		{"a stamp and no record", covCase{stamp: true, noRow: true}, 0, nil, want{clean: true}},
		// The stored key is stale once an analysis completes.
		{"the stored key set, and since analysed", covCase{outcome: model.ScanSent, stamp: true, analysed: true}, 0, &yes, want{clean: true}},
		// A stamp written by a scan that sent nothing proves nothing: a
		// bare 0 would be a fabricated clean result.
		{"nothing sendable, a stamp, no exposure", covCase{outcome: model.ScanUnsendable, stamp: true, findings: true}, 0, &no, want{blank: true}},
		{"nothing sendable, exposure from earlier", covCase{outcome: model.ScanUnsendable, stamp: true, findings: true}, 2, &no, want{}},
		// The same for a scan whose every query went unanswered.
		{"nothing answered, a stamp, no exposure", covCase{outcome: model.ScanUnanswered, stamp: true, findings: true, analysed: true}, 0, &no, want{blank: true}},
		{"nothing answered, exposure from earlier", covCase{outcome: model.ScanUnanswered, stamp: true, findings: true, analysed: true}, 2, &no, want{}},
		{"never scanned", covCase{}, 0, nil, want{blank: true}},
		{"scan failed, nothing known", covCase{outcome: model.ScanFailed}, 0, nil, want{blank: true}},
		{"scan failed, earlier findings", covCase{outcome: model.ScanFailed, findings: true}, 0, nil, want{}},
		{"scan failed, an earlier stamp", covCase{outcome: model.ScanFailed, stamp: true}, 0, nil, want{}},
		{"findings and no completed scan", covCase{findings: true}, 4, nil, want{}},
		{"not analysed", covCase{outcome: model.ScanInventoryIncomplete}, 0, &yes, want{blank: true, unknown: true}},
	} {
		in := c.in.inputs()
		in.CurrentExposure = c.current
		in.Coverage.Scan.DependenciesUnknown = c.depsKey
		got := Coverage(in)
		if got.CountBlank != c.want.blank || got.CleanResult != c.want.clean || got.DependenciesUnknown != c.want.unknown {
			t.Errorf("%s (%s): count_blank %v clean_result %v dependencies_unknown %v; want %+v", c.what, got.State,
				got.CountBlank, got.CleanResult, got.DependenciesUnknown, c.want)
		}
		if got.CleanResult && got.CountBlank {
			t.Errorf("%s: both a clean result and a blank count", c.what)
		}
	}
}

// A failed attempt with no reason recorded has no empty parentheses.
func TestAFailedAttemptWithNoReasonHasNoParentheses(t *testing.T) {
	v := CoverageView{SecondLine: SecondLineAttemptFailed, LastAnalysisAttempt: &AnalysisAttempt{At: covAttempt}}
	if got := CoverageSecondLineText(&v); got != "last analysis attempt failed" {
		t.Errorf("no reason: %q", got)
	}
	v.LastAnalysisAttempt.Reason = "clone_unavailable"
	if got := CoverageSecondLineText(&v); got != "last analysis attempt failed ("+AnalysisAttemptReasonWording("clone_unavailable")+")" {
		t.Errorf("with a reason: %q", got)
	}
}

// The second line is beside the state, never a value of it.
func TestCoverageSecondLine(t *testing.T) {
	yes := true
	later := covAnalysed.Add(24 * time.Hour)
	earlier := covAnalysed.Add(-24 * time.Hour)
	for _, c := range []struct {
		what string
		mut  func(*CoverageInputs)
		want string
	}{
		{"a legacy repository: rows, a stamp, no coverage row", func(in *CoverageInputs) { *in = covCase{stamp: true, noRow: true}.inputs() }, ""},
		{"scanned and analysed", func(in *CoverageInputs) {}, ""},
		{"no analysis half and a failed attempt", func(in *CoverageInputs) {
			in.Coverage.AnalysedAt = nil
			in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &later, model.InventoryAttemptCloneUnavailable
			in.Coverage.Scan.DependenciesUnknown = &yes
		}, SecondLineAttemptFailed},
		{"dependencies unknown, scanned", func(in *CoverageInputs) {
			in.Coverage.AnalysedAt = nil
			in.Coverage.Scan.DependenciesUnknown = &yes
		}, SecondLineNoInventory},
		// not_analysed already says it.
		{"dependencies unknown, not analysed", func(in *CoverageInputs) {
			*in = covCase{outcome: model.ScanInventoryIncomplete}.inputs()
			in.Coverage.Scan.DependenciesUnknown = &yes
		}, ""},
		{"an attempt later than the inventory", func(in *CoverageInputs) {
			in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &later, model.InventoryAttemptReplaceFailed
		}, SecondLineInventoryOlder},
		{"an attempt before the inventory", func(in *CoverageInputs) {
			in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &earlier, model.InventoryAttemptReplaceFailed
		}, ""},
	} {
		in := covCase{outcome: model.ScanSent, stamp: true, analysed: true}.inputs()
		c.mut(&in)
		got := Coverage(in)
		if got.SecondLine != c.want {
			t.Errorf("%s: second line %q, want %q (state %s)", c.what, got.SecondLine, c.want, got.State)
		}
	}
}

// What the object carries beside the decisions.
func TestCoverageCarriesItsDatesAndCounts(t *testing.T) {
	in := covCase{outcome: model.ScanSent, stamp: true, analysed: true}.inputs()
	in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &covAttempt, model.InventoryAttemptRootUnreadable
	in.Coverage.Inventory.ManifestsFailed = []model.ManifestFailure{
		{Path: "a/pom.xml", RowsCarried: 3}, {Path: "b/package.json"}, {Path: "c/setup.py", RowsCarried: 1}}
	in.Coverage.LockfilesFailed = []model.ManifestFailure{{Path: "yarn.lock", RowsCarried: 9}, {Path: "x/go.sum"}}
	in.Coverage.Inventory.DependenciesWithoutRow = map[string]int{model.NoRowNonRegistry: 2}
	got := Coverage(in)
	if got.LastCompletedScanAt == nil || !got.LastCompletedScanAt.Equal(covStamp) || !got.LastScanAttemptAt.Equal(covAttempt) || !got.AnalysedAt.Equal(covAnalysed) {
		t.Errorf("dates: %+v", got)
	}
	if got.LastAnalysisAttempt == nil || !got.LastAnalysisAttempt.At.Equal(covAttempt) || got.LastAnalysisAttempt.Reason != model.InventoryAttemptRootUnreadable {
		t.Errorf("last analysis attempt: %+v", got.LastAnalysisAttempt)
	}
	if got.ManifestsFailed != 3 || got.ManifestsCarried != 2 || got.LockfilesCarried != 1 || got.DependenciesWithoutRow[model.NoRowNonRegistry] != 2 {
		t.Errorf("counts: failed %d carried %d lockfiles carried %d without row %v", got.ManifestsFailed, got.ManifestsCarried, got.LockfilesCarried, got.DependenciesWithoutRow)
	}
	if len(got.ManifestPathsFailed) != 3 || got.ManifestPathsFailed[0] != "a/pom.xml" || len(got.ManifestPathsCarried) != 2 || got.ManifestPathsCarried[1] != "c/setup.py" {
		t.Errorf("paths: failed %v carried %v", got.ManifestPathsFailed, got.ManifestPathsCarried)
	}
	// Nothing recorded: no attempt object, and empty maps and lists that
	// are not nil (a client reads them without a null check).
	bare := Coverage(covCase{}.inputs())
	if bare.LastAnalysisAttempt != nil || bare.DependenciesWithoutRow == nil || bare.ManifestPathsFailed == nil || bare.ManifestPathsCarried == nil {
		t.Errorf("a repository with nothing recorded: %+v", bare)
	}
}

// HasRow false means there is no record: whatever the struct holds is not
// read (a caller that passes a zero-valued or stale record cannot make a
// repository read as scanned).
func TestCoverageIgnoresARecordThatIsNotThere(t *testing.T) {
	in := covCase{outcome: model.ScanSent, analysed: true}.inputs()
	in.HasRow = false
	if got := Coverage(in); got.State != CoverageNotScanned || got.AnalysedAt != nil || got.LastScanAttemptAt != nil {
		t.Errorf("with no record: %+v", got)
	}
}

// What a surface writes under a count, from the view alone (0.29.200): no
// surface puts the state, the date and the second line together itself.
func TestCoverageLines(t *testing.T) {
	line := func(c covCase) string { v := Coverage(c.inputs()); return CoverageLine(&v) }
	for what, got := range map[string][2]string{
		"scanned":                        {line(covCase{outcome: model.ScanSent, stamp: true, analysed: true}), "scanned 2026-11-01"},
		"scanned partial":                {line(covCase{outcome: model.ScanSent, partial: true, stamp: true}), "scanned; some packages not fully checked 2026-11-01"},
		"never scanned":                  {line(covCase{}), "not yet scanned"},
		"failed, with an earlier scan":   {line(covCase{outcome: model.ScanFailed, stamp: true}), "scan failed; last completed scan 2026-11-01"},
		"failed, with none":              {line(covCase{outcome: model.ScanFailed}), "scan failed"},
		"nothing sendable, with a stamp": {line(covCase{outcome: model.ScanUnsendable, stamp: true}), "no dependency could be sent for checking; last completed scan 2026-11-01"},
		"no view":                        {CoverageLine(nil), CoverageNotKnownWording},
	} {
		if got[0] != got[1] {
			t.Errorf("%s: %q, want %q", what, got[0], got[1])
		}
	}
	// A state from a later release reads as not known, with its date kept.
	later := Coverage(covCase{outcome: model.ScanSent, stamp: true}.inputs())
	later.State = "a-state-from-a-later-release"
	if got := CoverageLine(&later); got != CoverageNotKnownWording+"; last completed scan 2026-11-01" {
		t.Errorf("an unknown state: %q", got)
	}

	yes := true
	in := covCase{outcome: model.ScanSent, stamp: true}.inputs()
	in.Coverage.Scan.DependenciesUnknown = &yes
	v := Coverage(in)
	if CoverageSecondLineText(&v) != "no dependency inventory recorded" || CoverageAbsenceClause(&v) != "no dependency inventory recorded" {
		t.Errorf("dependencies unknown: second line %q, clause %q", CoverageSecondLineText(&v), CoverageAbsenceClause(&v))
	}
	in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &covAttempt, model.InventoryAttemptCloneUnavailable
	v = Coverage(in)
	if got := CoverageSecondLineText(&v); got != "last analysis attempt failed (no clone to analyse)" {
		t.Errorf("a failed attempt: %q", got)
	}
	clean := Coverage(covCase{outcome: model.ScanSent, stamp: true, analysed: true}.inputs())
	if CoverageSecondLineText(&clean) != "" || CoverageSecondLineText(nil) != "" {
		t.Error("no second line where none applies")
	}
	failed := Coverage(covCase{outcome: model.ScanFailed}.inputs())
	if CoverageAbsenceClause(&failed) != "scan failed" || CoverageAbsenceClause(nil) != CoverageNotKnownWording {
		t.Errorf("clauses: %q, %q", CoverageAbsenceClause(&failed), CoverageAbsenceClause(nil))
	}
	// Every result the store accepts (model's list), so that a new one
	// cannot be recorded and then shown as its raw value.
	if len(model.InventoryAttemptResults()) < 7 {
		t.Fatalf("model lists %d attempt results", len(model.InventoryAttemptResults()))
	}
	for _, r := range model.InventoryAttemptResults() {
		if AnalysisAttemptReasonWording(r) == "" || AnalysisAttemptReasonWording(r) == r {
			t.Errorf("attempt reason %q has no wording", r)
		}
	}
}
