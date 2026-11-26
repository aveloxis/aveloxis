// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import (
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The coverage state (v0.29.197; summary/70, 4.8): what is known about
// whether a repository's dependencies were read and checked. Coverage is THE
// function that decides it; the API, the built-in tile and the showcase call
// it and draw what it returns, so that a repository never reads "scanned" on
// one page and "not yet scanned" on another. It returns a value in every
// case. It reads nothing: internal/db reads its inputs.

// The states. Each has one wording (CoverageStateWording).
const (
	CoverageScannedPartial     = "scanned_partial"
	CoverageScanned            = "scanned"
	CoverageNothingToScan      = "nothing_to_scan"
	CoverageNothingSendable    = "nothing_sendable"
	CoverageNoAnswer           = "no_answer"
	CoverageScanFailed         = "scan_failed"
	CoverageNotAnalysed        = "not_analysed"
	CoverageAnalysedNotScanned = "analysed_not_scanned"
	CoverageScanHistoryUndated = "scan_history_undated"
	CoverageNotScanned         = "not_scanned"
)

// CoverageNotKnown is not a state Coverage returns. It is what a surface
// shows when it has no state: an input could not be read, or the state is
// one a later release wrote.
const CoverageNotKnown = "not_known"

// CoverageStates are the states and CoverageNotKnown, in the order the
// wordings are listed.
var CoverageStates = []string{
	CoverageScannedPartial, CoverageScanned, CoverageNothingToScan, CoverageNothingSendable, CoverageNoAnswer, CoverageScanFailed,
	CoverageNotAnalysed, CoverageAnalysedNotScanned, CoverageScanHistoryUndated, CoverageNotScanned, CoverageNotKnown,
}

// CoverageNotKnownWording is shown when there is no state to show.
const CoverageNotKnownWording = "coverage not known"

var coverageStateWordings = map[string]string{
	CoverageScannedPartial:     "scanned; some packages not fully checked",
	CoverageScanned:            "scanned",
	CoverageNothingToScan:      "nothing to scan",
	CoverageNothingSendable:    "no dependency could be sent for checking",
	CoverageNoAnswer:           "sent for checking; no complete answer came back",
	CoverageScanFailed:         "scan failed",
	CoverageNotAnalysed:        "not yet analysed",
	CoverageAnalysedNotScanned: "analysed, not yet scanned",
	CoverageScanHistoryUndated: "findings present, no completed scan recorded",
	CoverageNotScanned:         "not yet scanned",
	CoverageNotKnown:           CoverageNotKnownWording,
}

// CoverageStateWording is the one wording of a state, on every surface. A
// state this release does not know reads as not known.
func CoverageStateWording(state string) string {
	if w, ok := coverageStateWordings[state]; ok {
		return w
	}
	return CoverageNotKnownWording
}

// The second line: shown beside the state, never a value of it.
const (
	// SecondLineAttemptFailed: no inventory has been recorded and the last
	// analysis attempt failed.
	SecondLineAttemptFailed = "analysis_attempt_failed"
	// SecondLineNoInventory: nothing has ever said what the repository
	// depends on.
	SecondLineNoInventory = "no_inventory_recorded"
	// SecondLineInventoryOlder: the recorded inventory is older than the
	// last analysis attempt, which failed.
	SecondLineInventoryOlder = "inventory_older_than_attempt"
)

var coverageSecondLineWordings = map[string]string{
	SecondLineAttemptFailed:  "last analysis attempt failed",
	SecondLineNoInventory:    "no dependency inventory recorded",
	SecondLineInventoryOlder: "inventory older than the last attempt",
}

// CoverageSecondLineWording is the wording of a second line; "" for none.
func CoverageSecondLineWording(id string) string { return coverageSecondLineWordings[id] }

// CoverageInputs is everything the decision reads.
type CoverageInputs struct {
	// Coverage is the repository's coverage record; HasRow is false when it
	// has none (every repository analysed and scanned only before the
	// record existed).
	Coverage model.DependencyCoverage
	HasRow   bool
	// ScanStamp is the completed-scan stamp (repos.vuln_scan_last_run).
	ScanStamp *time.Time
	// CurrentExposure is the count of current exposure findings
	// (counts.current).
	CurrentExposure int
	// HasFindings: the repository has a finding row of any class, closed
	// and own-package included.
	HasFindings bool
}

// AnalysisAttempt is the last analysis that did not replace the inventory.
type AnalysisAttempt struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

// CoverageView is the decision and what a surface draws with it. Its JSON
// form is the API's `coverage` object.
type CoverageView struct {
	State string `json:"state"`
	// Partial is true exactly for scanned_partial.
	Partial bool `json:"partial"`
	// LastCompletedScanAt is the stamp; LastScanAttemptAt the scan half's
	// time, written on every outcome.
	LastCompletedScanAt *time.Time       `json:"last_completed_scan_at"`
	LastScanAttemptAt   *time.Time       `json:"last_scan_attempt_at"`
	AnalysedAt          *time.Time       `json:"analysed_at"`
	LastAnalysisAttempt *AnalysisAttempt `json:"last_analysis_attempt"`
	// ManifestsFailed counts the manifests that could not be read or
	// parsed; ManifestsCarried those of them whose earlier rows were kept.
	ManifestsFailed  int `json:"manifests_failed"`
	ManifestsCarried int `json:"manifests_carried"`
	LockfilesCarried int `json:"lockfiles_carried"`
	// DependenciesWithoutRow counts the declared dependencies that have no
	// inventory row, by the kind of reason.
	DependenciesWithoutRow map[string]int `json:"dependencies_without_row"`
	// SecondLine is which second line applies, by identifier; "" for none.
	SecondLine string `json:"second_line"`
	// DependenciesUnknown: nothing has ever said what the repository
	// depends on. CleanResult: a sentence that says there is no exposure at
	// a known version is true. CountBlank: a count of findings is shown
	// blank, because a 0 would claim a check that did not happen.
	DependenciesUnknown bool `json:"dependencies_unknown"`
	CleanResult         bool `json:"clean_result"`
	CountBlank          bool `json:"count_blank"`
	// The paths behind the two manifest counts. Sent on the list answer
	// only; the statistics answers leave them out.
	ManifestPathsFailed  []string `json:"manifest_paths_failed,omitempty"`
	ManifestPathsCarried []string `json:"manifest_paths_carried,omitempty"`
	// LockfilesUnread (0.29.214) is set when the repository's lockfiles
	// went unread at the last analysis, and null otherwise (they were read,
	// the repository has none, or nothing is known). It is a caution beside
	// the state, not a state: the state says what the scan did, and this
	// says the versions and transitive dependencies it worked from are out
	// of date or missing. CoverageLockfileCaution is its sentence.
	LockfilesUnread *LockfilesUnread `json:"lockfiles_unread"`
}

// LockfilesUnread is why the latest reading of a repository's lockfiles did
// not finish, and when the lockfile rows that are stored were read: null
// when none is stored. The key is always sent.
type LockfilesUnread struct {
	Reason     string     `json:"reason"`
	LastReadAt *time.Time `json:"last_read_at"`
}

// Coverage decides. The first row of the plan's table that applies wins.
func Coverage(in CoverageInputs) CoverageView {
	cov := in.Coverage
	outcome := ""
	if in.HasRow && cov.ScanAttemptAt != nil {
		outcome = cov.Scan.Outcome
	}
	analysed := in.HasRow && cov.AnalysedAt != nil
	stamp := in.ScanStamp != nil

	v := CoverageView{
		LastCompletedScanAt: in.ScanStamp, DependenciesWithoutRow: map[string]int{},
		ManifestPathsFailed: []string{}, ManifestPathsCarried: []string{},
	}
	switch {
	case outcome == model.ScanSent && cov.Scan.Partial:
		v.State = CoverageScannedPartial
	case outcome == model.ScanSent:
		v.State = CoverageScanned
	case outcome == model.ScanNothingToSend:
		v.State = CoverageNothingToScan
	case outcome == model.ScanUnsendable:
		v.State = CoverageNothingSendable
	case outcome == model.ScanUnanswered:
		v.State = CoverageNoAnswer
	case outcome == model.ScanFailed:
		v.State = CoverageScanFailed
	case outcome == model.ScanInventoryIncomplete && !stamp && !in.HasFindings && !analysed:
		v.State = CoverageNotAnalysed
	case outcome == model.ScanInventoryIncomplete && !stamp && !in.HasFindings:
		v.State = CoverageAnalysedNotScanned
	// From here: no scan half, an outcome of inventory_incomplete, or an
	// outcome this release does not know. None is a completed scan, so the
	// stamp and the findings are what is left to go on.
	case stamp:
		v.State = CoverageScanned
	case in.HasFindings:
		v.State = CoverageScanHistoryUndated
	case analysed:
		v.State = CoverageAnalysedNotScanned
	default:
		v.State = CoverageNotScanned
	}
	v.Partial = v.State == CoverageScannedPartial

	if in.HasRow {
		v.LastScanAttemptAt, v.AnalysedAt = cov.ScanAttemptAt, cov.AnalysedAt
		if cov.LastAttemptAt != nil {
			v.LastAnalysisAttempt = &AnalysisAttempt{At: *cov.LastAttemptAt, Reason: cov.LastAttemptResult}
		}
		for _, f := range cov.Inventory.ManifestsFailed {
			v.ManifestsFailed++
			v.ManifestPathsFailed = append(v.ManifestPathsFailed, f.Path)
			if f.RowsCarried > 0 {
				v.ManifestsCarried++
				v.ManifestPathsCarried = append(v.ManifestPathsCarried, f.Path)
			}
		}
		for _, f := range cov.LockfilesFailed {
			if f.RowsCarried > 0 {
				v.LockfilesCarried++
			}
		}
		for kind, n := range cov.Inventory.DependenciesWithoutRow {
			v.DependenciesWithoutRow[kind] = n
		}
		if cov.LockfilesUnreadReason != "" {
			v.LockfilesUnread = &LockfilesUnread{Reason: cov.LockfilesUnreadReason, LastReadAt: utcOrNil(cov.LockfilesLastRead)}
		}
	}

	// The stored key is stale between an analysis that completes and the
	// next scan: the fact is the key AND still no analysis half.
	v.DependenciesUnknown = in.HasRow && cov.Scan.DependenciesUnknown != nil && *cov.Scan.DependenciesUnknown && !analysed

	// A count is blank where a 0 would be a fabricated clean result: nothing
	// found and no completed scan, in a state that is not itself a completed
	// check; or there is no exposure to show and either nothing could be
	// sent, nothing was answered, or nothing has ever said what the
	// repository depends on,
	// whatever a stamp says (the second since 0.29.208: such a repository
	// read "0, scanned").
	completedCheck := v.State == CoverageScanned || v.State == CoverageScannedPartial || v.State == CoverageNothingToScan
	v.CountBlank = (!in.HasFindings && !stamp && !completedCheck) ||
		((v.State == CoverageNothingSendable || v.State == CoverageNoAnswer || v.DependenciesUnknown) && in.CurrentExposure == 0)

	// A clean result needs the LAST attempt to have been a completed scan,
	// or no scan half at all (then the stamp is all there is). An attempt
	// that found the inventory incomplete, or an outcome this release does
	// not know, is neither, and an older stamp does not stand in for it
	// (0.29.208).
	lastAttemptCompleted := outcome == "" || outcome == model.ScanSent || outcome == model.ScanNothingToSend
	//
	// Nor is a repository whose lockfiles went unread shown to be clean
	// (operator, 2026-11-21: "Withdraw, keep the count"): its transitive
	// dependencies were not checked, or were checked at versions that may
	// be out of date. The count beside it is still a true count of what
	// was checked, so CountBlank above does not look at the caution.
	v.CleanResult = (v.State == CoverageScanned || v.State == CoverageNothingToScan) && lastAttemptCompleted &&
		!v.DependenciesUnknown && in.CurrentExposure == 0 && v.LockfilesUnread == nil

	switch {
	case !analysed && v.LastAnalysisAttempt != nil:
		v.SecondLine = SecondLineAttemptFailed
	case v.DependenciesUnknown && v.State != CoverageNotAnalysed:
		v.SecondLine = SecondLineNoInventory
	case analysed && v.LastAnalysisAttempt != nil && v.LastAnalysisAttempt.At.After(*cov.AnalysedAt):
		v.SecondLine = SecondLineInventoryOlder
	}
	return v
}

var analysisAttemptReasonWordings = map[string]string{
	model.InventoryAttemptCloneUnavailable: "no clone to analyse",
	model.InventoryAttemptRootUnreadable:   "the clone could not be read",
	model.InventoryAttemptReplaceFailed:    "the inventory could not be stored",
	// 0.29.213: the manifests are read by a process of their own, with
	// limits; these are the four ways it can end without a result.
	model.InventoryAttemptReadCPULimit:    "reading the manifests took more processor time than allowed",
	model.InventoryAttemptReadMemoryLimit: "reading the manifests took more memory than allowed",
	model.InventoryAttemptReadTimedOut:    "reading the manifests took too long",
	model.InventoryAttemptReadFailed:      "the manifests could not be read",
}

// AnalysisAttemptReasonWording is the wording of why an analysis did not
// replace the inventory. A reason this release does not know is shown as
// written.
func AnalysisAttemptReasonWording(reason string) string {
	if w, ok := analysisAttemptReasonWordings[reason]; ok {
		return w
	}
	return reason
}

// lockfileUnreadReasonWordings are the reasons a reading of the lockfiles
// did not finish, in words about lockfiles ("them": the sentence's subject).
// The inventory's attempt wordings say "the manifests", which a lockfile is
// not.
var lockfileUnreadReasonWordings = map[string]string{
	model.InventoryAttemptReadCPULimit:    "reading them took more processor time than allowed",
	model.InventoryAttemptReadMemoryLimit: "reading them took more memory than allowed",
	model.InventoryAttemptReadTimedOut:    "reading them took too long",
	// Not "they could not be read": the never-read sentence already says
	// that, and read "Lockfiles could not be read (they could not be read)."
	model.InventoryAttemptReadFailed:     "the reader failed",
	model.InventoryAttemptRootUnreadable: "the clone could not be read",
	model.LockfileUnreadNotReached:       "the reader was stopped before it reached them",
	model.LockfileUnreadTooLarge:         "reading them would take more memory than allowed",
}

// LockfileUnreadReasonWording is the wording of why the lockfiles went
// unread. A reason this release does not know is shown as written.
func LockfileUnreadReasonWording(reason string) string {
	if w, ok := lockfileUnreadReasonWordings[reason]; ok {
		return w
	}
	return reason
}

// CoverageLockfileCaution is the caution a surface shows when a
// repository's lockfiles went unread at the last analysis, and "" when
// there is nothing to caution about (operator, 2026-11-20: a caution in
// these two cases and nothing otherwise). The two sentences:
//
//   - lockfile rows are stored, so they are as old as the date;
//   - none is stored, so transitive dependencies are not checked.
//
// Which of the two is the store's to decide, from what is stored
// (model.DependencyCoverage.LockfilesLastRead).
//
// One function, so that every surface says the same thing; the web client
// builds the same sentences from the API's lockfiles_unread object.
func CoverageLockfileCaution(v *CoverageView) string {
	if v == nil || v.LockfilesUnread == nil {
		return ""
	}
	reason := LockfileUnreadReasonWording(v.LockfilesUnread.Reason)
	p := lockfileCautionParts
	// Some lockfiles too large, the rest read at this analysis: no date is
	// said, because no one date is true of the rows (0.29.216). Only when
	// rows are stored: with none there is nothing to be out of date, and
	// the "could not be read" sentence below is the true one (0.29.217,
	// review round 10).
	if v.LockfilesUnread.Reason == model.LockfileUnreadTooLarge && v.LockfilesUnread.LastReadAt != nil {
		return p["some_before_reason"] + reason + p["some_after_reason"]
	}
	if at := v.LockfilesUnread.LastReadAt; at != nil {
		return p["behind_before_date"] + at.UTC().Format(coverageDate) + p["behind_before_reason"] + reason + p["behind_after_reason"]
	}
	return p["never_before_reason"] + reason + p["never_after_reason"]
}

// lockfileCautionParts are the fixed words of the two cautions, around the
// date and the reason. A map, like the wording maps above, so that the web
// client's copy (aveloxis-gui lib/wordings.js lockfileCautionParts) is held
// to it word for word by the same cross-repository test (round 8: the two
// sentences were written out twice and compared by nothing).
var lockfileCautionParts = map[string]string{
	"behind_before_date":   "Lockfiles last read ",
	"behind_before_reason": "; the latest read did not finish (",
	"behind_after_reason":  "). Versions and transitive dependencies may be out of date.",
	"never_before_reason":  "Lockfiles could not be read (",
	"never_after_reason":   "). Transitive dependencies are not checked.",
	"some_before_reason":   "Some lockfiles could not be read (",
	"some_after_reason":    "). Their versions and transitive dependencies may be out of date.",
}

// utcOrNil is the time in UTC: the date a surface prints, and the instant
// the API sends, do not depend on the zone the database driver handed it in.
func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

const coverageDate = "2006-01-02"

// CoverageLine is the line a surface writes under a count of findings: the
// state in words, with the date of the last completed scan. For a state
// that is itself a completed scan the date follows it ("scanned
// 2026-11-01"); for any other it is named ("scan failed; last completed
// scan 2026-11-01"). A nil view, or a state this release does not know,
// reads as not known.
func CoverageLine(v *CoverageView) string {
	if v == nil {
		return CoverageNotKnownWording
	}
	line := CoverageStateWording(v.State)
	if v.LastCompletedScanAt == nil {
		return line
	}
	date := v.LastCompletedScanAt.UTC().Format(coverageDate)
	if v.State == CoverageScanned || v.State == CoverageScannedPartial {
		return line + " " + date
	}
	return line + "; last completed scan " + date
}

// CoverageSecondLineText is the second line in words, with the reason of a
// failed attempt; "" when none applies.
func CoverageSecondLineText(v *CoverageView) string {
	if v == nil || v.SecondLine == "" {
		return ""
	}
	text := CoverageSecondLineWording(v.SecondLine)
	// The reason in parentheses, when one was recorded: an attempt with
	// none read "last analysis attempt failed ()".
	if v.SecondLine == SecondLineAttemptFailed && v.LastAnalysisAttempt != nil {
		if reason := AnalysisAttemptReasonWording(v.LastAnalysisAttempt.Reason); reason != "" {
			text += " (" + reason + ")"
		}
	}
	return text
}

// CoverageAbsenceClause is what a sentence says in place of "no
// vulnerabilities" when CleanResult is false and there is no exposure to
// name: the state's wording, or, for a repository that was scanned with
// nothing ever having said what it depends on, that fact.
//
// One case reads thinly and is left so (review round 2): the state is
// `scanned` by an older stamp while the last attempt was not a completed
// scan, so CleanResult is false and the clause is just "scanned". That
// holds between an analysis completing and the scan that follows it in the
// same collection job, or after a downgrade; the next scan settles it.
func CoverageAbsenceClause(v *CoverageView) string {
	if v == nil {
		return CoverageNotKnownWording
	}
	if v.State == CoverageScanned && v.DependenciesUnknown {
		return CoverageSecondLineWording(SecondLineNoInventory)
	}
	return CoverageStateWording(v.State)
}
