// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package linking

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// fakeStore records what linking asked of the store, in order.
type fakeStore struct {
	calls        []string
	inserted     []*model.VulnerabilityRow
	seen         []string
	preserve     []string
	selfOnly     bool
	reasons      map[string]string
	guard        model.DecisionGuard
	askedFor     map[string]time.Time
	decidedAfter []string
	decidedErr   error
	open         []model.OpenFinding
	notes        []model.InventoryNote
	openErr      error
	notesErr     error
	closed       int64
	insertErr    error
	closeErr     error
	markErr      error
}

func (f *fakeStore) InsertVulnerabilityBatch(_ context.Context, _ int64, rows []*model.VulnerabilityRow) error {
	f.calls = append(f.calls, "insert")
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserted = rows
	return nil
}

func (f *fakeStore) GetOpenVulnerabilityFindings(context.Context, int64) ([]model.OpenFinding, error) {
	return f.open, f.openErr
}

func (f *fakeStore) GetRepoInventoryNotes(context.Context, int64) ([]model.InventoryNote, error) {
	return f.notes, f.notesErr
}

func (f *fakeStore) GetFindingsDecidedAfter(_ context.Context, _ int64, askedAt map[string]time.Time) ([]string, error) {
	f.askedFor = askedAt
	return f.decidedAfter, f.decidedErr
}

func (f *fakeStore) MarkStaleVulnerabilitiesResolved(_ context.Context, _ int64, seen, preserve []string, selfOnly bool, reasons map[string]string, guard model.DecisionGuard) (int64, error) {
	f.calls = append(f.calls, "close")
	f.seen, f.preserve, f.selfOnly, f.reasons, f.guard = seen, preserve, selfOnly, reasons, guard
	if f.closeErr != nil {
		return 0, f.closeErr
	}
	return f.closed, nil
}

func (f *fakeStore) SetVulnScanLastRun(context.Context, int64) error {
	f.calls = append(f.calls, "mark")
	return f.markErr
}

func (f *fakeStore) did() string { return strings.Join(f.calls, ",") }

func finding(id, purl string) *model.VulnerabilityRow {
	return &model.VulnerabilityRow{VulnID: id, PackagePurl: purl}
}

// The four exits that are not errors, each by what it closes and marks.

func TestNothingToScanClosesEverythingAndMarks(t *testing.T) {
	s := &fakeStore{closed: 4}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{})
	if asked || out.Exit != NothingToScan || out.Closed != 4 || !out.Marked || out.CloseErr != nil || out.MarkErr != nil {
		t.Errorf("outcome %+v asked %v", out, asked)
	}
	if s.did() != "close,mark" || len(s.seen) != 0 || len(s.preserve) != 0 {
		t.Errorf("store calls %q, seen %v, preserve %v; want close,mark with nothing seen and nothing preserved", s.did(), s.seen, s.preserve)
	}
}

func TestNothingSendableTouchesNothing(t *testing.T) {
	s := &fakeStore{closed: 4}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{UnsendableNames: []string{"mkl"}})
	if asked || out.Exit != NothingSendable || out.Closed != 0 || out.Marked {
		t.Errorf("outcome %+v asked %v", out, asked)
	}
	if s.did() != "" {
		t.Errorf("store calls %q; a dependency that was never asked about is no evidence, and nothing was scanned", s.did())
	}
}

func TestAllMalformedClosesAllButTheUnsendableAndDoesNotMark(t *testing.T) {
	s := &fakeStore{closed: 9}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{Candidates: 3, Sendable: 0, UnsendableNames: []string{"mkl", "mkl"}})
	if asked || out.Exit != AllMalformed || out.Closed != 9 || out.Marked {
		t.Errorf("outcome %+v asked %v", out, asked)
	}
	if s.did() != "close" || len(s.seen) != 0 || !reflect.DeepEqual(s.preserve, []string{"mkl", "mkl"}) {
		t.Errorf("store calls %q, seen %v, preserve %v; want one close preserving the unsendable names, and no mark (nothing was asked)", s.did(), s.seen, s.preserve)
	}
}

func TestSendableTargetsMeanTheCallerMustAsk(t *testing.T) {
	s := &fakeStore{}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{Candidates: 3, Sendable: 1, UnsendableNames: []string{"mkl"}})
	if !asked || !reflect.DeepEqual(out, Outcome{}) || s.did() != "" {
		t.Errorf("outcome %+v asked %v calls %q; want nothing done and asked=true", out, asked, s.did())
	}
}

func TestScannedWritesThenClosesTheAbsentThenMarks(t *testing.T) {
	s := &fakeStore{closed: 2}
	rows := []*model.VulnerabilityRow{finding("GHSA-1", "pkg:npm/a@1"), finding("GHSA-2", "pkg:npm/a@1"), finding("GHSA-1", "pkg:npm/b@2")}
	out, err := WithAnswers(context.Background(), s, 7, Universe{Candidates: 2, Sendable: 2, UnsendableNames: []string{"mkl"}}, rows)
	if err != nil || out.Exit != Scanned || out.Written != 3 || out.Closed != 2 || !out.Marked {
		t.Errorf("outcome %+v err %v", out, err)
	}
	if s.did() != "insert,close,mark" {
		t.Errorf("store calls %q; want insert, then close, then mark", s.did())
	}
	if want := []string{"GHSA-1|pkg:npm/a@1", "GHSA-2|pkg:npm/a@1", "GHSA-1|pkg:npm/b@2"}; !reflect.DeepEqual(s.seen, want) {
		t.Errorf("seen = %v, want %v", s.seen, want)
	}
	if !reflect.DeepEqual(s.preserve, []string{"mkl"}) || len(s.inserted) != 3 {
		t.Errorf("preserve %v inserted %d", s.preserve, len(s.inserted))
	}
}

// Every target answered and none affected: nothing is written, every current
// finding is closed (the seen set is empty, not missing), and the repository
// is marked scanned and clean.
func TestScannedWithNoFindingsClosesEverythingCurrent(t *testing.T) {
	s := &fakeStore{closed: 5}
	out, err := WithAnswers(context.Background(), s, 7, Universe{Candidates: 2, Sendable: 2}, nil)
	if err != nil || out.Written != 0 || out.Closed != 5 || !out.Marked {
		t.Errorf("outcome %+v err %v", out, err)
	}
	if s.did() != "close,mark" {
		t.Errorf("store calls %q; an empty batch is not sent to the store", s.did())
	}
	if s.seen == nil || len(s.seen) != 0 {
		t.Errorf("seen = %#v; want an empty list, which the closing statement reads as \"nothing reported\"", s.seen)
	}
}

// The error exit: a partial view closes nothing and marks nothing.
func TestAWriteFailureClosesNothingAndMarksNothing(t *testing.T) {
	boom := errors.New("deadlock detected")
	s := &fakeStore{insertErr: boom}
	out, err := WithAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 1}, []*model.VulnerabilityRow{finding("GHSA-1", "pkg:npm/a@1")})
	if !errors.Is(err, boom) || err.Error() != "storing vulnerabilities: deadlock detected" {
		t.Errorf("err = %v; want it to wrap the cause as \"storing vulnerabilities: ...\"", err)
	}
	if out.Written != 0 || out.Closed != 0 || out.Marked || s.did() != "insert" {
		t.Errorf("outcome %+v, store calls %q; nothing may be closed or marked after a failed write", out, s.did())
	}
}

// Closing and marking may each fail without failing the scan; the Outcome
// carries the error for the caller to log, and the other still happens.
func TestClosingAndMarkingFailWithoutFailingTheScan(t *testing.T) {
	closeBoom, markBoom := errors.New("close failed"), errors.New("mark failed")

	s := &fakeStore{closeErr: closeBoom}
	out, err := WithAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 1}, []*model.VulnerabilityRow{finding("GHSA-1", "pkg:npm/a@1")})
	if err != nil || !errors.Is(out.CloseErr, closeBoom) || out.Closed != 0 || !out.Marked || out.Written != 1 {
		t.Errorf("close fails: outcome %+v err %v; the findings are written and the repository is still marked", out, err)
	}

	s = &fakeStore{markErr: markBoom, closed: 1}
	out, err = WithAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 1}, nil)
	if err != nil || !errors.Is(out.MarkErr, markBoom) || out.Marked || out.Closed != 1 {
		t.Errorf("mark fails: outcome %+v err %v", out, err)
	}

	s = &fakeStore{closeErr: closeBoom}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{})
	if asked || !errors.Is(out.CloseErr, closeBoom) || !out.Marked || s.did() != "close,mark" {
		t.Errorf("nothing to scan, close fails: outcome %+v, calls %q; it is still marked, as before", out, s.did())
	}

	s = &fakeStore{closeErr: closeBoom}
	out, _ = WithoutAnswers(context.Background(), s, 7, Universe{Candidates: 1})
	if !errors.Is(out.CloseErr, closeBoom) || out.Marked || s.did() != "close" {
		t.Errorf("all malformed, close fails: outcome %+v, calls %q", out, s.did())
	}
}

// The precondition of the closing rule (summary/70, section 3, rule 1): no
// DEPENDENCY finding closes for a repository about which nothing is known
// (no stored inventory rows, no lockfile rows, no completed analysis).
// Before 0.29.161 the nothing-to-scan exit closed every current finding of
// such a repository and marked it "scanned, clean".

func TestNothingKnownAndNothingToScanClosesNothingAndDoesNotMark(t *testing.T) {
	s := &fakeStore{closed: 4}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{DependenciesUnknown: true})
	if asked || out.Exit != InventoryUnknown || out.Closed != 0 || out.Marked {
		t.Errorf("outcome %+v asked %v; want the inventory-unknown exit with nothing closed and no mark", out, asked)
	}
	if s.did() != "" {
		t.Errorf("store calls %q; nothing has ever said what the repository depends on, so nothing may close", s.did())
	}
}

// When something IS known (an analysis ran and found nothing), the exit is
// the old one: every finding belongs to a dependency that is gone.
func TestAKnownEmptyInventoryStillClosesEverything(t *testing.T) {
	s := &fakeStore{closed: 4}
	out, _ := WithoutAnswers(context.Background(), s, 7, Universe{DependenciesUnknown: false})
	if out.Exit != NothingToScan || out.Closed != 4 || !out.Marked || s.selfOnly {
		t.Errorf("outcome %+v selfOnly %v", out, s.selfOnly)
	}
}

// A repository with a published package and nothing known about its
// dependencies is still scanned for advisories against itself: the scan is
// marked (OSV was consulted), and only findings about its own package may
// close.
func TestNothingKnownWithAnOwnPackageClosesOnlyOwnPackageFindings(t *testing.T) {
	s := &fakeStore{closed: 1}
	u := Universe{Candidates: 1, Sendable: 1, DependenciesUnknown: true}
	if _, asked := WithoutAnswers(context.Background(), s, 7, u); !asked {
		t.Fatal("a sendable own-package target must be asked about")
	}
	out, err := WithAnswers(context.Background(), s, 7, u, []*model.VulnerabilityRow{finding("PYSEC-1", "pkg:pypi/own")})
	if err != nil || out.Exit != Scanned || !out.Marked || out.Closed != 1 {
		t.Errorf("outcome %+v err %v", out, err)
	}
	if !s.selfOnly || s.did() != "insert,close,mark" {
		t.Errorf("selfOnly %v, calls %q; the close must be limited to own-package findings", s.selfOnly, s.did())
	}

	// The all-malformed exit under the same precondition.
	s = &fakeStore{}
	out, _ = WithoutAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 0, DependenciesUnknown: true})
	if out.Exit != AllMalformed || !s.selfOnly {
		t.Errorf("all malformed, nothing known: outcome %+v selfOnly %v; the close must be limited too", out, s.selfOnly)
	}
}

// With something known, the close is not limited (pinned on the three exits
// that close).
func TestAKnownInventoryIsNotLimitedToOwnPackageFindings(t *testing.T) {
	s := &fakeStore{}
	if _, err := WithAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 1}, nil); err != nil || s.selfOnly {
		t.Errorf("scanned: selfOnly %v err %v", s.selfOnly, err)
	}
	s = &fakeStore{}
	WithoutAnswers(context.Background(), s, 7, Universe{Candidates: 1})
	if s.selfOnly {
		t.Error("all malformed: the close was limited")
	}
}

// The scan labels its closures and carries first detection, end to end
// through the store interface: a lockfile appears, so `requests` moves from
// no version to 2.25.0; one advisory still applies there, one does not; and
// `left-pad` is simply gone.
func TestWithAnswersLabelsClosuresAndCarriesTheDate(t *testing.T) {
	old := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	s := &fakeStore{closed: 3, open: []model.OpenFinding{
		{VulnID: "GHSA-still", Purl: "pkg:pypi/requests", Kind: "direct", Ecosystem: "PyPI", FirstDetectedAt: old},
		{VulnID: "GHSA-old", Purl: "pkg:pypi/requests", Kind: "direct", Ecosystem: "PyPI", FirstDetectedAt: old},
		{VulnID: "GHSA-gone", Purl: "pkg:npm/left-pad@1.0.0", Kind: "direct", Ecosystem: "npm", FirstDetectedAt: old},
		{VulnID: "GHSA-kept", Purl: "pkg:npm/express@4.0.0", Kind: "direct", Ecosystem: "npm", FirstDetectedAt: old},
	}}
	u := Universe{Candidates: 2, Sendable: 2, Targets: []Target{
		{Purl: "pkg:pypi/requests@2.25.0", Kind: "direct"}, {Purl: "pkg:npm/express@4.0.0", Kind: "direct"}}}
	successor := finding("GHSA-still", "pkg:pypi/requests@2.25.0")
	kept := finding("GHSA-kept", "pkg:npm/express@4.0.0")
	out, err := WithAnswers(context.Background(), s, 7, u, []*model.VulnerabilityRow{successor, kept})
	if err != nil || out.Exit != Scanned {
		t.Fatalf("outcome %+v err %v", out, err)
	}
	want := map[string]string{
		"GHSA-still|pkg:pypi/requests": model.ClosedSuperseded,
		"GHSA-old|pkg:pypi/requests":   model.ClosedVersionDetermined,
		// GHSA-gone: no label. GHSA-kept: reported, so not a candidate.
	}
	if !reflect.DeepEqual(s.reasons, want) {
		t.Errorf("reasons = %v, want %v", s.reasons, want)
	}
	if !successor.FirstDetectedAt.Equal(old) {
		t.Errorf("the successor was written with first detection %v; want its predecessor's, %v", successor.FirstDetectedAt, old)
	}
	if !kept.FirstDetectedAt.IsZero() {
		t.Errorf("a finding that did not move was handed a date: %v", kept.FirstDetectedAt)
	}
	if s.did() != "insert,close,mark" {
		t.Errorf("store calls %q", s.did())
	}
}

// A scan that cannot read what is open writes nothing: not the findings, not
// a closure, not the mark.
func TestWithAnswersFailsClosedWhenTheOpenFindingsCannotBeRead(t *testing.T) {
	boom := errors.New("connection reset")
	for _, s := range []*fakeStore{{openErr: boom}, {notesErr: boom}} {
		out, err := WithAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 1}, []*model.VulnerabilityRow{finding("GHSA-1", "pkg:npm/a@1")})
		if !errors.Is(err, boom) || out.Written != 0 || out.Marked || s.did() != "" {
			t.Errorf("outcome %+v err %v calls %q; want the error and no write", out, err, s.did())
		}
	}
}

// The exits without answers label too: every target dropped by the gate is
// "unsendable", not a fix. And if the open findings cannot be read there,
// nothing is closed.
func TestTheExitsWithoutAnswersLabelTheirClosures(t *testing.T) {
	s := &fakeStore{closed: 1, open: []model.OpenFinding{{VulnID: "GHSA-1", Purl: "pkg:npm//raw@1.0.0", Kind: "direct", Ecosystem: "npm"}}}
	out, _ := WithoutAnswers(context.Background(), s, 7, Universe{Candidates: 1, Sendable: 0, DroppedPurls: []string{"pkg:npm//raw@1.0.0"}})
	if out.Exit != AllMalformed || s.reasons["GHSA-1|pkg:npm//raw@1.0.0"] != model.ClosedUnsendable {
		t.Errorf("all malformed: outcome %+v reasons %v; want the closure labelled unsendable", out, s.reasons)
	}

	boom := errors.New("connection reset")
	s = &fakeStore{openErr: boom}
	out, _ = WithoutAnswers(context.Background(), s, 7, Universe{})
	if !errors.Is(out.CloseErr, boom) || out.Closed != 0 || s.did() != "mark" {
		t.Errorf("nothing to scan, open findings unreadable: outcome %+v calls %q; want no close, the error recorded, and the mark as before", out, s.did())
	}
}

// Rule 1 with an answer that is not complete (summary/70, section 3). OSV
// refused the query for requests@2.25.0. Nothing may close on that: not the
// finding at that purl (rule 1(b) needs a complete answer), and not a finding
// at another purl of the same package that has left the target set (rule
// 1(a): its successor could not be seen, so closing it would read as a fix).
func TestAnIncompleteAnswerHoldsClosures(t *testing.T) {
	s := &fakeStore{closed: 1, open: []model.OpenFinding{
		{VulnID: "GHSA-at-rejected", Purl: "pkg:pypi/requests@2.25.0", Kind: "direct", Ecosystem: "PyPI"},
		{VulnID: "GHSA-old-purl", Purl: "pkg:pypi/requests", Kind: "direct", Ecosystem: "PyPI"},
		{VulnID: "GHSA-other-package", Purl: "pkg:npm/left-pad@1.0.0", Kind: "direct", Ecosystem: "npm"},
		{VulnID: "GHSA-answered", Purl: "pkg:npm/express@4.0.0", Kind: "direct", Ecosystem: "npm"},
	}}
	u := Universe{Candidates: 2, Sendable: 2, Targets: []Target{
		{Purl: "pkg:pypi/requests@2.25.0", Kind: "direct", Incomplete: true},
		{Purl: "pkg:npm/express@4.0.0", Kind: "direct"},
	}}
	out, err := WithAnswers(context.Background(), s, 7, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	kept := map[string]bool{}
	for _, k := range s.seen {
		kept[k] = true
	}
	for _, k := range []string{"GHSA-at-rejected|pkg:pypi/requests@2.25.0", "GHSA-old-purl|pkg:pypi/requests"} {
		if !kept[k] {
			t.Errorf("%s was offered for closing; an incomplete answer for its package must hold it", k)
		}
		if _, labelled := s.reasons[k]; labelled {
			t.Errorf("%s is held and must carry no label (it would be stored if the statement closed it)", k)
		}
	}
	for _, k := range []string{"GHSA-other-package|pkg:npm/left-pad@1.0.0", "GHSA-answered|pkg:npm/express@4.0.0"} {
		if kept[k] {
			t.Errorf("%s was held; its package's answers are complete (or it has none), so it closes as before", k)
		}
	}
	if out.Held != 2 || out.IncompleteTargets != 1 {
		t.Errorf("held %d, incomplete targets %d; want 2 and 1", out.Held, out.IncompleteTargets)
	}
	if !out.Marked || out.Exit != Scanned {
		t.Errorf("outcome %+v; OSV was consulted for another target, so the repository is still marked", out)
	}
}

// A self target's incomplete answer holds findings at ITS purl, and never a
// dependency's finding at another purl of the same key: a self target's
// whole-history answer is routinely incomplete and would hold them for good.
// It does not count toward the scan's partial mark either.
func TestASelfTargetsIncompleteAnswerHoldsOnlyItsOwnFindings(t *testing.T) {
	s := &fakeStore{open: []model.OpenFinding{
		{VulnID: "GHSA-self", Purl: "pkg:pypi/own", Kind: "self", Ecosystem: "PyPI"},
		{VulnID: "GHSA-dep", Purl: "pkg:pypi/own@1.0", Kind: "direct", Ecosystem: "PyPI"},
	}}
	u := Universe{Candidates: 2, Sendable: 2, Targets: []Target{
		{Purl: "pkg:pypi/own", Kind: "self", Incomplete: true},
		{Purl: "pkg:npm/express@4.0.0", Kind: "direct"},
	}}
	out, err := WithAnswers(context.Background(), s, 7, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.seen) != 1 || s.seen[0] != "GHSA-self|pkg:pypi/own" {
		t.Errorf("held %v; want only the self finding", s.seen)
	}
	if out.Held != 1 || out.IncompleteTargets != 0 {
		t.Errorf("held %d, incomplete (non-self) targets %d; want 1 and 0", out.Held, out.IncompleteTargets)
	}
}

// When no target was answered at all, OSV said nothing about the repository:
// the findings the scan was handed are still written (there are none), the
// holds apply, and the repository is NOT marked scanned.
func TestNothingAnsweredDoesNotMark(t *testing.T) {
	s := &fakeStore{open: []model.OpenFinding{
		{VulnID: "GHSA-1", Purl: "pkg:maven/g/a@1", Kind: "direct", Ecosystem: "maven"},
		{VulnID: "GHSA-gone", Purl: "pkg:npm/left-pad@1.0.0", Kind: "direct", Ecosystem: "npm"},
	}}
	u := Universe{Candidates: 1, Sendable: 1, Targets: []Target{{Purl: "pkg:maven/g/a@1", Kind: "direct", Incomplete: true}}}
	out, err := WithAnswers(context.Background(), s, 7, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Exit != NothingAnswered || out.Marked || s.did() != "close" {
		t.Errorf("outcome %+v, calls %q; want the NothingAnswered exit, a close and no mark", out, s.did())
	}
	if len(s.seen) != 1 || s.seen[0] != "GHSA-1|pkg:maven/g/a@1" {
		t.Errorf("held %v; want the finding at the unanswered purl (the gone dependency's closes: its purl is no target and no target shares its key)", s.seen)
	}
}

// Present but not evaluable is not absent (summary/70, 4.2). OSV named an
// advisory for a GitHub Action, and whether the pinned ref is affected could
// not be decided, because the advisory's detail could not be fetched. The
// scan writes no finding for the pair. An existing finding for it must not
// close by absence.
func TestANotEvaluableAdvisoryHoldsItsFinding(t *testing.T) {
	s := &fakeStore{open: []model.OpenFinding{
		// The pair itself.
		{VulnID: "GHSA-x", Purl: "pkg:githubactions/o/a@v4", Kind: "direct", Ecosystem: "githubactions"},
		// The same advisory at a ref that is no longer a target.
		{VulnID: "GHSA-x", Purl: "pkg:githubactions/o/a@v3", Kind: "direct", Ecosystem: "githubactions"},
		// Another advisory at the same purl: the answer omits it, and it closes.
		{VulnID: "GHSA-other", Purl: "pkg:githubactions/o/a@v4", Kind: "direct", Ecosystem: "githubactions"},
		// The same advisory id on another package: unrelated.
		{VulnID: "GHSA-x", Purl: "pkg:githubactions/o/b@v1", Kind: "direct", Ecosystem: "githubactions"},
	}}
	u := Universe{Candidates: 1, Sendable: 1,
		Targets:      []Target{{Purl: "pkg:githubactions/o/a@v4", Kind: "direct"}},
		NotEvaluable: []Pair{{VulnID: "GHSA-x", Purl: "pkg:githubactions/o/a@v4"}}}
	out, err := WithAnswers(context.Background(), s, 7, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	kept := map[string]bool{}
	for _, k := range s.seen {
		kept[k] = true
	}
	want := map[string]bool{"GHSA-x|pkg:githubactions/o/a@v4": true, "GHSA-x|pkg:githubactions/o/a@v3": true}
	if !reflect.DeepEqual(kept, want) {
		t.Errorf("held %v, want %v", kept, want)
	}
	if out.Held != 2 || out.IncompleteTargets != 0 || !out.Marked {
		t.Errorf("outcome %+v; want 2 held, no incomplete target, marked", out)
	}
}

// With no current finding for the pair there is nothing to hold and nothing
// is written. The advisory is still counted (0.29.208): the scan did not
// decide it, and its caller reports the scan as partial.
func TestANotEvaluableAdvisoryWithNoFindingHoldsNothing(t *testing.T) {
	s := &fakeStore{}
	u := Universe{Candidates: 1, Sendable: 1,
		Targets:      []Target{{Purl: "pkg:githubactions/o/a@v4", Kind: "direct"}},
		NotEvaluable: []Pair{{VulnID: "GHSA-x", Purl: "pkg:githubactions/o/a@v4"}}}
	out, err := WithAnswers(context.Background(), s, 7, u, nil)
	if err != nil || out.Held != 0 || len(s.seen) != 0 || !out.Marked || out.NotEvaluable != 1 {
		t.Errorf("outcome %+v err %v seen %v; want nothing held, one advisory not evaluable and the repository marked", out, err, s.seen)
	}
}

// The decision guards, as linking drives them (0.29.171): rows are stamped
// with the time their answer was asked and the time the scan read its
// targets; the close is told both; and the purls whose answer a newer one
// has already overtaken come back to be asked again.
func TestDecisionGuardStampsRowsAndReportsTheReask(t *testing.T) {
	read := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	stale, fresh := read.Add(-20*time.Hour), read.Add(-time.Minute)
	newer := read.Add(-time.Hour)
	s := &fakeStore{
		// A newer answer already decided the reported finding on express.
		decidedAfter: []string{"GHSA-reported|pkg:npm/express@4.0.0"},
		open: []model.OpenFinding{
			// About to close on a stale answer's silence, decided since.
			{VulnID: "GHSA-silent", Purl: "pkg:pypi/requests@2.0", Kind: "direct", Ecosystem: "PyPI", DecidedAskedAt: &newer},
			// About to close on a fresh answer's silence: no re-ask.
			{VulnID: "GHSA-fine", Purl: "pkg:npm/lodash@1.0.0", Kind: "direct", Ecosystem: "npm", DecidedAskedAt: &newer},
			// Its purl's answer is not complete: held, and not in the guard.
			{VulnID: "GHSA-held", Purl: "pkg:npm/cut@1.0.0", Kind: "direct", Ecosystem: "npm", DecidedAskedAt: &newer},
		},
	}
	u := Universe{Candidates: 4, Sendable: 4, TargetsReadAt: read, Targets: []Target{
		{Purl: "pkg:npm/express@4.0.0", Kind: "direct", AskedAt: stale},
		{Purl: "pkg:pypi/requests@2.0", Kind: "direct", AskedAt: stale},
		{Purl: "pkg:npm/lodash@1.0.0", Kind: "direct", AskedAt: fresh},
		{Purl: "pkg:npm/cut@1.0.0", Kind: "direct", AskedAt: stale, Incomplete: true},
	}}
	row := finding("GHSA-reported", "pkg:npm/express@4.0.0")
	out, err := WithAnswers(context.Background(), s, 7, u, []*model.VulnerabilityRow{row})
	if err != nil {
		t.Fatal(err)
	}
	if !row.AskedAt.Equal(stale) || !row.TargetsReadAt.Equal(read) {
		t.Errorf("the row was written with asked-at %v and targets-read %v; want %v and %v", row.AskedAt, row.TargetsReadAt, stale, read)
	}
	if !s.askedFor["GHSA-reported|pkg:npm/express@4.0.0"].Equal(stale) {
		t.Errorf("the store was asked about %v", s.askedFor)
	}
	wantGuard := map[string]time.Time{"pkg:npm/express@4.0.0": stale, "pkg:pypi/requests@2.0": stale, "pkg:npm/lodash@1.0.0": fresh}
	if !s.guard.TargetsReadAt.Equal(read) || !reflect.DeepEqual(s.guard.AnswerAskedAt, wantGuard) {
		t.Errorf("the close's guard = %+v; want the read time and the three complete answers", s.guard)
	}
	if want := []string{"pkg:npm/express@4.0.0", "pkg:pypi/requests@2.0"}; !reflect.DeepEqual(out.Reask, want) {
		t.Errorf("re-ask = %v, want %v", out.Reask, want)
	}
	// A pass that hands back questions to ask again is not the scan's last
	// word, and does not mark the repository scanned (0.29.208): if asking
	// again then failed, the scan returned an error over a fresh mark.
	if out.Marked {
		t.Error("the repository was marked scanned by a pass that asked for a re-ask")
	}
	// The pass after the re-ask is the last: a second refusal means another
	// scan decided in the meantime, its decision stands, and this scan is
	// complete.
	u.FinalPass = true
	out, err = WithAnswers(context.Background(), s, 7, u, []*model.VulnerabilityRow{finding("GHSA-reported", "pkg:npm/express@4.0.0")})
	if err != nil || !out.Marked || len(out.Reask) == 0 {
		t.Errorf("the final pass: outcome %+v err %v; want marked, the refusals still reported", out, err)
	}
}

// A scan that cannot read the findings' last decisions writes nothing.
func TestDecisionGuardFailsClosed(t *testing.T) {
	boom := errors.New("connection reset")
	s := &fakeStore{decidedErr: boom}
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	u := Universe{Candidates: 1, Sendable: 1, TargetsReadAt: at, Targets: []Target{{Purl: "pkg:npm/a@1", Kind: "direct", AskedAt: at}}}
	out, err := WithAnswers(context.Background(), s, 7, u, []*model.VulnerabilityRow{finding("GHSA-1", "pkg:npm/a@1")})
	if !errors.Is(err, boom) || out.Written != 0 || s.did() != "" {
		t.Errorf("outcome %+v err %v calls %q; want the error and no write", out, err, s.did())
	}
}

// The exits without answers hand the close the target-set read time.
func TestTheExitsWithoutAnswersGuardByTheTargetRead(t *testing.T) {
	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	s := &fakeStore{}
	if _, asked := WithoutAnswers(context.Background(), s, 7, Universe{TargetsReadAt: at}); asked {
		t.Fatal("nothing to scan must not ask")
	}
	if !s.guard.TargetsReadAt.Equal(at) || len(s.guard.AnswerAskedAt) != 0 {
		t.Errorf("guard = %+v; want the read time and no answers", s.guard)
	}
}

// Zero targets, one rule (summary/70, 4.2). A repository with no target
// declares nothing only if no dependency of it was unsendable. One whose
// dependencies all have names the identity check refused has no inventory
// row and no target, but it is not clean: nothing of it could be sent. It
// is not marked scanned. Its findings at those names close, labelled.
func TestOnlyRejectedNamesIsNotScannedClean(t *testing.T) {
	s := &fakeStore{closed: 1,
		notes: []model.InventoryNote{{Kind: model.NoRowNameRejected, PackageManager: "pypi", Name: "bad name", PackageKey: "pypi|bad name"}},
		open:  []model.OpenFinding{{VulnID: "GHSA-1", Purl: "pkg:pypi/bad%20name", Kind: "direct", Ecosystem: "PyPI"}},
	}
	out, asked := WithoutAnswers(context.Background(), s, 7, Universe{})
	if asked {
		t.Fatal("nothing to ask")
	}
	if out.Exit != NothingSendable || out.Marked || s.did() != "close" {
		t.Errorf("outcome %+v, calls %q; want the nothing-sendable exit, a close and no mark", out, s.did())
	}
	if s.reasons["GHSA-1|pkg:pypi/bad%20name"] != model.ClosedUnsendable {
		t.Errorf("reasons %v; the finding at the rejected name closes as unsendable", s.reasons)
	}

	// Notes of the other kinds do not block the stamp: a dependency no
	// registry holds, or one with no namespace, was never going to be sent.
	for _, kind := range []string{model.NoRowNonRegistry, model.NoRowNoNamespace, model.NoRowLookupFailed} {
		s := &fakeStore{notes: []model.InventoryNote{{Kind: kind, PackageManager: "npm", Name: "x"}}}
		if out, _ := WithoutAnswers(context.Background(), s, 7, Universe{}); out.Exit != NothingToScan || !out.Marked {
			t.Errorf("a %s note: outcome %+v; want nothing-to-scan, marked", kind, out)
		}
	}

	// If the notes cannot be read, the repository is not marked: "clean"
	// would be a guess.
	s = &fakeStore{notesErr: errors.New("connection reset")}
	if out, _ := WithoutAnswers(context.Background(), s, 7, Universe{}); out.Marked || out.CloseErr == nil {
		t.Errorf("notes unreadable: outcome %+v; want no mark and the error recorded", out)
	}
}
