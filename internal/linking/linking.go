// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package linking decides what a scan's answers mean for a repository's
// stored findings: which are written, which are kept, and which are closed.
//
// It is the decision half of the scan, which was the second half of
// collector.ScanVulnerabilities (summary/70, 4.1.7). The scan, the
// heal-vulnerabilities command and, from chunk 2, the advisory ticker reach
// it the same way: build the targets (scantarget), get the answers, link.
//
// The rule every exit follows: a finding is closed only on evidence. "Absent
// from the answers" is evidence only for a dependency that was asked about.
// So the exits that are not errors (the Exit constants) differ in what they
// may close and whether the repository may be marked scanned:
//
//	NothingToScan    no target at all and no dependency that could not be
//	                 sent: the inventory is the truth and it is empty, so
//	                 every current finding belongs to a dependency that is
//	                 gone. All are closed; the repository is marked scanned.
//	InventoryUnknown no target, nothing that could not be sent, and NOTHING
//	                 KNOWN about the repository's dependencies (no stored
//	                 inventory rows, no lockfile rows, no completed analysis).
//	                 Nothing is closed; not marked. An inventory nobody wrote
//	                 is not an empty inventory.
//	NothingSendable  no target, but some dependencies have no purl and were
//	                 never asked about. Nothing is closed; not marked.
//	AllMalformed     there were targets and the wire gate dropped every one.
//	                 Their findings were born of garbage purls, so they are
//	                 closed (the next analysis rewrites the purls and a real
//	                 finding returns), except those of dependencies that
//	                 could not be sent. Not marked: nothing was asked.
//	Scanned          answers came back for every sendable target. The
//	                 findings are written; current findings absent from them
//	                 are closed, except those of dependencies that could not
//	                 be sent; the repository is marked scanned.
//	NothingAnswered  there were sendable targets and none has a complete
//	                 answer. Written and closed under the same rules as
//	                 Scanned (so almost nothing closes); not marked.
//
// One precondition runs through all of them (summary/70, section 3, rule 1):
// when nothing is known about a repository's dependencies, no DEPENDENCY
// finding closes, whatever the exit. A finding about the repository's own
// package does not come from the inventory and closes as usual.
//
// And one error exit: writing the findings fails. Nothing is closed and the
// repository is not marked, because a partial view must never close anything.
//
// Closing and marking are each allowed to fail without failing the scan: the
// findings are written, and the next scan closes and marks again. The Outcome
// carries those errors for the caller to log.
//
// The answers arrive as the finding rows they produced, and each target
// says whether its answer is complete and when it was asked (Target).
//
// A closure that is not a fix is LABELLED (reasons.go): a finding whose purl
// was re-keyed because a version was worked out, lost or changed, or whose
// purl can no longer be sent, closes with a reason and is not a resolution. A
// reason never selects a finding to close. And a successor keeps its
// predecessor's first detection, so a version bump to a still-affected
// version is not a new problem.
//
// It reads and writes through an interface it declares, and imports
// internal/model and internal/pkgid: what it needs to know about the targets
// is the small Universe below, which its caller fills from the target set.
package linking

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// Store is what linking writes: findings, closures, and the mark that a scan
// completed. *db.PostgresStore satisfies it.
type Store interface {
	// InsertVulnerabilityBatch writes (upserts) a repository's findings.
	InsertVulnerabilityBatch(ctx context.Context, repoID int64, rows []*model.VulnerabilityRow) error
	// MarkStaleVulnerabilitiesResolved closes every current finding of the
	// repository whose "vuln_id|package_purl" is not in seenKeys and whose
	// package name is not in preserveNames, and returns how many it closed.
	//
	// selfOnly limits the close to findings about the repository's own
	// package. reasons maps a finding's SeenKey to why it would close; it
	// is stored only on a finding the statement closes.
	MarkStaleVulnerabilitiesResolved(ctx context.Context, repoID int64, seenKeys []string, preserveNames []string, selfOnly bool, reasons map[string]string, guard model.DecisionGuard) (int64, error)
	// GetFindingsDecidedAfter returns, of the findings named with the
	// asked-at time of the answer the scan holds for each, those a newer
	// answer has already decided.
	GetFindingsDecidedAfter(ctx context.Context, repoID int64, askedAt map[string]time.Time) ([]string, error)
	// GetOpenVulnerabilityFindings returns the repository's current
	// findings, read before anything is written.
	GetOpenVulnerabilityFindings(ctx context.Context, repoID int64) ([]model.OpenFinding, error)
	// GetRepoInventoryNotes returns the declared dependencies given no
	// inventory row at the last analysis.
	GetRepoInventoryNotes(ctx context.Context, repoID int64) ([]model.InventoryNote, error)
	// SetVulnScanLastRun marks the repository as scanned now.
	SetVulnScanLastRun(ctx context.Context, repoID int64) error
}

// Universe is what linking needs to know about a repository's scan targets.
type Universe struct {
	// Candidates is the number of targets built, before the wire gate.
	Candidates int
	// Sendable is the number the wire gate let through.
	Sendable int
	// UnsendableNames names each declared dependency that has no purl and so
	// was never asked about (a name may repeat). Their findings are never
	// closed for being absent from the answers.
	UnsendableNames []string
	// DependenciesUnknown is true when nothing has ever said what the
	// repository depends on: no stored inventory rows, no lockfile rows and
	// no completed analysis. It is the precondition of the closing rule: no
	// dependency finding of such a repository closes, whatever the exit.
	DependenciesUnknown bool
	// Targets are the targets the scan sent; DroppedPurls the purls the wire
	// gate refused. They are what a closing finding's purl is compared
	// with, by package, to choose its label.
	Targets      []Target
	DroppedPurls []string
	// TargetsReadAt (0.29.171) is when the scan read its targets, a
	// database time; zero when the caller does not track it.
	TargetsReadAt time.Time
	// NotEvaluable (0.29.170) are the advisories an answer reported for a
	// target that the scan could not evaluate, so it wrote no finding for
	// them and they are not known to be absent either.
	NotEvaluable []Pair
	// FinalPass (0.29.208) is set by the caller on the pass that follows a
	// re-ask (Outcome.Reask): whatever that pass reports, there is no
	// further one, so it marks the repository scanned.
	FinalPass bool
}

// Pair is one advisory at one purl.
type Pair struct {
	VulnID string
	Purl   string
}

// Exit names which of the non-error exits a scan took.
type Exit string

// The exits. See the package comment for what each may close and mark.
const (
	NothingToScan    Exit = "nothing_to_scan"
	InventoryUnknown Exit = "inventory_unknown"
	NothingSendable  Exit = "nothing_sendable"
	AllMalformed     Exit = "all_malformed"
	Scanned          Exit = "scanned"
	// NothingAnswered (0.29.168): there were sendable targets and not one
	// of them has a complete answer. Findings are written and closed under
	// the same rules, and the repository is not marked scanned: OSV said
	// nothing about it.
	NothingAnswered Exit = "nothing_answered"
)

// Outcome is what linking did.
type Outcome struct {
	Exit Exit
	// Written is the number of findings handed to the store.
	Written int
	// Closed is the number of findings closed. CloseErr is why closing
	// failed, when it did; Closed is then 0.
	Closed   int
	CloseErr error
	// Marked is true when the repository was marked scanned. MarkErr is why
	// marking failed, when it was attempted and did.
	Marked  bool
	MarkErr error
	// Held (0.29.168) is the number of open findings the scan did not
	// report and did not offer for closing, because an answer for their
	// package was not complete. IncompleteTargets is the number of targets
	// with such an answer, a target of kind self not counted: it is what
	// makes a scan partial.
	Held              int
	IncompleteTargets int
	// NotEvaluable (0.29.208) is the number of advisories an answer named
	// and the scan could not decide (Universe.NotEvaluable), whether or not
	// a finding was open for one. It makes a scan partial too: holding an
	// existing finding open is half of "not decided"; the other half is not
	// calling the package checked.
	NotEvaluable int
	// Reask (0.29.171) are target purls whose answer the scan holds is
	// OLDER than the answer that last decided one of their findings, so it
	// was refused for that finding. The caller asks about them again, live,
	// and links again.
	Reask []string

	// guard is what the close is told about this scan's age.
	guard model.DecisionGuard
}

// SeenKey is a finding's identity within a repository, as the closing
// statement compares it.
func SeenKey(v *model.VulnerabilityRow) string {
	return v.VulnID + "|" + v.PackagePurl
}

// WithoutAnswers handles a repository whose targets leave nothing to ask.
// asked is false when it did so: the scan is over, and the Outcome says which
// exit it took. asked is true when there ARE sendable targets: nothing was
// done, and the caller queries and then calls WithAnswers.
func WithoutAnswers(ctx context.Context, store Store, repoID int64, u Universe) (out Outcome, asked bool) {
	switch {
	case u.Candidates == 0 && len(u.UnsendableNames) == 0 && u.DependenciesUnknown:
		// No target, and no evidence of what the repository depends on. An
		// empty inventory that nothing wrote is not "declares nothing".
		out.Exit = InventoryUnknown
		return out, false
	case u.Candidates == 0 && len(u.UnsendableNames) == 0:
		// Zero targets, one rule (0.29.174): "declares nothing" only if no
		// dependency was unsendable. A dependency whose name the identity
		// check refused has no inventory row, so it is not a candidate;
		// its note is what says it exists. With one, the repository is
		// not clean: its findings close (labelled unsendable) and it is
		// not marked scanned. If the notes cannot be read it is not marked
		// either.
		rejected, err := hasRejectedName(ctx, store, repoID)
		out.close(ctx, store, repoID, u, nil, nil, false)
		switch {
		case err != nil:
			out.Exit = NothingToScan
			if out.CloseErr == nil {
				out.CloseErr = err
			}
		case rejected:
			out.Exit = NothingSendable
		default:
			out.Exit = NothingToScan
			out.mark(ctx, store, repoID)
		}
		return out, false
	case u.Candidates == 0:
		out.Exit = NothingSendable
		return out, false
	case u.Sendable == 0:
		out.Exit = AllMalformed
		out.close(ctx, store, repoID, u, nil, u.UnsendableNames, u.DependenciesUnknown)
		return out, false
	}
	return Outcome{}, true
}

// WithAnswers links the findings that the answers for the sendable targets
// produced. findings may be empty: every target was asked about and none is
// affected. An error means the findings could not be written; nothing was
// closed and the repository was not marked.
func WithAnswers(ctx context.Context, store Store, repoID int64, u Universe, findings []*model.VulnerabilityRow) (Outcome, error) {
	out := Outcome{Exit: Scanned}
	// What is open now, read BEFORE anything is written: the closures are
	// labelled against it, and a successor takes its predecessor's first
	// detection in the same insert. A scan that cannot read it writes
	// nothing: a label computed from half a view would be a wrong label.
	plan, err := planClosures(ctx, store, repoID, u, findings)
	if err != nil {
		return out, fmt.Errorf("reading current findings: %w", err)
	}
	carryFirstDetection(plan.closing, findings)
	// The decision guards (0.29.171). Each row is stamped with the time its
	// answer was asked and the time this scan read its targets; the store
	// applies a row, and closes a finding, only if neither is older than
	// what the finding was last decided by. Which answers that refuses is
	// read first, so the caller can ask about those packages again, live.
	guard, reask, err := decisionGuard(ctx, store, repoID, u, findings, plan.closing)
	if err != nil {
		return out, fmt.Errorf("reading the findings' last decisions: %w", err)
	}
	out.guard, out.Reask = guard, reask
	if len(findings) > 0 {
		if err := store.InsertVulnerabilityBatch(ctx, repoID, findings); err != nil {
			return out, fmt.Errorf("storing vulnerabilities: %w", err)
		}
		out.Written = len(findings)
	}
	out.Held, out.IncompleteTargets, out.NotEvaluable = len(plan.held), plan.incompleteTargets, len(u.NotEvaluable)
	out.closeWith(ctx, store, repoID, append(plan.seen, plan.held...), u.UnsendableNames, u.DependenciesUnknown, plan.reasons)
	if plan.nothingAnswered {
		out.Exit = NothingAnswered
		return out, nil
	}
	// A pass that hands back questions to ask again is not the scan's last
	// word: its caller asks and links once more, and that pass (FinalPass)
	// marks. Marking here left a fresh mark behind a scan that then failed
	// while asking again.
	if len(out.Reask) == 0 || u.FinalPass {
		out.mark(ctx, store, repoID)
	}
	return out, nil
}

// closurePlan is what a scan knows, before it writes, about the findings
// that are open and are not among those it reports.
type closurePlan struct {
	seen    []string            // SeenKey of every finding the scan reports
	closing []model.OpenFinding // open findings the scan does not report
	reasons map[string]string   // SeenKey -> label, for those with one
	// held: SeenKey of each open finding the scan does not report and must
	// not close, because an answer for its package is not complete. They
	// are not in closing and take no label.
	held              []string
	incompleteTargets int  // not counting targets of kind self
	nothingAnswered   bool // there are targets, and none has a complete answer
}

// planClosures reads the open findings and the notes and labels the
// candidates for closing. It decides nothing about WHICH findings close: the
// closing statement does (it also keeps the findings of dependencies that
// could not be sent, and applies the precondition); a label offered for a
// finding that stays open is not stored.
func planClosures(ctx context.Context, store Store, repoID int64, u Universe, findings []*model.VulnerabilityRow) (closurePlan, error) {
	plan := closurePlan{seen: make([]string, 0, len(findings)), reasons: map[string]string{}}
	reported := make(map[string]bool, len(findings))
	for _, v := range findings {
		k := SeenKey(v)
		plan.seen = append(plan.seen, k)
		reported[k] = true
	}
	open, err := store.GetOpenVulnerabilityFindings(ctx, repoID)
	if err != nil {
		return plan, err
	}
	notes, err := store.GetRepoInventoryNotes(ctx, repoID)
	if err != nil {
		return plan, err
	}
	idx := newReasonIndex(u.Targets, findings, notes, u.DroppedPurls)
	hold := newHoldIndex(u.Targets, u.NotEvaluable)
	plan.incompleteTargets, plan.nothingAnswered = hold.incompleteNonSelf, hold.nothingAnswered
	for _, f := range open {
		k := f.VulnID + "|" + f.Purl
		if reported[k] {
			continue
		}
		if hold.holds(f.VulnID, f.Purl) {
			plan.held = append(plan.held, k)
			continue
		}
		plan.closing = append(plan.closing, f)
		if reason := idx.reasonFor(f); reason != "" {
			plan.reasons[k] = reason
		}
	}
	return plan, nil
}

// close is closeWith for the exits that have no answers: it labels the
// closures first. If the open findings or the notes cannot be read it closes
// nothing and records the error as the closing failure; a closure with a
// label it could not compute would read as a fix.
func (o *Outcome) close(ctx context.Context, store Store, repoID int64, u Universe, seen, preserve []string, selfOnly bool) {
	plan, err := planClosures(ctx, store, repoID, u, nil)
	if err != nil {
		o.CloseErr = err
		return
	}
	// No answers on these exits: only the target-set read time guards.
	o.guard = model.DecisionGuard{TargetsReadAt: u.TargetsReadAt}
	o.closeWith(ctx, store, repoID, seen, preserve, selfOnly, plan.reasons)
}

// closeWith closes the current findings the scan no longer reports, with
// their labels. selfOnly is the closing rule's precondition: when nothing is
// known about the repository's dependencies, only findings about its own
// package may close.
func (o *Outcome) closeWith(ctx context.Context, store Store, repoID int64, seen, preserve []string, selfOnly bool, reasons map[string]string) {
	n, err := store.MarkStaleVulnerabilitiesResolved(ctx, repoID, seen, preserve, selfOnly, reasons, o.guard)
	if err != nil {
		o.CloseErr = err
		return
	}
	o.Closed = int(n)
}

func (o *Outcome) mark(ctx context.Context, store Store, repoID int64) {
	if err := store.SetVulnScanLastRun(ctx, repoID); err != nil {
		o.MarkErr = err
		return
	}
	o.Marked = true
}

// holdIndex answers the closing rule's first question for a finding the scan
// does not report: may absence close it? Not when its own purl is a target
// whose answer is not complete (rule 1(b) needs a complete answer that omits
// the advisory), and not when its purl has left the target set while another
// target of the same package, of a kind other than self, has an answer that
// is not complete (rule 1(a): the finding's successor could not be seen, so
// closing it would read as a fix and the successor would later open as new).
type holdIndex struct {
	incompleteAt      map[string]bool // target purl -> its answer is not complete
	incompleteKeys    map[string]bool // package keys with a non-self target whose answer is not complete
	incompleteNonSelf int
	nothingAnswered   bool
	// An advisory an answer reported and the scan could not evaluate is
	// neither a finding nor an absence, for that advisory alone: at its
	// purl, and at any purl of the same package that has left the target
	// set. Keyed "advisory|purl" and "advisory|package key".
	notEvaluableAt  map[string]bool
	notEvaluableKey map[string]bool
}

func newHoldIndex(targets []Target, notEvaluable []Pair) holdIndex {
	h := holdIndex{incompleteAt: make(map[string]bool, len(targets)), incompleteKeys: map[string]bool{},
		notEvaluableAt: map[string]bool{}, notEvaluableKey: map[string]bool{}}
	for _, p := range notEvaluable {
		h.notEvaluableAt[p.VulnID+"|"+p.Purl] = true
		if key, _, ok := pkgid.KeyOfPurl(p.Purl); ok {
			h.notEvaluableKey[p.VulnID+"|"+key] = true
		}
	}
	complete := 0
	for _, t := range targets {
		h.incompleteAt[t.Purl] = t.Incomplete
		if !t.Incomplete {
			complete++
			continue
		}
		if t.Kind == model.DependencyKindSelf {
			continue
		}
		h.incompleteNonSelf++
		if key, _, ok := pkgid.KeyOfPurl(t.Purl); ok {
			h.incompleteKeys[key] = true
		}
	}
	h.nothingAnswered = len(targets) > 0 && complete == 0
	return h
}

func (h holdIndex) holds(vulnID, purl string) bool {
	if incomplete, isTarget := h.incompleteAt[purl]; isTarget {
		return incomplete || h.notEvaluableAt[vulnID+"|"+purl]
	}
	key, _, ok := pkgid.KeyOfPurl(purl)
	return ok && (h.incompleteKeys[key] || h.notEvaluableKey[vulnID+"|"+key])
}

// decisionGuard stamps the rows about to be written with the times they are
// decided by, builds the guard the close is given, and returns the target
// purls to ask about again: those with a reported finding, or a finding about
// to close on the answer's silence, that a NEWER answer has already decided.
// The store refuses those writes and closes itself; this read only says which
// packages to re-ask, so a race with another scan costs a re-ask, never a
// wrong write.
func decisionGuard(ctx context.Context, store Store, repoID int64, u Universe, findings []*model.VulnerabilityRow, closing []model.OpenFinding) (model.DecisionGuard, []string, error) {
	guard := model.DecisionGuard{TargetsReadAt: u.TargetsReadAt, AnswerAskedAt: map[string]time.Time{}}
	for _, t := range u.Targets {
		// An answer that is not complete closes nothing (the hold rule),
		// so it has no place in the close's guard.
		if !t.Incomplete && !t.AskedAt.IsZero() {
			guard.AnswerAskedAt[t.Purl] = t.AskedAt
		}
	}
	askedAt := make(map[string]time.Time, len(u.Targets))
	for _, t := range u.Targets {
		askedAt[t.Purl] = t.AskedAt
	}
	reported := map[string]time.Time{}
	purlOf := map[string]string{}
	for _, f := range findings {
		f.TargetsReadAt = u.TargetsReadAt
		f.AskedAt = askedAt[f.PackagePurl]
		if !f.AskedAt.IsZero() {
			k := SeenKey(f)
			reported[k], purlOf[k] = f.AskedAt, f.PackagePurl
		}
	}
	again := map[string]bool{}
	if len(reported) > 0 {
		newer, err := store.GetFindingsDecidedAfter(ctx, repoID, reported)
		if err != nil {
			return guard, nil, err
		}
		for _, k := range newer {
			again[purlOf[k]] = true
		}
	}
	for _, f := range closing {
		if at, answered := guard.AnswerAskedAt[f.Purl]; answered && f.DecidedAskedAt != nil && at.Before(*f.DecidedAskedAt) {
			again[f.Purl] = true
		}
	}
	reask := make([]string, 0, len(again))
	for p := range again {
		reask = append(reask, p)
	}
	sort.Strings(reask)
	return guard, reask, nil
}

// hasRejectedName reports whether the last analysis left a note for a
// dependency whose name the identity check refused.
func hasRejectedName(ctx context.Context, store Store, repoID int64) (bool, error) {
	notes, err := store.GetRepoInventoryNotes(ctx, repoID)
	if err != nil {
		return false, err
	}
	for _, n := range notes {
		if n.Kind == model.NoRowNameRejected {
			return true, nil
		}
	}
	return false, nil
}
