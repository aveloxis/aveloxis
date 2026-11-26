// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package linking

import (
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// Target is what linking needs to know about one target the scan sent.
type Target struct {
	Purl string
	Kind string // direct, transitive or self
	// Incomplete (0.29.168): the target was sent and its answer is not a
	// complete one. OSV refused the query; later releases add an answer cut
	// short. Nothing closes on such an answer (the closing rule, 1).
	Incomplete bool
	// AskedAt (0.29.171) is when this target's answer was asked of OSV, a
	// database time; zero when it has none or the caller does not track it.
	AskedAt time.Time
}

// githubActions is the ecosystem whose purls carry a ref, not a version that
// can be absent.
const githubActions = "githubactions"

// reasonIndex answers "why would this finding close?" for one scan
// (summary/70, section 3, rule 2). It never decides WHETHER a finding
// closes: a reason only labels a closure, so that a finding that closed
// because its purl was re-keyed, or could no longer be sent, is not counted
// or shown as a fix.
//
// Everything is compared by PACKAGE, through pkgid's key, since a finding
// stores only its purl.
type reasonIndex struct {
	// By package key, over the targets the scan sent, self targets left out:
	// a self target's whole-history answer says nothing about a dependency
	// of the same name.
	versioned   map[string]map[string]bool // key -> versions that are targets
	versionless map[string]bool            // key -> a target with no version exists
	// reported[key][advisory] -> the versions ("" for none) at which this
	// scan reports the advisory for the package.
	reported map[string]map[string]map[string]bool
	// Rejected names, by key and by purl; and the purls the wire gate dropped.
	rejectedKeys  map[string]bool
	rejectedPurls map[string]bool
	dropped       map[string]bool
	// unusedPurls and unusedKeys are the managed_unused notes (0.29.183): a
	// Maven dependencyManagement entry no dependency took the version of.
	unusedPurls map[string]bool
	unusedKeys  map[string]bool
}

func newReasonIndex(targets []Target, findings []*model.VulnerabilityRow, notes []model.InventoryNote, dropped []string) *reasonIndex {
	idx := &reasonIndex{
		versioned: map[string]map[string]bool{}, versionless: map[string]bool{},
		reported:     map[string]map[string]map[string]bool{},
		rejectedKeys: map[string]bool{}, rejectedPurls: map[string]bool{}, dropped: map[string]bool{},
		unusedPurls: map[string]bool{}, unusedKeys: map[string]bool{},
	}
	for _, t := range targets {
		if t.Kind == model.DependencyKindSelf {
			continue
		}
		key, version, ok := pkgid.KeyOfPurl(t.Purl)
		if !ok {
			continue
		}
		if version == "" {
			idx.versionless[key] = true
			continue
		}
		if idx.versioned[key] == nil {
			idx.versioned[key] = map[string]bool{}
		}
		idx.versioned[key][version] = true
	}
	for _, f := range findings {
		if f.DependencyKind == model.DependencyKindSelf {
			continue
		}
		key, version, ok := pkgid.KeyOfPurl(f.PackagePurl)
		if !ok {
			continue
		}
		if idx.reported[key] == nil {
			idx.reported[key] = map[string]map[string]bool{}
		}
		if idx.reported[key][f.VulnID] == nil {
			idx.reported[key][f.VulnID] = map[string]bool{}
		}
		idx.reported[key][f.VulnID][version] = true
	}
	for _, n := range notes {
		if n.Kind == model.NoRowManagedUnused {
			if n.Purl != "" {
				idx.unusedPurls[n.Purl] = true
			}
			if n.PackageKey != "" {
				idx.unusedKeys[n.PackageKey] = true
			}
			continue
		}
		if n.Kind != model.NoRowNameRejected {
			continue
		}
		if n.PackageKey != "" {
			idx.rejectedKeys[n.PackageKey] = true
		}
		if n.Purl != "" {
			idx.rejectedPurls[n.Purl] = true
		}
	}
	for _, p := range dropped {
		idx.dropped[p] = true
	}
	return idx
}

// reasonFor returns the label a finding would take if it closed in this
// scan: one of model's Closed* constants, or "" (the dependency is gone, or
// it was moved past the advisory: a closure that needs no label).
//
// Where more than one fits, the first of these applies: never_applied by
// equal purl; superseded; version_changed; version_determined; version_lost;
// never_applied by key; unsendable.
func (idx *reasonIndex) reasonFor(f model.OpenFinding) string {
	// never_applied by EQUAL purl is first: the finding is on exactly the
	// managed entry that nothing uses.
	if idx.unusedPurls[f.Purl] {
		return model.ClosedNeverApplied
	}
	key, version, ok := pkgid.KeyOfPurl(f.Purl)
	if ok && f.Kind != model.DependencyKindSelf {
		// "Versionless" is a purl with no version on a dependency whose
		// purl could have one: not a self purl (versionless by design), not
		// a GitHub Action (whose purl carries its ref).
		noVersion := version == "" && f.Ecosystem != githubActions
		reportedAt := idx.reported[key][f.VulnID]
		switch {
		case noVersion && reportedAtAVersion(reportedAt):
			return model.ClosedSuperseded
		case version != "" && reportedAtAnother(reportedAt, version):
			return model.ClosedVersionChanged
		case noVersion && len(idx.versioned[key]) > 0:
			return model.ClosedVersionDetermined
		case version != "" && idx.versionless[key]:
			return model.ClosedVersionLost
		}
	}
	// never_applied by KEY: the finding's purl has no version (a managed
	// entry whose version was an unresolved property was stored so, while
	// the note carries the version the tree resolved), the note's key
	// matches, and no target of that key remains. With a target remaining,
	// it is a real dependency of that coordinate, in another tree.
	if ok && version == "" && f.Kind != model.DependencyKindSelf && idx.unusedKeys[key] &&
		!idx.versionless[key] && len(idx.versioned[key]) == 0 {
		return model.ClosedNeverApplied
	}
	if idx.dropped[f.Purl] || idx.rejectedPurls[f.Purl] || (ok && idx.rejectedKeys[key]) {
		return model.ClosedUnsendable
	}
	return ""
}

func reportedAtAVersion(versions map[string]bool) bool {
	for v := range versions {
		if v != "" {
			return true
		}
	}
	return false
}

func reportedAtAnother(versions map[string]bool, version string) bool {
	for v := range versions {
		if v != "" && v != version {
			return true
		}
	}
	return false
}

// carryFirstDetection passes the older date on: whenever a finding closes
// and the scan reports the same advisory for the same package, the successor
// keeps the earliest first detection among its predecessors, whatever label
// the closure takes. A version bump to a still-affected version, or a version
// being worked out, must not make an old problem read as a new one.
//
// "The same package" is by key (pkgid.KeyOfPurl), which is not a perfect
// identity: two Swift packages of one name under different owners share a
// key, and the key folds case. The carry needs the same ADVISORY on both as
// well, so a date crosses two packages only when one advisory names both
// and one replaces the other in the same scan. That is accepted, as it is
// for the labels (a date, like a label, closes nothing).
//
// It sets FirstDetectedAt on the rows about to be written; the store keeps
// the earlier of that and what it already holds. It also hands on the time
// the problem entered exposure (0.29.166), under its own rule below.
func carryFirstDetection(closing []model.OpenFinding, findings []*model.VulnerabilityRow) {
	type pkgAdvisory struct{ key, advisory string }
	type predecessor struct {
		first time.Time
		// exposure: some predecessor was exposure when it closed. entered is
		// the earliest time among those, nil if any of them had none.
		exposure bool
		entered  *time.Time
	}
	before := map[pkgAdvisory]*predecessor{}
	for _, c := range closing {
		if c.Kind == model.DependencyKindSelf || c.FirstDetectedAt.IsZero() {
			continue
		}
		key, _, ok := pkgid.KeyOfPurl(c.Purl)
		if !ok {
			continue
		}
		k := pkgAdvisory{key, c.VulnID}
		p := before[k]
		if p == nil {
			p = &predecessor{first: c.FirstDetectedAt}
			before[k] = p
		}
		if c.FirstDetectedAt.Before(p.first) {
			p.first = c.FirstDetectedAt
		}
		if !wasExposure(c) {
			continue
		}
		switch {
		case !p.exposure:
			p.exposure, p.entered = true, c.EnteredExposureAt
		case c.EnteredExposureAt == nil:
			p.entered = nil
		case p.entered != nil && c.EnteredExposureAt.Before(*p.entered):
			p.entered = c.EnteredExposureAt
		}
	}
	for _, f := range findings {
		if f.DependencyKind == model.DependencyKindSelf {
			continue
		}
		key, _, ok := pkgid.KeyOfPurl(f.PackagePurl)
		if !ok {
			continue
		}
		p := before[pkgAdvisory{key, f.VulnID}]
		if p == nil {
			continue
		}
		if f.FirstDetectedAt.IsZero() || p.first.Before(f.FirstDetectedAt) {
			f.FirstDetectedAt = p.first
		}
		// The time of entering exposure is carried only from a predecessor
		// that was exposure: a version bump to a still-affected version is
		// not mailed again. A finding whose predecessor was not exposure (its
		// version has just been worked out) enters exposure now.
		if p.exposure {
			f.EnteredExposureCarried, f.EnteredExposureAt = true, p.entered
		}
	}
}

// wasExposure reports whether an open finding is of the exposure class, by
// the one Go rule (vulnclass.Of).
func wasExposure(c model.OpenFinding) bool {
	return vulnclass.Of(&model.VulnerabilityRow{DependencyKind: c.Kind, VersionResolution: c.VersionResolution, Ecosystem: c.Ecosystem}) == vulnclass.Exposure
}
