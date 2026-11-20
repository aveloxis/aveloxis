// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package linking

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// A reason never selects a finding to close; it only labels one that closes
// (summary/70, section 3, rule 2). These tests are over the labelling alone.

func open(vulnID, purl, kind, ecosystem string) model.OpenFinding {
	return model.OpenFinding{VulnID: vulnID, Purl: purl, Kind: kind, Ecosystem: ecosystem,
		FirstDetectedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func target(purl string) Target { return Target{Purl: purl, Kind: model.DependencyKindDirect} }

func TestReasonFor(t *testing.T) {
	const adv = "GHSA-1"
	cases := []struct {
		name     string
		closing  model.OpenFinding
		targets  []Target
		findings []*model.VulnerabilityRow // what the scan reports now
		notes    []model.InventoryNote
		dropped  []string
		want     string
	}{
		{"a version was worked out, and the advisory affects it",
			open(adv, "pkg:pypi/requests", "direct", "PyPI"),
			[]Target{target("pkg:pypi/requests@2.25.0")},
			[]*model.VulnerabilityRow{finding(adv, "pkg:pypi/requests@2.25.0")}, nil, nil,
			model.ClosedSuperseded},
		{"a version was worked out, and the advisory does not affect it",
			open(adv, "pkg:pypi/requests", "direct", "PyPI"),
			[]Target{target("pkg:pypi/requests@2.31.0")},
			[]*model.VulnerabilityRow{finding("GHSA-other", "pkg:pypi/requests@2.31.0")}, nil, nil,
			model.ClosedVersionDetermined},
		{"the same package under another spelling of its name",
			open(adv, "pkg:pypi/flask-sqlalchemy", "direct", "PyPI"),
			[]Target{target("pkg:pypi/flask-sqlalchemy@2.5.1")}, nil, nil, nil,
			model.ClosedVersionDetermined},
		{"the dependency moved to a version the advisory still affects",
			open(adv, "pkg:npm/lodash@4.17.20", "direct", "npm"),
			[]Target{target("pkg:npm/lodash@4.17.21")},
			[]*model.VulnerabilityRow{finding(adv, "pkg:npm/lodash@4.17.21")}, nil, nil,
			model.ClosedVersionChanged},
		{"the dependency moved to a version the advisory does not affect: a fix, no label",
			open(adv, "pkg:npm/lodash@4.17.20", "direct", "npm"),
			[]Target{target("pkg:npm/lodash@4.17.21")}, nil, nil, nil,
			""},
		{"the version was lost (a lockfile was deleted)",
			open(adv, "pkg:npm/lodash@4.17.20", "transitive", "npm"),
			[]Target{target("pkg:npm/lodash")}, nil, nil, nil,
			model.ClosedVersionLost},
		{"the version was lost, though the advisory is reported at the versionless target",
			open(adv, "pkg:npm/lodash@4.17.20", "direct", "npm"),
			[]Target{target("pkg:npm/lodash")},
			[]*model.VulnerabilityRow{finding(adv, "pkg:npm/lodash")}, nil, nil,
			model.ClosedVersionLost},
		{"the dependency is gone: no label",
			open(adv, "pkg:npm/lodash@4.17.20", "direct", "npm"),
			[]Target{target("pkg:npm/express@4.0.0")}, nil, nil, nil,
			""},
		{"the wire gate dropped the purl",
			open(adv, "pkg:npm//raw@1.0.0", "direct", "npm"),
			nil, nil, nil, []string{"pkg:npm//raw@1.0.0"},
			model.ClosedUnsendable},
		{"a rejected name carrying that purl",
			open(adv, "pkg:npm/not%20a%20name!@1.0.0", "direct", "npm"),
			nil, nil, []model.InventoryNote{{Kind: model.NoRowNameRejected, PackageKey: "npm|not a name!", Purl: "pkg:npm/not%20a%20name!@1.0.0"}}, nil,
			model.ClosedUnsendable},
		{"a rejected name of that key, with no purl",
			open(adv, "pkg:pypi/badname@1.0", "direct", "PyPI"),
			nil, nil, []model.InventoryNote{{Kind: model.NoRowNameRejected, PackageKey: "pypi|badname"}}, nil,
			model.ClosedUnsendable},
		{"a note of another kind labels nothing",
			open(adv, "pkg:npm/local-tool@1.0.0", "direct", "npm"),
			nil, nil, []model.InventoryNote{{Kind: model.NoRowNonRegistry, PackageKey: "npm|local-tool"}, {Kind: model.NoRowNoNamespace, PackageKey: "npm|local-tool"}}, nil,
			""},
		// A same-key target wins over "unsendable": the package is still
		// being checked, at another purl.
		{"superseded is chosen before unsendable",
			open(adv, "pkg:pypi/requests", "direct", "PyPI"),
			[]Target{target("pkg:pypi/requests@2.25.0")},
			[]*model.VulnerabilityRow{finding(adv, "pkg:pypi/requests@2.25.0")}, nil, []string{"pkg:pypi/requests"},
			model.ClosedSuperseded},
		// "Versionless" does not mean "no version in the purl" for these two.
		{"a finding about the repository's own package is never 'versionless'",
			open(adv, "pkg:pypi/own", "self", "PyPI"),
			[]Target{target("pkg:pypi/own@1.0.0")}, nil, nil, nil,
			""},
		{"a GitHub Actions purl carries its ref: a ref change is a version change",
			open(adv, "pkg:githubactions/actions/checkout@v3", "direct", "githubactions"),
			[]Target{target("pkg:githubactions/actions/checkout@v4")},
			[]*model.VulnerabilityRow{finding(adv, "pkg:githubactions/actions/checkout@v4")}, nil, nil,
			model.ClosedVersionChanged},
		// A self target is never "a same-key target": its whole-history
		// answer would otherwise label a dependency's closure.
		{"a same-key target of kind self does not count",
			open(adv, "pkg:pypi/requests@2.25.0", "direct", "PyPI"),
			[]Target{{Purl: "pkg:pypi/requests", Kind: model.DependencyKindSelf}}, nil, nil, nil,
			""},
		{"a purl that cannot be keyed gets no label",
			open(adv, "not-a-purl", "direct", "npm"),
			[]Target{target("pkg:npm/lodash@4.17.21")}, nil, nil, nil,
			""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			idx := newReasonIndex(c.targets, c.findings, c.notes, c.dropped)
			if got := idx.reasonFor(c.closing); got != c.want {
				t.Errorf("reason = %q, want %q", got, c.want)
			}
		})
	}
}

// The older date is passed to a successor: whenever a finding closes and the
// scan reports the same advisory at a same-key target, the successor keeps the
// predecessor's first detection, whatever the label. A routine version bump
// must not read as a new problem.
func TestCarryFirstDetection(t *testing.T) {
	old := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)
	closing := []model.OpenFinding{
		{VulnID: "GHSA-1", Purl: "pkg:npm/lodash@4.17.20", Kind: "direct", Ecosystem: "npm", FirstDetectedAt: old},
		{VulnID: "GHSA-1", Purl: "pkg:npm/lodash", Kind: "direct", Ecosystem: "npm", FirstDetectedAt: older}, // two predecessors: the oldest wins
		{VulnID: "GHSA-2", Purl: "pkg:npm/lodash@4.17.20", Kind: "direct", Ecosystem: "npm", FirstDetectedAt: old},
		{VulnID: "GHSA-3", Purl: "pkg:npm/express@4.0.0", Kind: "direct", Ecosystem: "npm", FirstDetectedAt: old},
	}
	successor := finding("GHSA-1", "pkg:npm/lodash@4.17.21")
	otherAdvisory := finding("GHSA-9", "pkg:npm/lodash@4.17.21")
	otherPackage := finding("GHSA-3", "pkg:npm/qs@6.0.0")
	carryFirstDetection(closing, []*model.VulnerabilityRow{successor, otherAdvisory, otherPackage})
	if !successor.FirstDetectedAt.Equal(older) {
		t.Errorf("successor first detected %v; want the oldest predecessor's, %v", successor.FirstDetectedAt, older)
	}
	if !otherAdvisory.FirstDetectedAt.IsZero() || !otherPackage.FirstDetectedAt.IsZero() {
		t.Errorf("a date was carried to another advisory (%v) or another package (%v)", otherAdvisory.FirstDetectedAt, otherPackage.FirstDetectedAt)
	}
}

// The time of entering exposure is carried from a predecessor that was
// exposure, and only from one.
func TestCarryEnteredExposure(t *testing.T) {
	first := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	entered := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	later := entered.Add(24 * time.Hour)
	open := func(id, purl, resolution string, at *time.Time) model.OpenFinding {
		return model.OpenFinding{VulnID: id, Purl: purl, Kind: "direct", Ecosystem: "PyPI", FirstDetectedAt: first, VersionResolution: resolution, EnteredExposureAt: at}
	}
	row := func(id, purl string) *model.VulnerabilityRow {
		return &model.VulnerabilityRow{VulnID: id, PackagePurl: purl, DependencyKind: "direct"}
	}
	bump := row("A", "pkg:pypi/a@2.0")          // predecessor was exposure, with a time
	determined := row("B", "pkg:pypi/b@1.0")    // predecessor had no version: not exposure
	preRelease := row("C", "pkg:pypi/c@2.0")    // predecessor was exposure, no time recorded
	two := row("D", "pkg:pypi/d@3.0")           // two exposure predecessors: the earlier time
	twoOneUnknown := row("E", "pkg:pypi/e@3.0") // two, one with no time: none
	unrelated := row("F", "pkg:pypi/f@1.0")     // no predecessor
	carryFirstDetection([]model.OpenFinding{
		open("A", "pkg:pypi/a@1.0", "locked", &entered),
		open("B", "pkg:pypi/b", "unpinned", nil),
		open("C", "pkg:pypi/c@1.0", "locked", nil),
		open("D", "pkg:pypi/d@1.0", "locked", &later), open("D", "pkg:pypi/d@2.0", "exact", &entered),
		open("E", "pkg:pypi/e@1.0", "locked", &entered), open("E", "pkg:pypi/e@2.0", "locked", nil),
	}, []*model.VulnerabilityRow{bump, determined, preRelease, two, twoOneUnknown, unrelated})

	check := func(name string, r *model.VulnerabilityRow, carried bool, at *time.Time) {
		t.Helper()
		if r.EnteredExposureCarried != carried {
			t.Errorf("%s: carried = %v, want %v", name, r.EnteredExposureCarried, carried)
			return
		}
		if (r.EnteredExposureAt == nil) != (at == nil) || (at != nil && !r.EnteredExposureAt.Equal(*at)) {
			t.Errorf("%s: entered exposure %v, want %v", name, r.EnteredExposureAt, at)
		}
	}
	check("a version bump", bump, true, &entered)
	check("a version worked out", determined, false, nil)
	check("a bump of a pre-release finding", preRelease, true, nil)
	check("two predecessors", two, true, &entered)
	check("two predecessors, one with no time", twoOneUnknown, true, nil)
	check("no predecessor", unrelated, false, nil)
	if !determined.FirstDetectedAt.Equal(first) {
		t.Error("first detection is carried whatever the predecessor's class")
	}
}

// never_applied (0.29.183): a finding on a Maven dependencyManagement entry
// whose version no dependency took. It was never exposure.
func TestReasonNeverApplied(t *testing.T) {
	note := model.InventoryNote{Kind: model.NoRowManagedUnused, PackageManager: "maven", Name: "com.acme:unused",
		PackageKey: "maven|com.acme:unused", Purl: "pkg:maven/com.acme/unused@3.0"}
	open := func(purl string) model.OpenFinding {
		return model.OpenFinding{VulnID: "GHSA-1", Purl: purl, Kind: "direct", Ecosystem: "maven"}
	}
	for _, c := range []struct {
		name    string
		purl    string
		targets []Target
		want    string
	}{
		{"the note's own purl", "pkg:maven/com.acme/unused@3.0", nil, model.ClosedNeverApplied},
		// The entry was stored versionless (its version was an unresolved
		// property); the note carries the version the tree worked out.
		{"a versionless purl of the note's key, no target left", "pkg:maven/com.acme/unused", nil, model.ClosedNeverApplied},
		// The coordinate is a real dependency elsewhere, now at a version:
		// that is a version being worked out, not "never applied".
		{"a versionless purl, and a target of that key remains", "pkg:maven/com.acme/unused",
			[]Target{{Purl: "pkg:maven/com.acme/unused@5.0", Kind: "direct"}}, model.ClosedVersionDetermined},
		// The coordinate is still a dependency with no version (a target of
		// the same key, spelled with other capitals): the finding closes
		// because a complete answer omits its advisory. That is a fix, and
		// takes no label.
		{"a versionless purl, and a versionless target of that key remains", "pkg:maven/com.acme/unused",
			[]Target{{Purl: "pkg:maven/com.acme/Unused", Kind: "direct"}}, ""},
		// Another version of the coordinate, with no note for it: gone.
		{"another version, no note for it", "pkg:maven/com.acme/unused@2.0", nil, ""},
		{"another package", "pkg:maven/com.acme/other@3.0", nil, ""},
	} {
		idx := newReasonIndex(c.targets, nil, []model.InventoryNote{note}, nil)
		if got := idx.reasonFor(open(c.purl)); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	// The equal-purl rule is FIRST: it wins over a label the same finding
	// would otherwise take.
	idx := newReasonIndex([]Target{{Purl: "pkg:maven/com.acme/unused@4.5", Kind: "direct"}},
		[]*model.VulnerabilityRow{{VulnID: "GHSA-1", PackagePurl: "pkg:maven/com.acme/unused@4.5", DependencyKind: "direct"}},
		[]model.InventoryNote{note}, nil)
	if got := idx.reasonFor(open("pkg:maven/com.acme/unused@3.0")); got != model.ClosedNeverApplied {
		t.Errorf("an overridden managed entry with the advisory reported at the overriding version: %q, want never_applied (not version_changed)", got)
	}
}
