// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package numfocus

import (
	"strings"
	"testing"
)

// TestEmbeddedCatalogParses pins that the binary-embedded data.yaml
// loads cleanly. If the YAML grows a syntax error during a manual
// edit, this test fails before any operator hits the runtime parse.
func TestEmbeddedCatalogParses(t *testing.T) {
	c, err := LoadCatalog()
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	if len(c.Sponsored) == 0 {
		t.Error("catalog has zero sponsored entries — should have ~63")
	}
	if len(c.Affiliated) == 0 {
		t.Error("catalog has zero affiliated entries — should have ~103")
	}
}

// TestEmbeddedCatalogShapeMatchesNumfocus2026 pins the expected
// entry counts. Both numbers came from the 2026-05-25 numfocus.org
// crawl. The test allows for small numfocus.org additions (a few
// new affiliated projects between catalog updates) but flags
// large divergences as drift.
//
// If this test fails, that's the trigger to run
// `aveloxis load-numfocus-projects --detect-new` and update the
// catalog with the new entries.
func TestEmbeddedCatalogShapeMatchesNumfocus2026(t *testing.T) {
	c, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Sponsored) < 50 || len(c.Sponsored) > 100 {
		t.Errorf("sponsored entries = %d; expected ~63 (range 50-100 to absorb churn)", len(c.Sponsored))
	}
	if len(c.Affiliated) < 80 || len(c.Affiliated) > 150 {
		t.Errorf("affiliated entries = %d; expected ~103 (range 80-150 to absorb churn)", len(c.Affiliated))
	}
}

// TestProjectURLConstruction pins the github/gitlab URL synthesis.
// The shape is operator-facing — the URLs flow into the
// repos.repo_git column via AddRepoToGroup, and a wrong URL there
// silently misroutes a collection job.
func TestProjectURLConstruction(t *testing.T) {
	cases := []struct {
		name        string
		project     Project
		wantPrimary string
		wantOrg     string
	}{
		{
			"github",
			Project{Platform: "github", Org: "numpy", PrimaryRepo: "numpy"},
			"https://github.com/numpy/numpy",
			"https://github.com/numpy",
		},
		{
			"gitlab",
			Project{Platform: "gitlab", Org: "petsc", PrimaryRepo: "petsc"},
			"https://gitlab.com/petsc/petsc",
			"https://gitlab.com/petsc",
		},
		{
			"other (self-hosted) — both URLs empty",
			Project{Platform: "other", Org: "dynare", PrimaryRepo: "dynare"},
			"",
			"",
		},
		{
			"missing org — both URLs empty",
			Project{Platform: "github", Org: "", PrimaryRepo: "x"},
			"",
			"",
		},
		{
			"missing primary_repo — primary empty, org still valid",
			Project{Platform: "github", Org: "x", PrimaryRepo: ""},
			"",
			"https://github.com/x",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.project.PrimaryURL(); got != tc.wantPrimary {
				t.Errorf("PrimaryURL = %q, want %q", got, tc.wantPrimary)
			}
			if got := tc.project.OrgURL(); got != tc.wantOrg {
				t.Errorf("OrgURL = %q, want %q", got, tc.wantOrg)
			}
		})
	}
}

// TestIsActionableGatesNeedsReview pins that needs_review entries
// (no org / no primary_repo) are filtered out by AllProjects so
// the load-numfocus-projects command doesn't try to insert empty
// URLs.
func TestIsActionableGatesNeedsReview(t *testing.T) {
	c := &Catalog{
		Sponsored: []Project{
			{Slug: "real", Org: "x", PrimaryRepo: "y", Platform: "github"},
			{Slug: "review-needed", Org: "", PrimaryRepo: "", Platform: "other", Confidence: "needs_review"},
		},
	}
	got := c.AllProjects()
	if len(got) != 1 {
		t.Errorf("AllProjects returned %d entries; want 1 (review-needed must be filtered)", len(got))
	}
	if got[0].Slug != "real" {
		t.Errorf("AllProjects[0].Slug = %q; want \"real\"", got[0].Slug)
	}
}

// TestEmbeddedCatalogIsActionableForMost pins that the embedded
// catalog has very few needs_review entries — a regression that
// flips many entries to needs_review (e.g. a YAML refactor that
// breaks the inline-style parsing) would silently halt most
// inserts.
func TestEmbeddedCatalogIsActionableForMost(t *testing.T) {
	c, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	total := len(c.Sponsored) + len(c.Affiliated)
	actionable := len(c.AllProjects())
	skipped := total - actionable
	if skipped > 10 {
		t.Errorf("catalog has %d needs_review/non-actionable entries out of %d; expected at most ~5. A higher count usually means a YAML parsing regression dropped org/primary_repo fields silently.",
			skipped, total)
	}
}

// TestAllProjectsIncludesKnownAnchors pins that a handful of
// household-name projects survive every parse pass. If a YAML
// refactor accidentally drops numpy or pandas, this test screams.
func TestAllProjectsIncludesKnownAnchors(t *testing.T) {
	c, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	anchors := map[string]bool{
		"numpy":        false,
		"pandas":       false,
		"matplotlib":   false,
		"scipy":        false,
		"scikit-learn": false,
		"napari":       false,
		"bambi":        false, // recent affiliated, in case sponsored ones get reshuffled
	}
	for _, p := range c.AllProjects() {
		if _, want := anchors[p.Slug]; want {
			anchors[p.Slug] = true
		}
	}
	for slug, found := range anchors {
		if !found {
			t.Errorf("catalog anchor %q missing — a YAML edit likely dropped it", slug)
		}
	}
}

// TestNeedsReviewEntriesHaveExplanatoryNotes pins that every
// needs_review entry carries a Note. Without a note, operators
// see "skipped X" with no actionable context — defeats the
// purpose of having the needs_review section at all.
func TestNeedsReviewEntriesHaveExplanatoryNotes(t *testing.T) {
	c, err := LoadCatalog()
	if err != nil {
		t.Fatal(err)
	}
	check := func(section string, ps []Project) {
		for _, p := range ps {
			if p.Confidence == "needs_review" && strings.TrimSpace(p.Note) == "" {
				t.Errorf("[%s] %s has confidence=needs_review but no Note — operators need an explanation of why it was flagged", section, p.Slug)
			}
		}
	}
	check("sponsored", c.Sponsored)
	check("affiliated", c.Affiliated)
	check("needs_review", c.NeedsReview)
}
