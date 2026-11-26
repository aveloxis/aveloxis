// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// Every export says what was known when it was made (summary/70, 8.8): a
// reader of the document alone can tell a repository that was checked from
// one nobody has read the dependencies of.

// neverAnalysedPublisher is the plan's first fixture: a repository never
// analysed that publishes a package. Its own package was sent and answered,
// so its state is "scanned"; the state alone would read as a clean result.
func neverAnalysedPublisher() *vulnclass.CoverageView {
	yes := true
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	attempt := at.Add(time.Hour)
	in := vulnclass.CoverageInputs{HasRow: true, ScanStamp: &at}
	in.Coverage.ScanAttemptAt = &at
	in.Coverage.Scan.Outcome = model.ScanSent
	in.Coverage.Scan.DependenciesUnknown = &yes
	in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &attempt, model.InventoryAttemptCloneUnavailable
	v := vulnclass.Coverage(in)
	return &v
}

// carriedManifest is the second: analysed, with a manifest that failed and
// whose earlier rows were kept.
func carriedManifest() *vulnclass.CoverageView {
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	in := vulnclass.CoverageInputs{HasRow: true, ScanStamp: &at}
	in.Coverage.AnalysedAt, in.Coverage.ScanAttemptAt = &at, &at
	in.Coverage.Scan.Outcome = model.ScanSent
	in.Coverage.Inventory.ManifestsFailed = []model.ManifestFailure{{Path: "a/pom.xml", RowsCarried: 4}, {Path: "b/package.json"}}
	v := vulnclass.Coverage(in)
	return &v
}

func cdxCoverageProperties(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	var doc struct {
		Metadata struct {
			Properties []struct{ Name, Value string } `json:"properties"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, p := range doc.Metadata.Properties {
		out[p.Name] = p.Value
	}
	return out
}

func TestCycloneDXCarriesTheCoverageMarker(t *testing.T) {
	repo, deps, scan := sbomSeedInputs()
	repo.Coverage = neverAnalysedPublisher()
	raw, err := generateCycloneDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	got := cdxCoverageProperties(t, raw)
	want := map[string]string{
		"aveloxis:coverage:state":                        "scanned",
		"aveloxis:coverage:dependencies_unknown":         "true",
		"aveloxis:coverage:last_analysis_attempt_reason": "clone_unavailable",
		"aveloxis:coverage:manifests_failed":             "0",
		"aveloxis:coverage:manifests_carried":            "0",
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %q, want %q (all: %v)", k, got[k], w, got)
		}
	}
	// A never-analysed publisher must not read as a plain "scanned".
	if got["aveloxis:coverage:state"] == "scanned" && got["aveloxis:coverage:dependencies_unknown"] != "true" {
		t.Error("the marker lets a never-analysed repository read as scanned")
	}
	assertCycloneDXSchemaConstraints(t, loadSchema(t, "bom-1.7.schema.json"), raw)

	repo.Coverage = carriedManifest()
	raw, err = generateCycloneDX(repo, deps, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	got = cdxCoverageProperties(t, raw)
	if got["aveloxis:coverage:manifests_failed"] != "2" || got["aveloxis:coverage:manifests_carried"] != "1" || got["aveloxis:coverage:dependencies_unknown"] != "false" {
		t.Errorf("the carried-manifest fixture: %v", got)
	}
	if _, has := got["aveloxis:coverage:last_analysis_attempt_reason"]; has {
		t.Error("an attempt reason is given where no attempt is recorded")
	}
	if _, has := got["aveloxis:coverage:lockfiles_unread_reason"]; has {
		t.Error("a lockfile reason is given where the lockfiles were read")
	}

	// 0.29.214: lockfiles that went unread. The document's versions and
	// transitive components come from the stored lockfile rows, so it says
	// when those are behind, and since when.
	behind := carriedManifest()
	read := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	behind.LockfilesUnread = &vulnclass.LockfilesUnread{Reason: model.InventoryAttemptReadMemoryLimit, LastReadAt: &read}
	repo.Coverage = behind
	raw, err = generateCycloneDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	got = cdxCoverageProperties(t, raw)
	if got["aveloxis:coverage:lockfiles_unread_reason"] != "manifest_read_memory_limit" || got["aveloxis:coverage:lockfiles_last_read_at"] != "2026-11-02T09:00:00Z" {
		t.Errorf("lockfiles behind: %v", got)
	}
	assertCycloneDXSchemaConstraints(t, loadSchema(t, "bom-1.7.schema.json"), raw)
	// Some lockfiles too large (0.29.216): the reason, and no date although
	// the view holds one.
	behind.LockfilesUnread = &vulnclass.LockfilesUnread{Reason: model.LockfileUnreadTooLarge, LastReadAt: &read}
	raw, err = generateCycloneDX(repo, deps, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	got = cdxCoverageProperties(t, raw)
	if _, has := got["aveloxis:coverage:lockfiles_last_read_at"]; has || got["aveloxis:coverage:lockfiles_unread_reason"] != "lockfiles_too_large" {
		t.Errorf("some lockfiles too large: %v", got)
	}
	behind.LockfilesUnread = &vulnclass.LockfilesUnread{Reason: model.InventoryAttemptReadFailed}
	raw, err = generateCycloneDX(repo, deps, scan, nil)
	if err != nil {
		t.Fatal(err)
	}
	got = cdxCoverageProperties(t, raw)
	if _, has := got["aveloxis:coverage:lockfiles_last_read_at"]; has || got["aveloxis:coverage:lockfiles_unread_reason"] != "manifest_read_failed" {
		t.Errorf("lockfiles never read: %v", got)
	}

	// With no coverage read (a unit test, a caller that has none) there is
	// no marker, and no empty properties list.
	repo.Coverage = nil
	raw, _ = generateCycloneDX(repo, deps, scan, nil)
	if strings.Contains(string(raw), "aveloxis:coverage") || strings.Contains(string(raw), `"properties": []`) {
		t.Error("a document with no coverage carries a marker or an empty list")
	}
}

func TestSPDXCarriesTheCoverageMarker(t *testing.T) {
	repo, deps, scan := sbomSeedInputs()
	repo.Coverage = neverAnalysedPublisher()
	raw, err := generateSPDX(repo, deps, scan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Comment string `json:"comment"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"aveloxis:coverage", "state=scanned", "dependencies_unknown=true", "last_analysis_attempt_reason=clone_unavailable", "manifests_failed=0", "manifests_carried=0"} {
		if !strings.Contains(doc.Comment, part) {
			t.Errorf("the document comment %q lacks %q", doc.Comment, part)
		}
	}
	assertSPDXSchemaConstraints(t, loadSchema(t, "spdx-2.3.schema.json"), raw)

	repo.Coverage = carriedManifest()
	raw, _ = generateSPDX(repo, deps, scan, nil)
	_ = json.Unmarshal(raw, &doc)
	if !strings.Contains(doc.Comment, "manifests_failed=2") || !strings.Contains(doc.Comment, "manifests_carried=1") || strings.Contains(doc.Comment, "attempt_reason") {
		t.Errorf("the carried-manifest fixture: %q", doc.Comment)
	}
	// The lockfile caution's two facts (0.29.214) are in the SPDX marker
	// too, in both cases, and absent when there is nothing to caution about.
	if strings.Contains(doc.Comment, "lockfiles_") {
		t.Errorf("a document with nothing to caution about says something of lockfiles: %q", doc.Comment)
	}
	read := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	behind := *carriedManifest()
	behind.LockfilesUnread = &vulnclass.LockfilesUnread{Reason: model.InventoryAttemptReadMemoryLimit, LastReadAt: &read}
	repo.Coverage = &behind
	raw, _ = generateSPDX(repo, deps, scan, sbomSeedGraph())
	_ = json.Unmarshal(raw, &doc)
	if !strings.Contains(doc.Comment, "lockfiles_unread_reason=manifest_read_memory_limit") || !strings.Contains(doc.Comment, "lockfiles_last_read_at=2026-11-02T09:00:00Z") {
		t.Errorf("behind: %q", doc.Comment)
	}
	assertSPDXSchemaConstraints(t, loadSchema(t, "spdx-2.3.schema.json"), raw)
	never := *carriedManifest()
	never.LockfilesUnread = &vulnclass.LockfilesUnread{Reason: model.LockfileUnreadNotReached}
	repo.Coverage = &never
	raw, _ = generateSPDX(repo, deps, scan, nil)
	_ = json.Unmarshal(raw, &doc)
	if !strings.Contains(doc.Comment, "lockfiles_unread_reason=lockfiles_not_reached") || strings.Contains(doc.Comment, "lockfiles_last_read_at") {
		t.Errorf("never read: %q", doc.Comment)
	}
	repo.Coverage = nil
	raw, _ = generateSPDX(repo, deps, scan, nil)
	if strings.Contains(string(raw), "aveloxis:coverage") || strings.Contains(string(raw), `"comment": ""`) {
		t.Error("a document with no coverage carries a marker or an empty comment")
	}
}

// A finding whose version is not determined says so exactly, in a property:
// the analysis state stays in_triage, the nearest value the schema has.
func TestCycloneDXMarksVersionNotDeterminedWithAProperty(t *testing.T) {
	vRepo, vDeps, vScan, vulns := sbomVulnSeed()
	raw, err := generateCycloneDX(vRepo, vDeps, vScan, sbomSeedGraph())
	if err != nil {
		t.Fatal(err)
	}
	ann, err := AnnotateCycloneDXWithVulns(raw, vulns)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vulnerabilities []struct {
			ID         string                         `json:"id"`
			Analysis   *struct{ State string }        `json:"analysis"`
			Properties []struct{ Name, Value string } `json:"properties"`
		} `json:"vulnerabilities"`
	}
	if err := json.Unmarshal(ann, &doc); err != nil {
		t.Fatal(err)
	}
	marked, unmarked := 0, 0
	for _, v := range doc.Vulnerabilities {
		has := false
		for _, p := range v.Properties {
			if p.Name == "aveloxis:version_not_determined" && p.Value == "true" {
				has = true
			}
		}
		inTriage := v.Analysis != nil && v.Analysis.State == "in_triage"
		if has != inTriage {
			t.Errorf("%s: property %v, in_triage %v; the property is on exactly the findings whose version is not determined", v.ID, has, inTriage)
		}
		if has {
			marked++
		} else {
			unmarked++
		}
	}
	if marked == 0 || unmarked == 0 {
		t.Fatalf("the seed has %d marked and %d unmarked findings; it needs both", marked, unmarked)
	}
	assertCycloneDXSchemaConstraints(t, loadSchema(t, "bom-1.7.schema.json"), ann)
}
