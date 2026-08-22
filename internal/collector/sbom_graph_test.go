// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// sbom_graph_test.go — v0.27.134: SBOM dependency graphs from the C2
// lockfile edges. The load-bearing contracts:
//   - graph == nil => pre-v0.27.134 output shape verbatim (knob-off
//     fleets stay byte-stable);
//   - every ref/SPDXID a graph edge emits resolves to a component/
//     package actually in the document (validity rule);
//   - transitives dedup against direct deps (unique bom-ref / SPDXID);
//   - no path data => leaves/relationship-less packages, never
//     fabricated root links.

package collector

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func sbomGraphFixture() *sbomGraph {
	return &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "pypi", PackageName: "click", ResolvedVersion: "8.1.7", LockfilePath: "poetry.lock"},
			{Ecosystem: "pypi", PackageName: "werkzeug", ResolvedVersion: "2.3.7", LockfilePath: "poetry.lock"},
		},
		Edges: []db.RepoLockfileEdge{
			{Ecosystem: "pypi", LockfilePath: "poetry.lock", ParentName: "flask", ParentVersion: "2.0.0", ChildName: "werkzeug", ChildConstraint: ">=2.3"},
			{Ecosystem: "pypi", LockfilePath: "poetry.lock", ParentName: "werkzeug", ParentVersion: "2.3.7", ChildName: "click", ChildConstraint: ">=8"},
		},
	}
}

func TestGenerateCycloneDX_RealGraph(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0", Type: "runtime"},
	}
	data, err := generateCycloneDX(repo, deps, nil, sbomGraphFixture())
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	// 1 direct + 2 transitive components.
	if len(bom.Components) != 3 {
		t.Fatalf("expected 3 components, got %d", len(bom.Components))
	}
	refs := map[string]bool{bom.Metadata.Component.BOMRef: true}
	depsOf := map[string][]string{}
	for _, c := range bom.Components {
		refs[c.BOMRef] = true
	}
	for _, d := range bom.Dependencies {
		depsOf[d.Ref] = d.DependsOn
	}
	// Validity rule: every ref and every dependsOn entry resolves.
	for _, d := range bom.Dependencies {
		if !refs[d.Ref] {
			t.Errorf("dangling dependency ref %q", d.Ref)
		}
		for _, c := range d.DependsOn {
			if !refs[c] {
				t.Errorf("dangling dependsOn %q under %q", c, d.Ref)
			}
		}
	}
	// Root depends on the DIRECT set only.
	root := depsOf[bom.Metadata.Component.BOMRef]
	if len(root) != 1 || root[0] != "pkg:pypi/flask@2.0.0" {
		t.Errorf("root must depend on the direct set only, got %v", root)
	}
	// The real chain: flask → werkzeug → click, click a leaf.
	if got := depsOf["pkg:pypi/flask@2.0.0"]; len(got) != 1 || got[0] != "pkg:pypi/werkzeug@2.3.7" {
		t.Errorf("flask children = %v, want [pkg:pypi/werkzeug@2.3.7]", got)
	}
	if got := depsOf["pkg:pypi/werkzeug@2.3.7"]; len(got) != 1 || got[0] != "pkg:pypi/click@8.1.7" {
		t.Errorf("werkzeug children = %v, want [pkg:pypi/click@8.1.7]", got)
	}
	if got, ok := depsOf["pkg:pypi/click@8.1.7"]; !ok || len(got) != 0 {
		t.Errorf("click must be an explicit leaf, got %v (present=%v)", got, ok)
	}
}

func TestGenerateCycloneDX_NilGraphKeepsFlatShape(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0", Type: "runtime"},
	}
	data, err := generateCycloneDX(repo, deps, nil, nil)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(bom.Components) != 1 {
		t.Fatalf("nil graph must add nothing: %d components", len(bom.Components))
	}
	// Pre-v0.27.134 shape: root→all, each dep an empty-list leaf.
	if len(bom.Dependencies) != 2 {
		t.Fatalf("expected root + 1 leaf, got %d dependency entries", len(bom.Dependencies))
	}
	if len(bom.Dependencies[1].DependsOn) != 0 {
		t.Errorf("leaf must have empty dependsOn, got %v", bom.Dependencies[1].DependsOn)
	}
}

func TestGenerateCycloneDX_UnresolvableEdgeContributesNothing(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0", Type: "runtime"},
	}
	graph := &sbomGraph{
		Edges: []db.RepoLockfileEdge{
			// Child never appears as a package anywhere — the edge must
			// not manufacture a dangling ref.
			{Ecosystem: "pypi", ParentName: "flask", ChildName: "ghost-package"},
		},
	}
	data, err := generateCycloneDX(repo, deps, nil, graph)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	if strings.Contains(string(data), "ghost-package") {
		t.Error("unresolvable edge endpoint leaked into the document")
	}
}

func TestGenerateCycloneDX_TransitiveDedupsAgainstDirect(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0", Type: "runtime"},
	}
	graph := &sbomGraph{
		// Same purl as the direct dep — bom-ref must stay unique.
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "pypi", PackageName: "flask", ResolvedVersion: "2.0.0"},
		},
	}
	data, err := generateCycloneDX(repo, deps, nil, graph)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(bom.Components) != 1 {
		t.Errorf("colliding transitive must dedup, got %d components", len(bom.Components))
	}
}

func TestGenerateCycloneDX_EcosystemAliasFoldResolvesDirectParent(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	// Libyear writes package_manager "gem"; the Gemfile.lock roster
	// entry emits ecosystem "rubygems" on its edges. The fold must
	// bridge the two vocabularies or every Ruby chain dangles.
	deps := []db.SBOMDep{
		{Name: "rails", CurrentVersion: "7.0.0", PackageManager: "gem", Purl: "pkg:gem/rails@7.0.0", Type: "runtime"},
	}
	graph := &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "rubygems", PackageName: "actionpack", ResolvedVersion: "7.0.0"},
		},
		Edges: []db.RepoLockfileEdge{
			{Ecosystem: "rubygems", ParentName: "rails", ChildName: "actionpack"},
		},
	}
	data, err := generateCycloneDX(repo, deps, nil, graph)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	found := false
	for _, d := range bom.Dependencies {
		if d.Ref == "pkg:gem/rails@7.0.0" && len(d.DependsOn) == 1 &&
			d.DependsOn[0] == "pkg:gem/actionpack@7.0.0" {
			found = true
		}
	}
	if !found {
		t.Errorf("rubygems edge failed to resolve gem-side direct parent: %s", data)
	}
}

// Round-19 (Copilot): an edge names the parent's RESOLVED version —
// its children must hang off that version's component only, never
// every same-name component (a multi-version npm tree would otherwise
// show p@1's dependencies under p@2).
func TestGenerateCycloneDX_ParentVersionExactAttach(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	graph := &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "p", ResolvedVersion: "1.0.0"},
			{Ecosystem: "npm", PackageName: "p", ResolvedVersion: "2.0.0"},
			{Ecosystem: "npm", PackageName: "c", ResolvedVersion: "3.0.0"},
		},
		Edges: []db.RepoLockfileEdge{
			// Only p@1.0.0 requires c.
			{Ecosystem: "npm", ParentName: "p", ParentVersion: "1.0.0", ChildName: "c"},
		},
	}
	data, err := generateCycloneDX(repo, nil, nil, graph)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	depsOf := map[string][]string{}
	for _, d := range bom.Dependencies {
		depsOf[d.Ref] = d.DependsOn
	}
	if got := depsOf["pkg:npm/p@1.0.0"]; len(got) != 1 || got[0] != "pkg:npm/c@3.0.0" {
		t.Errorf("p@1.0.0 children = %v, want [pkg:npm/c@3.0.0]", got)
	}
	if got := depsOf["pkg:npm/p@2.0.0"]; len(got) != 0 {
		t.Errorf("p@2.0.0 must NOT inherit p@1.0.0's children, got %v", got)
	}
}

func TestGenerateSPDX_GraphRelationshipsAndPackages(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0"},
	}
	data, err := generateSPDX(repo, deps, nil, sbomGraphFixture())
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	// root + flask + 2 transitives.
	if len(doc.Packages) != 4 {
		t.Fatalf("expected 4 packages, got %d", len(doc.Packages))
	}
	ids := map[string]string{} // name -> SPDXID
	for _, p := range doc.Packages {
		ids[p.Name] = p.SPDXID
		if p.Name == "click" || p.Name == "werkzeug" {
			if p.LicenseDeclared != "NOASSERTION" || p.LicenseConcluded != "NOASSERTION" {
				t.Errorf("transitive %s licenses must be NOASSERTION (lockfiles carry none)", p.Name)
			}
			if len(p.ExternalRefs) != 1 || !strings.HasPrefix(p.ExternalRefs[0].ReferenceLocator, "pkg:pypi/") {
				t.Errorf("transitive %s missing purl externalRef: %+v", p.Name, p.ExternalRefs)
			}
		}
	}
	// pkg→pkg DEPENDS_ON mirroring the edges, with resolving SPDXIDs.
	validIDs := map[string]bool{"SPDXRef-DOCUMENT": true, "SPDXRef-RootPackage": true}
	for _, p := range doc.Packages {
		validIDs[p.SPDXID] = true
	}
	var flaskToWerkzeug, werkzeugToClick bool
	for _, r := range doc.Relationships {
		if !validIDs[r.SpdxElementId] || !validIDs[r.RelatedSpdxElement] {
			t.Errorf("dangling relationship %+v", r)
		}
		if r.RelationshipType == "DEPENDS_ON" {
			if r.SpdxElementId == ids["flask"] && r.RelatedSpdxElement == ids["werkzeug"] {
				flaskToWerkzeug = true
			}
			if r.SpdxElementId == ids["werkzeug"] && r.RelatedSpdxElement == ids["click"] {
				werkzeugToClick = true
			}
		}
	}
	if !flaskToWerkzeug || !werkzeugToClick {
		t.Errorf("edge relationships missing: flask→werkzeug=%v werkzeug→click=%v", flaskToWerkzeug, werkzeugToClick)
	}
}

func TestGenerateSPDX_TransitiveIDCollisionDedups(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0"},
	}
	graph := &sbomGraph{
		// Same name@version as the direct dep — spdxPackageID hashes
		// identically; a duplicate SPDXID is document-invalid.
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "pypi", PackageName: "flask", ResolvedVersion: "2.0.0"},
		},
	}
	data, err := generateSPDX(repo, deps, nil, graph)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(doc.Packages) != 2 { // root + flask, once
		t.Errorf("colliding transitive must dedup, got %d packages", len(doc.Packages))
	}
}

// TestGenerateSBOMLoadsGraphFromStore pins the wiring: the options
// path loads BOTH C2 reads and fails closed on lookup errors (SR-5 —
// a DB error must never silently downgrade the document to flat).
func TestGenerateSBOMLoadsGraphFromStore(t *testing.T) {
	body := extractFuncBody(t, "sbom.go", "func GenerateSBOMWithOptions(")
	// v0.27.151 (round 30): the PATH-preserving getter — the folded
	// GetRepoTransitivePackages loses lockfile provenance, and the
	// graph resolves edges per lockfile.
	for _, needle := range []string{"GetRepoTransitivePackagesWithPaths(", "GetRepoLockfileEdges("} {
		if !strings.Contains(body, needle) {
			t.Errorf("GenerateSBOMWithOptions must call %s", needle)
		}
	}
	if !strings.Contains(body, "loading lockfile transitives") || !strings.Contains(body, "loading lockfile edges") {
		t.Error("C2 lookups must FAIL generation on error, not degrade silently")
	}
}

// Round-24 (Copilot): spdxPackageID hashed bare name@version, so
// npm/foo@1.0.0 and pypi/foo@1.0.0 shared one SPDXID — seenIDs
// dropped the second package while its graph key kept the shared ID,
// silently pointing one ecosystem's relationships at the other's
// purl. IDs are now ecosystem-scoped (alias-folded, so gem/rubygems
// spellings of the SAME package still deduplicate).
func TestGenerateSPDX_CrossEcosystemNameCollision(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	graph := &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "foo", ResolvedVersion: "1.0.0"},
			{Ecosystem: "pypi", PackageName: "foo", ResolvedVersion: "1.0.0"},
		},
		Edges: []db.RepoLockfileEdge{
			{Ecosystem: "pypi", ParentName: "flask", ChildName: "foo"},
		},
	}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", PackageManager: "pypi", Purl: "pkg:pypi/flask@2.0.0"},
	}
	data, err := generateSPDX(repo, deps, nil, graph)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	// root + flask + BOTH foos = 4 packages, with distinct SPDXIDs and
	// each carrying its OWN ecosystem's purl.
	if len(doc.Packages) != 4 {
		t.Fatalf("expected 4 packages (cross-eco collision must NOT dedup), got %d", len(doc.Packages))
	}
	purlByID := map[string]string{}
	for _, p := range doc.Packages {
		if len(p.ExternalRefs) == 1 {
			purlByID[p.SPDXID] = p.ExternalRefs[0].ReferenceLocator
		}
	}
	npmID := spdxPackageID("npm", "foo", "1.0.0")
	pypiID := spdxPackageID("pypi", "foo", "1.0.0")
	if npmID == pypiID {
		t.Fatal("SPDXIDs must be ecosystem-scoped")
	}
	if purlByID[npmID] != "pkg:npm/foo@1.0.0" || purlByID[pypiID] != "pkg:pypi/foo@1.0.0" {
		t.Errorf("each collision package must carry its own ecosystem's purl: npm=%q pypi=%q", purlByID[npmID], purlByID[pypiID])
	}
	// The pypi edge must relate flask to the PYPI foo, never npm's.
	flaskID := spdxPackageID("pypi", "flask", "2.0.0")
	for _, r := range doc.Relationships {
		if r.RelationshipType == "DEPENDS_ON" && r.SpdxElementId == flaskID {
			if r.RelatedSpdxElement != pypiID {
				t.Errorf("pypi edge resolved to the wrong ecosystem's package: %s", r.RelatedSpdxElement)
			}
		}
	}
	// Same-package alias spellings still dedup (gem vs rubygems).
	if spdxPackageID("gem", "rails", "7.0") != spdxPackageID("rubygems", "rails", "7.0") {
		t.Error("alias-folded ecosystems must produce the SAME ID — that is one real package")
	}
}

// Round-30 (v0.27.151): SBOM edge resolution never crosses lockfiles.
// The round-19 rule reached the chain index but the SBOM's byName maps
// stayed repo-wide — apps/a's `p@1 -> c` edge attached BOTH c@1.0.0
// (apps/a) and c@2.0.0 (apps/b) as p's children, fabricating a
// dependency on a version that exists only in an unrelated workspace.
func TestGenerateCycloneDX_EdgesNeverCrossLockfiles(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "mono", Owner: "org", GitURL: "https://github.com/org/mono"}
	graph := &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "p", ResolvedVersion: "1.0.0", LockfilePath: "apps/a/package-lock.json"},
			{Ecosystem: "npm", PackageName: "c", ResolvedVersion: "1.0.0", LockfilePath: "apps/a/package-lock.json"},
			{Ecosystem: "npm", PackageName: "c", ResolvedVersion: "2.0.0", LockfilePath: "apps/b/package-lock.json"},
		},
		Edges: []db.RepoLockfileEdge{
			{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json",
				ParentName: "p", ParentVersion: "1.0.0", ChildName: "c"},
		},
	}
	data, err := generateCycloneDX(repo, nil, nil, graph)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom struct {
		Dependencies []struct {
			Ref       string   `json:"ref"`
			DependsOn []string `json:"dependsOn"`
		} `json:"dependencies"`
	}
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatal(err)
	}
	for _, d := range bom.Dependencies {
		if d.Ref != "pkg:npm/p@1.0.0" {
			continue
		}
		if len(d.DependsOn) != 1 || d.DependsOn[0] != "pkg:npm/c@1.0.0" {
			t.Fatalf("p@1's children must resolve inside apps/a ONLY, got %v (c@2.0.0 lives in an unrelated lockfile — a fabricated dependency)", d.DependsOn)
		}
		return
	}
	t.Fatal("p@1.0.0 dependency entry missing")
}

// The SPDX twin of the cross-lockfile pin — one resolver serves both
// formats (SR-17), so this guards against the formats diverging again.
func TestGenerateSPDX_EdgesNeverCrossLockfiles(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "mono", Owner: "org", GitURL: "https://github.com/org/mono"}
	graph := &sbomGraph{
		Transitives: []db.RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "p", ResolvedVersion: "1.0.0", LockfilePath: "apps/a/package-lock.json"},
			{Ecosystem: "npm", PackageName: "c", ResolvedVersion: "1.0.0", LockfilePath: "apps/a/package-lock.json"},
			{Ecosystem: "npm", PackageName: "c", ResolvedVersion: "2.0.0", LockfilePath: "apps/b/package-lock.json"},
		},
		Edges: []db.RepoLockfileEdge{
			{Ecosystem: "npm", LockfilePath: "apps/a/package-lock.json",
				ParentName: "p", ParentVersion: "1.0.0", ChildName: "c"},
		},
	}
	data, err := generateSPDX(repo, nil, nil, graph)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc struct {
		Relationships []struct {
			SpdxElementId      string `json:"spdxElementId"`
			RelationshipType   string `json:"relationshipType"`
			RelatedSpdxElement string `json:"relatedSpdxElement"`
		} `json:"relationships"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	pID := spdxPackageID("npm", "p", "1.0.0")
	c1 := spdxPackageID("npm", "c", "1.0.0")
	c2 := spdxPackageID("npm", "c", "2.0.0")
	var got []string
	for _, r := range doc.Relationships {
		if r.SpdxElementId == pID && r.RelationshipType == "DEPENDS_ON" {
			got = append(got, r.RelatedSpdxElement)
		}
	}
	if len(got) != 1 || got[0] != c1 {
		t.Fatalf("p@1 must DEPENDS_ON apps/a's c@1 only (want [%s], got %v; c@2 is %s from an unrelated lockfile)", c1, got, c2)
	}
}

// v0.27.154 (round 33): repo_deps_libyear has no unique and the
// manifest walk appends from every manifest — a monorepo declaring
// the same eco/name/version in two manifests emitted DUPLICATE
// SPDXIDs, making the document invalid. First occurrence wins,
// matching the transitive guard.
func TestGenerateSPDX_DuplicateDirectDepsEmitOnePackage(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "mono", Owner: "org", GitURL: "https://github.com/org/mono"}
	deps := []db.SBOMDep{
		{PackageManager: "npm", Name: "left-pad", CurrentVersion: "1.3.0", Purl: "pkg:npm/left-pad@1.3.0"},
		{PackageManager: "npm", Name: "left-pad", CurrentVersion: "1.3.0", Purl: "pkg:npm/left-pad@1.3.0"},
	}
	data, err := generateSPDX(repo, deps, nil, nil)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc struct {
		Packages []struct {
			SPDXID string `json:"SPDXID"`
		} `json:"packages"`
		Relationships []struct {
			RelatedSpdxElement string `json:"relatedSpdxElement"`
		} `json:"relationships"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	id := spdxPackageID("npm", "left-pad", "1.3.0")
	pkgCount, relCount := 0, 0
	for _, p := range doc.Packages {
		if p.SPDXID == id {
			pkgCount++
		}
	}
	for _, r := range doc.Relationships {
		if r.RelatedSpdxElement == id {
			relCount++
		}
	}
	if pkgCount != 1 {
		t.Errorf("duplicate direct deps must emit ONE package (SPDXIDs are document-unique per spec), got %d", pkgCount)
	}
	if relCount != 1 {
		t.Errorf("duplicate direct deps must emit ONE root relationship, got %d", relCount)
	}
}
