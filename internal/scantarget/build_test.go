// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scantarget

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// fakeStore is a repository's stored rows. A read named in failOn returns
// that error; calls records which reads ran, in order.
type fakeStore struct {
	deps       []model.VulnScanDep
	selfNames  map[string]bool
	locked     []model.RepoLockfilePackage
	transitive []model.RepoLockfilePackage
	selfPkgs   []model.SelfAdvisoryPackage
	evidence   model.InventoryEvidence
	failOn     map[string]error
	calls      []string
}

func (f *fakeStore) read(name string) error {
	f.calls = append(f.calls, name)
	return f.failOn[name]
}

func (f *fakeStore) GetRepoDepsForVulnScan(context.Context, int64) ([]model.VulnScanDep, error) {
	if err := f.read("deps"); err != nil {
		return nil, err
	}
	// A copy: Build normalises versions in place, as it does to the slice
	// the real store hands it.
	return append([]model.VulnScanDep(nil), f.deps...), nil
}

func (f *fakeStore) GetRepoSelfPackageNames(context.Context, int64) (map[string]bool, error) {
	if err := f.read("self-names"); err != nil {
		return nil, err
	}
	return f.selfNames, nil
}

func (f *fakeStore) GetRepoLockedVersions(context.Context, int64) ([]model.RepoLockfilePackage, error) {
	if err := f.read("locked"); err != nil {
		return nil, err
	}
	return f.locked, nil
}

func (f *fakeStore) GetRepoTransitivePackages(context.Context, int64) ([]model.RepoLockfilePackage, error) {
	if err := f.read("transitive"); err != nil {
		return nil, err
	}
	return f.transitive, nil
}

func (f *fakeStore) GetRepoSelfAdvisoryPackages(context.Context, int64) ([]model.SelfAdvisoryPackage, error) {
	if err := f.read("self-packages"); err != nil {
		return nil, err
	}
	return f.selfPkgs, nil
}

func (f *fakeStore) GetRepoInventoryEvidence(context.Context, int64) (model.InventoryEvidence, error) {
	if err := f.read("evidence"); err != nil {
		return model.InventoryEvidence{}, err
	}
	return f.evidence, nil
}

func dep(manager, name, version, requirement, typ string) model.VulnScanDep {
	typeOf := map[string]string{"npm": "npm", "pypi": "pypi", "go": "golang", "maven": "maven", "githubactions": "githubactions"}
	d := model.VulnScanDep{Name: name, CurrentVersion: version, PackageManager: manager, Requirement: requirement, Type: typ}
	if t, ok := typeOf[manager]; ok {
		d.Purl = pkgid.Purl(t, strings.Replace(name, ":", "/", 1), version)
	}
	return d
}

func build(t *testing.T, s *fakeStore, transitive bool) *Set {
	t.Helper()
	set, err := Build(context.Background(), s, 7, transitive)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return set
}

func TestBuildAnEmptyRepositoryHasNoCandidates(t *testing.T) {
	set := build(t, &fakeStore{}, true)
	if set.Candidates != 0 || len(set.Purls) != 0 || len(set.Targets) != 0 || len(set.Dropped) != 0 {
		t.Errorf("empty repository: %+v", set)
	}
	if set.Purls == nil {
		t.Error("Purls must be an empty list, not nil: the scan ranges over it")
	}
}

func TestBuildDirectTargets(t *testing.T) {
	s := &fakeStore{
		deps: []model.VulnScanDep{
			dep("npm", "express", "4.17.1", "^4.17.1", "runtime"),
			dep("pypi", "requests", "2.25.0", ">=2.25", "dev"),
			dep("pypi", "loose", "", "", "runtime"),
			dep("go", "github.com/pkg/errors", "v0.9.1", "v0.9.1", "runtime"),
			dep("npm", "lodash", "4.17.20", "^4.17.20", "runtime"),
		},
		locked: []model.RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "lodash", ResolvedVersion: "4.17.21"},
			{Ecosystem: "npm", PackageName: "lodash", ResolvedVersion: "4.17.21"}, // the same version from a second lockfile
			{Ecosystem: "npm", PackageName: "Lodash", ResolvedVersion: "3.10.1"},  // a second version, another spelling
		},
	}
	set := build(t, s, false)
	want := []string{
		"pkg:npm/express@4.17.1", "pkg:pypi/requests@2.25.0", "pkg:pypi/loose",
		"pkg:golang/github.com/pkg/errors@v0.9.1", "pkg:npm/lodash@4.17.21", "pkg:npm/lodash@3.10.1",
	}
	if !reflect.DeepEqual(set.Purls, want) {
		t.Fatalf("purls = %v\n   want %v", set.Purls, want)
	}
	for purl, wantRes := range map[string]string{
		"pkg:npm/express@4.17.1":                  versionuse.BoundedRange,
		"pkg:pypi/requests@2.25.0":                versionuse.RangeFloor,
		"pkg:pypi/loose":                          versionuse.Unpinned,
		"pkg:golang/github.com/pkg/errors@v0.9.1": versionuse.Locked,
		"pkg:npm/lodash@4.17.21":                  versionuse.Locked,
		"pkg:npm/lodash@3.10.1":                   versionuse.Locked,
	} {
		tgt := set.Targets[purl]
		if tgt.Resolution != wantRes || tgt.Kind != model.DependencyKindDirect || tgt.Purl != purl {
			t.Errorf("%s: resolution %q kind %q purl %q, want %q direct", purl, tgt.Resolution, tgt.Kind, tgt.Purl, wantRes)
		}
	}
	if got := set.Targets["pkg:pypi/requests@2.25.0"].Scope; got != "dev" {
		t.Errorf("a dev dependency's target scope = %q, want dev", got)
	}
	if got := set.Targets["pkg:npm/lodash@4.17.21"].Requirement; got != "^4.17.20" {
		t.Errorf("a locked target keeps the declared requirement; got %q", got)
	}
	if set.Candidates != 6 || set.Transitive != 0 || set.Self != 0 {
		t.Errorf("candidates %d transitive %d self %d", set.Candidates, set.Transitive, set.Self)
	}
	if got := strings.Join(s.calls, ","); got != "deps,self-names,locked,self-packages,evidence" {
		t.Errorf("reads = %s; with transitive off the transitive packages must not be read", got)
	}
}

// Self-exclusion is decided before the purl is looked at: a purl-less
// dependency that names the repository's own package is a deliberate
// exclusion, not an unsendable dependency.
func TestBuildSelfExclusionComesBeforeTheBlankPurl(t *testing.T) {
	own := model.VulnScanDep{Name: "My_Pkg", PackageManager: "conda"} // no purl
	s := &fakeStore{
		deps: []model.VulnScanDep{
			own, own, // declared in two manifests: counted twice, sampled once
			{Name: "numpy", PackageManager: "conda"}, // no purl, not the repository's own
			dep("pypi", "requests", "2.25.0", "==2.25.0", "runtime"),
		},
		selfNames: map[string]bool{"my-pkg": true}, // the self set's folded spelling
	}
	set := build(t, s, false)
	if set.SelfExcludedDirect != 2 || set.SelfExcluded != 2 || !reflect.DeepEqual(set.SelfSamples, []string{"My_Pkg"}) {
		t.Errorf("self-excluded direct %d total %d samples %v; want 2, 2, [My_Pkg]", set.SelfExcludedDirect, set.SelfExcluded, set.SelfSamples)
	}
	if set.BlankPurlSkipped != 1 || !reflect.DeepEqual(set.BlankPurlNames, []string{"numpy"}) {
		t.Errorf("blank-purl skipped %d %v; want 1 [numpy]", set.BlankPurlSkipped, set.BlankPurlNames)
	}
	if !reflect.DeepEqual(set.Purls, []string{"pkg:pypi/requests@2.25.0"}) {
		t.Errorf("purls = %v", set.Purls)
	}
}

func TestBuildKeepsAtMostFiveSelfSamples(t *testing.T) {
	s := &fakeStore{selfNames: map[string]bool{}}
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		s.deps = append(s.deps, model.VulnScanDep{Name: n, PackageManager: "pypi", Purl: "pkg:pypi/" + n})
		s.selfNames[n] = true
	}
	set := build(t, s, false)
	if set.SelfExcludedDirect != 7 || len(set.SelfSamples) != selfSampleLimit {
		t.Errorf("excluded %d, samples %v; want 7 and %d samples", set.SelfExcludedDirect, set.SelfSamples, selfSampleLimit)
	}
}

// The same name at the same version declared as a build requirement and as a
// runtime dependency is runtime exposure, whichever is read first.
func TestBuildFoldsScopeToRuntimeOnACollision(t *testing.T) {
	for _, order := range [][2]string{{"build", "runtime"}, {"runtime", "build"}} {
		s := &fakeStore{deps: []model.VulnScanDep{
			dep("pypi", "setuptools", "67.0.0", "==67.0.0", order[0]),
			dep("pypi", "setuptools", "67.0.0", "==67.0.0", order[1]),
		}}
		set := build(t, s, false)
		if len(set.Purls) != 1 {
			t.Fatalf("order %v: purls = %v, want one", order, set.Purls)
		}
		if scope := set.Targets[set.Purls[0]].Scope; !model.IsRuntimeScope(scope) {
			t.Errorf("order %v: scope %q; a purl reachable from a runtime declaration is runtime", order, scope)
		}
	}
}

func TestBuildTransitiveAndOwnPackages(t *testing.T) {
	s := &fakeStore{
		deps:      []model.VulnScanDep{dep("npm", "express", "4.17.1", "4.17.1", "runtime")},
		selfNames: map[string]bool{"own-lib": true},
		transitive: []model.RepoLockfilePackage{
			{Ecosystem: "npm", PackageName: "express", ResolvedVersion: "4.17.1", Scope: "dev"}, // a direct target has this purl
			{Ecosystem: "npm", PackageName: "qs", ResolvedVersion: "6.7.0", Scope: "dev"},
			{Ecosystem: "npm", PackageName: "own-lib", ResolvedVersion: "1.0.0"},       // the repository's own
			{Ecosystem: "conda", PackageName: "mkl", ResolvedVersion: "2024.0"},        // no purl type
			{Ecosystem: "swiftpm", PackageName: "alamofire", ResolvedVersion: "5.8.0"}, // namespace required and absent
			{Ecosystem: "swiftpm", Namespace: "github.com/apple", PackageName: "swift-nio", ResolvedVersion: "2.60.0"},
		},
		selfPkgs: []model.SelfAdvisoryPackage{
			{Ecosystem: "npm", PackageName: "own-lib"},
			{Ecosystem: "conda", PackageName: "own-lib"}, // no purl type
			{Ecosystem: "npm", PackageName: "own-lib"},   // the same package twice
		},
	}
	set := build(t, s, true)
	want := []string{"pkg:npm/express@4.17.1", "pkg:npm/qs@6.7.0", "pkg:swift/github.com/apple/swift-nio@2.60.0", "pkg:npm/own-lib"}
	if !reflect.DeepEqual(set.Purls, want) {
		t.Fatalf("purls = %v\n   want %v", set.Purls, want)
	}
	if tgt := set.Targets["pkg:npm/express@4.17.1"]; tgt.Kind != model.DependencyKindDirect {
		t.Errorf("a direct target must win a purl collision with a transitive one; kind %q", tgt.Kind)
	}
	if tgt := set.Targets["pkg:npm/qs@6.7.0"]; tgt.Kind != model.DependencyKindTransitive || tgt.Resolution != versionuse.Locked || tgt.Scope != "dev" || tgt.Dep.Name != "qs" || tgt.Dep.PackageManager != "npm" {
		t.Errorf("transitive target = %+v", tgt)
	}
	if tgt := set.Targets["pkg:npm/own-lib"]; tgt.Kind != model.DependencyKindSelf || tgt.Resolution != "" || tgt.Scope != "" {
		t.Errorf("own-package target = %+v; want kind self with no resolution and no scope", tgt)
	}
	if set.Transitive != 2 || set.Self != 1 || set.SelfExcludedDirect != 0 || set.SelfExcluded != 1 || set.Candidates != 4 {
		t.Errorf("transitive %d self %d excluded direct %d total %d candidates %d; want 2, 1, 0, 1, 4",
			set.Transitive, set.Self, set.SelfExcludedDirect, set.SelfExcluded, set.Candidates)
	}
	if got := strings.Join(s.calls, ","); got != "deps,self-names,locked,transitive,self-packages,evidence" {
		t.Errorf("reads = %s", got)
	}
}

// A repository with no dependencies and a published package: its own package
// IS the scan.
func TestBuildOwnPackageAlone(t *testing.T) {
	set := build(t, &fakeStore{selfPkgs: []model.SelfAdvisoryPackage{{Ecosystem: "pypi", PackageName: "NumPy"}}}, true)
	if !reflect.DeepEqual(set.Purls, []string{"pkg:pypi/numpy"}) || set.Candidates != 1 || set.Self != 1 {
		t.Errorf("set = %+v", set)
	}
}

// Stored versions are normalised when read, and the purl rebuilt with them.
func TestBuildNormalisesStoredVersions(t *testing.T) {
	garbage := model.VulnScanDep{Name: "pyyaml", PackageManager: "pypi", CurrentVersion: `6.0.3 \`, Purl: `pkg:pypi/pyyaml@6.0.3 \`, Requirement: "==6.0.3"}
	if manifest.NormalizeParsedVersion("pypi", garbage.CurrentVersion) == garbage.CurrentVersion {
		t.Fatal("the fixture's version is not one the normaliser changes; the test would prove nothing")
	}
	set := build(t, &fakeStore{deps: []model.VulnScanDep{garbage}}, false)
	want := pkgid.ReplaceVersion(garbage.Purl, manifest.NormalizeParsedVersion("pypi", garbage.CurrentVersion))
	if !reflect.DeepEqual(set.Purls, []string{want}) || len(set.Dropped) != 0 {
		t.Errorf("purls = %v dropped = %v; want [%s] and nothing dropped", set.Purls, set.Dropped, want)
	}
	if got := set.Targets[want].Dep.CurrentVersion; got != manifest.NormalizeParsedVersion("pypi", garbage.CurrentVersion) {
		t.Errorf("the target's dependency keeps version %q", got)
	}
}

func TestBuildWireGate(t *testing.T) {
	s := &fakeStore{deps: []model.VulnScanDep{
		dep("npm", "express", "4.17.1", "4.17.1", "runtime"),
		{Name: "raw", PackageManager: "npm", CurrentVersion: "1.0.0", Purl: "pkg:npm/raw/@1.0.0"},                    // a name that ends in a slash
		{Name: "artifact", PackageManager: "maven", CurrentVersion: "1.0", Purl: "pkg:maven/artifact@1.0"},           // no group
		{Name: "", PackageManager: "githubactions", CurrentVersion: "v4", Purl: "pkg:githubactions/x@v4"},            // an Action with no name
		{Name: "actions/checkout", PackageManager: "githubactions", CurrentVersion: "v4", Purl: "not a purl at all"}, // an Action is not a purl query
	}}
	set := build(t, s, false)
	if !reflect.DeepEqual(set.Purls, []string{"pkg:npm/express@4.17.1", "not a purl at all"}) {
		t.Errorf("sendable = %v", set.Purls)
	}
	wantDropped := []Dropped{
		{Purl: "pkg:npm/raw/@1.0.0", DepName: "raw", Kind: model.DependencyKindDirect},
		{Purl: "pkg:maven/artifact@1.0", DepName: "artifact", Kind: model.DependencyKindDirect},
		{Purl: "pkg:githubactions/x@v4", DepName: "", Kind: model.DependencyKindDirect},
	}
	if !reflect.DeepEqual(set.Dropped, wantDropped) {
		t.Errorf("dropped = %+v\n   want %+v", set.Dropped, wantDropped)
	}
	if set.Candidates != 5 || len(set.Targets) != 2 {
		t.Errorf("candidates %d, targets kept %d; want 5 and 2 (a dropped target leaves the map)", set.Candidates, len(set.Targets))
	}
}

// Every target malformed: candidates, and nothing sendable. The scan tells
// this apart from an empty repository by Candidates.
func TestBuildAllMalformed(t *testing.T) {
	set := build(t, &fakeStore{deps: []model.VulnScanDep{{Name: "x", PackageManager: "maven", CurrentVersion: "1", Purl: "pkg:maven/x@1"}}}, false)
	if set.Candidates != 1 || len(set.Purls) != 0 || len(set.Dropped) != 1 {
		t.Errorf("set = %+v", set)
	}
}

// Each read fails the build, with the message the scan has always logged, and
// nothing after it is read.
func TestBuildFailsClosedOnEveryRead(t *testing.T) {
	boom := errors.New("connection reset")
	for _, c := range []struct{ read, message, readsRun string }{
		{"deps", "loading dependencies: connection reset", "deps"},
		{"self-names", "loading self-package set: connection reset", "deps,self-names"},
		{"locked", "loading locked versions: connection reset", "deps,self-names,locked"},
		{"transitive", "loading transitive lockfile packages: connection reset", "deps,self-names,locked,transitive"},
		{"self-packages", "loading self-advisory packages: connection reset", "deps,self-names,locked,transitive,self-packages"},
		{"evidence", "loading inventory evidence: connection reset", "deps,self-names,locked,transitive,self-packages,evidence"},
	} {
		s := &fakeStore{failOn: map[string]error{c.read: boom}}
		set, err := Build(context.Background(), s, 7, true)
		if set != nil || !errors.Is(err, boom) || err.Error() != c.message {
			t.Errorf("%s fails: set %v, err %v; want nil and %q wrapping the cause", c.read, set, err, c.message)
		}
		if got := strings.Join(s.calls, ","); got != c.readsRun {
			t.Errorf("%s fails: reads %s, want %s", c.read, got, c.readsRun)
		}
	}
}

// oldDirectTargets is vulnScanTargets as it was in internal/collector at
// 0.29.153, before the order of evidence moved to versionuse.Of.
func oldDirectTargets(dep model.VulnScanDep, locked map[string][]string) []model.VulnScanTarget {
	if dep.Purl == "" {
		return nil
	}
	scope := model.StoredScope(dep.Type)
	if dep.PackageManager == "githubactions" {
		return []model.VulnScanTarget{{
			Purl: dep.Purl, Dep: dep,
			Requirement:       dep.Requirement,
			Resolution:        versionuse.ClassifyActionRef(dep.CurrentVersion),
			Kind:              model.DependencyKindDirect,
			Scope:             scope,
			OSVQueryName:      dep.Name,
			OSVQueryEcosystem: "GitHub Actions",
		}}
	}
	// Go is exact when go.mod gives a version; with none it falls to the
	// no-version rule below (0.29.209: this oracle said "locked" for a Go
	// row with no version, as the code did).
	if dep.PackageManager == "go" && dep.CurrentVersion != "" {
		return []model.VulnScanTarget{{
			Purl: dep.Purl, Dep: dep,
			Requirement: dep.Requirement,
			Resolution:  versionuse.Locked,
			Kind:        model.DependencyKindDirect,
			Scope:       scope,
		}}
	}
	if versions := locked[pkgid.MatchKey(dep.PackageManager, dep.Name)]; len(versions) > 0 {
		targets := make([]model.VulnScanTarget, 0, len(versions))
		for _, v := range versions {
			targets = append(targets, model.VulnScanTarget{
				Purl: pkgid.WithVersion(dep.Purl, v), Dep: dep,
				Requirement: dep.Requirement,
				Resolution:  versionuse.Locked,
				Kind:        model.DependencyKindDirect,
				Scope:       scope,
			})
		}
		return targets
	}
	return []model.VulnScanTarget{{
		Purl: dep.Purl, Dep: dep,
		Requirement: dep.Requirement,
		Resolution:  versionuse.ClassifyRequirement(manifest.ClassificationText(dep.PackageManager, dep.Requirement), dep.CurrentVersion),
		Kind:        model.DependencyKindDirect,
		Scope:       scope,
	}}
}

func TestDirectTargetsEqualTheOldPrecedence(t *testing.T) {
	locked := map[string][]string{
		pkgid.MatchKey("npm", "lodash"):                     {"4.17.21", "3.10.1"},
		pkgid.MatchKey("pypi", "Flask_SQLAlchemy"):          {"2.5.1"},
		pkgid.MatchKey("go", "github.com/pkg/errors"):       {"v0.8.0"}, // ignored: go.mod is exact
		pkgid.MatchKey("githubactions", "actions/checkout"): {"v3"},     // ignored: an Action is known by its ref
	}
	n := 0
	for _, manager := range []string{"npm", "pypi", "go", "maven", "rubygems", "githubactions", "conda"} {
		for _, name := range []string{"lodash", "flask.sqlalchemy", "github.com/pkg/errors", "actions/checkout", "org.x:y", "other"} {
			for _, v := range []struct{ version, requirement string }{
				{"4.17.20", "^4.17.20"}, {"2.5.0", ">=2.5"}, {"1.0.0", "==1.0.0"}, {"", ""}, {"", "*"},
				{"v4", "v4"}, {"v4.1.1", "v4.1.1"}, {"a1b2c3d4e5f6a7b8c9d0a1b2c3d4e5f6a7b8c9d0", ""}, {"1.2", "gem 'x', '~> 1.2'"},
			} {
				for _, typ := range []string{"", "runtime", "dev", "build", "test"} {
					for _, purl := range []string{"", "pkg:x/" + name + "@" + v.version} {
						d := model.VulnScanDep{Name: name, PackageManager: manager, CurrentVersion: v.version, Requirement: v.requirement, Type: typ, Purl: purl}
						n++
						if got, want := DirectTargets(d, locked), oldDirectTargets(d, locked); !reflect.DeepEqual(got, want) && (len(got) > 0 || len(want) > 0) {
							t.Errorf("%+v:\n  now %+v\n  old %+v", d, got, want)
						}
						if got, want := DirectTargets(d, nil), oldDirectTargets(d, nil); !reflect.DeepEqual(got, want) && (len(got) > 0 || len(want) > 0) {
							t.Errorf("%+v, no lockfile:\n  now %+v\n  old %+v", d, got, want)
						}
					}
				}
			}
		}
	}
	if n != 7*6*9*5*2 {
		t.Errorf("examined %d dependencies, want %d", n, 7*6*9*5*2)
	}
}

// The evidence read with the targets is handed on untouched: it is the
// closing rule's precondition and decides whether this can be a first scan.
func TestBuildCarriesTheInventoryEvidence(t *testing.T) {
	want := model.InventoryEvidence{InventoryRows: 3, LockfileRows: 2, AnalysisHalf: true, ScanStamp: true, Findings: true}
	if set := build(t, &fakeStore{evidence: want}, false); set.Evidence != want || set.Evidence.DependenciesUnknown() {
		t.Errorf("evidence = %+v, want %+v", set.Evidence, want)
	}
	if set := build(t, &fakeStore{}, false); !set.Evidence.DependenciesUnknown() {
		t.Error("a repository with no rows, no lockfile rows and no analysis half is one nothing is known about")
	}
	for _, e := range []model.InventoryEvidence{{InventoryRows: 1}, {LockfileRows: 1}, {AnalysisHalf: true}} {
		if e.DependenciesUnknown() {
			t.Errorf("%+v: any one of the three is evidence", e)
		}
	}
	// A scan stamp or old findings say nothing about the dependencies.
	if !(model.InventoryEvidence{ScanStamp: true, Findings: true}).DependenciesUnknown() {
		t.Error("a scan stamp and findings are not evidence of what the repository depends on")
	}
}

// A version a lockfile supplied is not put through the manifest version
// hygiene when the scan reads the row (0.29.177). The hygiene blanks a
// version with no digit; a Composer "dev-master" is a locked version, and
// blanking it would re-key the finding to a versionless purl.
func TestALockedVersionIsNotNormalisedAtRead(t *testing.T) {
	deps := func(source string) []model.VulnScanDep {
		return []model.VulnScanDep{{Name: "vendor/pkg", PackageManager: "packagist", CurrentVersion: "dev-master",
			Purl: "pkg:composer/vendor/pkg@dev-master", VersionSource: source}}
	}
	locked, err := Build(context.Background(), &fakeStore{deps: deps(versionuse.SourceLocked)}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(locked.Purls) != 1 || locked.Purls[0] != "pkg:composer/vendor/pkg@dev-master" {
		t.Errorf("a locked row's target is %v; want it at the locked version", locked.Purls)
	}
	// A declared version of the same shape is still normalised, as before.
	declared, err := Build(context.Background(), &fakeStore{deps: deps("")}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(declared.Purls) != 1 || declared.Purls[0] == locked.Purls[0] {
		t.Errorf("a declared row's target is %v; the hygiene still applies to a declared version", declared.Purls)
	}
}

// A Bioconductor package (renv.lock records them under their own ecosystem,
// 0.29.205) has no purl, so it is asked of OSV by name, ecosystem AND
// version: OSV then matches the version itself, unlike a GitHub Action's
// ref, which is matched on this side.
func TestBuildBioconductorPackagesAreAskedByName(t *testing.T) {
	s := &fakeStore{
		transitive: []model.RepoLockfilePackage{
			{Ecosystem: "cran", PackageName: "Rcpp", ResolvedVersion: "1.0.11"},
			{Ecosystem: "bioconductor", PackageName: "limma", ResolvedVersion: "3.56.2", Scope: "dev"},
			{Ecosystem: "bioconductor", PackageName: "limma", ResolvedVersion: "3.56.2"}, // the same package twice
			{Ecosystem: "bioconductor", PackageName: "edgeR", ResolvedVersion: "3.42.4"},
			{Ecosystem: "bioconductor", PackageName: "   ", ResolvedVersion: "1.0"}, // no name to ask about
		},
	}
	set := build(t, s, true)
	want := []string{"pkg:cran/Rcpp@1.0.11", "pkg:bioconductor/limma@3.56.2", "pkg:bioconductor/edgeR@3.42.4"}
	if !reflect.DeepEqual(set.Purls, want) {
		t.Fatalf("purls = %v\n   want %v", set.Purls, want)
	}
	tgt := set.Targets["pkg:bioconductor/limma@3.56.2"]
	if tgt.OSVQueryName != "limma" || tgt.OSVQueryEcosystem != "Bioconductor" || tgt.OSVQueryVersion != "3.56.2" {
		t.Errorf("the question = name %q, ecosystem %q, version %q; want limma, Bioconductor, 3.56.2", tgt.OSVQueryName, tgt.OSVQueryEcosystem, tgt.OSVQueryVersion)
	}
	if tgt.Kind != model.DependencyKindTransitive || tgt.Resolution != versionuse.Locked || tgt.Scope != "dev" || tgt.Dep.Name != "limma" || tgt.Dep.PackageManager != "bioconductor" || tgt.Dep.CurrentVersion != "3.56.2" {
		t.Errorf("target = %+v", tgt)
	}
	// A CRAN package is asked by its purl.
	if c := set.Targets["pkg:cran/Rcpp@1.0.11"]; c.OSVQueryEcosystem != "" || c.OSVQueryVersion != "" {
		t.Errorf("the CRAN target is asked by name: %+v", c)
	}
	if set.Transitive != 3 {
		t.Errorf("transitive = %d, want 3", set.Transitive)
	}
}

// A repository that publishes to Bioconductor (the registry evidence says
// so: ninety-nine such rows on one production database) is asked about its
// own package by name and ecosystem, with no version: every advisory the
// package has, as for every other publisher.
func TestBuildABioconductorPublisherIsAskedByName(t *testing.T) {
	s := &fakeStore{selfPkgs: []model.SelfAdvisoryPackage{
		{Ecosystem: "bioconductor", PackageName: "limma"},
		{Ecosystem: "Bioconductor", PackageName: "limma"}, // the same package, as another source spells the ecosystem
		{Ecosystem: "bioconductor", PackageName: "  "},
		{Ecosystem: "cran", PackageName: "Rcpp"},
	}}
	set := build(t, s, false)
	if want := []string{"pkg:bioconductor/limma", "pkg:cran/Rcpp"}; !reflect.DeepEqual(set.Purls, want) {
		t.Fatalf("purls = %v, want %v", set.Purls, want)
	}
	tgt := set.Targets["pkg:bioconductor/limma"]
	if tgt.Kind != model.DependencyKindSelf || tgt.OSVQueryName != "limma" || tgt.OSVQueryEcosystem != "Bioconductor" || tgt.OSVQueryVersion != "" || tgt.Dep.Name != "limma" {
		t.Errorf("target = %+v; want kind self, asked by name with no version", tgt)
	}
	if set.Self != 2 {
		t.Errorf("self = %d, want 2", set.Self)
	}
}
