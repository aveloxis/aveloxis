// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 7 (R7-4, R7-5, R7-6, R7-12): the rule of round 6, complete.
//
//	A managed version is taken from a pom only if every pom consulted
//	before it for the artifact is KNOWN not to manage it.

// notAskedSource answers RemoteNotAsked for what it does not have: the
// coordinate is not one a request can be built from.
type notAskedSource struct{ has map[string]*RemotePom }

func (s notAskedSource) RemotePom(g, a, v string, _ []string) (*RemotePom, RemoteOutcome) {
	if p, ok := s.has[g+":"+a+":"+v]; ok {
		return p, RemoteFound
	}
	return nil, RemoteNotAsked
}

// R7-5. A parent or BOM that was never asked for has content that is not
// known, and withholds what a later one would give. Each was passed over as
// "not found".
func TestAPomThatWasNotAskedForWithholdsWhatComesAfter(t *testing.T) {
	second := r6Bom("second", "g", "x", "2.0")
	app := func(parent, firstImport string) string {
		return "<project>" + parent + "<groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			firstImport + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>"
	}
	parent := func(version string) string {
		return "<parent><groupId>t</groupId><artifactId>up</artifactId><version>" + version + "</version><relativePath/></parent>"
	}
	found := outcomeSource{outcome: RemoteNotFound}
	for name, c := range map[string]struct {
		files   map[string]string
		src     PomSource
		version string
		why     string
	}{
		"a snapshot parent":                         {map[string]string{"pom.xml": app(parent("3-SNAPSHOT"), ""), "second/pom.xml": second}, found, "", model.VersionReasonInheritedNotFound},
		"a ranged parent":                           {map[string]string{"pom.xml": app(parent("[1,2)"), ""), "second/pom.xml": second}, found, "", model.VersionReasonInheritedNotFound},
		"a parent whose version is unset":           {map[string]string{"pom.xml": app(parent("${nope}"), ""), "second/pom.xml": second}, found, "", model.VersionReasonInheritedNotFound},
		"a first import at a snapshot":              {map[string]string{"pom.xml": app("", r4import("t", "first", "1-SNAPSHOT")), "second/pom.xml": second}, found, "", model.VersionReasonInheritedNotFound},
		"a first import at a range":                 {map[string]string{"pom.xml": app("", r4import("t", "first", "[1,2)")), "second/pom.xml": second}, found, "", model.VersionReasonInheritedNotFound},
		"a first import at a reference":             {map[string]string{"pom.xml": app("", r4import("t", "first", "${nope}")), "second/pom.xml": second}, found, "", model.VersionReasonInheritedNotFound},
		"a first import the source did not ask for": {map[string]string{"pom.xml": app("", r4import("t", "first", "1")), "second/pom.xml": second}, notAskedSource{}, "", model.VersionReasonInheritedNotFound},
		"a first import that two poms of the clone could be": {map[string]string{"pom.xml": app("", r4import("t", "first", "1")), "second/pom.xml": second,
			"one/pom.xml": r6Bom("first"), "two/pom.xml": r6Bom("first")}, found, "", model.VersionReasonInheritedNotFound},
		"no source is installed": {map[string]string{"pom.xml": app("", r4import("t", "first", "1")), "second/pom.xml": second}, nil, "", model.VersionReasonInheritedNotFound},
		// The control: every repository answered that there is no such pom.
		"a first import on no repository": {map[string]string{"pom.xml": app("", r4import("t", "first", "1")), "second/pom.xml": second}, found, "2.0", ""},
		"a parent on no repository":       {map[string]string{"pom.xml": app(parent("3"), ""), "second/pom.xml": second}, found, "2.0", ""},
		// And an import AFTER the one that answers withholds nothing.
		"a later import at a snapshot": {map[string]string{"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "second", "1") + r4import("t", "late", "1-SNAPSHOT") + "</dependencies></dependencyManagement><dependencies>" + r4dep("g", "x", "") + "</dependencies></project>",
			"second/pom.xml": second}, found, "2.0", ""},
	} {
		x := r6Versions(t, c.files, "pom.xml", c.src)["g:x"]
		if x.Version != c.version || x.VersionReason != c.why {
			t.Errorf("%s: g:x has version %q, reason %q; want %q, %q", name, x.Version, x.VersionReason, c.version, c.why)
		}
	}
}

// R7-4. A BOM that DOES manage the artifact, at a version it cannot give,
// has answered: the next BOM's version is not the answer.
func TestABOMThatManagesWithoutAVersionEndsTheSearch(t *testing.T) {
	app := "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
		r4import("t", "first", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
		r4dep("g", "x", "") + r4dep("g", "y", "") + "</dependencies></project>"
	second := r6Bom("second", "g", "x", "2.0", "g", "y", "9.0")
	firstWithParent := "<project><parent><groupId>t</groupId><artifactId>up</artifactId><version>1</version><relativePath/></parent>" +
		"<artifactId>first</artifactId><dependencyManagement><dependencies>" + r4dep("g", "x", "${v}") + "</dependencies></dependencyManagement></project>"
	// Its parent, which sets v, cannot be obtained: the lookup failed.
	got := r6Versions(t, map[string]string{"pom.xml": app, "first/pom.xml": firstWithParent, "second/pom.xml": second}, "pom.xml", outcomeSource{outcome: RemoteFailed})
	if x := got["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed || x.VersionSource != "" {
		t.Errorf("first manages g:x at ${v} and its parent's lookup failed: g:x has version %q, source %q, reason %q; want none and %q",
			x.Version, x.VersionSource, x.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
	// What first does not manage is another question: its parent might, and
	// its lookup failed, so that is withheld too (round 6).
	if y := got["g:y"]; y.Version != "" || y.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("g:y has version %q, reason %q; want none and a failed lookup", y.Version, y.VersionReason)
	}
	// The parent is there and sets nothing: first still manages g:x, at
	// nothing it can say. No version, and nothing could not be obtained.
	up := &RemotePom{Group: "t", Artifact: "up", Version: "1", ServedBy: "repo.example"}
	got = r6Versions(t, map[string]string{"pom.xml": app, "first/pom.xml": firstWithParent, "second/pom.xml": second}, "pom.xml",
		outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:up:1": up}})
	if x := got["g:x"]; x.Version != "" || x.VersionSource != "" {
		t.Errorf("first manages g:x at a reference nothing sets: g:x has version %q (source %q); want none", x.Version, x.VersionSource)
	}
	if y := got["g:y"]; y.Version != "9.0" {
		t.Errorf("g:y has version %q; want 9.0 from the second BOM (the first is whole and does not manage it)", y.Version)
	}
	// The same BOM fetched, not in the clone, with settle's verdict.
	remoteFirst := &RemotePom{Group: "t", Artifact: "first", Version: "1", Parent: &RemotePomParent{Group: "t", Artifact: "up", Version: "1"},
		Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "${v}"}}, ServedBy: "repo.example"}
	got = r6Versions(t, map[string]string{"pom.xml": app, "second/pom.xml": second}, "pom.xml",
		outcomeSource{RemoteFailed, map[string]*RemotePom{"t:first:1": remoteFirst}})
	if x := got["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("a fetched first: g:x has version %q, reason %q; want none and a failed lookup", x.Version, x.VersionReason)
	}
}

// R7-6. A tree is whole only when the BOMs it imports are: their own parent
// chains and their own imports. One whose BOM's parent could not be
// obtained had a dependency's version withheld and was reported whole, its
// managed entries noted as unused.
func TestATreeWhoseBOMIsNotWholeIsNotWhole(t *testing.T) {
	app := "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
		r4dep("g", "kept", "1.0") + r4import("t", "first", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
		r4dep("g", "x", "") + "</dependencies></project>"
	second := r6Bom("second", "g", "x", "2.0")
	settle := func(files map[string]string, src PomSource) (MavenManaged, model.LibyearDep) {
		t.Helper()
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		if src != nil {
			w.SetPomSource(src)
		}
		got := w.Read(root+"/pom.xml", Options{})
		var x model.LibyearDep
		for i := range got.Deps {
			got.Deps[i].ManifestPath = "pom.xml"
			if got.Deps[i].Name == "g:x" && !got.Deps[i].ManagedEntry {
				x = got.Deps[i]
			}
		}
		return w.SettleMavenManaged(got.Deps), x
	}
	up := RemotePomParent{Group: "t", Artifact: "up", Version: "1"}
	for name, c := range map[string]struct {
		first *RemotePom
		src   func(first *RemotePom) PomSource
		whole bool
	}{
		// first is obtained; its parent's lookup fails.
		"the BOM's parent could not be obtained": {&RemotePom{Group: "t", Artifact: "first", Version: "1", Parent: &up, ServedBy: "h"},
			func(f *RemotePom) PomSource {
				return outcomeSource{RemoteFailed, map[string]*RemotePom{"t:first:1": f}}
			}, false},
		// first is obtained; a BOM it imports is on no repository.
		"a BOM the BOM imports is on no repository": {&RemotePom{Group: "t", Artifact: "first", Version: "1", ServedBy: "h",
			Managed: []RemoteManaged{{Group: "t", Artifact: "inner", Version: "1", Import: true}}},
			func(f *RemotePom) PomSource {
				return outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:first:1": f}}
			}, false},
		// The control: first is obtained whole and says nothing.
		"the BOM is whole": {&RemotePom{Group: "t", Artifact: "first", Version: "1", ServedBy: "h"},
			func(f *RemotePom) PomSource {
				return outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:first:1": f}}
			}, true},
	} {
		res, x := settle(map[string]string{"pom.xml": app, "second/pom.xml": second}, c.src(c.first))
		if whole := len(res.NotWhole) == 0; whole != c.whole {
			t.Errorf("%s: whole %v (not whole: %+v), want %v", name, whole, res.NotWhole, c.whole)
		}
		kept := false
		for _, d := range res.Kept {
			kept = kept || (d.ManagedEntry && d.Name == "g:kept")
		}
		if kept == c.whole {
			t.Errorf("%s: the tree's unused managed entry kept as a row: %v; a tree that is not whole keeps its entries, a whole one notes them", name, kept)
		}
		if withheld := x.Version == "" && x.VersionReason != ""; withheld && len(res.NotWhole) == 0 {
			t.Errorf("%s: g:x was withheld (%q) and the tree is reported whole", name, x.VersionReason)
		}
	}
}

// R7-12. Gradle resolves its platforms' recommendations together.
func TestGradlePlatformsAreResolvedTogether(t *testing.T) {
	build := func(lines ...string) string {
		s := "dependencies {\n"
		for _, l := range lines {
			s += "  implementation " + l + "\n"
		}
		return s + "  implementation 'g:x'\n}\n"
	}
	bom := func(artifact, version string) string { return r6Bom(artifact, "g", "x", version) }
	failed, notFound := outcomeSource{outcome: RemoteFailed}, outcomeSource{outcome: RemoteNotFound}
	for name, c := range map[string]struct {
		files   map[string]string
		src     PomSource
		version string
		why     string
	}{
		"the higher of two, written second": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:b:1')"),
			"a/pom.xml": bom("a", "1.9.0"), "b/pom.xml": bom("b", "1.10.0")}, notFound, "1.10.0", ""},
		"the higher of two, written first": {map[string]string{"build.gradle": build("platform('t:b:1')", "platform('t:a:1')"),
			"a/pom.xml": bom("a", "1.9.0"), "b/pom.xml": bom("b", "1.10.0")}, notFound, "1.10.0", ""},
		"a longer version that begins with the shorter is the higher": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:b:1')"),
			"a/pom.xml": bom("a", "2.1.1"), "b/pom.xml": bom("b", "2.1")}, notFound, "2.1.1", ""},
		"two that agree": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:b:1')"),
			"a/pom.xml": bom("a", "3.0"), "b/pom.xml": bom("b", "3.0")}, notFound, "3.0", ""},
		"two that are not plain release numbers": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:b:1')"),
			"a/pom.xml": bom("a", "1.0-rc1"), "b/pom.xml": bom("b", "1.0")}, notFound, "", ""},
		"an enforced platform's lower version": {map[string]string{"build.gradle": build("platform('t:b:1')", "enforcedPlatform('t:a:1')"),
			"a/pom.xml": bom("a", "1.9.0"), "b/pom.xml": bom("b", "1.10.0")}, notFound, "1.9.0", ""},
		"a platform AFTER the one that manages it could not be obtained": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:gone:1')"),
			"a/pom.xml": bom("a", "1.9.0")}, failed, "", model.VersionReasonInheritedLookupFailed},
		"that platform is on no repository": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:gone:1')"),
			"a/pom.xml": bom("a", "1.9.0")}, notFound, "1.9.0", ""},
		"an enforced platform answers and another could not be obtained": {map[string]string{"build.gradle": build("enforcedPlatform('t:a:1')", "platform('t:gone:1')"),
			"a/pom.xml": bom("a", "1.9.0")}, failed, "1.9.0", ""},
		"an enforced platform could not be obtained": {map[string]string{"build.gradle": build("platform('t:a:1')", "enforcedPlatform('t:gone:1')"),
			"a/pom.xml": bom("a", "1.9.0")}, failed, "", model.VersionReasonInheritedLookupFailed},
		"a platform at a snapshot": {map[string]string{"build.gradle": build("platform('t:first:1-SNAPSHOT')", "platform('t:a:1')"),
			"a/pom.xml": bom("a", "1.9.0")}, notFound, "", model.VersionReasonInheritedNotFound},
		"a platform that manages it at a reference nothing sets": {map[string]string{"build.gradle": build("platform('t:a:1')", "platform('t:b:1')"),
			"a/pom.xml": bom("a", "${v}"), "b/pom.xml": bom("b", "1.10.0")}, notFound, "", ""},
	} {
		x := r6Versions(t, c.files, "build.gradle", c.src)["g:x"]
		if x.Version != c.version || x.VersionReason != c.why {
			t.Errorf("%s: g:x has version %q, reason %q; want %q, %q", name, x.Version, x.VersionReason, c.version, c.why)
		}
	}
}

func TestGradleHighestOrdersOnlyPlainReleaseNumbers(t *testing.T) {
	for _, c := range []struct {
		in   []string
		want string
		ok   bool
	}{
		{[]string{"1.0"}, "1.0", true}, {[]string{"1.0-rc1"}, "1.0-rc1", true}, {[]string{"1.9", "1.10"}, "1.10", true},
		{[]string{"1.10", "1.9"}, "1.10", true}, {[]string{"2", "10"}, "10", true}, {[]string{"1.2", "1.2.0"}, "1.2.0", true},
		{[]string{"1.02", "1.1"}, "1.02", true}, {[]string{"99999999999999999999999", "1"}, "99999999999999999999999", true},
		{[]string{"1.0", "1.0.Final"}, "", false}, {[]string{"1.0", "1..0"}, "", false}, {[]string{"1.0", ""}, "", false},
		{[]string{"1.0", "v2"}, "", false}, {[]string{"1.0", "1.0-rc1", "0.9"}, "", false},
	} {
		if got, ok := gradleHighest(c.in); got != c.want || ok != c.ok {
			t.Errorf("gradleHighest(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

// A BOM that was not asked for because the walk had asked for as many as it
// may is not known either: the version a BOM behind it would give is
// withheld, as a failed lookup's is.
func TestAPomPastTheRequestLimitWithholdsWhatComesAfter(t *testing.T) {
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "b1", "1") + r4import("t", "b2", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
			r4dep("g", "x", "") + "</dependencies></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	read := func(limit int) model.LibyearDep {
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		w.SetPomSource(outcomeSource{outcome: RemoteNotFound})
		w.SetRemotePomLimit(limit)
		for _, d := range w.Read(root+"/pom.xml", Options{}).Deps {
			if d.Name == "g:x" && !d.ManagedEntry {
				return d
			}
		}
		t.Fatal("g:x was not read")
		return model.LibyearDep{}
	}
	if x := read(1); x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("the second BOM was never asked for: g:x has version %q, reason %q; want none and %q", x.Version, x.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
	// Both asked, and both on no repository: the third BOM's version stands.
	if x := read(2); x.Version != "2.0" || x.VersionReason != "" {
		t.Errorf("both asked for and not found: g:x has version %q, reason %q; want 2.0", x.Version, x.VersionReason)
	}
}
