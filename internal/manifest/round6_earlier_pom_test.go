// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 6 (M6-3). Maven consults a pom's own entries, then its
// parents', then the BOMs they import, in order, and the first that manages
// a dependency gives its version. A pom in that order that could not be
// OBTAINED has said nothing: what it manages is not known. When a later one
// manages the dependency, its version is not the answer (SR-5: an absent
// answer is not "no"), and the dependency is reported with no version and
// the reason the earlier pom was not obtained. Before, the later one's
// version was stored as if the earlier had been read and said nothing.

// outcomeSource answers every coordinate with one outcome, but for the poms
// it has.
type outcomeSource struct {
	outcome RemoteOutcome
	has     map[string]*RemotePom
}

func (s outcomeSource) RemotePom(g, a, v string, _ []string) (*RemotePom, RemoteOutcome) {
	if p, ok := s.has[g+":"+a+":"+v]; ok {
		return p, RemoteFound
	}
	return nil, s.outcome
}

func r6Versions(t *testing.T, files map[string]string, rel string, src PomSource) map[string]model.LibyearDep {
	t.Helper()
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	if src != nil {
		w.SetPomSource(src)
	}
	got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
	if got.Failure != nil {
		t.Fatalf("%s: %v", rel, got.Failure)
	}
	out := map[string]model.LibyearDep{}
	for _, d := range got.Deps {
		if !d.ManagedEntry {
			out[d.Name] = d
		}
	}
	return out
}

func r6Bom(artifact string, entries ...string) string {
	s := "<project><groupId>t</groupId><artifactId>" + artifact + "</artifactId><version>1</version><dependencyManagement><dependencies>"
	for i := 0; i+2 < len(entries); i += 3 {
		s += r4dep(entries[i], entries[i+1], entries[i+2])
	}
	return s + "</dependencies></dependencyManagement></project>"
}

func TestAVersionBehindAPomThatCouldNotBeObtainedIsNotDetermined(t *testing.T) {
	// first is not in the tree; second is, and manages g:x at 2.0. own is
	// managed by the pom itself, and g:none by nobody.
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4dep("g", "own", "7.0") + r4import("t", "first", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
			r4dep("g", "x", "") + r4dep("g", "own", "") + r4dep("g", "none", "") + "</dependencies></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	first := &RemotePom{Group: "t", Artifact: "first", Version: "1", Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "1.0"}}, ServedBy: "repo.example"}
	firstSilent := &RemotePom{Group: "t", Artifact: "first", Version: "1", ServedBy: "repo.example"}
	for name, c := range map[string]struct {
		src            PomSource
		version, why   string
		noneWhy        string
		source         string
		managedByFirst bool
	}{
		"the first BOM is obtained and manages it":   {src: outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:first:1": first}}, version: "1.0", source: SourceManaged},
		"the first BOM is obtained and says nothing": {src: outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:first:1": firstSilent}}, version: "2.0", source: SourceManaged},
		"the first BOM's lookup failed":              {src: outcomeSource{outcome: RemoteFailed}, why: model.VersionReasonInheritedLookupFailed, noneWhy: model.VersionReasonInheritedLookupFailed},
		"the first BOM's repository is refused":      {src: outcomeSource{outcome: RemoteRefused}, why: model.VersionReasonInheritedRepositoryRefused, noneWhy: model.VersionReasonInheritedRepositoryRefused},
		// "Not found" is an answer: every repository read has no such pom.
		// The next BOM's version stands, as it always did, and a dependency
		// nothing manages says a pom was not found.
		"the first BOM is on no repository": {src: outcomeSource{outcome: RemoteNotFound}, version: "2.0", source: SourceManaged, noneWhy: model.VersionReasonInheritedNotFound},
		// With no source nothing outside the clone is asked, and nothing is
		// said about what was not asked: the tree's own answer stands.
		"no source is installed": {src: nil, version: "2.0", source: SourceManaged},
	} {
		got := r6Versions(t, files, "pom.xml", c.src)
		if x := got["g:x"]; x.Version != c.version || x.VersionReason != c.why || x.VersionSource != c.source {
			t.Errorf("%s: g:x has version %q (source %q, reason %q); want %q (source %q, reason %q)", name, x.Version, x.VersionSource, x.VersionReason, c.version, c.source, c.why)
		}
		// The pom's own entry comes before every import: it is the answer
		// whatever became of them.
		if own := got["g:own"]; own.Version != "7.0" || own.VersionReason != "" {
			t.Errorf("%s: g:own has version %q, reason %q; want 7.0 from the pom's own entry", name, own.Version, own.VersionReason)
		}
		if none := got["g:none"]; none.Version != "" || none.VersionReason != c.noneWhy {
			t.Errorf("%s: g:none has version %q, reason %q; want none, reason %q", name, none.Version, none.VersionReason, c.noneWhy)
		}
	}
}

// The same for the far end of a parent chain: a parent that could not be
// obtained comes before every import (its own entries would win), so a
// BOM's answer behind it is not determined; an entry of a pom NEARER than
// the missing parent is the answer, since the nearer pom wins.
func TestAVersionBehindAMissingParentIsNotDetermined(t *testing.T) {
	files := map[string]string{
		"pom.xml": "<project><parent><groupId>far</groupId><artifactId>away</artifactId><version>3</version><relativePath/></parent><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4dep("g", "own", "7.0") + r4import("t", "bom", "1") + "</dependencies></dependencyManagement><dependencies>" +
			r4dep("g", "x", "") + r4dep("g", "own", "") + "</dependencies></project>",
		"bom/pom.xml": r6Bom("bom", "g", "x", "2.0"),
	}
	got := r6Versions(t, files, "pom.xml", outcomeSource{outcome: RemoteFailed})
	if x := got["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("g:x has version %q, reason %q; want none and %q: the parent that could not be looked up comes before the BOM", x.Version, x.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
	if own := got["g:own"]; own.Version != "7.0" || own.VersionReason != "" {
		t.Errorf("g:own has version %q, reason %q; want 7.0: the pom's own entry is nearer than its parent", own.Version, own.VersionReason)
	}
	parent := &RemotePom{Group: "far", Artifact: "away", Version: "3", ServedBy: "repo.example"}
	got = r6Versions(t, files, "pom.xml", outcomeSource{RemoteFailed, map[string]*RemotePom{"far:away:3": parent}})
	if x := got["g:x"]; x.Version != "2.0" || x.VersionReason != "" {
		t.Errorf("with the parent obtained: g:x has version %q, reason %q; want 2.0", x.Version, x.VersionReason)
	}
}

// A BOM that was obtained but whose OWN import could not be: what that
// import manages is consulted before the next BOM of the importing pom.
func TestAVersionBehindABOMThatIsNotWholeIsNotDetermined(t *testing.T) {
	files := map[string]string{
		"pom.xml": "<project><groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4import("t", "first", "1") + r4import("t", "second", "1") + "</dependencies></dependencyManagement><dependencies>" +
			r4dep("g", "x", "") + r4dep("g", "infirst", "") + "</dependencies></project>",
		"first/pom.xml": "<project><groupId>t</groupId><artifactId>first</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4dep("g", "infirst", "5.0") + r4import("t", "inner", "1") + "</dependencies></dependencyManagement></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	got := r6Versions(t, files, "pom.xml", outcomeSource{outcome: RemoteFailed})
	if x := got["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("g:x has version %q, reason %q; want none and %q: first's import could not be looked up and comes before second", x.Version, x.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
	// first's own entry is in front of first's import.
	if in := got["g:infirst"]; in.Version != "5.0" || in.VersionReason != "" {
		t.Errorf("g:infirst has version %q, reason %q; want 5.0", in.Version, in.VersionReason)
	}
}

// A Gradle platform that was obtained, does not manage the dependency, and
// has an import of its own whose lookup failed: what that import manages is
// not known, and it comes before the next platform.
func TestAGradleVersionBehindAPlatformThatIsNotWholeIsNotDetermined(t *testing.T) {
	files := map[string]string{
		"build.gradle": "dependencies {\n  implementation platform('t:first:1')\n  implementation platform('t:second:1')\n  implementation 'g:x'\n  implementation 'g:infirst'\n}\n",
		"first/pom.xml": "<project><groupId>t</groupId><artifactId>first</artifactId><version>1</version><dependencyManagement><dependencies>" +
			r4dep("g", "infirst", "5.0") + r4import("t", "inner", "1") + "</dependencies></dependencyManagement></project>",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	got := r6Versions(t, files, "build.gradle", outcomeSource{outcome: RemoteFailed})
	if x := got["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("g:x has version %q, reason %q; want none and %q", x.Version, x.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
	if in := got["g:infirst"]; in.Version != "5.0" {
		t.Errorf("g:infirst has version %q; want 5.0: the platform's own entry comes before its import", in.Version)
	}
	// With the inner import answered "not found", the second platform's
	// version stands.
	got = r6Versions(t, files, "build.gradle", outcomeSource{outcome: RemoteNotFound})
	if x := got["g:x"]; x.Version != "2.0" {
		t.Errorf("with the import not found: g:x has version %q, reason %q; want 2.0", x.Version, x.VersionReason)
	}
}

// Gradle platforms, in the order they are applied.
func TestAGradleVersionBehindAPlatformThatCouldNotBeObtainedIsNotDetermined(t *testing.T) {
	files := map[string]string{
		"build.gradle":   "dependencies {\n  implementation platform('t:first:1')\n  implementation platform('t:second:1')\n  implementation 'g:x'\n  implementation 'g:none'\n}\n",
		"second/pom.xml": r6Bom("second", "g", "x", "2.0"),
	}
	first := &RemotePom{Group: "t", Artifact: "first", Version: "1", Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "1.0"}}, ServedBy: "repo.example"}
	for name, c := range map[string]struct {
		src          PomSource
		version, why string
		noneWhy      string
	}{
		"obtained":         {outcomeSource{RemoteNotFound, map[string]*RemotePom{"t:first:1": first}}, "1.0", "", ""},
		"lookup failed":    {outcomeSource{outcome: RemoteFailed}, "", model.VersionReasonInheritedLookupFailed, model.VersionReasonInheritedLookupFailed},
		"on no repository": {outcomeSource{outcome: RemoteNotFound}, "2.0", "", model.VersionReasonInheritedNotFound},
	} {
		got := r6Versions(t, files, "build.gradle", c.src)
		if x := got["g:x"]; x.Version != c.version || x.VersionReason != c.why {
			t.Errorf("%s: g:x has version %q, reason %q; want %q, %q", name, x.Version, x.VersionReason, c.version, c.why)
		}
		if none := got["g:none"]; none.Version != "" || none.VersionReason != c.noneWhy {
			t.Errorf("%s: g:none has version %q, reason %q; want none, %q", name, none.Version, none.VersionReason, c.noneWhy)
		}
	}
}
