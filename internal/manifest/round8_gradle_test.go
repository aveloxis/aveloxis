// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Review round 8 (R8-1, R8-5, R8-10, R8-12 and the order of an applied
// script): what a Gradle build's BOMs give a dependency.

// r8Boms is a source with BOMs by "g:a:v"; outcomes are what it says of the
// coordinates it does not have (RemoteNotFound when not listed).
type r8Boms struct {
	has      map[string]map[string]string // BOM -> "g:a" -> version
	outcomes map[string]RemoteOutcome
}

func (s r8Boms) RemotePom(g, a, v string, _ []string) (*RemotePom, RemoteOutcome) {
	key := g + ":" + a + ":" + v
	if o, ok := s.outcomes[key]; ok {
		return nil, o
	}
	entries, ok := s.has[key]
	if !ok {
		return nil, RemoteNotFound
	}
	pom := &RemotePom{Group: g, Artifact: a, Version: v, ServedBy: "repo.example"}
	for name, version := range entries {
		mg, ma, _ := strings.Cut(name, ":")
		pom.Managed = append(pom.Managed, RemoteManaged{Group: mg, Artifact: ma, Version: version})
	}
	return pom, RemoteFound
}

var r8Remote = r8Boms{has: map[string]map[string]string{
	"org.springframework.boot:spring-boot-dependencies:3.3.0": {"g:x": "2.17.1", "g:n": "4.1.110.Final", "g:only-boot": "1.0"},
	"b:boot:3":   {"g:x": "2.17.1", "g:n": "4.1.110.Final", "g:guava": "32.0-jre"},
	"j:jack:2":   {"g:x": "2.16.2"},
	"o:other:1":  {"g:n": "4.1.107.Final", "g:guava": "31.1-jre"},
	"o:tie:1":    {"g:x": "2.17-1"},
	"o:range:1":  {"g:x": "[2,3)"},
	"o:higher:1": {"g:x": "10"},
	"o:lower:1":  {"g:x": "8"},
}}

func r8Gradle(t *testing.T, src PomSource, files map[string]string, rel string) map[string]model.LibyearDep {
	t.Helper()
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	w.SetPomSource(src)
	got := w.Read(root+"/"+rel, Options{})
	if got.Failure != nil {
		t.Fatalf("%s: %v", rel, got.Failure)
	}
	out := map[string]model.LibyearDep{}
	for _, d := range got.Deps {
		if !d.BOMImport {
			out[d.Name] = d
		}
	}
	return out
}

func r8Build(body string) map[string]string { return map[string]string{"build.gradle": body} }

// Spring's dependency-management plugin is not Gradle's platform
// resolution: the LAST BOM imported that manages an artifact answers, and
// the Boot plugin's own BOM is applied before the build's imports. 0.29.214
// took the highest of them, and a build that pins a library below Boot's
// version read as Boot's (R8-1).
func TestTheLastImportedBOMAnswersForSpringsPlugin(t *testing.T) {
	imports := func(boms ...string) string {
		s := "dependencyManagement {\n imports {\n"
		for _, b := range boms {
			s += "  mavenBom '" + b + "'\n"
		}
		return s + " }\n}\ndependencies {\n implementation 'g:x'\n implementation 'g:only-boot'\n}\n"
	}
	boot := "plugins {\n id 'org.springframework.boot' version '3.3.0'\n id 'io.spring.dependency-management' version '1.1.4'\n}\n"
	for name, c := range map[string]struct {
		build string
		want  string
	}{
		"the Boot plugin, then the build pins lower":           {boot + imports("j:jack:2"), "2.16.2"},
		"two imports, the lower last":                          {imports("b:boot:3", "j:jack:2"), "2.16.2"},
		"two imports, the higher last":                         {imports("j:jack:2", "b:boot:3"), "2.17.1"},
		"the Boot plugin alone":                                {boot + imports(), "2.17.1"},
		"an import that is on no repository, then one that is": {imports("nowhere:bom:1", "j:jack:2"), "2.16.2"},
	} {
		if x := r8Gradle(t, r8Remote, r8Build(c.build), "build.gradle")["g:x"]; x.Version != c.want || x.VersionReason != "" {
			t.Errorf("%s: g:x at %q (%s), want %q", name, x.Version, x.VersionReason, c.want)
		}
	}
	// What is not known matters only AFTER the BOM that answers: a later one
	// could override it, an earlier one could not.
	failed := r8Remote
	failed.outcomes = map[string]RemoteOutcome{"corp:bom:1": RemoteFailed}
	if x := r8Gradle(t, failed, r8Build(imports("corp:bom:1", "j:jack:2")), "build.gradle")["g:x"]; x.Version != "2.16.2" {
		t.Errorf("a BOM that could not be obtained BEFORE the one that answers: g:x at %q (%s), want 2.16.2 (the later import overrides whatever it says)", x.Version, x.VersionReason)
	}
	if x := r8Gradle(t, failed, r8Build(imports("j:jack:2", "corp:bom:1")), "build.gradle")["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("a BOM that could not be obtained AFTER the one that answers: g:x at %q (%s); want no version and inherited_lookup_failed", x.Version, x.VersionReason)
	}
	// The Boot plugin's BOM is before the build's imports whatever the file's
	// order of blocks.
	late := "dependencyManagement {\n imports {\n  mavenBom 'j:jack:2'\n }\n}\n" + boot + "dependencies {\n implementation 'g:x'\n}\n"
	if x := r8Gradle(t, r8Remote, r8Build(late), "build.gradle")["g:x"]; x.Version != "2.16.2" {
		t.Errorf("the plugins block written after the imports: g:x at %q, want the build's own 2.16.2", x.Version)
	}
}

// A version the build gives the plugin outright overrides every import, and
// is not a dependency: `dependency 'g:a:v'` in a dependencyManagement block
// was read as a declaration, a row for a package the build may not use.
func TestAVersionGivenToSpringsPluginOutrightIsNotADependency(t *testing.T) {
	build := "dependencyManagement {\n dependencies {\n  dependency 'g:x:5'\n  dependency group: 'g', name: 'mapped', version: '6'\n" +
		"  dependencySet(group: 'gs', version: '7') {\n   entry 'one'\n   entry 'two'\n  }\n  dependencySet('gt:8') {\n   entry 'three'\n  }\n }\n" +
		" imports {\n  mavenBom 'j:jack:2'\n }\n}\n" +
		"dependencies {\n implementation 'g:x'\n implementation 'g:mapped'\n implementation 'gs:one'\n implementation 'gs:two'\n implementation 'gt:three'\n implementation 'g:unmanaged'\n}\n"
	root := writeTree(t, r8Build(build))
	w := NewWalk(root, nil)
	w.SetPomSource(r8Remote)
	rows := map[string][]string{}
	for _, d := range w.Read(root+"/build.gradle", Options{}).Deps {
		if !d.BOMImport {
			rows[d.Name] = append(rows[d.Name], d.Version)
		}
	}
	for name, want := range map[string]string{"g:x": "5", "g:mapped": "6", "gs:one": "7", "gs:two": "7", "gt:three": "8", "g:unmanaged": ""} {
		if got := rows[name]; len(got) != 1 || got[0] != want {
			t.Errorf("%s: rows %q, want one row at %q", name, got, want)
		}
	}
	if len(rows) != 6 {
		t.Errorf("rows for %d packages, want the 6 dependencies and none for a managed entry: %v", len(rows), rows)
	}
}

// Gradle's platforms: the highest recommendation wins, in Gradle's version
// ordering, which orders qualified versions too (R8-5: 4.1.110.Final against
// 4.1.107.Final left the dependency with no version and no reason).
func TestPlatformsAreSettledInGradlesVersionOrdering(t *testing.T) {
	deps := func(platforms ...string) string {
		s := "dependencies {\n"
		for _, p := range platforms {
			s += " implementation platform('" + p + "')\n"
		}
		return s + " implementation 'g:n'\n implementation 'g:guava'\n implementation 'g:x'\n}\n"
	}
	got := r8Gradle(t, r8Remote, r8Build(deps("b:boot:3", "o:other:1")), "build.gradle")
	if n := got["g:n"]; n.Version != "4.1.110.Final" {
		t.Errorf("g:n under 4.1.110.Final and 4.1.107.Final: %q (%s), want 4.1.110.Final", n.Version, n.VersionReason)
	}
	if g := got["g:guava"]; g.Version != "32.0-jre" {
		t.Errorf("g:guava under 32.0-jre and 31.1-jre: %q (%s), want 32.0-jre", g.Version, g.VersionReason)
	}
	// The same in the other order.
	if n := r8Gradle(t, r8Remote, r8Build(deps("o:other:1", "b:boot:3")), "build.gradle")["g:n"]; n.Version != "4.1.110.Final" {
		t.Errorf("the platforms in the other order: g:n at %q", n.Version)
	}
	// Two versions that are different texts and equal in the ordering are
	// not ordered, and the reason says so.
	x := r8Gradle(t, r8Remote, r8Build(deps("b:boot:3", "o:tie:1")), "build.gradle")["g:x"]
	if x.Version != "" || x.VersionReason != model.VersionReasonPlatformsDisagree {
		t.Errorf("g:x under 2.17.1 and 2.17-1: %q (%s); want no version and platforms_disagree", x.Version, x.VersionReason)
	}
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"1.10", "1.9", 1}, {"1.0", "1.0.0", -1}, {"1.1", "1.1.a", 1}, {"1.a", "1.1", -1}, {"1.A", "1.B", -1}, {"1.B", "1.a", -1},
		{"1.0-dev", "1.0-alpha", -1}, {"1.0-alpha", "1.0-rc", -1}, {"1.0-rc", "1.0-SNAPSHOT", -1}, {"1.0-snapshot", "1.0-final", -1},
		{"1.0-final", "1.0-ga", -1}, {"1.0-ga", "1.0-release", -1}, {"1.0-release", "1.0-sp", -1}, {"1.0-sp", "1.0", -1},
		{"1.0-RC", "1.0-rc", 0}, {"1.a.1", "1-a+1", 0}, {"1a1", "1.a.1", 0}, {"1.01", "1.1", 0}, {"2.0", "2.0", 0},
		{"4.1.110.Final", "4.1.107.Final", 1}, {"31.1-jre", "32.0-jre", -1}, {"99999999999999999999999999.1", "99999999999999999999999998.9", 1},
	} {
		if got := gradleCompareVersions(c.a, c.b); got != c.want {
			t.Errorf("gradleCompareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := gradleCompareVersions(c.b, c.a); got != -c.want {
			t.Errorf("gradleCompareVersions(%q, %q) = %d, want %d", c.b, c.a, got, -c.want)
		}
	}
}

// A version the build writes is what the build declares, and it stands,
// whatever a platform says of the artifact (as until 0.29.214). Gradle would
// raise it to a higher recommendation and replace it by an enforced
// platform's: 0.29.215 did both, across configurations and for a strict
// declaration too, and round 9 took it out (a known difference).
func TestAWrittenVersionStands(t *testing.T) {
	failed := r8Remote
	failed.outcomes = map[string]RemoteOutcome{"corp:plat:1": RemoteFailed}
	for name, c := range map[string]struct {
		platforms string
		want      string
		source    string
	}{
		"no platform manages it higher":         {" implementation platform('o:lower:1')\n", "9", ""},
		"a platform recommends higher":          {" implementation platform('o:higher:1')\n", "9", ""},
		"an enforced platform, lower":           {" implementation enforcedPlatform('o:lower:1')\n", "9", ""},
		"an enforced platform beside a higher":  {" implementation enforcedPlatform('o:lower:1')\n implementation platform('o:higher:1')\n", "9", ""},
		"a platform of another configuration":   {" testImplementation platform('o:higher:1')\n", "9", ""},
		"a platform that could not be had":      {" implementation platform('corp:plat:1')\n", "9", ""},
		"an enforced one that could not be":     {" implementation enforcedPlatform('corp:plat:1')\n implementation platform('o:higher:1')\n", "9", ""},
		"a platform that manages it at a range": {" implementation platform('o:range:1')\n", "9", ""},
	} {
		x := r8Gradle(t, failed, r8Build("dependencies {\n"+c.platforms+" implementation 'g:x:9'\n}\n"), "build.gradle")["g:x"]
		if x.Version != c.want || x.VersionSource != c.source || x.VersionReason != "" {
			t.Errorf("%s: g:x at %q (source %q, reason %q), want %q (%q)", name, x.Version, x.VersionSource, x.VersionReason, c.want, c.source)
		}
	}
}

// When Gradle's platforms and Spring's plugin both manage a dependency that
// has no version, one version from both is that version, and two are not
// settled here.
func TestAPlatformAndSpringsPluginThatBothManageADependency(t *testing.T) {
	build := func(platform, bom string) string {
		return "dependencyManagement {\n imports {\n  mavenBom '" + bom + "'\n }\n}\ndependencies {\n implementation platform('" + platform + "')\n implementation 'g:x'\n}\n"
	}
	if x := r8Gradle(t, r8Remote, r8Build(build("b:boot:3", "j:jack:2")), "build.gradle")["g:x"]; x.Version != "" || x.VersionReason != model.VersionReasonPlatformsDisagree {
		t.Errorf("a platform at 2.17.1 and the plugin at 2.16.2: g:x at %q (%s); want no version and platforms_disagree", x.Version, x.VersionReason)
	}
	if x := r8Gradle(t, r8Remote, r8Build(build("b:boot:3", "b:boot:3")), "build.gradle")["g:x"]; x.Version != "2.17.1" {
		t.Errorf("a platform and the plugin at one version: g:x at %q (%s), want 2.17.1", x.Version, x.VersionReason)
	}
}

// Gradle runs a build file from the top: a script applied after the file
// assigned a variable assigns over it, and that is read in the one case
// where it is sure (gradleBuild.applyOrder): both set a property of the
// project at the top of their files, and every "apply from:" is at the top
// too. Everywhere else the file's own value is read, as until 0.29.214
// (round 9: 0.29.215 read the script's for all of these).
func TestAnAppliedScriptAssignsOverTheFileOnlyWhereThatIsSure(t *testing.T) {
	deps := "dependencies {\n implementation \"g:foo:$fooVersion\"\n}\n"
	for name, c := range map[string]struct {
		build string
		want  string
	}{
		"assigned, then the script applied":           {"ext.fooVersion = '1.0.0'\napply from: 'v.gradle'\n" + deps, "2.0.0"},
		"in an ext block, then the script applied":    {"ext {\n fooVersion = '1.0.0'\n}\napply from: 'v.gradle'\n" + deps, "2.0.0"},
		"ext['..'], then the script applied":          {"ext['fooVersion'] = '1.0.0'\napply from: 'v.gradle'\n" + deps, "2.0.0"},
		"assigned, then a script with an ext block":   {"ext.fooVersion = '1.0.0'\napply from: 'block.gradle'\n" + deps, "2.0.0"},
		"assigned twice the same, then the script":    {"ext.fooVersion = '1.0.0'\next.fooVersion = '1.0.0'\napply from: 'v.gradle'\n" + deps, "2.0.0"},
		"assigned under a condition, then the script": {"if (true) {\n ext.fooVersion = '1.0.0'\n}\napply from: 'v.gradle'\n" + deps, "1.0.0"},
		"as a local and then as a property":           {"def fooVersion = '1.0.0'\next.fooVersion = '1.0.0'\napply from: 'v.gradle'\n" + deps, "1.0.0"},
		"as a property and as a local, the script":    {"ext.fooVersion = '1.0.0'\ndef fooVersion = '1.0.0'\napply from: 'v.gradle'\n" + deps, "1.0.0"},
		"assigned, then a script that sets a local":   {"ext.fooVersion = '1.0.0'\napply from: 'local.gradle'\n" + deps, "1.0.0"},
		"assigned, then a script inside a script":     {"ext.fooVersion = '1.0.0'\napply from: 'outer.gradle'\n" + deps, "2.0.0"},
		"assigned, then a script that agrees":         {"ext.fooVersion = '1.0.0'\napply from: 'same.gradle'\n" + deps, "1.0.0"},
		"assigned, then a script under a condition":   {"ext.fooVersion = '1.0.0'\napply from: 'cond.gradle'\n" + deps, "1.0.0"},
		"a local of the file, then the script":        {"def fooVersion = '1.0.0'\napply from: 'v.gradle'\n" + deps, "1.0.0"},
		"assigned, the script applied inside a block": {"ext.fooVersion = '1.0.0'\nsubprojects {\n apply from: 'v.gradle'\n}\n" + deps, "1.0.0"},
		"assigned, one of two applies inside a block": {"ext.fooVersion = '1.0.0'\napply from: 'v.gradle'\nif (ci) {\n apply from: 'other.gradle'\n}\n" + deps, "1.0.0"},
		"the script applied, then assigned":           {"apply from: 'v.gradle'\next.fooVersion = '1.0.0'\n" + deps, "1.0.0"},
		"assigned between two applied scripts":        {"apply from: 'other.gradle'\next.fooVersion = '1.0.0'\napply from: 'v.gradle'\n" + deps, "1.0.0"},
		"only the script assigns it":                  {"apply from: 'v.gradle'\n" + deps, "2.0.0"},
		"assigned after the last of two scripts":      {"apply from: 'other.gradle'\napply from: 'v.gradle'\next.fooVersion = '1.0.0'\n" + deps, "1.0.0"},
		"assigned before, a script that does not set": {"ext.fooVersion = '1.0.0'\napply from: 'other.gradle'\n" + deps, "1.0.0"},
	} {
		files := map[string]string{"build.gradle": c.build, "v.gradle": "ext.fooVersion = '2.0.0'\n", "other.gradle": "ext.unrelated = '3'\n",
			"block.gradle": "ext {\n fooVersion = '2.0.0'\n}\n", "local.gradle": "def fooVersion = '2.0.0'\n", "outer.gradle": "apply from: 'v.gradle'\n",
			"same.gradle": "ext.fooVersion = '1.0.0'\n", "cond.gradle": "if (!project.hasProperty('fooVersion')) {\n ext.fooVersion = '2.0.0'\n}\n"}
		if foo := r8Gradle(t, r8Remote, files, "build.gradle")["g:foo"]; foo.Version != c.want {
			t.Errorf("%s: g:foo at %q, want %q", name, foo.Version, c.want)
		}
	}
}

// A value longer than is read is not read, and no longer in silence: the
// dependency whose version is written with it says so (R8-12). And an
// honest map of versions is read: 4,096 entries were not, since 0.29.214.
func TestAValueTooLongToBeReadSaysSo(t *testing.T) {
	versions := func(n int) string {
		var sb strings.Builder
		sb.WriteString("ext.versions = [\n")
		for i := 0; i < n; i++ {
			fmt.Fprintf(&sb, " k%d: '1.%d',\n", i, i)
		}
		sb.WriteString("]\ndependencies {\n implementation \"g:first:${versions.k0}\"\n implementation 'g:plain:9'\n}\n")
		return sb.String()
	}
	for _, n := range []int{1000, 4096, 5000, 20000} {
		got := r8Gradle(t, r8Remote, r8Build(versions(n)), "build.gradle")
		if first := got["g:first"]; first.Version != "1.0" {
			t.Errorf("a map of %d versions: g:first at %q (%s), want 1.0", n, first.Version, first.VersionReason)
		}
		if plain := got["g:plain"]; plain.Version != "9" || plain.VersionReason != "" {
			t.Errorf("beside a map of %d versions, a dependency written without it: at %q (%s), want 9", n, plain.Version, plain.VersionReason)
		}
	}
	// A statement that only begins like a map (its bracket is closed, and
	// more follows) is not one.
	notAMap := "ext.versions = [k0: '1.0'] + [k1: '2.0']\ndependencies {\n implementation \"g:first:${versions.k0}\"\n}\n"
	if first := r8Gradle(t, r8Remote, r8Build(notAMap), "build.gradle")["g:first"]; first.Version != "" {
		t.Errorf("two maps added together: g:first at %q; the reader does not add maps, and must not read the first as the whole", first.Version)
	}
	// One value of more parts than a value can have.
	long := "ext.v = ''" + strings.Repeat(" + ''", gradleValueTokens) + " + '1'\ndependencies {\n implementation \"g:a:$v\"\n implementation \"g:b:$vOther\"\n}\n"
	got := r8Gradle(t, r8Remote, r8Build(long), "build.gradle")
	if a := got["g:a"]; a.Version != "" || a.VersionReason != model.VersionReasonValueNotRead {
		t.Errorf("a version built from more parts than is read: g:a at %q (%s); want value_not_read", a.Version, a.VersionReason)
	}
	if b := got["g:b"]; b.VersionReason != "" {
		t.Errorf("g:b, written with another variable (vOther): reason %q, want none", b.VersionReason)
	}
}

// A repository a Gradle build declares that is never asked leaves "not
// found" unknown there too (R8-4).
func TestAGradlePlatformBehindARepositoryThatWasNotAsked(t *testing.T) {
	build := func(repo string) string {
		return "repositories {\n mavenCentral()\n" + repo + "}\ndependencies {\n implementation platform('corp:plat:1')\n implementation 'g:x:9'\n implementation 'g:y'\n}\n"
	}
	// With nothing unasked the platform is on no repository: it is known to
	// say nothing, and g:y simply has no version.
	if y := r8Gradle(t, r8Remote, r8Build(build("")), "build.gradle")["g:y"]; y.Version != "" || y.VersionReason != model.VersionReasonInheritedNotFound {
		t.Errorf("no unasked repository: g:y reason %q", y.VersionReason)
	}
	spring := "dependencyManagement {\n imports {\n  mavenBom 'j:jack:2'\n  mavenBom 'corp:bom:1'\n }\n}\ndependencies {\n implementation 'g:x'\n}\n"
	for name, c := range map[string]struct {
		repo string
		want string
	}{
		"none":         {"", "2.16.2"},
		"http":         {"repositories {\n maven { url 'http://nexus.corp.example/repo' }\n}\n", ""},
		"mavenLocal":   {"repositories {\n mavenLocal()\n}\n", ""},
		"an https one": {"repositories {\n maven { url 'https://nexus.corp.example/repo' }\n}\n", "2.16.2"},
	} {
		x := r8Gradle(t, r8Remote, r8Build(c.repo+spring), "build.gradle")["g:x"]
		if x.Version != c.want {
			t.Errorf("a later BOM that is on no repository asked, with repository %s: g:x at %q (%s), want %q", name, x.Version, x.VersionReason, c.want)
		}
	}
}
