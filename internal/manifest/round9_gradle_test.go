// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// r9Boot is a repository with a spring-boot-dependencies that manages g:x
// by a property, g:plain outright, and g:j through a BOM whose version is a
// property.
func r9Boot() *fakePoms {
	return &fakePoms{poms: map[string]*RemotePom{
		"org.springframework.boot:spring-boot-dependencies:3.3.0": {Group: "org.springframework.boot", Artifact: "spring-boot-dependencies", Version: "3.3.0",
			Properties: map[string]string{"x.version": "2.14.1", "j-bom.version": "1", "plainVersion": "1.0"},
			Managed: []RemoteManaged{{Group: "g", Artifact: "x", Version: "${x.version}"}, {Group: "g", Artifact: "plain", Version: "${plainVersion}"},
				{Group: "j", Artifact: "j-bom", Version: "${j-bom.version}", Import: true}}},
		"j:j-bom:1": {Group: "j", Artifact: "j-bom", Version: "1", Managed: []RemoteManaged{{Group: "g", Artifact: "j", Version: "1.1"}}},
		"j:j-bom:2": {Group: "j", Artifact: "j-bom", Version: "2", Managed: []RemoteManaged{{Group: "g", Artifact: "j", Version: "2.2"}}},
	}}
}

const (
	r9Plugins = "plugins {\n id 'org.springframework.boot' version '3.3.0'\n id 'io.spring.dependency-management' version '1.1.4'\n}\n"
	r9Deps    = "dependencies {\n implementation 'g:x'\n implementation 'g:plain'\n implementation 'g:j'\n}\n"
)

// Round 9 (R9-3). Spring's dependency-management plugin reads a BOM with
// the project's properties over the BOM's own: ext['x.version'] = '...' is
// the documented way to change a version Spring Boot manages. None of the
// forms was read, and the BOM's default was stored.
func TestAProjectPropertyOverridesASpringBOMsProperty(t *testing.T) {
	kts := "plugins {\n  id(\"org.springframework.boot\") version \"3.3.0\"\n  id(\"io.spring.dependency-management\") version \"1.1.4\"\n}\n"
	ktsDeps := "dependencies {\n  implementation(\"g:x\")\n  implementation(\"g:plain\")\n  implementation(\"g:j\")\n}\n"
	importing := func(closure string) string {
		return "plugins { id 'io.spring.dependency-management' version '1.1.4' }\ndependencyManagement {\n imports {\n  mavenBom('org.springframework.boot:spring-boot-dependencies:3.3.0')" + closure + "\n }\n}\n" + r9Deps
	}
	for name, c := range map[string]struct {
		files map[string]string
		rel   string
		x, j  string
	}{
		"nothing set (the neighbour)":           {r8Build(r9Plugins + r9Deps), "build.gradle", "2.14.1", "1.1"},
		"ext['x.version']":                      {r8Build(r9Plugins + "ext['x.version'] = '2.17.1'\n" + r9Deps), "build.gradle", "2.17.1", "1.1"},
		"ext { set('x.version', ..) }":          {r8Build(r9Plugins + "ext {\n set('x.version', '2.17.1')\n}\n" + r9Deps), "build.gradle", "2.17.1", "1.1"},
		"extra[\"x.version\"] in Kotlin":        {map[string]string{"build.gradle.kts": kts + "extra[\"x.version\"] = \"2.17.1\"\n" + ktsDeps}, "build.gradle.kts", "2.17.1", "1.1"},
		"the version of a BOM the BOM imports":  {r8Build(r9Plugins + "ext['j-bom.version'] = '2'\n" + r9Deps), "build.gradle", "2.14.1", "2.2"},
		"set by the build file above":           {map[string]string{"build.gradle": "ext['x.version'] = '2.17.1'\n", "settings.gradle": "include 'app'\n", "app/build.gradle": r9Plugins + r9Deps}, "app/build.gradle", "2.17.1", "1.1"},
		"set after the dependencies block":      {r8Build(r9Plugins + r9Deps + "ext['x.version'] = '2.17.1'\n"), "build.gradle", "2.17.1", "1.1"},
		"bomProperty in the import's closure":   {r8Build(importing(" {\n   bomProperty 'x.version', '2.17.1'\n  }")), "build.gradle", "2.17.1", "1.1"},
		"bomProperties in the import's closure": {r8Build(importing(" {\n   bomProperties(['x.version': '2.17.1', 'j-bom.version': '2'])\n  }")), "build.gradle", "2.17.1", "2.2"},
		"bomProperty over the project's":        {r8Build("ext['x.version'] = '0.1'\n" + importing(" {\n   bomProperty 'x.version', '2.17.1'\n  }")), "build.gradle", "2.17.1", "1.1"},
		"two values in the file: not read":      {r8Build(r9Plugins + "ext['x.version'] = '2.17.1'\next['x.version'] = '2.18.0'\n" + r9Deps), "build.gradle", "", "1.1"},
		// A platform(...) is Gradle's own: no property of the project
		// reaches the BOM.
		"a platform of the same BOM": {r8Build("ext['x.version'] = '2.17.1'\ndependencies {\n implementation platform('org.springframework.boot:spring-boot-dependencies:3.3.0')\n implementation 'g:x'\n implementation 'g:plain'\n implementation 'g:j'\n}\n"), "build.gradle", "2.14.1", "1.1"},
		// A name a local variable can have is not taken for a property of
		// the project (a "def" is not one).
		"a local of the file with a property's name": {r8Build(r9Plugins + "def plainVersion = '7'\next['x.version'] = '2.17.1'\n" + r9Deps), "build.gradle", "2.17.1", "1.1"},
	} {
		got := r8Gradle(t, r9Boot(), c.files, c.rel)
		if x, j, plain := got["g:x"], got["g:j"], got["g:plain"]; x.Version != c.x || j.Version != c.j || plain.Version != "1.0" {
			t.Errorf("%s: g:x at %q (%s), g:j at %q (%s), g:plain at %q; want %q, %q and 1.0", name, x.Version, x.VersionReason, j.Version, j.VersionReason, plain.Version, c.x, c.j)
		}
	}
}

// Round 9 (R9-7). What Spring's plugin is given reaches the projects it is
// given for: a subprojects block's is for the projects below and not the
// file's own; a block inside dependencyManagement that names a
// configuration is for that configuration.
func TestSpringsManagementReachesTheProjectsAndConfigurationsItIsFor(t *testing.T) {
	settings := "include 'app'\n"
	app := "dependencies {\n implementation 'g:x'\n testImplementation 'g:n'\n}\n"
	read := func(root, rel string) (x, n string) {
		got := r8Gradle(t, r8Remote, map[string]string{"build.gradle": root, "settings.gradle": settings, "app/build.gradle": app}, rel)
		return got["g:x"].Version + "(" + got["g:x"].VersionReason + ")", got["g:n"].Version + "(" + got["g:n"].VersionReason + ")"
	}
	own := "dependencies {\n implementation 'g:x'\n testImplementation 'g:n'\n}\n"
	for name, c := range map[string]struct {
		root                     string
		rootX, rootN, appX, appN string
	}{
		"a version given inside subprojects": {"subprojects {\n dependencyManagement {\n  dependencies {\n   dependency 'g:x:5.5'\n  }\n }\n}\n" + own,
			"()", "()", "5.5()", "()"},
		"a version given inside allprojects": {"allprojects {\n dependencyManagement {\n  dependencies {\n   dependency 'g:x:5.5'\n  }\n }\n}\n" + own,
			"5.5()", "()", "5.5()", "()"},
		"a version given at the top of the root": {"dependencyManagement {\n dependencies {\n  dependency 'g:x:5.5'\n }\n}\n" + own,
			"5.5()", "()", "()", "()"},
		"a mavenBom inside subprojects": {"subprojects {\n dependencyManagement {\n  imports {\n   mavenBom 'b:boot:3'\n  }\n }\n}\n" + own,
			"()", "()", "2.17.1()", "4.1.110.Final()"},
		"a mavenBom inside allprojects": {"allprojects {\n dependencyManagement {\n  imports {\n   mavenBom 'b:boot:3'\n  }\n }\n}\n" + own,
			"2.17.1()", "4.1.110.Final()", "2.17.1()", "4.1.110.Final()"},
		"a version given for testImplementation": {"dependencyManagement {\n testImplementation {\n  dependencies {\n   dependency 'g:x:5.5'\n   dependency 'g:n:6.6'\n  }\n }\n}\n" + own,
			"()", "6.6()", "()", "()"},
		"a mavenBom for testImplementation": {"dependencyManagement {\n testImplementation {\n  imports {\n   mavenBom 'b:boot:3'\n  }\n }\n}\n" + own,
			"()", "4.1.110.Final()", "()", "()"},
		"for one configuration, written before the one for all": {"dependencyManagement {\n testImplementation {\n  dependencies {\n   dependency 'g:n:6.6'\n  }\n }\n dependencies {\n  dependency 'g:n:1.0'\n  dependency 'g:x:5.5'\n }\n}\n" + own,
			"5.5()", "6.6()", "()", "()"},
		"a BOM for one configuration, a version given for all": {"dependencyManagement {\n testImplementation {\n  imports {\n   mavenBom 'b:boot:3'\n  }\n }\n dependencies {\n  dependency 'g:n:1.0'\n }\n}\n" + own,
			"()", "4.1.110.Final()", "()", "()"},
		"for one configuration, over the one for all": {"dependencyManagement {\n dependencies {\n  dependency 'g:n:1.0'\n  dependency 'g:x:5.5'\n }\n testImplementation {\n  dependencies {\n   dependency 'g:n:6.6'\n  }\n }\n}\n" + own,
			"5.5()", "6.6()", "()", "()"},
	} {
		rootX, rootN := read(c.root, "build.gradle")
		appX, appN := read(c.root, "app/build.gradle")
		if rootX != c.rootX || rootN != c.rootN || appX != c.appX || appN != c.appN {
			t.Errorf("%s: the root has g:x %s and g:n %s, the project below g:x %s and g:n %s; want %s, %s, %s, %s", name, rootX, rootN, appX, appN, c.rootX, c.rootN, c.appX, c.appN)
		}
	}
}

// Round 9 (R9-8). Inside a dependencySet the entries are "entry"; a
// "dependency" statement there is not one of the set's and is not read as a
// managed version (no test said so).
func TestOnlyEntriesAreReadInsideADependencySet(t *testing.T) {
	build := "dependencyManagement {\n dependencies {\n  dependencySet(group: 'g', version: '5.5') {\n   entry 'x'\n   dependency 'g:n:7.7'\n  }\n }\n}\ndependencies {\n implementation 'g:x'\n implementation 'g:n'\n}\n"
	got := r8Gradle(t, r8Remote, r8Build(build), "build.gradle")
	if x, n := got["g:x"], got["g:n"]; x.Version != "5.5" || n.Version != "" {
		t.Errorf("g:x at %q, g:n at %q; want the set's 5.5 and no version", x.Version, n.Version)
	}
}

// What a build file declares outside a shared block is for its own project:
// a dependency its subprojects block declares, which is a dependency of the
// projects below, is not managed by it.
func TestWhatTheRootDeclaresForItselfIsNotForItsSubprojectsDependencies(t *testing.T) {
	for name, c := range map[string]struct{ management, want string }{
		"a mavenBom at the top":         {"dependencyManagement {\n imports {\n  mavenBom 'b:boot:3'\n }\n}\n", ""},
		"a platform at the top":         {"dependencies {\n implementation platform('b:boot:3')\n}\n", ""},
		"a mavenBom inside allprojects": {"allprojects {\n dependencyManagement {\n  imports {\n   mavenBom 'b:boot:3'\n  }\n }\n}\n", "2.17.1"},
		"a platform inside subprojects": {"subprojects {\n dependencies {\n  implementation platform('b:boot:3')\n }\n}\n", "2.17.1"},
	} {
		build := c.management + "subprojects {\n dependencies {\n  implementation 'g:x'\n }\n}\n"
		if x := r8Gradle(t, r8Remote, r8Build(build), "build.gradle")["g:x"]; x.Version != c.want {
			t.Errorf("%s: g:x, declared inside subprojects, at %q (%s), want %q", name, x.Version, x.VersionReason, c.want)
		}
	}
}

// Nor does a platform the subprojects block names lend the file's own
// dependencies its reason: the reason was one for the whole file (found by
// the independent reference once its root project had dependencies).
func TestAPlatformForTheProjectsBelowGivesTheRootNoReason(t *testing.T) {
	failed := r8Remote
	failed.outcomes = map[string]RemoteOutcome{"corp:plat:1": RemoteFailed}
	build := "subprojects {\n dependencies {\n  implementation platform('corp:plat:1')\n  implementation 'g:below'\n }\n}\ndependencies {\n implementation 'g:own'\n}\n"
	got := r8Gradle(t, failed, r8Build(build), "build.gradle")
	if own, below := got["g:own"], got["g:below"]; own.VersionReason != "" || below.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("the root's own dependency has reason %q and its subprojects' %q; want none and %q", own.VersionReason, below.VersionReason, model.VersionReasonInheritedLookupFailed)
	}
}

// Round 9, after R9-1. A Gradle platform(...) or mavenBom named by
// coordinates is a published artifact: Gradle asks its repositories for it
// and never reads a pom.xml of the checkout for it. A pom of the clone that
// gives itself a published BOM's coordinates (a test fixture, an example)
// was taken for the platform, as a parent was for Maven before rule A.
func TestAPomOfTheCloneIsNotTakenForAGradlePlatform(t *testing.T) {
	fixture := "<project><groupId>j</groupId><artifactId>j-bom</artifactId><version>1</version><packaging>pom</packaging>" +
		"<dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>j</artifactId><version>0.0.1-fixture</version></dependency></dependencies></dependencyManagement></project>"
	deps := "dependencies {\n implementation 'g:j'\n}\n"
	for name, body := range map[string]string{
		"platform": "dependencies {\n implementation platform('j:j-bom:1')\n implementation 'g:j'\n}\n",
		"mavenBom": "plugins { id 'io.spring.dependency-management' version '1.1.4' }\ndependencyManagement {\n imports {\n  mavenBom 'j:j-bom:1'\n }\n}\n" + deps,
	} {
		files := map[string]string{"build.gradle": body, "src/test/resources/fixture/pom.xml": fixture}
		if got := r8Gradle(t, r9Boot(), files, "build.gradle")["g:j"]; got.Version != "1.1" {
			t.Errorf("%s: g:j at %q (%s); want the published BOM's 1.1", name, got.Version, got.VersionReason)
		}
		// With no repository to ask, the platform is not known: the
		// fixture is still not it.
		if got := r8Gradle(t, &fakePoms{poms: map[string]*RemotePom{}}, files, "build.gradle")["g:j"]; got.Version != "" || got.VersionReason == "" {
			t.Errorf("%s, nothing published: g:j at %q (%q); want no version and a reason", name, got.Version, got.VersionReason)
		}
		// Nor is one that cannot be read "maybe the platform": the
		// repositories' "not found" is the answer, as it is with no such
		// file (for a pom's parent, an unread pom of the clone may be it).
		// The walk has read the pom (it is a manifest of the clone) before
		// it reads the build file.
		files["src/test/resources/fixture/pom.xml"] = fixture[:len(fixture)/2]
		root := writeTree(t, files)
		w := NewWalk(root, nil)
		w.SetPomSource(outcomeSource{outcome: RemoteNotFound})
		w.Read(root+"/src/test/resources/fixture/pom.xml", Options{})
		for _, d := range w.Read(root+"/build.gradle", Options{}).Deps {
			if d.Name == "g:j" && (d.Version != "" || d.VersionReason != model.VersionReasonInheritedNotFound) {
				t.Errorf("%s, an unreadable pom with the coordinates: g:j at %q (%q); want no version and %s", name, d.Version, d.VersionReason, model.VersionReasonInheritedNotFound)
			}
		}
	}
}

var r9Entry = regexp.MustCompile(`<dependency><groupId>([^<]*)</groupId><artifactId>([^<]*)</artifactId>(?:<version>([^<]*)</version>)?(<type>pom</type><scope>import</scope>)?</dependency>`)

// r9Published is a repository that has the BOMs a test wrote as poms of the
// clone (r6Bom's shape: group t, version 1) and answers outcome for every
// other pom. A Gradle platform is a published pom and never one of the
// clone (0.29.216), so the tests of what Gradle does with its platforms
// publish theirs; the files stay in the clone, where nothing reads them
// for a platform (TestAPomOfTheCloneIsNotTakenForAGradlePlatform).
func r9Published(files map[string]string, outcome RemoteOutcome) outcomeSource {
	src := outcomeSource{outcome: outcome, has: map[string]*RemotePom{}}
	for rel, content := range files {
		if !strings.HasSuffix(rel, "pom.xml") {
			continue
		}
		head := strings.SplitN(content, "<dependencyManagement>", 2)[0]
		artifact := regexp.MustCompile(`<artifactId>([^<]*)</artifactId>`).FindStringSubmatch(head)
		if artifact == nil {
			continue
		}
		pom := &RemotePom{Group: "t", Artifact: artifact[1], Version: "1"}
		for _, m := range r9Entry.FindAllStringSubmatch(content, -1) {
			pom.Managed = append(pom.Managed, RemoteManaged{Group: m[1], Artifact: m[2], Version: m[3], Import: m[4] != ""})
		}
		src.has["t:"+artifact[1]+":1"] = pom
	}
	return src
}
