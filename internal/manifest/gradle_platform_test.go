// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// Gradle platforms through the resolver (summary/70, 4.6): a dependency
// written with no version takes the version its platform (a BOM) manages,
// exactly as a pom's does.

func gradlePlatformDeps(t *testing.T, src PomSource, files map[string]string, build string) (*Walk, map[string]model.LibyearDep, []model.LibyearDep) {
	t.Helper()
	root := gradleClone(t, files)
	w := NewWalk(root, nil)
	if src != nil {
		w.SetPomSource(src)
	}
	deps, err := w.Declared(filepath.Join(root, filepath.FromSlash(build)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]model.LibyearDep{}
	for i := range deps {
		deps[i].ManifestPath = build
		by[deps[i].Name] = deps[i]
	}
	return w, by, deps
}

func bootPoms() *fakePoms {
	poms := springPoms()
	for _, p := range poms {
		p.ServedBy = "repo.example"
	}
	return &fakePoms{poms: poms}
}

const bootBuild = `
dependencies {
    implementation platform('org.springframework.boot:spring-boot-dependencies:3.2.0')
    implementation 'com.fasterxml.jackson.core:jackson-databind'
    implementation 'io.netty:netty-all'
    implementation 'com.acme:unmanaged'
    implementation 'com.acme:explicit:9.9'
    implementation "com.fasterxml.jackson.core:jackson-core:$unreadVariable"
}
`

func TestGradlePlatformSuppliesVersions(t *testing.T) {
	src := bootPoms()
	w, by, deps := gradlePlatformDeps(t, src, map[string]string{"build.gradle": bootBuild}, "build.gradle")
	jackson := by["com.fasterxml.jackson.core:jackson-databind"]
	if jackson.Version != "2.15.3" || jackson.VersionSource != SourceManaged {
		t.Errorf("a version-less dependency under a platform = %q (source %q); want the platform's 2.15.3, managed", jackson.Version, jackson.VersionSource)
	}
	if strings.Join(jackson.VersionHosts, ",") != "repo.example" {
		t.Errorf("the hosts the version came through = %v", jackson.VersionHosts)
	}
	if d := by["io.netty:netty-all"]; d.Version != "4.1.101.Final" {
		t.Errorf("netty = %q", d.Version)
	}
	// Nothing manages it, and the platform was read whole: it has no
	// version and no reason.
	if d := by["com.acme:unmanaged"]; d.Version != "" || d.VersionReason != "" || d.VersionSource != "" {
		t.Errorf("an unmanaged dependency = %+v", d)
	}
	// A written version is the build's choice, whatever the platform says.
	if d := by["com.acme:explicit"]; d.Version != "9.9" || d.VersionSource == SourceManaged {
		t.Errorf("an explicit version = %q (source %q)", d.Version, d.VersionSource)
	}
	// A version that was written and could not be read is not replaced by
	// the platform's: the build asked for something else.
	if d := by["com.fasterxml.jackson.core:jackson-core"]; d.Version != "" || d.VersionSource == SourceManaged {
		t.Errorf("a written, unread version = %q (source %q); the platform must not answer for it", d.Version, d.VersionSource)
	}
	// The platform itself is a managed entry now: applied, it leaves the
	// inventory with a note, as a pom's BOM import does.
	bom := by["org.springframework.boot:spring-boot-dependencies"]
	if !bom.BOMImport || !bom.ManagedEntry {
		t.Errorf("the platform row = %+v; want a BOM import that is a managed entry", bom)
	}
	settled := w.SettleMavenManaged(deps)
	var kept, unused []string
	for _, d := range settled.Kept {
		kept = append(kept, d.Name)
	}
	for _, d := range settled.Unused {
		unused = append(unused, d.Name)
	}
	sort.Strings(kept)
	if strings.Join(unused, ",") != "org.springframework.boot:spring-boot-dependencies" || len(kept) != 5 {
		t.Errorf("settled: kept %v, noted %v; want the platform noted and the five dependencies kept", kept, unused)
	}
}

// A platform that could not be obtained says so on each dependency it might
// have answered for, and stays a row: nothing was applied.
func TestGradlePlatformNotObtained(t *testing.T) {
	for outcome, reason := range map[RemoteOutcome]string{
		RemoteFailed:  model.VersionReasonInheritedLookupFailed,
		RemoteRefused: model.VersionReasonInheritedRepositoryRefused,
	} {
		src := &fakePoms{outcome: map[string]RemoteOutcome{"org.springframework.boot:spring-boot-dependencies:3.2.0": outcome}}
		_, by, _ := gradlePlatformDeps(t, src, map[string]string{"build.gradle": bootBuild}, "build.gradle")
		if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "" || d.VersionReason != reason {
			t.Errorf("outcome %d: %q reason %q; want no version and %q", outcome, d.Version, d.VersionReason, reason)
		}
		if d := by["com.acme:explicit"]; d.VersionReason != "" {
			t.Errorf("an explicit version carries the reason %q", d.VersionReason)
		}
		if bom := by["org.springframework.boot:spring-boot-dependencies"]; bom.ManagedEntry || bom.Version != "3.2.0" {
			t.Errorf("outcome %d: the platform row = %+v; want it kept as a row", outcome, bom)
		}
	}
	// With no source at all nothing outside the clone is asked, and nothing
	// is claimed.
	_, by, _ := gradlePlatformDeps(t, nil, map[string]string{"build.gradle": bootBuild}, "build.gradle")
	if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "" || d.VersionReason != "" {
		t.Errorf("with no source: %+v", d)
	}
	if by["org.springframework.boot:spring-boot-dependencies"].ManagedEntry {
		t.Error("with no source the platform was treated as applied")
	}
}

// The ways a build names its BOM: a platform in the build file, one for all
// subprojects in a build file above, a catalogue accessor, an in-clone pom,
// Spring's dependency-management imports, and the Spring Boot plugin with
// the dependency-management plugin.
func TestGradlePlatformForms(t *testing.T) {
	const want = "2.15.3"
	versionless := "\ndependencies { implementation 'com.fasterxml.jackson.core:jackson-databind' }\n"
	cases := map[string]map[string]string{
		"from the root's subprojects block": {
			"build.gradle":     "subprojects { dependencies { implementation platform('org.springframework.boot:spring-boot-dependencies:3.2.0') } }",
			"app/build.gradle": versionless,
		},
		"from the root's allprojects block (kts)": {
			"build.gradle.kts": `allprojects { dependencies { "implementation"(platform("org.springframework.boot:spring-boot-dependencies:3.2.0")) } }`,
			"app/build.gradle": versionless,
		},
		"a catalogue accessor": {
			"gradle/libs.versions.toml": "[libraries]\nboot-bom = { module = \"org.springframework.boot:spring-boot-dependencies\", version = \"3.2.0\" }\n",
			"app/build.gradle":          "dependencies { implementation platform(libs.boot.bom)\n implementation 'com.fasterxml.jackson.core:jackson-databind' }",
		},
		"dependency-management imports": {
			"app/build.gradle": "dependencyManagement { imports { mavenBom \"org.springframework.boot:spring-boot-dependencies:${bootVersion}\" } }\next.bootVersion = '3.2.0'" + versionless,
		},
		"the Spring Boot plugin with dependency management": {
			"app/build.gradle": "plugins {\n id 'java'\n id 'org.springframework.boot' version '3.2.0'\n id 'io.spring.dependency-management' version '1.1.4'\n}" + versionless,
		},
		"the Spring Boot plugin (kts)": {
			"app/build.gradle": "plugins {\n id(\"org.springframework.boot\") version \"3.2.0\"\n id(\"io.spring.dependency-management\") version \"1.1.4\"\n}" + versionless,
		},
	}
	for name, files := range cases {
		_, by, _ := gradlePlatformDeps(t, bootPoms(), files, "app/build.gradle")
		if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != want || d.VersionSource != SourceManaged {
			t.Errorf("%s: %q (source %q); want %s, managed", name, d.Version, d.VersionSource, want)
		}
	}
	// A platform of the root's OWN dependencies is not the subprojects'.
	_, by, _ := gradlePlatformDeps(t, bootPoms(), map[string]string{
		"build.gradle":     "dependencies { implementation platform('org.springframework.boot:spring-boot-dependencies:3.2.0') }",
		"app/build.gradle": versionless,
	}, "app/build.gradle")
	if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "" {
		t.Errorf("the root's own platform answered for a subproject: %q", d.Version)
	}
	// The Boot plugin alone manages nothing: the dependency-management
	// plugin is what applies its BOM.
	_, by, _ = gradlePlatformDeps(t, bootPoms(), map[string]string{
		"app/build.gradle": "plugins { id 'org.springframework.boot' version '3.2.0' }" + versionless,
	}, "app/build.gradle")
	if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "" {
		t.Errorf("the Boot plugin without dependency management supplied %q", d.Version)
	}
	// "apply false" declares the plugin for other projects; it is not
	// applied to this one.
	_, by, _ = gradlePlatformDeps(t, bootPoms(), map[string]string{
		"app/build.gradle": "plugins {\n id 'org.springframework.boot' version '3.2.0' apply false\n id 'io.spring.dependency-management' version '1.1.4'\n}" + versionless,
	}, "app/build.gradle")
	if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "" {
		t.Errorf("a plugin declared with apply false supplied %q", d.Version)
	}
	// A version that was written and could not be read is the build's own
	// choice: the platform manages this very coordinate and still does not
	// answer for it.
	_, by, _ = gradlePlatformDeps(t, bootPoms(), map[string]string{
		"app/build.gradle": "dependencies { implementation platform('org.springframework.boot:spring-boot-dependencies:3.2.0')\n implementation \"com.fasterxml.jackson.core:jackson-databind:$unread\" }",
	}, "app/build.gradle")
	if d := by["com.fasterxml.jackson.core:jackson-databind"]; d.Version != "" || d.VersionSource == SourceManaged {
		t.Errorf("a written, unread version was replaced by the platform's %q", d.Version)
	}
	// A pom of the clone with the platform's coordinates is not the
	// platform (0.29.216: until then it was taken, a test fixture's too).
	// Gradle asks its repositories; with none to ask, it is not known.
	_, by, _ = gradlePlatformDeps(t, nil, map[string]string{
		"bom/pom.xml": `<project><groupId>own</groupId><artifactId>bom</artifactId><version>1.0</version><packaging>pom</packaging>
<dependencyManagement><dependencies><dependency><groupId>own</groupId><artifactId>lib</artifactId><version>4.5</version></dependency></dependencies></dependencyManagement></project>`,
		"app/build.gradle": "dependencies { implementation platform('own:bom:1.0')\n implementation 'own:lib' }",
	}, "app/build.gradle")
	if d := by["own:lib"]; d.Version != "" || d.VersionSource == SourceManaged {
		t.Errorf("a pom of the clone was taken for a platform: %q (source %q, reason %q)", d.Version, d.VersionSource, d.VersionReason)
	}
}

// A platform whose own imported BOM could not be obtained was not read
// whole: it stays a row, and what it might have managed says why.
func TestGradlePlatformWithAnUnobtainedImportIsNotWhole(t *testing.T) {
	src := &fakePoms{
		poms: map[string]*RemotePom{"p:outer:1": {Group: "p", Artifact: "outer", Version: "1",
			Managed: []RemoteManaged{{Group: "p", Artifact: "direct", Version: "1.1"}, {Group: "p", Artifact: "inner", Version: "2", Import: true}}}},
		outcome: map[string]RemoteOutcome{"p:inner:2": RemoteFailed},
	}
	_, by, _ := gradlePlatformDeps(t, src, map[string]string{"build.gradle": `
dependencies {
    implementation platform('p:outer:1')
    implementation 'p:direct'
    implementation 'p:from-the-inner-bom'
}`}, "build.gradle")
	if d := by["p:direct"]; d.Version != "1.1" {
		t.Errorf("what the platform manages itself = %q", d.Version)
	}
	if d := by["p:from-the-inner-bom"]; d.Version != "" || d.VersionReason != model.VersionReasonInheritedLookupFailed {
		t.Errorf("what the unobtained import might manage = %q reason %q", d.Version, d.VersionReason)
	}
	if by["p:outer"].ManagedEntry {
		t.Error("a platform with an import that could not be obtained was treated as read whole")
	}
	// The same with only a dependency the platform does manage: the import
	// still failed, so the platform is still not whole.
	_, by, _ = gradlePlatformDeps(t, src, map[string]string{"build.gradle": "dependencies { implementation platform('p:outer:1')\n implementation 'p:direct' }"}, "build.gradle")
	if by["p:outer"].ManagedEntry {
		t.Error("with no dependency reaching the failed import, the platform was treated as read whole")
	}
}
