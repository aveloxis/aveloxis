// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// gradleClone writes files (path -> content) under a temporary clone and
// returns its root.
func gradleClone(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func wantGradleTree(t *testing.T, what string, files map[string]string, build, want string) {
	t.Helper()
	root := gradleClone(t, files)
	deps, err := NewWalk(root, nil).Declared(filepath.Join(root, filepath.FromSlash(build)), Options{})
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if got := gradleRows(deps); got != strings.TrimSpace(want) {
		t.Errorf("%s:\n--- got\n%s\n--- want\n%s", what, got, strings.TrimSpace(want))
	}
}

// A version, or part of a name, held outside the build file (summary/70,
// 4.6): gradle.properties from the root down, settings.gradle, the build
// files above, a file applied with "apply from". The nearest wins.
func TestGradleTreeVariables(t *testing.T) {
	files := map[string]string{
		"gradle.properties": "# comment\nfromRootProps=1.0\noverridden = root\nspaced : 1.5\ndotted.key=1.7\n! also a comment\n",
		"settings.gradle": `
rootProject.name = 'demo'
gradle.ext.scala = [depVersion: '2.13', other: 'x']
gradle.ext.pekko = [version : '1.1.5']
include ':app'
`,
		"build.gradle": `
ext { fromRootBuild = '2.0' }
ext.versions = [guava: '33.0']
allprojects { ext.fromAllProjects = '2.5' }
`,
		"gradle/versions.gradle":    "ext.fromApplied = '3.0'\n",
		"app/gradle.properties":     "overridden=nearest\nfromAppProps=4.0\n",
		"app/local-versions.gradle": "ext { fromLocalApplied = '5.0' }\n",
		"app/build.gradle": `
apply from: "$rootDir/gradle/versions.gradle"
apply from: 'local-versions.gradle'
apply from: '../../outside-the-clone.gradle'
apply plugin: 'java'
ext.own = '6.0'
dependencies {
    implementation "t:root-props:$fromRootProps"
    implementation "t:spaced:$spaced"
    implementation "t:dotted:${project.property('dotted.key')}"
    implementation "t:nearest:$overridden"
    implementation "t:app-props:$fromAppProps"
    implementation "t:settings-name_${gradle.scala.depVersion}:${gradle.pekko.version}"
    implementation "t:root-build:${rootProject.ext.fromRootBuild}"
    implementation "t:root-map:${versions.guava}"
    implementation "t:all-projects:$fromAllProjects"
    implementation "t:applied:$fromApplied"
    implementation "t:local-applied:$fromLocalApplied"
    implementation "t:own:$own"
    implementation "t:nowhere:$nowhere"
}
`,
	}
	wantGradleTree(t, "tree variables", files, "app/build.gradle", `
t:all-projects@2.5 runtime
t:app-props@4.0 runtime
t:applied@3.0 runtime
t:dotted@1.7 runtime
t:local-applied@5.0 runtime
t:nearest@nearest runtime
t:nowhere@ runtime
t:own@6.0 runtime
t:root-build@2.0 runtime
t:root-map@33.0 runtime
t:root-props@1.0 runtime
t:settings-name_2.13@1.1.5 runtime
t:spaced@1.5 runtime`)
	// The root build file sees the root's values, not a module's.
	files["build.gradle"] += "dependencies { implementation \"t:from-root:$overridden\"\n implementation \"t:not-visible:$fromAppProps\" }\n"
	wantGradleTree(t, "the root build file", files, "build.gradle", `
t:from-root@root runtime
t:not-visible@ runtime`)
}

// A variable given two different values in one file (one per branch of a
// condition, usually) is not known: choosing one would be a guess, and in a
// name it would be another package.
func TestGradleConflictingAssignmentsAreNotKnown(t *testing.T) {
	wantGradleTree(t, "conflict", map[string]string{
		"settings.gradle": `
if (scala213) {
    gradle.ext.scala = [depVersion: '2.13', same: 'k']
} else {
    gradle.ext.scala = [depVersion: '2.12', same: 'k']
}
`,
		"build.gradle": `
dependencies {
    implementation "c:lib_${gradle.scala.depVersion}:1"
    implementation "c:version:${gradle.scala.depVersion}"
    implementation "c:agreed:${gradle.scala.same}"
}
`}, "build.gradle", `
c:agreed@k runtime
c:version@ runtime`)
}

// A version catalogue (gradle/*.versions.toml) and its accessors.
func TestGradleVersionCatalogue(t *testing.T) {
	files := map[string]string{
		"gradle/libs.versions.toml": `
[versions]
junit = "5.10.0"
aws_sdk = "2.55.2"   # trailing comment
rich = { strictly = "1.2.3" }
range = { strictly = "[1.0, 2.0[", prefer = "1.5" }

[libraries]
plain = "cat:plain:1.0"
module-ref = { module = "cat:module-ref", version.ref = "junit" }
module-inline = { module = "cat:module-inline", version = "2.0" }
group-name = { group = "cat", name = "group-name", version.ref = "aws_sdk" }
no-version = { module = "cat:no-version" }
androidx-core_ktx = { module = "cat:core-ktx", version.ref = "rich" }
bad-ref = { module = "cat:bad-ref", version.ref = "missing" }
ranged = { module = "cat:ranged", version.ref = "range" }
bom = { module = "cat:bom", version = "9.0" }
multi = {
    module = "cat:multi-line",
    version.ref = "junit"
}

[bundles]
pair = ["plain", "module-inline"]

[plugins]
kotlin-jvm = { id = "org.jetbrains.kotlin.jvm", version.ref = "junit" }
`,
		"gradle/testLibs.versions.toml": "[libraries]\nmockito = \"cat:mockito:5.0\"\n",
		"app/build.gradle.kts": `
plugins { alias(libs.plugins.kotlin.jvm) }
dependencies {
    implementation(libs.plain)
    implementation(libs.module.ref)
    implementation(libs.module.inline)
    api(libs.group.name)
    implementation(libs.no.version)
    implementation(libs.androidx.core.ktx)
    implementation(libs.bad.ref)
    implementation(libs.ranged)
    implementation(platform(libs.bom))
    implementation(libs.multi)
    testImplementation(libs.bundles.pair)
    testImplementation(testLibs.mockito)
    implementation(libs.not.in.the.catalogue)
    implementation("cat:by-version-accessor:${libs.versions.junit.get()}")
    implementation("cat:by-version-accessor-sdk:${libs.versions.aws.sdk.get()}")
}
`}
	wantGradleTree(t, "catalogue", files, "app/build.gradle.kts", `
cat:bad-ref@ runtime
cat:bom@9.0 runtime platform
cat:by-version-accessor-sdk@2.55.2 runtime
cat:by-version-accessor@5.10.0 runtime
cat:core-ktx@1.2.3 runtime
cat:group-name@2.55.2 runtime
cat:mockito@5.0 dev
cat:module-inline@2.0 dev
cat:module-inline@2.0 runtime
cat:module-ref@5.10.0 runtime
cat:multi-line@5.10.0 runtime
cat:no-version@ runtime
cat:plain@1.0 dev
cat:plain@1.0 runtime
cat:ranged@ runtime`)
	// Without a catalogue an accessor declares nothing, and nothing fails.
	wantGradleTree(t, "no catalogue", map[string]string{"build.gradle": "dependencies { implementation libs.guava\n implementation 'real:one:1' }"}, "build.gradle", `real:one@1 runtime`)
}

// Constants in buildSrc (object Versions { const val x = "1" }) and
// coordinates held in them.
func TestGradleBuildSrcConstants(t *testing.T) {
	wantGradleTree(t, "buildSrc", map[string]string{
		"buildSrc/src/main/kotlin/Dependencies.kt": `
object Versions {
    const val kotlin = "1.9.0"
    object Android { const val core = "1.12.0" }
}
object Deps {
    const val stdlib = "bs:kotlin-stdlib:${Versions.kotlin}"
    const val core = "bs:core:" + Versions.Android.core
}
`,
		"app/build.gradle.kts": `
dependencies {
    implementation("bs:by-version:${Versions.kotlin}")
    implementation("bs:nested:${Versions.Android.core}")
    implementation(Deps.stdlib)
    implementation(Deps.core)
    implementation(Deps.missing)
}
`}, "app/build.gradle.kts", `
bs:by-version@1.9.0 runtime
bs:core@1.12.0 runtime
bs:kotlin-stdlib@1.9.0 runtime
bs:nested@1.12.0 runtime`)
}

// The tree's other files are bounded and stay inside the clone: an applied
// file that applies itself, and one reached through a symbolic link out of
// the clone, end quietly.
func TestGradleTreeIsBoundedAndStaysInTheClone(t *testing.T) {
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.gradle"), []byte("ext.leaked = '6.6.6'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := gradleClone(t, map[string]string{
		"loop.gradle":  "apply from: 'loop.gradle'\next.looped = '1'\n",
		"build.gradle": "apply from: 'loop.gradle'\napply from: 'link.gradle'\napply from: 'RELATIVE'\ndependencies { implementation \"b:looped:$looped\"\n implementation \"b:leaked:$leaked\" }\n",
	})
	// The same file by a relative path that leaves the clone.
	rel, err := filepath.Rel(root, filepath.Join(outside, "secret.gradle"))
	if err != nil {
		t.Fatal(err)
	}
	build := filepath.Join(root, "build.gradle")
	data, _ := os.ReadFile(build)
	if err := os.WriteFile(build, []byte(strings.Replace(string(data), "RELATIVE", filepath.ToSlash(rel), 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.gradle"), filepath.Join(root, "link.gradle")); err != nil {
		t.Fatal(err)
	}
	deps, _ := NewWalk(root, nil).Declared(filepath.Join(root, "build.gradle"), Options{})
	if got := gradleRows(deps); got != "b:leaked@ runtime\nb:looped@1 runtime" {
		t.Errorf("got:\n%s", got)
	}
}
