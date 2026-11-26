// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// gradleRows renders what a build file declares as sorted
// "name@version scope[ flags]" lines.
func gradleRows(deps []model.LibyearDep) string {
	var out []string
	for _, d := range deps {
		line := d.Name + "@" + d.Version + " " + d.Type
		if d.BOMImport {
			line += " platform"
		}
		if d.Manager != "maven" {
			line += " manager=" + d.Manager
		}
		out = append(out, line)
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

func wantGradle(t *testing.T, what, content, want string) {
	t.Helper()
	want = strings.TrimSpace(want)
	if got := gradleRows(parseBuildGradleVersions(content)); got != want {
		t.Errorf("%s:\n--- got\n%s\n--- want\n%s", what, got, want)
	}
}

// The notations a dependency is written in (summary/70, 4.6): the string,
// the map, Kotlin's named arguments, with or without parentheses, on one
// line or several, with a closure after it.
func TestGradleNotations(t *testing.T) {
	wantGradle(t, "groovy", `
plugins { id 'java' }
mainClassName = 'not:a:dependency'
dependencies {
    implementation 'a:string:1'
    implementation "a:double:2"
    implementation group: 'a', name: 'map', version: '3'
    implementation(group: 'a', name: 'map-paren', version: '4')
    implementation('a:closure:5') {
        exclude group: 'x', module: 'y'
        because 'z:not-a-dep:9'
    }
    implementation(
        'a:multiline:6'
    )
    implementation 'a:ext:7@aar'
    implementation 'a:classifier:8:sources'
    implementation 'a:versionless'
    implementation 'a:first:1', 'a:second:2'
    implementation name: 'only-a-name'
    implementation ':groupless:9'
}
`, `
:groupless@9 runtime
:only-a-name@ runtime
a:classifier@8 runtime
a:closure@5 runtime
a:double@2 runtime
a:ext@7 runtime
a:first@1 runtime
a:map-paren@4 runtime
a:map@3 runtime
a:multiline@6 runtime
a:second@2 runtime
a:string@1 runtime
a:versionless@ runtime`)

	wantGradle(t, "kotlin", `
dependencies {
    implementation("k:string:1")
    implementation(group = "k", name = "named", version = "2")
    "implementation"("k:quoted-config:3")
    add("implementation", "k:add:4")
    implementation("k:closure:5") { isTransitive = false }
}
`, `
k:add@4 runtime
k:closure@5 runtime
k:named@2 runtime
k:quoted-config@3 runtime
k:string@1 runtime`)
}

// Every configuration is read, not seven of them, and its scope follows
// from its name.
func TestGradleConfigurationsAndScopes(t *testing.T) {
	wantGradle(t, "configurations", `
buildscript {
    dependencies { classpath 'b:plugin:1' }
}
dependencies {
    api 'c:api:1'
    compileOnly 'c:compile-only:1'
    runtimeOnly 'c:runtime-only:1'
    debugImplementation 'c:variant:1'
    releaseApi 'c:variant-api:1'
    testImplementation 'c:test:1'
    testRuntimeOnly 'c:test-runtime:1'
    testCompileOnly 'c:test-compile:1'
    androidTestImplementation 'c:android-test:1'
    testFixturesApi 'c:fixtures:1'
    integrationTestImplementation 'c:integration:1'
    annotationProcessor 'c:processor:1'
    kapt 'c:kapt:1'
    ksp 'c:ksp:1'
    kaptTest 'c:kapt-test:1'
    compile 'c:legacy-compile:1'
    testCompile 'c:legacy-test:1'
    customConfiguration 'c:custom:1'
}
`, `
b:plugin@1 build
c:android-test@1 dev
c:api@1 runtime
c:compile-only@1 runtime
c:custom@1 runtime
c:fixtures@1 dev
c:integration@1 dev
c:kapt-test@1 dev
c:kapt@1 build
c:ksp@1 build
c:legacy-compile@1 runtime
c:legacy-test@1 dev
c:processor@1 build
c:runtime-only@1 runtime
c:test-compile@1 dev
c:test-runtime@1 dev
c:test@1 dev
c:variant-api@1 runtime
c:variant@1 runtime`)
}

// What is not an external dependency declares nothing: the repository's own
// projects, files on disk, Gradle's own API, and whatever is written outside
// a dependencies block.
func TestGradleNonDependencies(t *testing.T) {
	wantGradle(t, "non-dependencies", `
// implementation 'commented:out:1'
/* dependencies { implementation 'block:comment:1' } */
def notADependency = 'x:y:1'
task copy(type: Copy) { from 'a:b:c' }
dependencies {
    implementation project(':core:util')
    implementation(project(":other"))
    implementation files('libs/a.jar')
    implementation fileTree(dir: 'libs', include: ['*.jar'])
    implementation gradleApi()
    implementation localGroovy()
    implementation 'real:dep:1' // trailing comment 'not:this:2'
    constraints {
        implementation 'constrained:lib:9'
    }
    components { all { } }
    println 'not:a:dep'
}
`, `
real:dep@1 runtime`)
}

// A declaration under a condition is a declaration, and a dependencies
// block is one however it is reached.
func TestGradleConditionalAndQualifiedBlocks(t *testing.T) {
	wantGradle(t, "conditions", `
subprojects {
    if (hasTests) {
        dependencies {
            val testImplementation by configurations
            testImplementation("q:always:1")
            if (project.props.bool("junit4")) {
                testImplementation("q:conditional:2")
            } else {
                testImplementation("q:otherwise:3")
            }
            for (x in list) { implementation("q:looped:4") }
            if (System.getenv('UI') != 'false'
                || (project.hasProperty('UI')
                && project.getProperty('UI') != 'false')) {
                implementation 'q:multiline-condition:6'
            }
        }
    }
}
project.dependencies {
    implementation 'q:qualified:5'
}
`, `
q:always@1 dev
q:conditional@2 dev
q:looped@4 runtime
q:multiline-condition@6 runtime
q:otherwise@3 dev
q:qualified@5 runtime`)
}

// A platform (a BOM) is marked as one, in each notation.
func TestGradlePlatforms(t *testing.T) {
	wantGradle(t, "platforms", `
dependencies {
    implementation platform('p:bom:1')
    implementation(platform("p:bom-kts:2"))
    api enforcedPlatform('p:enforced:3')
    testImplementation platform(group: 'p', name: 'bom-map', version: '4')
    implementation 'p:managed-by-the-platform'
}
`, `
p:bom-kts@2 runtime platform
p:bom-map@4 dev platform
p:bom@1 runtime platform
p:enforced@3 runtime platform
p:managed-by-the-platform@ runtime`)
}

// A version held in a variable of the same file is read; one that cannot be
// read leaves the version unknown, with what was written kept, and never
// puts the expression in the version.
func TestGradleVariablesOfTheSameFile(t *testing.T) {
	groovy := `
ext.plain = '1.0'
ext {
    inBlock = '2.0'
    versions = [mapped: '3.0', other: "3.1"]
}
def local = '4.0'
ext['indexed'] = '5.0'
project.ext.set('viaSet', '6.0')
dependencies {
    implementation "v:plain:$plain"
    implementation "v:braces:${inBlock}"
    implementation "v:mapped:${versions.mapped}"
    implementation "v:mapped-bare:$versions.other"
    implementation "v:local:${local}"
    implementation "v:indexed:$indexed"
    implementation "v:via-set:${viaSet}"
    implementation "v:root:${rootProject.ext.plain}"
    implementation "v:project:${project.inBlock}"
    implementation group: 'v', name: 'map-var', version: local
    implementation 'v:concat:' + plain
    implementation "v:unknown:$nowhere"
    implementation "v:unknown-expr:${compute()}"
    implementation 'v:single-quoted:$plain'
    implementation "v:dynamic:1.+"
}
`
	wantGradle(t, "groovy variables", groovy, `
v:braces@2.0 runtime
v:concat@1.0 runtime
v:dynamic@ runtime
v:indexed@5.0 runtime
v:local@4.0 runtime
v:map-var@4.0 runtime
v:mapped-bare@3.1 runtime
v:mapped@3.0 runtime
v:plain@1.0 runtime
v:project@2.0 runtime
v:root@1.0 runtime
v:single-quoted@ runtime
v:unknown-expr@ runtime
v:unknown@ runtime
v:via-set@6.0 runtime`)
	for _, d := range parseBuildGradleVersions(groovy) {
		if d.Name == "v:unknown" && d.Requirement != "v:unknown:$nowhere" {
			t.Errorf("an unread variable: requirement %q; want what was written", d.Requirement)
		}
		if d.Name == "v:dynamic" && d.Requirement != "v:dynamic:1.+" {
			t.Errorf("a dynamic version: requirement %q; want what was written", d.Requirement)
		}
		if strings.ContainsAny(d.Version, "${}+[(") {
			t.Errorf("%s: version %q is an expression or a range, not a version", d.Name, d.Version)
		}
	}

	wantGradle(t, "kotlin variables", `
val plain = "1.0"
val typed: String = "1.5"
val delegated by extra("2.0")
extra["indexed"] = "3.0"
extra.set("viaSet", "4.0")
const val constant = "5.0"
dependencies {
    implementation("kv:plain:$plain")
    implementation("kv:typed:${typed}")
    implementation("kv:delegated:$delegated")
    implementation("kv:indexed:${extra["indexed"]}")
    implementation("kv:via-set:${property("viaSet")}")
    implementation("kv:constant:$constant")
    implementation(group = "kv", name = "named-var", version = plain)
}
`, `
kv:constant@5.0 runtime
kv:delegated@2.0 runtime
kv:indexed@3.0 runtime
kv:named-var@1.0 runtime
kv:plain@1.0 runtime
kv:typed@1.5 runtime
kv:via-set@4.0 runtime`)
}

// A project property set at the top of the file is read back; an assignment
// that configures some other block is not a variable; and a name that cannot
// be read in full names nothing (half a name is another package's name).
func TestGradlePropertiesAndUnreadNames(t *testing.T) {
	wantGradle(t, "properties", `
version = '2.5'
allprojects { group = 'own.group' }
android { ndkVersion = "25.1" }
dependencies {
    implementation "own:sibling:${project.version}"
    implementation "$group:same-group:1"
    implementation "x:ndk:$ndkVersion"
    implementation "half:artifact_${nowhere}:1"
    implementation "${nowhere}:artifact:1"
}
`, `
own.group:same-group@1 runtime
own:sibling@2.5 runtime
x:ndk@ runtime`)
}

// A variable assigned below the block that uses it is still read: a build
// file's ext block often follows its dependencies.
func TestGradleVariableAssignedAfterItsUse(t *testing.T) {
	wantGradle(t, "late assignment", `
dependencies { implementation "late:lib:$lateVersion" }
ext.lateVersion = '9.9'
`, `late:lib@9.9 runtime`)
}

// The dependency-name inventory reads the same declarations.
func TestGradleNamesComeFromTheSameReader(t *testing.T) {
	names := parseBuildGradle(`
dependencies {
    kapt 'n:kapt:1'
    implementation group: 'n', name: 'map', version: '2'
    implementation project(':own')
    implementation platform('n:bom:3')
}
`)
	sort.Strings(names)
	if got := strings.Join(names, ","); got != "n:bom,n:kapt,n:map" {
		t.Errorf("names = %q", got)
	}
}

// Text that is not a build file, or is cut off, declares what could be read
// and does not panic.
func TestGradleMalformedInput(t *testing.T) {
	for _, content := range []string{
		"dependencies {", "dependencies { implementation '", "dependencies { implementation(", "}}}}", "dependencies { implementation \"a:b:${",
		"dependencies { implementation group: }", "/* never closed", "'''", "dependencies { implementation 'a:b:1' ", "\x00\xff{{{",
		"dependencies { implementation 'a:b:1'\n implementation",
	} {
		_ = parseBuildGradleVersions(content)
		_ = parseBuildGradle(content)
	}
	if got := gradleRows(parseBuildGradleVersions("dependencies { implementation 'a:b:1' ")); got != "a:b@1 runtime" {
		t.Errorf("an unclosed block lost its declaration: %q", got)
	}
}

// parseBuildGradleVersions extracts the declarations of a build.gradle or
// build.gradle.kts, with the versions the file itself settles.
func parseBuildGradleVersions(content string) []model.LibyearDep {
	return readGradleBuild(content, nil).libyearDeps()
}
