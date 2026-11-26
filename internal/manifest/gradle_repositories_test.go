// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"path/filepath"
	"strings"
	"testing"
)

// A Gradle repositories block is a declaration like a pom's (summary/70,
// 4.6): every entry is recorded, with why it is never fetched from when it
// is not, and the shorthands stand for their known addresses.

func TestGradleRepositoriesAreRecordedAndClassified(t *testing.T) {
	root := gradleClone(t, map[string]string{
		"settings.gradle": `
pluginManagement {
    repositories {
        gradlePluginPortal()
        maven { url 'https://plugins.example/m2' }
    }
}
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven { url = uri("https://settings.example/repo/") }
    }
}
`,
		"build.gradle": `
ext.known = 'https://known.example'
buildscript {
    repositories { maven { url 'https://buildscript.example/m2' } }
}
allprojects {
    repositories { maven { url "https://shared.example/m2" } }
}
repositories {
    mavenLocal()
    jcenter()
    flatDir { dirs 'libs' }
    ivy { url 'https://ivy.example/repo' }
    maven { url 'http://plain.example/m2' }
    maven { url "https://user:secret@creds.example/m2" }
    maven { url "https://var.example/${nowhere}" }
    maven { url "$known/m2" }
    maven {
        name = 'named'
        url = 'https://named.example/m2'
        credentials { username = 'u'; password = 'p' }
    }
    exclusiveContent {
        forRepository { maven { url "https://exclusive.example/m2" } }
        filter { includeGroup "x" }
    }
    maven { url 'https://named.example/m2' }
}
`,
		"app/build.gradle.kts": `
repositories {
    maven("https://kts.example/m2")
    maven(url = "https://kts-named.example/m2")
    maven { url = uri("https://kts-block.example/m2") }
    maven { setUrl("https://kts-set.example/m2") }
    maven("http://plain-app.example/m2")
    mavenLocal()
}
dependencies {
    implementation(platform("p:bom:1"))
    implementation("p:lib")
}
`,
	})
	w := NewWalk(root, nil)
	src := &declaredRecorder{}
	w.SetPomSource(src)
	for _, f := range []string{"build.gradle", "app/build.gradle.kts"} {
		if _, err := w.Declared(filepath.Join(root, filepath.FromSlash(f)), Options{}); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	for _, d := range w.DeclaredRepositories() {
		line := d.ManifestPath + " " + d.Kind + " " + d.URL
		if d.NotFetched != "" {
			line += " [" + d.NotFetched + "]"
		}
		got = append(got, line)
	}
	want := strings.TrimSpace(`
app/build.gradle.kts dependencies http://plain-app.example/m2 [not_https]
app/build.gradle.kts dependencies https://kts-block.example/m2
app/build.gradle.kts dependencies https://kts-named.example/m2
app/build.gradle.kts dependencies https://kts-set.example/m2
app/build.gradle.kts dependencies https://kts.example/m2
app/build.gradle.kts dependencies mavenLocal [local]
build.gradle dependencies flatDir [flat_dir]
build.gradle dependencies http://plain.example/m2 [not_https]
build.gradle dependencies https://creds.example/m2 [credentials_in_url]
build.gradle dependencies https://exclusive.example/m2
build.gradle dependencies https://ivy.example/repo [ivy]
build.gradle dependencies https://jcenter.bintray.com [shut_down]
build.gradle dependencies https://known.example/m2
build.gradle dependencies https://named.example/m2
build.gradle dependencies https://shared.example/m2
build.gradle dependencies https://var.example/${nowhere} [unreadable]
build.gradle dependencies mavenLocal [local]
build.gradle plugins https://buildscript.example/m2 [plugins_only]
settings.gradle dependencies https://dl.google.com/dl/android/maven2
settings.gradle dependencies https://repo1.maven.org/maven2
settings.gradle dependencies https://settings.example/repo
settings.gradle plugins https://plugins.example/m2 [plugins_only]
settings.gradle plugins https://plugins.gradle.org/m2 [plugins_only]`)
	if strings.Join(got, "\n") != want {
		t.Errorf("declared repositories:\n--- got\n%s\n--- want\n%s", strings.Join(got, "\n"), want)
	}
	for _, d := range w.DeclaredRepositories() {
		if strings.Contains(d.URL, "secret") || strings.Contains(d.URL, "user:") {
			t.Errorf("a credential was kept: %s", d.URL)
		}
	}

	// The platform of the module is asked for with the repositories that
	// serve the module's dependencies and may be fetched from: its own
	// first, then the ones a build file above shares, then the settings'.
	// Never a plugin repository, one that may not be fetched from, or the
	// root project's own.
	asked, was := src.asked["p:bom:1"]
	if !was || len(src.asked) != 1 {
		t.Fatalf("asked for %v; want the platform, once", src.asked)
	}
	wantAsked := "https://kts.example/m2,https://kts-named.example/m2,https://kts-block.example/m2,https://kts-set.example/m2," +
		"https://shared.example/m2,https://dl.google.com/dl/android/maven2,https://repo1.maven.org/maven2,https://settings.example/repo"
	if got := strings.Join(asked, ","); got != wantAsked {
		t.Errorf("the platform was asked for with\n  %s\nwant\n  %s", got, wantAsked)
	}
}

// A repositories entry that is not one of the known kinds is recorded as
// unreadable, never dropped and never fetched.
func TestGradleUnknownRepositoryKinds(t *testing.T) {
	root := gradleClone(t, map[string]string{"build.gradle": `
repositories {
    maven { url someFunction() }
    maven { }
    customRepo()
    mavenCentral { content { includeGroup "x" } }
}
`})
	w := NewWalk(root, nil)
	if _, err := w.Declared(filepath.Join(root, "build.gradle"), Options{}); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, d := range w.DeclaredRepositories() {
		got = append(got, d.URL+" ["+d.NotFetched+"]")
	}
	if strings.Join(got, "\n") != "customRepo [unreadable]\nhttps://repo1.maven.org/maven2 []\nmaven [unreadable]" {
		t.Errorf("got:\n%s", strings.Join(got, "\n"))
	}
}
