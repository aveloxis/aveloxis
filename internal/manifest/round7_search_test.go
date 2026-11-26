// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// THE SEARCH for input a reader takes more than linear time to read (round
// 7). The scaling harness holds a list of shapes someone thought of, and
// three rounds running a reviewer thought of one more. This test does not
// hold a list of shapes: for every manifest kind that has a reader it takes
// the words that kind of file is written in, makes EVERY unit of one and of
// two of them and a seeded sample of longer ones, repeats each unit to a
// size, and reads the result through the roster as an analysis does. A unit
// whose reading is slow at the small size is read at four times the size; a
// reader that is linear takes about four times as long, one that is
// quadratic sixteen.
//
// What it claims, run whole (MANIFEST_SEARCH=pairs, two minutes; it was for
// 0.29.214): no reader is super-linear in a repeated unit of one or two of
// these words. MANIFEST_SEARCH=triples reads every unit of three as well
// (an hour for all files; MANIFEST_SEARCH_FILE names one). An ordinary run
// reads every unit of one word, a seeded sixteenth of the pairs and a seeded
// sample of longer units, so that a reader that turns quadratic in a common
// construct fails the suite and not only a review. What it does not claim:
// products across files (the scaling harness's tree shapes), and words the
// lists do not hold. A new construct a reader learns belongs in its list.
//
// MANIFEST_SEARCH_SEED changes the sample.

// searchFamilies: the files the search writes, by the language they are
// written in, with that language's words. One file for every kind of the
// roster that reads anything (TestTheSearchReadsEveryKindThatHasAReader).
// "a+b" writes the unit to a and reads b.
var searchFamilies = []struct {
	files []string
	words []string
}{
	{[]string{"build.gradle", "build.gradle.kts", "settings.gradle+build.gradle", "x.gradle+build.gradle", "gradle.properties+build.gradle",
		"buildSrc/src/main/kotlin/V.kt+build.gradle.kts"},
		[]string{"{", "}", "(", ")", "[", "]", ",", ";", ":", "=", "+", ".", "$", "${", `"`, "'", `"""`, "'a'", `"g:a:1"`, `"g:a:$a"`, "a", "x",
			"def", "val", "val x by extra", "by", "extra", "ext", "ext.a", "set(", "mapOf(", "listOf(", "implementation", "api", "platform(",
			"enforcedPlatform(", "dependencies", "plugins", "subprojects", "allprojects", "buildscript", "apply", "from:", "plugin:", "id",
			"version", "'x.gradle'", "libs.x", "project(", "group:", "name:", "//", "/*", "*/", "->", "if", "else", `const val A = "1"`, "a=1",
			"imports", "mavenBom", "dependencyManagement", "repositories", "maven", "url", "constraints", "object V", "to", "ext[", "]=",
			"add(", `"implementation"`, "testFixtures(", "exclude", "because", "catalog", "versionCatalogs", "include", "pluginManagement"}},
	{[]string{"pom.xml", "packages.config", "Directory.Packages.props", "x.csproj"},
		[]string{"<a>", "</a>", "<project>", "</project>", "<dependency>", "</dependency>", "<dependencies>", "</dependencies>", "<version>",
			"</version>", "<dependencyManagement>", "</dependencyManagement>", "<properties>", "</properties>", "<parent>", "</parent>",
			"<modules>", "<module>a</module>", "<groupId>g</groupId>", "<artifactId>a</artifactId>", "<version>1</version>",
			"<version>${a}</version>", "<scope>import</scope><type>pom</type>", "<a>${a}</a>", "<a>${b}</a><b>${a}</b>", "${a}", "${", "}",
			"<!--", "-->", "<![CDATA[", "]]>", `<PackageReference Include="a" Version="1"`, `<PackageVersion Include="a"`, "/>", "<", ">",
			"&", "&amp;", `"`, "'", "=", "a", "<plugin>", "</plugin>", "<profile>", "</profile>", "<relativePath/>", `<package id="a" version="1"/>`}},
	{[]string{"pyproject.toml", "Pipfile", "Cargo.toml", "setup.cfg", "gradle/libs.versions.toml+build.gradle", "conanfile.txt"},
		[]string{"[", "]", "{", "}", "=", ",", ".", `"`, "'", `"""`, "'''", "#", "a", `a = "1"`, `a.b = "1"`, "version", "[project]",
			"dependencies = [", `"a>=1",`, "[tool.poetry.dependencies]", "[tool.poetry.group.dev.dependencies]", "[dependencies]",
			"[dev-dependencies]", "[workspace.dependencies]", "[packages]", "[dev-packages]", "[versions]", "[libraries]", "[plugins]",
			"[bundles]", `x = { module = "g:x", version.ref = "k" }`, "workspace = true", "a = {", "a = [", "path =", "git =",
			"[options]", "install_requires =", "[options.extras_require]", "[requires]", "a/1.0", "optional-dependencies", "[build-system]",
			"requires = [", `\`}},
	{[]string{"Gemfile", "mix.exs", "build.sbt", "Package.swift"},
		[]string{"gem", "'a'", `"a"`, ";", ",", "do", "end", "group :x", "if", "then", "else", "case", "when", "{", "}", "(", ")", "[", "]", "|", "#",
			":", "=>", "source", "path:", "git:", "'~> 1.0'", "defp deps do", `{:a, "~> 1.0"}`, "{:a,", `"#{`, "libraryDependencies +=",
			"libraryDependencies ++= Seq(", "%%", "%", `"a" % "b" % "1"`, `.package(url: "https://github.com/a/b", from: "1.0.0")`,
			".package(", "url:", "from:", `"`, "'", `\`, "a", "//", "/*", "*/", "dependencies:", "val", "="}},
	{[]string{"pubspec.yaml", "package.yaml", "stack.yaml", ".github/workflows/x.yml", "action.yml", "x.cabal", "DESCRIPTION"},
		[]string{"a:", "a: 1", "- a", "-", "  ", ":", "dependencies:", "dev_dependencies:", "version:", "sdk: flutter", "path:", "git:", "ref: v1",
			"hosted:", "jobs:", "steps:", "uses:", "- uses: a/b@v1", "runs:", "using: composite", "extra-deps:", "- a-1.0", "build-depends:",
			"a >= 1.0,", ",", "Imports:", "Depends:", "Suggests:", "Remotes:", "a (>= 1.0),", "(>= 1.0)", "#", `"`, "'", "|", ">", "&a", "*a",
			"<<:", "{", "}", "[", "]", "!!str", "---", "library", "if flag(a)", "--"}},
	{[]string{"package.json", "composer.json", "vcpkg.json"},
		[]string{"{", "}", "[", "]", ":", ",", `"`, `"a"`, `"a":"1"`, `"dependencies":{`, `"devDependencies":{`, `"require":{`,
			`"require-dev":{`, `"repositories":[`, `"type":"path"`, `"url":"../*"`, `"name":"a"`, `"version>=":"1"`,
			`"overrides":[`, `\`, `a`, "1", "null", "true", `"workspaces":[`, `"peerDependencies":{`, `"optionalDependencies":{`}},
	{[]string{"requirements.txt", "requirements-dev.txt", "setup.py", "go.mod", "CMakeLists.txt"},
		[]string{"a==1.0", "a>=1,<2", "-r ", "-e ", "-c ", "git+https://x/y", "#egg=a", "#", ";", `\`, "[x]", "@", "--hash=sha256:a", "(", ")",
			"[", "]", ",", `"`, "'", `"""`, "'''", "install_requires=[", "extras_require={", "setup(", `"a>=1",`, "=", "{", "}", "require (",
			"require", "a v1.0.0", "replace", "=>", "exclude", "// indirect", "module a", "go 1.22", "toolchain", "find_package(", "a REQUIRED)",
			"FetchContent_Declare(", "GIT_TAG v1", "pkg_check_modules(", "${a}", "$"}},
}

// searchCommon are words every family's units are also made of.
var searchCommon = []string{"\n", " ", "\t", "\r", "a", "1.0"}

func searchFilesOf() (files []string) {
	for _, f := range searchFamilies {
		files = append(files, f.files...)
	}
	return files
}

func searchRead(root, file, content string) time.Duration {
	read := file
	if at := strings.Index(file, "+"); at >= 0 {
		side := filepath.Join(root, file[:at])
		os.MkdirAll(filepath.Dir(side), 0o755)
		os.WriteFile(side, []byte(content), 0o644)
		read, content = file[at+1:], "apply from: 'x.gradle'\ndependencies {\n implementation libs.x\n implementation \"g:a:$a\"\n}\n"
	}
	path := filepath.Join(root, read)
	os.MkdirAll(filepath.Dir(path), 0o755)
	os.WriteFile(path, []byte(content), 0o644)
	start := time.Now()
	NewWalk(root, nil).Read(path, Options{DevBuildDeps: true, GitHubActionsDeps: true})
	Names(path)
	return time.Since(start)
}

func TestTheSearchReadsEveryKindThatHasAReader(t *testing.T) {
	read := map[string]bool{}
	for _, f := range searchFilesOf() {
		for _, name := range strings.Split(f, "+") {
			_, kinds := kindsFor(filepath.Join("/r", name))
			for _, k := range kinds {
				read[k.Name] = true
			}
		}
	}
	for _, k := range roster {
		if (k.names != nil || k.declared != nil) && !read[k.Name] {
			t.Errorf("the roster's kind %q has a reader and the search writes no file it claims: add one to searchFamilies", k.Name)
		}
	}
}

const (
	searchSmall = 32 << 10
	// searchSlow: a unit is read again at four times the size when its
	// small reading takes this long. A linear reader takes about a
	// millisecond for 32 KB; this is eight of them. (Round 8: it was 40 ms,
	// and a quadratic reader that takes 10 ms for 32 KB, which is eleven
	// minutes for a file of MaxBuildFileBytes, was never looked at twice.)
	searchSlow = 8 * time.Millisecond
	// searchLimit: between linear (4) and quadratic (16).
	searchLimit = 8
	// A time floor cannot see a quadratic reader whose small reading is as
	// quick as a linear one's. So one unit in searchDeepEvery that is under
	// the floor is read at searchDeep times the size as well: a linear
	// reader takes searchDeep times as long, a quadratic one its square,
	// and the bound between them is searchDeepLimit times the small
	// reading, taken as no less than searchDeepFloor (a reading quicker
	// than that is mostly the cost of opening the file). What passes is a
	// reader that is under MaxBuildFileBytes/searchSmall squared times
	// searchDeepFloor/4 at the largest file read: some four seconds.
	searchDeepEvery = 16
	searchDeep      = 16
	searchDeepLimit = 64
	searchDeepFloor = 250 * time.Microsecond
)

func TestNoRepeatedUnitIsReadInSuperLinearTime(t *testing.T) {
	if raceEnabled {
		t.Log("the detector's slowdown is not uniform: times say nothing under it (the plain run judges)")
		return
	}
	sampled, seed := 150, int64(7)
	mode, only := os.Getenv("MANIFEST_SEARCH"), os.Getenv("MANIFEST_SEARCH_FILE")
	triples, pairs := mode == "triples", mode == "triples" || mode == "pairs"
	if v, err := strconv.ParseInt(os.Getenv("MANIFEST_SEARCH_SEED"), 10, 64); err == nil {
		seed = v
	}
	rng := rand.New(rand.NewSource(seed))
	tried, slow, deep := 0, 0, 0
	for _, family := range searchFamilies {
		words := append(append([]string{}, family.words...), searchCommon...)
		var units []string
		for _, a := range words {
			units = append(units, a)
			for _, b := range words {
				if !pairs && rng.Intn(16) != 0 {
					continue
				}
				units = append(units, a+b, a+" "+b)
				if triples {
					for _, c := range words {
						units = append(units, a+b+c, a+" "+b+" "+c)
					}
				}
			}
		}
		for _, file := range family.files {
			if only != "" && file != only {
				continue
			}
			root := t.TempDir()
			mine := units[:len(units):len(units)]
			for k := 0; k < sampled; k++ {
				var b strings.Builder
				for n := 3 + rng.Intn(3); n > 0; n-- {
					b.WriteString(words[rng.Intn(len(words))])
					if rng.Intn(2) == 0 {
						b.WriteString(" ")
					}
				}
				mine = append(mine, b.String())
			}
			for k, unit := range mine {
				if strings.TrimSpace(unit) == "" && len(unit) > 1 {
					continue
				}
				// Every unit bare, and some after an opening word: what a
				// reader does with the unit depends on the block it is in.
				prefix := ""
				if k%3 == 1 {
					prefix = words[rng.Intn(len(words))] + "\n"
				}
				at := func(n int) time.Duration { return searchRead(root, file, prefix+strings.Repeat(unit, n/len(unit))) }
				tried++
				small := at(searchSmall)
				if small < searchSlow {
					if k%searchDeepEvery != 0 {
						continue
					}
					deep++
					bound := func(small time.Duration) time.Duration { return searchDeepLimit * max(small, searchDeepFloor) }
					if big := at(searchDeep * searchSmall); big > bound(small) {
						// Once more, as below.
						small, big = at(searchSmall), at(searchDeep*searchSmall)
						if big > bound(small) {
							t.Errorf("%s: %q after %q, repeated: %d bytes in %v, %d bytes in %v (x%.1f); a linear reader takes about %d times",
								file, unit, prefix, searchSmall, small, searchDeep*searchSmall, big, float64(big)/float64(small), searchDeep)
						}
					}
					continue
				}
				slow++
				big := at(4 * searchSmall)
				if float64(big) > searchLimit*float64(small) {
					// Once more: a ratio of times is what a busy machine can move.
					small, big = at(searchSmall), at(4*searchSmall)
					if small >= searchSlow/2 && float64(big) > searchLimit*float64(small) {
						t.Errorf("%s: %q after %q, repeated: %d bytes in %v, %d bytes in %v (x%.1f); a linear reader takes about four times",
							file, unit, prefix, searchSmall, small, 4*searchSmall, big, float64(big)/float64(small))
					}
				}
			}
		}
	}
	t.Logf("%d units read (seed %d), %d slow enough at %d bytes to be read at four times the size, %d of the others read at %d times", tried, seed, slow, searchSmall, deep, searchDeep)
	if only == "" && (tried < 15000 || deep < tried/(2*searchDeepEvery)) {
		t.Errorf("%d units tried and %d read at %d times the size: the search has lost its units", tried, deep, searchDeep)
	}
}
