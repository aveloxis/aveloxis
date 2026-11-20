// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// THE scaling harness for the build files a collected repository controls
// (pom.xml, build.gradle(.kts), settings.gradle, applied scripts, version
// catalogues). Three review rounds each found paths whose cost grows with
// the SQUARE of what is read, or without bound: a worker reads such a build
// for hours, or holds hundreds of megabytes, and nothing above the readers
// ends an analysis. They were closed one at a time until review round 2 of
// the vulnerability program's first part; from 0.29.209 the rule is a
// class, widened in 0.29.210 from one file to the walk:
//
//	A WALK'S TOTAL WORK AND WHAT IT RETAINS ARE LINEAR IN THE BYTES IT
//	READS: every loop is linear, or is counted by a work counter that is.
//
// This harness is the pin of that class. Each shape is generated at a size
// N and at 4N and read through the roster, exactly as an analysis reads it.
// A linear reader costs four times as much at 4N; a quadratic one sixteen.
// The test fails past scalingLimit. A new hostile shape is one entry here.
//
// HOW TO THINK OF THE NEXT ONE (round 4). Every multi-file shape found so
// far is the same sentence with other nouns: "for each A, something is done
// or kept for each B, and A and B are in different files". So, for any
// structure that outlives one file's reading (a map on the tree, a memo, a
// chain of scopes, a list handed to the projects below), ask what it is
// keyed or counted by. A key that is a pair (this file, something another
// file declares) is a product, whatever each element weighs; so is a list
// that every file extends by what a shared file names. The shape to add is
// then: k files of the one kind, k declarations of the other in one shared
// file, every element as heavy as its syntax lets it be (a long value, a
// reference that interpolates to one), and the file READ chosen so that the
// structure is really kept (a directory's scope is kept for the project
// below it, not for the directory). Judge it on what the walk retains: a
// product that is allocated and dropped shows in the allocation column, one
// that is kept shows only there.
//
// The shapes are of two kinds. A single-file shape is one hostile file. A
// multi-file shape (round 3) is a TREE whose files are each ordinary and
// whose cost is a product across them: modules times what their parent
// manages, project directories times the properties above them, projects
// times a script they all apply. Its N scales both sides of the product,
// and every file of it is read on one walk.
//
// What is measured, each as the ratio of 4N to N:
//   - bytes allocated (runtime.MemStats.TotalAlloc, the least of three
//     runs): exact, and independent of the machine's load;
//   - bytes RETAINED by the walk when it has read everything (HeapAlloc
//     after two collections, the walk still referenced): the copies per
//     file that round 3 found are allocated once and kept, and it is the
//     keeping that takes a worker's memory;
//   - wall time, the best of three runs, for the paths that scan again and
//     again without allocating.
//
// Time is the one measure a machine's load can move, so it is judged with
// care: only when the larger read took longer than scalingTimeFloor and the
// smaller a quarter of that (below that a ratio is noise); a shape that is
// too fast to judge at 4N is read again at four and sixteen times the size
// (scalingEscalations), where a quadratic path is no longer fast; a ratio
// past the limit is measured a second time before it fails; and not at all
// under the race detector, whose slowdown is not uniform. Where a path
// cannot be shown by time at any size worth running (a scan of a few
// milliseconds), the test counts its steps instead (pomSpanSteps).
//
// scalingLimit is 8: the geometric mean of what a linear reader shows (4)
// and what a quadratic one shows (16), so each is as far from the line as
// the other. Measured on the plain shapes (a build with nothing hostile in
// it) the allocation ratios are 3.6 to 5.1, and the retained ratios of the
// multi-file shapes 3.2 to 4.4. The retained measure has a floor as well
// (scalingRetainedFloor): a walk that keeps a megabyte at 4N is not judged,
// since at that size the ratio is the allocator's rounding.
const (
	scalingLimit         = 8.0
	scalingTimeFloor     = 40 * time.Millisecond
	scalingRetainedFloor = 4 << 20
	scalingN             = 48 << 10 // bytes of the small read, about
	scalingEscalations   = 2        // 4N -> 16N -> 64N for the time judgement
)

type scalingShape struct {
	name string
	// files generates the tree at about n bytes; read is the file to read.
	files func(n int) map[string]string
	read  string
	// reads, for a multi-file shape, lists the files to read at size n, in
	// order, all on one walk; read is then unused.
	reads func(n int) []string
	// gradleWalkFactor, when set, is the Gradle walk's allowance for this
	// shape, in place of the constant of that name: a shape whose measure
	// is what happens PAST the allowance reaches it at a size a test can
	// run.
	gradleWalkFactor int
}

// many is a multi-file shape: k files named by name(i), each read.
func many(name string, k func(n int) int, file func(i int) string, files func(n, k int) map[string]string) scalingShape {
	return scalingShape{name: name,
		files: func(n int) map[string]string { return files(n, k(n)) },
		reads: func(n int) []string {
			out := make([]string, k(n))
			for i := range out {
				out[i] = file(i)
			}
			return out
		}}
}

func one(name, read string, gen func(n int) string) scalingShape {
	return scalingShape{name: name, read: read, files: func(n int) map[string]string { return map[string]string{read: gen(n)} }}
}

func pomOf(inner string) string {
	return `<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version>` + inner + `</project>`
}

func scalingShapes() []scalingShape {
	rep := strings.Repeat
	dep := func(i int, version string) string {
		return fmt.Sprintf("<dependency><groupId>x</groupId><artifactId>d%d</artifactId>%s</dependency>", i, version)
	}
	return []scalingShape{
		// --- the lines: nothing hostile ---
		one("plain pom", "pom.xml", func(n int) string {
			var b strings.Builder
			for i := 0; b.Len() < n; i++ {
				b.WriteString(dep(i, "<version>1.0</version>"))
			}
			return pomOf("<dependencies>" + b.String() + "</dependencies>")
		}),
		one("plain gradle", "build.gradle", func(n int) string {
			var b strings.Builder
			b.WriteString("ext.v = '1.0'\ndependencies {\n")
			for i := 0; b.Len() < n; i++ {
				fmt.Fprintf(&b, "  implementation \"g:a%d:$v\"\n", i)
			}
			return b.String() + "}\n"
		}),
		// --- pom.xml ---
		// Round 2, M1: the file's own reader substituted one reference at a
		// time with no bound on what came out.
		one("pom: a version that is one property many times over", "pom.xml", func(n int) string {
			k := n / 12
			var fill strings.Builder
			for i := 0; i < k/8; i++ {
				fmt.Fprintf(&fill, "<f%d>x</f%d>", i, i)
			}
			return pomOf("<properties><a>" + rep("y", 64) + "</a>" + fill.String() + "</properties><dependencies>" +
				dep(0, "<version>"+rep("${a}", k/2)+"</version>") + "</dependencies>")
		}),
		// Round 2, M2: one pass of the tree's interpolation was built whole
		// before the bound was looked at.
		one("pom: a managed group that is one long property many times over", "pom.xml", func(n int) string {
			return pomOf("<properties><a>" + rep("y", n/2) + "</a></properties><dependencyManagement><dependencies><dependency><groupId>" +
				rep("${a}", n/8) + "</groupId><artifactId>m</artifactId><version>1</version></dependency></dependencies></dependencyManagement><dependencies>" +
				dep(0, "") + "</dependencies>")
		}),
		// Round 1: properties that multiply, five levels deep.
		one("pom: properties that multiply", "pom.xml", func(n int) string {
			var props strings.Builder
			for i := 0; i < 5; i++ {
				fmt.Fprintf(&props, "<p%d>%s</p%d>", i, rep(fmt.Sprintf("${p%d}", i+1), 20), i)
			}
			var deps strings.Builder
			for i := 0; deps.Len() < n; i++ {
				deps.WriteString(dep(i, "<version>${p0}</version>"))
			}
			return pomOf("<properties>" + props.String() + "<p5>x</p5></properties><dependencies>" + deps.String() + "</dependencies>")
		}),
		// Round 2, M13: every version-less dependency read every managed
		// entry, interpolating each again.
		one("pom: version-less dependencies against as many managed entries", "pom.xml", func(n int) string {
			var managed, deps strings.Builder
			for i := 0; managed.Len() < n/2; i++ {
				fmt.Fprintf(&managed, "<dependency><groupId>${p}</groupId><artifactId>m%d</artifactId><version>1</version></dependency>", i)
			}
			for i := 0; deps.Len() < n/2; i++ {
				deps.WriteString(dep(i, ""))
			}
			return pomOf("<properties><p>q</p></properties><dependencyManagement><dependencies>" + managed.String() +
				"</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")
		}),
		// The same against BOM imports that are nowhere: each lookup walked
		// every import.
		one("pom: version-less dependencies against as many BOM imports", "pom.xml", func(n int) string {
			var managed, deps strings.Builder
			for i := 0; managed.Len() < n/2; i++ {
				fmt.Fprintf(&managed, "<dependency><groupId>b</groupId><artifactId>bom%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
			}
			for i := 0; deps.Len() < n/2; i++ {
				deps.WriteString(dep(i, ""))
			}
			return pomOf("<dependencyManagement><dependencies>" + managed.String() +
				"</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")
		}),
		// Found while closing the class: each dependency block was compared
		// with every plugin (and every dependencyManagement) span.
		one("pom: many plugins and many dependencies", "pom.xml", func(n int) string {
			var plugins, deps strings.Builder
			for i := 0; plugins.Len() < n/2; i++ {
				fmt.Fprintf(&plugins, "<plugin><artifactId>p%d</artifactId></plugin>", i)
			}
			for i := 0; deps.Len() < n/2; i++ {
				deps.WriteString(dep(i, "<version>1</version>"))
			}
			return pomOf("<build><plugins>" + plugins.String() + "</plugins></build><dependencies>" + deps.String() + "</dependencies>")
		}),
		// A BOM of the clone used to be given a new context, and its
		// entries read again, for every dependency looked up in it.
		{name: "pom: version-less dependencies against one large BOM of the clone", read: "pom.xml", files: func(n int) map[string]string {
			var managed, deps strings.Builder
			for i := 0; managed.Len() < n/2; i++ {
				fmt.Fprintf(&managed, "<dependency><groupId>${p}</groupId><artifactId>m%d</artifactId><version>1</version></dependency>", i)
			}
			for i := 0; deps.Len() < n/2; i++ {
				deps.WriteString(dep(i, ""))
			}
			return map[string]string{
				"bom/pom.xml": `<project><groupId>b</groupId><artifactId>bom</artifactId><version>1</version><properties><p>q</p></properties><dependencyManagement><dependencies>` + managed.String() + `</dependencies></dependencyManagement></project>`,
				"pom.xml":     pomOf(`<dependencyManagement><dependencies><dependency><groupId>b</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement><dependencies>` + deps.String() + "</dependencies>"),
			}
		}},
		// --- build.gradle ---
		// Every dependency asks every platform: counted by the pom tree.
		{name: "gradle: many dependencies against many platforms of the clone", read: "build.gradle", files: func(n int) map[string]string {
			files := map[string]string{}
			var b strings.Builder
			b.WriteString("dependencies {\n")
			for i := 0; i < n/200; i++ {
				files[fmt.Sprintf("boms/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>b</groupId><artifactId>bom%d</artifactId><version>1</version></project>", i)
				fmt.Fprintf(&b, " implementation platform('b:bom%d:1')\n", i)
			}
			for i := 0; i < n/50; i++ {
				fmt.Fprintf(&b, " implementation 'x:d%d'\n", i)
			}
			files["build.gradle"] = b.String() + "}\n"
			return files
		}},
		// Round 2, M3.
		one("gradle: a coordinate inside many parentheses", "build.gradle", func(n int) string {
			return "dependencies {\n implementation " + rep("(", n/2) + "'g:a:1'" + rep(")", n/2) + "\n}\n"
		}),
		one("gradle: platform inside platform", "build.gradle", func(n int) string {
			k := n / 10
			return "dependencies {\n implementation " + rep("platform(", k) + "'g:a:1'" + rep(")", k) + "\n}\n"
		}),
		// Round 2, M4.
		one("gradle: statements inside many named types", "build.gradle", func(n int) string {
			var b strings.Builder
			for i := 0; i < 50; i++ {
				fmt.Fprintf(&b, "object %s%d {\n", rep("N", n/200), i)
			}
			return b.String() + rep("val a = '1'\n", n/24)
		}),
		// Round 2, M6.
		one("gradle: one long concatenation", "build.gradle", func(n int) string {
			return "ext.x = " + rep("'a'+", n/4) + "'a'\n"
		}),
		one("gradle: a coordinate that is one long concatenation", "build.gradle", func(n int) string {
			return "dependencies {\n implementation " + rep("'a'+", n/4) + "'g:a:1'\n}\n"
		}),
		one("gradle: many repositories", "build.gradle", func(n int) string {
			var b strings.Builder
			b.WriteString("repositories {\n")
			for i := 0; b.Len() < n; i++ {
				fmt.Fprintf(&b, " maven('https://h.example/%d')\n", i)
			}
			return b.String() + "}\n"
		}),
		// A variable that is itself twice over, line after line: its value
		// doubles (found while closing the class: `value` had no bound).
		one("gradle: a variable that doubles at every line", "build.gradle", func(n int) string {
			var b strings.Builder
			b.WriteString("def x0 = 'aaaaaaaaaaaaaaaa'\n")
			for i := 1; b.Len() < n; i++ {
				fmt.Fprintf(&b, "def x%d = x%d + x%d\n", i, i-1, i-1)
			}
			return b.String()
		}),
		// Round 1's shapes.
		one("gradle: an opening brace and an unclosed call, repeated", "build.gradle", func(n int) string { return rep("{y(", n/3) }),
		one("gradle: lines that each end in a comma", "build.gradle", func(n int) string { return rep("a,\n", n/3) }),
		one("gradle: calls nested inside closing braces", "build.gradle", func(n int) string { return rep("} a (", n/6) + rep(")", n/6) }),
		one("gradle: blocks nested without end", "build.gradle", func(n int) string { return rep("x {\n", n/4) }),
		one("gradle: conditions nested inside a dependencies block", "build.gradle", func(n int) string {
			return "dependencies {\n" + rep("if ( {\n", n/7)
		}),
		// --- a version catalogue (round 2, M5) ---
		{name: "catalogue: an array that never closes", read: "build.gradle", files: func(n int) map[string]string {
			return map[string]string{"build.gradle": "dependencies { implementation libs.x }\n",
				"gradle/libs.versions.toml": "[versions]\na = [\n" + rep("1,\n", n/3)}
		}},
		{name: "catalogue: many libraries", read: "build.gradle", files: func(n int) map[string]string {
			var b strings.Builder
			b.WriteString("[libraries]\n")
			for i := 0; b.Len() < n; i++ {
				fmt.Fprintf(&b, "lib-%d = { module = \"g:a%d\", version = \"1.0\" }\n", i, i)
			}
			return map[string]string{"build.gradle": "dependencies { implementation libs.lib.1 }\n", "gradle/libs.versions.toml": b.String()}
		}},
		// --- the files around a build ---
		{name: "settings: a hostile settings file", read: "app/build.gradle", files: func(n int) map[string]string {
			return map[string]string{"settings.gradle": "rootProject.name = 'x'\n" + rep("(", n/2) + rep(")", n/2) + "\n" + rep("def y = "+rep("'a'+", 200)+"'a'\n", n/1700+1),
				"app/build.gradle": "dependencies { implementation 'g:a:1' }\n"}
		}},
		{name: "gradle.properties: many lines", read: "build.gradle", files: func(n int) map[string]string {
			var b strings.Builder
			for i := 0; b.Len() < n; i++ {
				fmt.Fprintf(&b, "p%d=1\n", i)
			}
			return map[string]string{"gradle.properties": b.String(), "build.gradle": "dependencies { implementation \"g:a:$p1\" }\n"}
		}},
		// --- trees: a product across files (round 3) ---
		// R3-M1: every module kept its own copy of everything its parent
		// manages (2,000 modules under 2,000 entries, 763 KB: 498 MB).
		many("tree: modules under a parent with as many managed entries", func(n int) int { return n / 300 },
			func(i int) string { return fmt.Sprintf("m%d/pom.xml", i) },
			func(n, k int) map[string]string {
				var managed strings.Builder
				for i := 0; i < k; i++ {
					fmt.Fprintf(&managed, "<dependency><groupId>org.x</groupId><artifactId>a%d</artifactId><version>${v}</version></dependency>", i)
				}
				files := map[string]string{"pom.xml": `<project><groupId>g</groupId><artifactId>root</artifactId><version>1</version><properties><v>1.0</v></properties><dependencyManagement><dependencies>` + managed.String() + `</dependencies></dependencyManagement></project>`}
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("m%d/pom.xml", i)] = fmt.Sprintf(`<project><parent><groupId>g</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m%d</artifactId><dependencies><dependency><groupId>org.x</groupId><artifactId>a7</artifactId></dependency></dependencies></project>`, i)
				}
				return files
			}),
		// The same with the entries in a BOM every module's parent imports.
		many("tree: modules that all import one large BOM", func(n int) int { return n / 300 },
			func(i int) string { return fmt.Sprintf("m%d/pom.xml", i) },
			func(n, k int) map[string]string {
				var managed strings.Builder
				for i := 0; i < k; i++ {
					fmt.Fprintf(&managed, "<dependency><groupId>org.x</groupId><artifactId>a%d</artifactId><version>1.%d</version></dependency>", i, i)
				}
				files := map[string]string{
					"bom/pom.xml": `<project><groupId>g</groupId><artifactId>bom</artifactId><version>1</version><dependencyManagement><dependencies>` + managed.String() + `</dependencies></dependencyManagement></project>`,
					"pom.xml":     `<project><groupId>g</groupId><artifactId>root</artifactId><version>1</version><dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement></project>`,
				}
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("m%d/pom.xml", i)] = fmt.Sprintf(`<project><parent><groupId>g</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m%d</artifactId><dependencies><dependency><groupId>org.x</groupId><artifactId>a%d</artifactId></dependency></dependencies></project>`, i, i)
				}
				return files
			}),
		// Modules whose parent's entries each hold a reference, so each
		// must be compared by interpolating: counted, and it ends.
		// (More modules than the two shapes above: the comparing is cheap,
		// and must outweigh the reading for the ratio to show it.)
		many("tree: modules under a parent whose managed entries hold references", func(n int) int { return n / 60 },
			func(i int) string { return fmt.Sprintf("m%d/pom.xml", i) },
			func(n, k int) map[string]string {
				var managed strings.Builder
				for i := 0; i < k; i++ {
					fmt.Fprintf(&managed, "<dependency><groupId>${g}</groupId><artifactId>a%d</artifactId><version>1</version></dependency>", i)
				}
				files := map[string]string{"pom.xml": `<project><groupId>g</groupId><artifactId>root</artifactId><version>1</version><properties><g>org.x</g></properties><dependencyManagement><dependencies>` + managed.String() + `</dependencies></dependencyManagement></project>`}
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("m%d/pom.xml", i)] = fmt.Sprintf(`<project><parent><groupId>g</groupId><artifactId>root</artifactId><version>1</version></parent><artifactId>m%d</artifactId><dependencies><dependency><groupId>org.x</groupId><artifactId>a%d</artifactId></dependency></dependencies></project>`, i, k-1)
				}
				return files
			}),
		// R3-M2: every project directory kept its own copy of every
		// variable above it (2,000 directories under 2,000 properties,
		// 105 KB: 313 MB).
		many("tree: project directories under as many gradle.properties lines", func(n int) int { return n / 80 },
			func(i int) string { return fmt.Sprintf("p%d/build.gradle", i) },
			func(n, k int) map[string]string {
				var props strings.Builder
				for i := 0; i < k; i++ {
					fmt.Fprintf(&props, "v%d=1.%d\n", i, i)
				}
				files := map[string]string{"gradle.properties": props.String()}
				for i := 0; i < k; i++ {
					// A variable of its own in every directory: a reader
					// that copies what is above when a directory adds
					// to it is the product again.
					files[fmt.Sprintf("p%d/gradle.properties", i)] = fmt.Sprintf("mine=%d\n", i)
					files[fmt.Sprintf("p%d/build.gradle", i)] = "dependencies { implementation \"g:a:$v1\" }\n"
				}
				return files
			}),
		// The same for a catalogue's accessors, and for the platforms and
		// repositories a root build shares with every project.
		many("tree: project directories under a catalogue of as many libraries", func(n int) int { return n / 100 },
			func(i int) string { return fmt.Sprintf("p%d/build.gradle", i) },
			func(n, k int) map[string]string {
				var cat strings.Builder
				cat.WriteString("[libraries]\n")
				for i := 0; i < k; i++ {
					fmt.Fprintf(&cat, "lib-%d = { module = \"g:a%d\", version = \"1.0\" }\n", i, i)
				}
				files := map[string]string{"gradle/libs.versions.toml": cat.String()}
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("p%d/build.gradle", i)] = "dependencies { implementation libs.lib.1 }\n"
				}
				return files
			}),
		many("tree: project directories under a root that shares as many repositories and platforms", func(n int) int { return n / 160 },
			func(i int) string { return fmt.Sprintf("p%d/build.gradle", i) },
			func(n, k int) map[string]string {
				var root strings.Builder
				root.WriteString("subprojects {\n repositories {\n")
				for i := 0; i < k; i++ {
					fmt.Fprintf(&root, "  maven { url 'https://h%d.example/m2' }\n", i)
				}
				root.WriteString(" }\n dependencies {\n")
				for i := 0; i < k/4; i++ {
					fmt.Fprintf(&root, "  implementation platform('b:bom%d:1')\n", i)
				}
				root.WriteString(" }\n}\n")
				files := map[string]string{"build.gradle": root.String()}
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("p%d/build.gradle", i)] = "dependencies { implementation 'g:a:1' }\n"
				}
				return files
			}),
		// R3-M3: a script applied by every project was read again for each.
		many("tree: projects that all apply one script of as many lines", func(n int) int { return n / 90 },
			func(i int) string { return fmt.Sprintf("p%d/build.gradle", i) },
			func(n, k int) map[string]string {
				var script strings.Builder
				for i := 0; i < k; i++ {
					fmt.Fprintf(&script, "ext.v%d = '1.%d'\n", i, i)
				}
				files := map[string]string{"gradle/versions.gradle": script.String()}
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("p%d/build.gradle", i)] = "apply from: \"$rootDir/gradle/versions.gradle\"\ndependencies { implementation \"g:a:$v1\" }\n"
				}
				return files
			}),
		// The same when every project's scope answers differently for a
		// name the script looks up, so no reading can stand for another:
		// each is counted against the walk, which ends it.
		// (Sized so that both N and 4N are past the walk's allowance: the
		// measure is that the cost then grows with the script, not with
		// the projects times the script.)
		func() scalingShape {
			s := many("tree: projects that apply one script, each with its own value for what it reads", func(n int) int { return n / 40 },
				func(i int) string { return fmt.Sprintf("p%d/build.gradle", i) },
				func(n, k int) map[string]string {
					var script strings.Builder
					script.WriteString("ext.name = \"lib-$POM_ARTIFACT\"\n")
					for i := 0; i < k; i++ {
						fmt.Fprintf(&script, "ext.v%d = '1.%d'\n", i, i)
					}
					files := map[string]string{"gradle/publish.gradle": script.String()}
					for i := 0; i < k; i++ {
						files[fmt.Sprintf("p%d/gradle.properties", i)] = fmt.Sprintf("POM_ARTIFACT=a%d\n", i)
						files[fmt.Sprintf("p%d/build.gradle", i)] = "apply from: \"$rootDir/gradle/publish.gradle\"\ndependencies { implementation \"g:a:$v1\" }\n"
					}
					return files
				})
			s.gradleWalkFactor = gradleWorkFactor
			return s
		}(),
		// R4-M3: directories whose build files apply one script that
		// applies as many small scripts. Every directory took a layer, and
		// a check, for every small script. The file read is the project
		// BELOW each directory: it is what the directory's scope is kept
		// for.
		many("tree: directories that apply one script which applies as many small scripts", func(n int) int { return n / 200 },
			func(i int) string { return fmt.Sprintf("p%d/c/build.gradle", i) },
			func(n, k int) map[string]string { files, _ := r4Fanout(k); return files }),
		// R4-M2: modules under a parent that imports as many BOMs by
		// coordinates that hold references, each of them long: what was
		// worked out for a (module, import) pair was kept for the walk.
		many("tree: modules under a parent with as many reference-keyed BOM imports of long coordinates", func(n int) int { return n / 800 },
			func(i int) string { return fmt.Sprintf("m%d/pom.xml", i) },
			func(n, k int) map[string]string { return r4LongImports(k) }),
		// Projects at the bottom of a chain of a hundred directories, each of
		// which adds a layer of variables: every lookup of a name set at
		// the top walks the chain. The chain is as deep as a path lets it
		// be and no deeper, so this is a constant times the projects.
		many("tree: projects at the bottom of a chain of nested directories", func(n int) int { return n / 60 },
			func(i int) string { return strings.Repeat("d/", 100) + fmt.Sprintf("p%d/build.gradle", i) },
			func(n, k int) map[string]string {
				files := map[string]string{"gradle.properties": "v0=1.0\n"}
				for i := 1; i <= 100; i++ {
					files[strings.Repeat("d/", i)+"gradle.properties"] = fmt.Sprintf("w%d=2.%d\n", i, i)
				}
				for i := 0; i < k; i++ {
					files[strings.Repeat("d/", 100)+fmt.Sprintf("p%d/build.gradle", i)] = "dependencies { implementation \"g:a:$v0\" }\n"
				}
				return files
			}),
	}
}

// scalingCost reads one shape at size n: the bytes it allocated (the least
// of three runs), what the walk retained when it had read everything (the
// least of three), and the best of three times.
func scalingCost(t *testing.T, s scalingShape, n int) (alloc, retained uint64, best time.Duration) {
	t.Helper()
	root := writeTree(t, s.files(n))
	rels := []string{s.read}
	if s.reads != nil {
		rels = s.reads(n)
	}
	for run := 0; run < 3; run++ {
		var before, after, kept runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		done := make(chan *Walk, 1)
		go func() {
			w := NewWalk(root, nil)
			if s.gradleWalkFactor != 0 {
				w.gradle.walkFactor = s.gradleWalkFactor
			}
			for _, rel := range rels {
				w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
			}
			done <- w
		}()
		var w *Walk
		select {
		case w = <-done:
		case <-time.After(2 * time.Minute):
			t.Fatalf("%s at %d bytes did not finish in two minutes", s.name, n)
		}
		took := time.Since(start)
		runtime.ReadMemStats(&after)
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&kept)
		runtime.KeepAlive(w)
		if a := after.TotalAlloc - before.TotalAlloc; run == 0 || a < alloc {
			alloc = a
		}
		r := uint64(0)
		if kept.HeapAlloc > before.HeapAlloc {
			r = kept.HeapAlloc - before.HeapAlloc
		}
		if run == 0 || r < retained {
			retained = r
		}
		if run == 0 || took < best {
			best = took
		}
	}
	return alloc, retained, best
}

func TestBuildFileReadersScaleLinearly(t *testing.T) {
	shapes := scalingShapes()
	// The count is stated where a reader looks for it (the ledger once said
	// 28 for 26): change it with the list.
	if want := 37; len(shapes) != want {
		t.Errorf("%d shapes; this test's comment and the ledger say %d", len(shapes), want)
	}
	for _, s := range shapes {
		smallAlloc, smallKept, smallTime := scalingCost(t, s, scalingN)
		bigAlloc, bigKept, bigTime := scalingCost(t, s, 4*scalingN)
		allocRatio := float64(bigAlloc) / float64(smallAlloc+1)
		keptRatio := float64(bigKept) / float64(smallKept+1)
		t.Logf("%-78s alloc %6.1f -> %7.1f MB (x%.1f)  kept %5.1f -> %5.1f MB (x%.1f)  time %8s -> %8s", s.name,
			float64(smallAlloc)/1e6, float64(bigAlloc)/1e6, allocRatio, float64(smallKept)/1e6, float64(bigKept)/1e6, keptRatio,
			smallTime.Round(time.Microsecond), bigTime.Round(time.Microsecond))
		if allocRatio > scalingLimit {
			t.Errorf("%s: four times the input allocated %.1f times as much (%.1f MB -> %.1f MB); a reader that is linear allocates about four times",
				s.name, allocRatio, float64(smallAlloc)/1e6, float64(bigAlloc)/1e6)
		}
		if bigKept > scalingRetainedFloor && keptRatio > scalingLimit {
			t.Errorf("%s: four times the input left the walk holding %.1f times as much (%.1f MB -> %.1f MB); what a walk keeps is linear in what it read",
				s.name, keptRatio, float64(smallKept)/1e6, float64(bigKept)/1e6)
		}
		if raceEnabled {
			continue // the detector's slowdown is not uniform: time says nothing under it
		}
		// Time: at 4N, or where that is too fast to judge, larger.
		n := scalingN
		for step := 0; bigTime < scalingTimeFloor && step < scalingEscalations; step++ {
			n *= 4
			smallTime = bigTime
			_, _, bigTime = scalingCost(t, s, 4*n)
		}
		slow := func() bool {
			return bigTime > scalingTimeFloor && smallTime > scalingTimeFloor/4 && float64(bigTime)/float64(smallTime+1) > scalingLimit
		}
		if slow() {
			// Once more before it fails: a ratio of times is the one
			// measure here that a busy machine can move.
			_, _, smallTime = scalingCost(t, s, n)
			_, _, bigTime = scalingCost(t, s, 4*n)
			if slow() {
				t.Errorf("%s: four times the input (%d -> %d bytes) took %.1f times as long (%s -> %s), twice; a reader that is linear takes about four times",
					s.name, n, 4*n, float64(bigTime)/float64(smallTime+1), smallTime, bigTime)
			}
		}
	}
}

// A span is found by halving, not by looking at every span (round 2). On
// this path the scan costs a few milliseconds at any size worth running
// and allocates nothing, so the steps are counted.
func TestAPomsPluginSpansAreSearchedByHalving(t *testing.T) {
	const k = 4000
	var plugins, deps strings.Builder
	for i := 0; i < k; i++ {
		fmt.Fprintf(&plugins, "<plugin><artifactId>p%d</artifactId></plugin>", i)
		fmt.Fprintf(&deps, "<dependency><groupId>x</groupId><artifactId>d%d</artifactId><version>1</version></dependency>", i)
	}
	root := writeTree(t, map[string]string{"pom.xml": pomOf("<build><plugins>" + plugins.String() + "</plugins></build><dependencies>" + deps.String() + "</dependencies>")})
	pomSpanSteps.Store(0)
	got := NewWalk(root, nil).Read(filepath.Join(root, "pom.xml"), Options{})
	if len(got.Deps) != k {
		t.Fatalf("%d dependencies read, want %d", len(got.Deps), k)
	}
	// Each dependency asks at least once, and halving 4,000 spans takes
	// twelve steps; it is asked of the plugin spans and of the managed
	// ones, so twice that and room to spare. A scan would be k per
	// question: sixteen million.
	if steps := int(pomSpanSteps.Load()); steps < k || steps > k*40 {
		t.Errorf("%d span steps for %d dependencies against %d plugins; want at least one per dependency and no more than forty", steps, k, k)
	}
}

// The one thing left that is not linear is counted: every version-less
// dependency may look through every BOM its chain imports. A pom that asks
// for more looks than the poms read allow (pomWorkFactor) is a manifest
// that could not be read, by name, and the reading ends there.
func TestAPomPastItsWorkBoundIsAReadFailure(t *testing.T) {
	// The allowance and the refusal, on the counter itself.
	tree := newPomTree(t.TempDir(), nil)
	tree.allow(100)
	bud := tree.budget(100)
	if !bud.spend(pomWorkFactor*101) || bud.spend(1) || !bud.tripped {
		t.Errorf("a file of 100 bytes: used %d of %d, tripped %v; want exactly the allowance spent and the next look refused", bud.used, bud.limit, bud.tripped)
	}
	if only := tree.treeOnly(); only.work != tree.work {
		t.Error("the tree without its remote source has a work counter of its own")
	}
	// A file's allowance grows with each pom its questions reach, once.
	reached := &pomModel{size: 50}
	bud = tree.budget(10)
	bud.reach(reached, reached)
	if bud.limit != pomWorkFactor*(11+51) {
		t.Errorf("a file of 10 bytes that reaches a pom of 50: allowance %d, want %d", bud.limit, pomWorkFactor*(11+51))
	}
	// And the walk's allowance bounds every file's: here it is used up.
	if bud.spend(1) || !bud.tripped {
		t.Error("a look was allowed past the walk's allowance")
	}

	// End to end: k version-less dependencies that no BOM manages, and k
	// BOMs of the clone, each a real pom: k squared looks.
	const k = 600
	files := map[string]string{}
	var imports, deps strings.Builder
	for i := 0; i < k; i++ {
		files[fmt.Sprintf("b/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version></project>", i)
		fmt.Fprintf(&imports, "<dependency><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
		fmt.Fprintf(&deps, "<dependency><groupId>x</groupId><artifactId>d%d</artifactId></dependency>", i)
	}
	files["pom.xml"] = pomOf("<dependencyManagement><dependencies>" + imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")
	root := writeTree(t, files)
	start := time.Now()
	got := NewWalk(root, nil).Read(filepath.Join(root, "pom.xml"), Options{})
	t.Logf("%d dependencies against %d BOMs: %s", k, k, time.Since(start).Round(time.Millisecond))
	if got.Failure == nil || !strings.Contains(got.Failure.Error(), "pomWorkFactor") || len(got.Deps) != 0 {
		t.Errorf("%d dependencies, failure %v; want none and the work bound's failure", len(got.Deps), got.Failure)
	}
	// The same for a Gradle build: every dependency asks every platform.
	var gradle strings.Builder
	gradle.WriteString("dependencies {\n")
	for i := 0; i < k; i++ {
		fmt.Fprintf(&gradle, " implementation platform('b:b%d:1')\n implementation 'x:d%d'\n", i, i)
	}
	files["app/build.gradle"] = gradle.String() + "}\n"
	delete(files, "pom.xml")
	rootG := writeTree(t, files)
	gotG := NewWalk(rootG, nil).Read(filepath.Join(rootG, "app", "build.gradle"), Options{})
	if gotG.Failure == nil || !strings.Contains(gotG.Failure.Error(), "pomWorkFactor") || len(gotG.Deps) != 0 {
		t.Errorf("a Gradle build of %d dependencies and %d platforms: %d dependencies, failure %v; want none and the work bound's failure", k, k, len(gotG.Deps), gotG.Failure)
	}
}

// One pathological pom costs itself and not its neighbours (review round
// 3). The allowance was one counter for the tree: a fixture pom with 600
// imports and 600 dependencies nothing manages used all of it, and every
// pom read after it on the same walk, however ordinary, was refused. Work
// is charged to the file being read.
func TestAGreedyPomDoesNotStarveItsNeighbours(t *testing.T) {
	const k = 600
	files := map[string]string{}
	var imports, deps strings.Builder
	for i := 0; i < k; i++ {
		files[fmt.Sprintf("b/%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version></project>", i)
		fmt.Fprintf(&imports, "<dependency><groupId>b</groupId><artifactId>b%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", i)
		fmt.Fprintf(&deps, "<dependency><groupId>x</groupId><artifactId>d%d</artifactId></dependency>", i)
	}
	files["fixture/pom.xml"] = pomOf("<dependencyManagement><dependencies>" + imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")
	files["bom/pom.xml"] = `<project><groupId>n</groupId><artifactId>bom</artifactId><version>1</version><dependencyManagement><dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId><version>2.0</version></dependency></dependencies></dependencyManagement></project>`
	const neighbours = 5
	for i := 0; i < neighbours; i++ {
		files[fmt.Sprintf("m%d/pom.xml", i)] = fmt.Sprintf(`<project><groupId>n</groupId><artifactId>m%d</artifactId><version>1</version><dependencyManagement><dependencies><dependency><groupId>n</groupId><artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency></dependencies></dependencyManagement><dependencies><dependency><groupId>x</groupId><artifactId>y</artifactId></dependency></dependencies></project>`, i)
	}
	files["app/build.gradle"] = "dependencies {\n implementation platform('n:bom:1')\n implementation 'x:y'\n}\n"
	root := writeTree(t, files)
	w := NewWalk(root, nil) // ONE walk for all of them
	if got := w.Read(filepath.Join(root, "fixture", "pom.xml"), Options{}); got.Failure == nil || len(got.Deps) != 0 {
		t.Fatalf("the fixture pom: %d dependencies, failure %v; want its own work bound's failure (the premise)", len(got.Deps), got.Failure)
	}
	check := func(rel string) {
		got := w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
		version := ""
		for _, d := range got.Deps {
			if d.Name == "x:y" && !d.ManagedEntry {
				version = d.Version
			}
		}
		if got.Failure != nil || version != "2.0" {
			t.Errorf("%s, read after the fixture on the same walk: x:y at %q, failure %v; want 2.0 and none", rel, version, got.Failure)
		}
	}
	for i := 0; i < neighbours; i++ {
		check(fmt.Sprintf("m%d/pom.xml", i))
	}
	check("app/build.gradle")
}

// A build shaped like a large real one is far inside the pom work bound:
// 300 version-less dependencies that nothing manages (the worst case: each
// looks through every BOM) and forty BOMs of 200 managed entries each.
func TestARealShapedMavenBuildIsFarFromTheWorkBound(t *testing.T) {
	files := map[string]string{}
	var imports, deps strings.Builder
	for b := 0; b < 40; b++ {
		var managed strings.Builder
		for e := 0; e < 200; e++ {
			fmt.Fprintf(&managed, "<dependency><groupId>org.example.b%d</groupId><artifactId>artifact-%d</artifactId><version>1.%d.0</version></dependency>", b, e, e)
		}
		files[fmt.Sprintf("boms/%d/pom.xml", b)] = fmt.Sprintf("<project><groupId>org.example</groupId><artifactId>bom-%d</artifactId><version>1</version><packaging>pom</packaging><dependencyManagement><dependencies>%s</dependencies></dependencyManagement></project>", b, managed.String())
		fmt.Fprintf(&imports, "<dependency><groupId>org.example</groupId><artifactId>bom-%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", b)
	}
	// Fifty imports that lead nowhere (no such pom in the clone, and no
	// repository to ask): each is looked for once, not once for every
	// dependency, so they do not eat the allowance of a real build.
	const nowhere = 50
	for b := 0; b < nowhere; b++ {
		fmt.Fprintf(&imports, "<dependency><groupId>org.elsewhere</groupId><artifactId>bom-%d</artifactId><version>1</version><type>pom</type><scope>import</scope></dependency>", b)
	}
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&deps, "<dependency><groupId>org.unmanaged</groupId><artifactId>dep-%d</artifactId></dependency>", i)
	}
	deps.WriteString("<dependency><groupId>org.example.b39</groupId><artifactId>artifact-7</artifactId></dependency>")
	files["pom.xml"] = pomOf("<dependencyManagement><dependencies>" + imports.String() + "</dependencies></dependencyManagement><dependencies>" + deps.String() + "</dependencies>")
	root := writeTree(t, files)
	w := NewWalk(root, nil)
	got := w.Read(filepath.Join(root, "pom.xml"), Options{})
	if got.Failure != nil {
		t.Fatalf("a real-shaped build was refused: %v", got.Failure)
	}
	last := ""
	for _, d := range got.Deps {
		if d.Name == "org.example.b39:artifact-7" {
			last = d.Version
		}
	}
	if last != "1.7.0" {
		t.Errorf("the dependency the last BOM manages has version %q, want 1.7.0", last)
	}
	used, limit := w.pom.work.used, w.pom.work.limit
	t.Logf("looks %d of %d allowed", used, limit)
	if used < 300*40 {
		t.Errorf("only %d looks counted; 300 unmanaged dependencies against 40 BOMs are 12,000: the counter is not counting", used)
	}
	// And the one pass that drops the dead imports from the list, charged
	// at pomCompactPerUnit entries a unit.
	if most := 301*40 + nowhere + (40+nowhere)/pomCompactPerUnit; used > most {
		t.Errorf("%d looks counted; 301 dependencies against 40 BOMs and %d imports that lead nowhere, each looked for once, are at most %d", used, nowhere, most)
	}
	// And once found to lead nowhere they are not even passed over: they
	// leave the pom's list of imports. (Passing them costs too little for
	// a clock to show; it is the dependencies times the dead imports all
	// the same.)
	if most := 301*40 + nowhere; w.pom.importSteps > most {
		t.Errorf("%d imports passed over or looked at; with the %d dead ones dropped after the first question it is at most %d", w.pom.importSteps, nowhere, most)
	}
	if used*pomHonestMargin > limit {
		t.Errorf("a real-shaped build used %d of %d looks; the bound has no %d-fold margin", used, limit, pomHonestMargin)
	}
}
