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
		// Round 6 (M6-6): the reference-keyed entries that could answer a
		// question were merged into a list for every question, and the list
		// thrown away when the pom's first, plain entry answered: a parent
		// of many entries with a reference in both halves and a child of
		// many dependencies it manages plainly was the product of the two.
		{name: "pom: questions answered by a plain entry, past many reference-keyed ones", read: "child/pom.xml", files: func(n int) map[string]string {
			var managed, deps strings.Builder
			managed.WriteString("<dependency><groupId>g</groupId><artifactId>x</artifactId><version>1</version></dependency>")
			for i := 0; managed.Len() < n/2; i++ {
				fmt.Fprintf(&managed, "<dependency><groupId>${p}%d</groupId><artifactId>${p}%d</artifactId><version>1</version></dependency>", i, i)
			}
			for deps.Len() < n/2 {
				deps.WriteString("<dependency><groupId>g</groupId><artifactId>x</artifactId></dependency>")
			}
			return map[string]string{
				"pom.xml":       `<project><groupId>t</groupId><artifactId>parent</artifactId><version>1</version><packaging>pom</packaging><properties><p>q</p></properties><dependencyManagement><dependencies>` + managed.String() + `</dependencies></dependencyManagement></project>`,
				"child/pom.xml": `<project><parent><groupId>t</groupId><artifactId>parent</artifactId><version>1</version></parent><artifactId>child</artifactId><dependencies>` + deps.String() + `</dependencies></project>`,
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
		// Round 6: a statement that begins again inside itself, and a reader
		// of statements that goes through each to its end (the parts of a
		// call, the scripts an `apply` names, a plugin's line): the index of
		// the file's tokens answers each without reading the statement.
		one("gradle: set( with two arguments inside braces, repeated", "build.gradle", func(n int) string { return rep("{set('a',b,", n/11) }),
		one("gradle: lines that apply two scripts and end in a comma", "build.gradle", func(n int) string { return rep("apply from: 'a', from: 'b',\n", n/28) }),
		one("gradle: plugin lines that end in a comma", "build.gradle", func(n int) string { return "plugins {\n" + rep("id 'x' version 'y',\n", n/20) }),
		// --- a version catalogue (round 2, M5) ---
		{name: "catalogue: an array that never closes", read: "build.gradle", files: func(n int) map[string]string {
			return map[string]string{"build.gradle": "dependencies { implementation libs.x }\n",
				"gradle/libs.versions.toml": "[versions]\na = [\n" + rep("1,\n", n/3)}
		}},
		// Round 6 (M6-1): every level of an inline table split what it held
		// again, so a value nested n deep was read n times: 640 KB of braces
		// took 56 seconds. A catalogue's value is read two tables deep
		// (gradleCatalogueDepth), which is all its grammar has.
		{name: "catalogue: a value nested without end", read: "build.gradle", files: func(n int) map[string]string {
			return map[string]string{"build.gradle": "dependencies { implementation libs.x }\n",
				"gradle/libs.versions.toml": "[versions]\nk = " + rep("{a=", n/4) + "\"1.0\"" + rep("}", n/4) + "\n"}
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

		// Round 7. Each of these was found by a reviewer after the
		// allowances were gone; the search (round7_search_test.go) now looks
		// for their like without being told.
		//
		// A statement that does not end is begun again at every line of it
		// (gradleValueTokens).
		one("gradle: a declaration by extra continued by + on every line", "build.gradle.kts",
			func(n int) string { return strings.Repeat("val x by extra a +\n", n/19) }),
		one("gradle: an assignment of an empty string continued by + on every line", "build.gradle",
			func(n int) string { return strings.Repeat("x = \"\" +\n", n/9) }),
		one("gradle: blocks that each open a list, closed once at the end", "build.gradle",
			func(n int) string { return strings.Repeat("{def x=[", n/8) + "]" }),
		one("gradle: blocks that each open a mapOf that is never closed", "build.gradle",
			func(n int) string { return strings.Repeat("{def x=mapOf(", n/13) }),
		one("gradle: mavenBom lines continued by +", "build.gradle",
			func(n int) string {
				return "dependencyManagement { imports {\n" + strings.Repeat("mavenBom 'g:a:1' +\n", n/19)
			}),
		// One line of many statements (gemLogicalLines).
		one("gemfile: one line of gem declarations", "Gemfile",
			func(n int) string { return strings.Repeat("gem 'a';", n/8) + "\n" }),
		one("gemfile: one line of blocks that no pattern takes for whole", "Gemfile",
			func(n int) string { return strings.Repeat("if do gem end q;", n/16) + "\n" }),
		// A value that never closes (scanTOMLDepTables), a field of many
		// lines (rDescriptionFields).
		one("toml: an array that is never closed, and blank lines", "pyproject.toml",
			func(n int) string { return "[tool.poetry.dependencies]\na = [\n" + strings.Repeat("\n", n) }),
		one("cargo: a table that is never closed, and lines", "Cargo.toml",
			func(n int) string { return "[dependencies]\na = {\n" + strings.Repeat("x\n", n/2) }),
		one("description: a field of many continuation lines", "DESCRIPTION",
			func(n int) string { return "Imports:\n" + strings.Repeat(" a,\n", n/4) }),
		// Version-less dependencies against entries keyed by references in
		// both halves, under a chain of parents (pomContext.firstRef).
		scalingShape{name: "pom: version-less dependencies against as many entries keyed by references, under thirty parents",
			read: "pom.xml",
			files: func(n int) map[string]string {
				files := map[string]string{}
				for i := 1; i <= 30; i++ {
					parent := ""
					if i < 30 {
						parent = fmt.Sprintf("<parent><groupId>t</groupId><artifactId>c%d</artifactId><version>1</version><relativePath>../c%d</relativePath></parent>", i+1, i+1)
					}
					files[fmt.Sprintf("c%d/pom.xml", i)] = "<project>" + parent + fmt.Sprintf("<groupId>t</groupId><artifactId>c%d</artifactId><version>1</version></project>", i)
				}
				entry := "<dependency><groupId>${a}${b}${c}</groupId><artifactId>${d}${e}${f}</artifactId><version>1</version></dependency>"
				dep := "<dependency><groupId>g</groupId><artifactId>x</artifactId></dependency>"
				files["pom.xml"] = "<project><parent><groupId>t</groupId><artifactId>c1</artifactId><version>1</version><relativePath>c1</relativePath></parent>" +
					"<groupId>t</groupId><artifactId>app</artifactId><version>1</version><dependencyManagement><dependencies>" +
					strings.Repeat(entry, n/2/len(entry)) + "</dependencies></dependencyManagement><dependencies>" +
					strings.Repeat(dep, n/2/len(dep)) + "</dependencies></project>"
				return files
			}},
		one("pom: version-less dependencies against as many entries keyed by one reference each half", "pom.xml",
			func(n int) string {
				entry := "<dependency><groupId>${a}</groupId><artifactId>${d}</artifactId><version>1</version></dependency>"
				dep := "<dependency><groupId>g</groupId><artifactId>x</artifactId></dependency>"
				return pomOf("<dependencyManagement><dependencies>" + strings.Repeat(entry, n/2/len(entry)) + "</dependencies></dependencyManagement><dependencies>" +
					strings.Repeat(dep, n/2/len(dep)) + "</dependencies>")
			}),
		scalingShape{name: "pom: version-less dependencies that nothing manages against as many BOMs of the clone",
			read: "pom.xml",
			files: func(n int) map[string]string {
				files := map[string]string{}
				k, imports := n/3/200, ""
				for i := 0; i < k; i++ {
					files[fmt.Sprintf("b%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>t</groupId><artifactId>b%d</artifactId><version>1</version></project>", i)
					imports += r4import("t", fmt.Sprintf("b%d", i), "1")
				}
				dep := "<dependency><groupId>g</groupId><artifactId>x</artifactId></dependency>"
				files["pom.xml"] = pomOf("<dependencyManagement><dependencies>" + imports + "</dependencies></dependencyManagement><dependencies>" + strings.Repeat(dep, n/3/len(dep)) + "</dependencies>")
				return files
			}},
		// Round 9 (R9-10). A pom that is on no repository was looked for
		// among the poms that could not be read by reading the whole list.
		scalingShape{name: "pom: imports that are nowhere against as many poms that could not be read",
			read: "pom.xml",
			files: func(n int) map[string]string {
				files := map[string]string{}
				imports := ""
				for i := 0; len(imports) < n/2; i++ {
					files[fmt.Sprintf("u%d/pom.xml", i)] = fmt.Sprintf("<project><groupId>u</groupId><artifactId>u%d</artifactId><version>1</version><dependencies>", i)
					imports += r4import("t", fmt.Sprintf("b%d", i), "1")
				}
				files["pom.xml"] = pomOf("<dependencyManagement><dependencies>" + imports + "</dependencies></dependencyManagement><dependencies><dependency><groupId>g</groupId><artifactId>x</artifactId></dependency></dependencies>")
				return files
			}},
		// Round 9 (R9-10). Each variable a file assigns is looked for in
		// what the scripts it applies assign (gradleBuild.applyOrder).
		scalingShape{name: "gradle: a file's assignments against as many scripts it applies",
			read: "build.gradle",
			files: func(n int) map[string]string {
				files := map[string]string{}
				var build strings.Builder
				for i := 0; build.Len() < n/2; i++ {
					fmt.Fprintf(&build, "ext.v%d = '1'\napply from: 's%d.gradle'\n", i, i)
					files[fmt.Sprintf("s%d.gradle", i)] = fmt.Sprintf("ext.w%d = '2'\n", i)
				}
				build.WriteString("dependencies {\n implementation \"g:a:$v0\"\n}\n")
				files["build.gradle"] = build.String()
				return files
			}},
		one("pom: version-less dependencies against as many entries keyed by a reference under one artifact", "pom.xml",
			func(n int) string {
				entry := "<dependency><groupId>${a}</groupId><artifactId>x</artifactId><version>1</version></dependency>"
				dep := "<dependency><groupId>g</groupId><artifactId>x</artifactId></dependency>"
				return pomOf("<dependencyManagement><dependencies>" + strings.Repeat(entry, n/2/len(entry)) + "</dependencies></dependencyManagement><dependencies>" +
					strings.Repeat(dep, n/2/len(dep)) + "</dependencies>")
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
			for _, rel := range rels {
				w.Read(filepath.Join(root, filepath.FromSlash(rel)), Options{})
			}
			done <- w
		}()
		var w *Walk
		select {
		case w = <-done:
		case <-time.After(scalingTimeout):
			t.Errorf("%s at %d bytes did not finish in %s", s.name, n, scalingTimeout)
			return 0, 0, -1
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

// scalingProductFloor is THE FLOOR (round 7): every entry of
// scalingNotLinear is a product of two things the build
// itself writes (dependencies and the platforms or BOMs each is asked of,
// projects and what a root shares with each), and none reaches the default
// CPU limit (180 s since 0.29.214) with less than scalingProductFloor of
// files written for the purpose. An entry whose measured size at the limit
// is under the floor is not a product to be listed: it is a reader to be
// fixed, as the statement readers of round 7 were. Everything that is NOT a
// product across what a build writes is held to linear cost, by the shapes
// above and by the search (round7_search_test.go).
const scalingProductFloor = 2 << 20

// scalingNotLinear are the shapes this test does NOT hold to linear cost,
// each with why. Until 0.29.212 each ended at an allowance counted in the
// readers (units of work per byte read); the allowances are gone
// (pom_tree.go, "What bounds the reading of a build tree"), and a repository
// of one of these shapes now has its analysis stopped at the limit on the
// CPU time or the heap of the process that reads its manifests
// (collection.manifest_read_cpu_seconds, collection.manifest_read_memory_mb),
// and reported so. They stay in the list so that making one linear is a
// one-line change here, and so that the count of shapes found is not lost.
var scalingNotLinear = map[string]string{
	// Times are of this reader on an Apple M-series core, 0.29.213 (the
	// fifth, 0.29.214); the size at the limit is each one's quadratic curve
	// carried to 180 s.
	"gradle: many dependencies against many platforms of the clone":                             "every dependency no platform manages is asked of every platform: 0.5 s for 358 KB (1,500 platforms), 5 s for 1.2 MB; the limit at about 7 MB. An honest build has a handful of platforms",
	"tree: project directories under a root that shares as many repositories and platforms":     "the platforms the root shares are applied to every project: 0.4 s for 177 KB (1,900 files), 3.2 s for 609 KB; the limit at about 4.5 MB. An honest root shares one or two",
	"tree: projects that apply one script, each with its own value for what it reads":           "the script is read again for each project that gives it another answer, projects times its size: 17 s for 972 KB (15,000 projects); the limit at about 3.2 MB. An honest publishing build of 1,000 modules and a 60 KB script takes under a second",
	"tree: modules under a parent with as many reference-keyed BOM imports of long coordinates": "each module's imports are other coordinates, each interpolated (2,600 bytes) and asked for: 0.4 s for 155 KB, 4.9 s for 525 KB; the limit at about 3.2 MB. An honest parent has a few such imports",
	"pom: version-less dependencies that nothing manages against as many BOMs of the clone":     "every dependency no BOM manages is asked of every BOM the pom imports: 0.2 s for 214 KB (512 BOMs), 0.8 s for 429 KB, 3.3 s for 860 KB; the limit at about 6.3 MB. An honest pom imports a handful of BOMs",
}

// scalingTimeout is how long one reading of a shape may take before the
// shape is failed and passed over.
var scalingTimeout = 2 * time.Minute

func TestBuildFileReadersScaleLinearly(t *testing.T) {
	shapes := scalingShapes()
	// The count is stated where a reader looks for it (the ledger once said
	// 28 for 26): change it with the list.
	if want := 58; len(shapes) != want {
		t.Errorf("%d shapes; this test's comment and the ledger say %d", len(shapes), want)
	}
	known := map[string]bool{}
	for _, s := range shapes {
		known[s.name] = true
	}
	for name, why := range scalingNotLinear {
		if !known[name] {
			t.Errorf("scalingNotLinear names %q, which is no shape", name)
		}
		// Each states the size at which it reaches the limit, and that is
		// not under the floor.
		var at float64
		if i := strings.Index(why, "the limit at about "); i < 0 {
			t.Errorf("%s: its entry does not say at what size it reaches the CPU limit", name)
		} else if _, err := fmt.Sscanf(why[i:], "the limit at about %f MB", &at); err != nil || at*(1<<20) < scalingProductFloor {
			t.Errorf("%s reaches the limit at %.1f MB (%v), under the floor of %d bytes: it is a reader to fix, not a product to list", name, at, err, scalingProductFloor)
		}
	}
	for _, s := range shapes {
		if scalingNotLinear[s.name] != "" {
			continue
		}
		smallAlloc, smallKept, smallTime := scalingCost(t, s, scalingN)
		if smallTime < 0 {
			continue
		}
		bigAlloc, bigKept, bigTime := scalingCost(t, s, 4*scalingN)
		if bigTime < 0 {
			continue
		}
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
