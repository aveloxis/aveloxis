// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// The scaling gate for the lockfile parsers (0.29.213), on the plan of
// internal/manifest's scaling_test.go: each shape is generated at a size N
// and at 4N and parsed as an analysis parses it (ParseLockfile). A linear
// parser costs four times as much at 4N, a quadratic one sixteen; the gate
// fails past the geometric mean of the two. What is measured is the bytes
// allocated (the least of three runs; exact) and the time (the best of
// three; judged only where the larger parse is slow enough to time, a shape
// too fast at 4N being parsed again at 16N and 64N, and not at all under
// the race detector).
//
// A lockfile is read whole and has no largest size, and it is read in the
// child process whose CPU time is limited; this gate is what keeps a file of
// a few megabytes from reaching that limit. It found one such path when it
// was written: yaml.v3 compares every key of a mapping with every other
// when it decodes into a Go map, so pnpm-lock.yaml and pubspec.lock cost
// the square of their packages (lockfile_yaml.go). A new hostile shape is
// one entry here.
const (
	lockScalingLimit       = 8.0
	lockScalingN           = 48 << 10
	lockScalingTimeFloor   = 40 * time.Millisecond
	lockScalingEscalations = 2
)

type lockShape struct {
	name, kind string
	gen        func(n int) string
}

// fill repeats what unit(i) gives until the text is n bytes.
func fill(n int, head, tail string, unit func(i int) string) string {
	var b strings.Builder
	b.WriteString(head)
	for i := 0; b.Len() < n; i++ {
		b.WriteString(unit(i))
	}
	b.WriteString(tail)
	return b.String()
}

func lockShapes() []lockShape {
	rep := strings.Repeat
	f := fmt.Sprintf
	return []lockShape{
		// --- a plain file of every format ---
		{"package-lock v3", "package-lock.json", func(n int) string {
			return fill(n, `{"lockfileVersion":3,"packages":{"":{"dependencies":{"p0":"1"}}`, "}}", func(i int) string {
				return f(`,"node_modules/p%d":{"version":"1.%d.0","dev":true,"dependencies":{"p%d":"^1","q%d":"~2"}}`, i, i, i+1, i)
			})
		}},
		{"package-lock v1", "package-lock.json", func(n int) string {
			return fill(n, `{"lockfileVersion":1,"dependencies":{"p":{"version":"1.0.0"}`, "}}", func(i int) string {
				return f(`,"p%d":{"version":"1.%d.0","dependencies":{"c%d":{"version":"2.0.0"}}}`, i, i, i)
			})
		}},
		{"yarn v1", "yarn.lock", func(n int) string {
			return fill(n, "# yarn lockfile v1\n\n", "", func(i int) string {
				return f("\"@s/p%d@^1.0.0\", \"@s/p%d@^1.1.0\":\n  version \"1.%d.0\"\n  resolved \"https://registry.yarnpkg.com/@s/p%d/-/p%d-1.%d.0.tgz#abc\"\n  dependencies:\n    c%d \"^2\"\n\n", i, i, i, i, i, i, i)
			})
		}},
		{"yarn berry", "yarn.lock", func(n int) string {
			return fill(n, "__metadata:\n  version: 8\n\n\"app@workspace:.\":\n  version: 0.0.0-use.local\n  dependencies:\n    p0: ^1\n\n", "", func(i int) string {
				return f("\"p%d@npm:^1.0.0\":\n  version: 1.%d.0\n  resolution: \"p%d@npm:1.%d.0\"\n  dependencies:\n    c%d: ^2\n\n", i, i, i, i, i)
			})
		}},
		{"pnpm v9", "pnpm-lock.yaml", func(n int) string {
			var snaps strings.Builder
			snaps.WriteString("\nsnapshots:\n")
			packages := fill(n/2, "lockfileVersion: '9.0'\nimporters:\n  .:\n    dependencies:\n      p0:\n        specifier: ^1\n        version: 1.0.0\npackages:\n", "", func(i int) string {
				fmt.Fprintf(&snaps, "  p%d@1.%d.0:\n    dependencies:\n      c%d: 2.0.0\n", i, i, i)
				return f("  p%d@1.%d.0:\n    resolution: {integrity: sha512-x}\n", i, i)
			})
			return packages + snaps.String()
		}},
		{"pnpm v6", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "lockfileVersion: '6.0'\ndependencies:\n  p0:\n    specifier: ^1\n    version: 1.0.0\npackages:\n", "", func(i int) string {
				return f("  /@s/p%d@1.%d.0(peer@1.0.0):\n    resolution: {integrity: sha512-x}\n    dependencies:\n      c%d: 2.0.0\n    dev: false\n", i, i, i)
			})
		}},
		{"bun.lock", "bun.lock", func(n int) string {
			return fill(n, "{\n  // a comment\n  \"lockfileVersion\": 1,\n  \"workspaces\": {\"\": {\"dependencies\": {\"p0\": \"^1\",},},},\n  \"packages\": {\n    \"p\": [\"p@1.0.0\", \"\", {}, \"sha512-x\"],\n", "  },\n}\n", func(i int) string {
				return f("    \"p%d\": [\"p%d@1.%d.0\", \"\", { \"dependencies\": { \"c%d\": \"^2\", }, }, \"sha512-x\"], /* c */\n", i, i, i, i)
			})
		}},
		{"poetry.lock", "poetry.lock", func(n int) string {
			return fill(n, "", "[metadata]\nlock-version = \"2.0\"\n", func(i int) string {
				return f("[[package]]\nname = \"p%d\"\nversion = \"1.%d.0\"\ncategory = \"dev\"\n\n[package.dependencies]\nc%d = \">=2\"\nd%d = {version = \">=3\", optional = true}\n\n", i, i, i, i)
			})
		}},
		{"Cargo.lock", "Cargo.lock", func(n int) string {
			return fill(n, "version = 3\n\n", "", func(i int) string {
				return f("[[package]]\nname = \"p%d\"\nversion = \"1.%d.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\ndependencies = [\n \"c%d\",\n \"d%d 2.0.0\",\n]\n\n", i, i, i, i)
			})
		}},
		{"uv.lock", "uv.lock", func(n int) string {
			return fill(n, "version = 1\n\n", "", func(i int) string {
				return f("[[package]]\nname = \"p%d\"\nversion = \"1.%d.0\"\nsource = { registry = \"https://pypi.org/simple\" }\n\n", i, i)
			})
		}},
		{"pdm.lock", "pdm.lock", func(n int) string {
			return fill(n, "", "", func(i int) string {
				return f("[[package]]\nname = \"p%d\"\nversion = \"1.%d.0\"\nsummary = \"s\"\n\n", i, i)
			})
		}},
		{"Pipfile.lock", "Pipfile.lock", func(n int) string {
			return fill(n, `{"_meta":{},"default":{"p":{"version":"==1"}`, "}}", func(i int) string {
				return f(`,"p%d":{"hashes":["sha256:x"],"version":"==1.%d.0"}`, i, i)
			})
		}},
		{"Gemfile.lock", "Gemfile.lock", func(n int) string {
			return fill(n, "GEM\n  remote: https://rubygems.org/\n  specs:\n", "\nPLATFORMS\n  ruby\n\nDEPENDENCIES\n  p0\n", func(i int) string {
				return f("    p%d (1.%d.0)\n      c%d (>= 2.0, < 3)\n      d%d\n", i, i, i, i)
			})
		}},
		{"composer.lock", "composer.lock", func(n int) string {
			return fill(n, `{"packages":[{"name":"v/p","version":"1.0.0"}`, `],"packages-dev":[]}`, func(i int) string {
				return f(`,{"name":"v/p%d","version":"v1.%d.0","require":{"php":">=8","v/c%d":"^2"},"require-dev":{"v/d%d":"^3"}}`, i, i, i, i)
			})
		}},
		{"mix.lock", "mix.lock", func(n int) string {
			return fill(n, "%{\n", "}\n", func(i int) string {
				return f("  \"p%d\": {:hex, :p%d, \"1.%d.0\", \"abc\", [:mix], [{:c%d, \"~> 2.0\", [hex: :c%d, repo: \"hexpm\", optional: false]}], \"hexpm\", \"def\"},\n", i, i, i, i, i)
			})
		}},
		{"pubspec.lock", "pubspec.lock", func(n int) string {
			return fill(n, "packages:\n", "sdks:\n  dart: \">=3.0.0\"\n", func(i int) string {
				return f("  p%d:\n    dependency: \"direct main\"\n    description:\n      name: p%d\n      url: \"https://pub.dev\"\n    source: hosted\n    version: \"1.%d.0\"\n", i, i, i)
			})
		}},
		{"Package.resolved", "Package.resolved", func(n int) string {
			return fill(n, `{"pins":[{"identity":"p","location":"https://github.com/o/p","state":{"version":"1.0.0"}}`, `],"version":2}`, func(i int) string {
				return f(`,{"identity":"p%d","kind":"remoteSourceControl","location":"https://github.com/o%d/P%d.git","state":{"revision":"abc","version":"1.%d.0"}}`, i, i, i, i)
			})
		}},
		{"packages.lock.json", "packages.lock.json", func(n int) string {
			return fill(n, `{"version":1,"dependencies":{"net8.0":{"P":{"type":"Direct","resolved":"1.0.0"}`, "}}}", func(i int) string {
				return f(`,"P%d":{"type":"Transitive","resolved":"1.%d.0","contentHash":"x","dependencies":{"C%d":"2.0.0"}}`, i, i, i)
			})
		}},
		{"gradle.lockfile", "gradle.lockfile", func(n int) string {
			return fill(n, "# This is a Gradle generated file for dependency locking.\n", "empty=\n", func(i int) string {
				return f("com.example.g%d:a%d:1.%d.0=compileClasspath,runtimeClasspath\n", i, i, i)
			})
		}},
		{"stack.yaml.lock", "stack.yaml.lock", func(n int) string {
			return fill(n, "packages:\n", "snapshots: []\n", func(i int) string {
				return f("- completed:\n    hackage: p%d-1.%d.0@sha256:abc,123\n    pantry-tree:\n      sha256: def\n      size: 1\n  original:\n    hackage: p%d-1.%d.0\n", i, i, i, i)
			})
		}},
		{"renv.lock", "renv.lock", func(n int) string {
			return fill(n, `{"R":{"Version":"4.3.1","Repositories":[{"Name":"CRAN","URL":"https://cloud.r-project.org"}]},"Packages":{"p":{"Package":"p","Version":"1","Source":"Repository","Repository":"CRAN"}`, "}}", func(i int) string {
				return f(`,"p%d":{"Package":"p%d","Version":"1.%d.0","Source":"Repository","Repository":"CRAN","Requirements":["c%d","R","d%d"]}`, i, i, i, i, i)
			})
		}},

		// --- what an author can write to make a parser work harder ---
		{"package-lock v1: nested as deep as JSON goes", "package-lock.json", func(n int) string {
			// Each level is decoded again from its own text (the v1 walk).
			const open, shut = `{"version":"1.0.0","dependencies":{"a":`, `}}`
			depth := min(n/(len(open)+len(shut)), 4000) // below encoding/json's own limit on nesting
			chain := rep(open, depth) + `{"version":"1.0.0"}` + rep(shut, depth)
			return fill(n, `{"lockfileVersion":1,"dependencies":{"x":{"version":"1"}`, "}}", func(i int) string {
				return f(`,"t%d":%s`, i, chain)
			})
		}},
		{"package-lock v1: one chain, as deep as the size", "package-lock.json", func(n int) string {
			const open, shut = `{"version":"1.0.0","dependencies":{"a":`, `}}`
			depth := n / (len(open) + len(shut))
			return `{"lockfileVersion":1,"dependencies":{"a":` + rep(open, depth) + `{"version":"1.0.0"}` + rep(shut, depth) + "}}"
		}},
		{"package-lock v3: one package, every dependency", "package-lock.json", func(n int) string {
			return fill(n, `{"lockfileVersion":3,"packages":{"node_modules/p":{"version":"1.0.0","dependencies":{"a":"1"`, "}}}}", func(i int) string {
				return f(`,"c%d":"^1"`, i)
			})
		}},
		{"package-lock v3: the same key again and again", "package-lock.json", func(n int) string {
			return fill(n, `{"lockfileVersion":3,"packages":{"node_modules/p":{"version":"1.0.0"}`, "}}", func(int) string {
				return `,"node_modules/p":{"version":"1.0.0","dependencies":{"a":"1"}}`
			})
		}},
		{"package-lock: tarball URLs for versions", "package-lock.json", func(n int) string {
			return fill(n, `{"lockfileVersion":3,"packages":{"":{}`, "}}", func(i int) string {
				return f(`,"node_modules/@s/p%d":{"version":"https://registry.npmjs.org/@s%%2fp%d/-/%s/-/p%d-1.0.0.tgz"}`, i, i, rep("/-/x", 40), i)
			})
		}},
		{"package-lock: aliases, sources and requirements", "package-lock.json", func(n int) string {
			return fill(n, `{"lockfileVersion":3,"packages":{"":{}`, "}}", func(i int) string {
				return f(`,"node_modules/a%d":{"version":"npm:@s/real%d@1.0.0","dependencies":{"c":"1"}},"node_modules/g%d":{"version":"git+https://h/o/r.git#x"},"node_modules/r%d":{"version":">= 2 || < 1"}`, i, i, i, i)
			})
		}},
		{"yarn v1: a header of nothing but selectors", "yarn.lock", func(n int) string {
			return fill(n, "", ":\n  version \"1.0.0\"\n", func(i int) string { return f("p@^%d, ", i) })
		}},
		{"yarn v1: headers and no body", "yarn.lock", func(n int) string {
			return fill(n, "", "", func(i int) string { return f("\"@@@@p%d@@@\":\n", i) })
		}},
		{"yarn v1: dependencies of nobody", "yarn.lock", func(n int) string {
			return fill(n, "p@1:\n  version \"1\"\n", "", func(i int) string { return f("  dependencies:\n    \"c%d\n    \"\" \"\"\n", i) })
		}},
		{"yarn v1: one line", "yarn.lock", func(n int) string { return rep("a@,\" ", n/5) + ":" }},
		{"yarn berry: a workspace that depends on everything", "yarn.lock", func(n int) string {
			return fill(n, "__metadata:\n  version: 8\n\n\"app@workspace:.\":\n  version: 0.0.0-use.local\n  dependencies:\n", "", func(i int) string { return f("    \"@s/p%d\": ^1\n", i) })
		}},
		{"pnpm: keys as short as they come", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "packages:\n", "", func(i int) string { return f(" a@%d.0: {}\n", i) })
		}},
		{"pnpm: one package, every dependency", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "packages:\n  p@1.0.0:\n    dependencies:\n", "", func(i int) string { return f("      c%d: 1.0.0\n", i) })
		}},
		{"pnpm: importers of everything", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "importers:\n", "", func(i int) string {
				return f("  i%d:\n    dependencies:\n      c%d: {specifier: ^1, version: 1.0.0}\n", i, i)
			})
		}},
		{"pnpm: the same key again and again", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "packages:\n", "", func(int) string { return " a@1.0.0: {}\n" })
		}},
		{"pnpm: every value an alias", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "x: &x {dependencies: {a: 1.0.0, b: 1.0.0, c: 1.0.0}}\nsnapshots:\n", "", func(i int) string { return f(" a@%d.0: *x\n", i) })
		}},
		{"pnpm: nested flow collections", "pnpm-lock.yaml", func(n int) string {
			depth := min(n/2, 9000) // below yaml.v3's own limit on nesting
			return fill(n, "packages:\n", "", func(i int) string { return f(" a@%d.0: %s%s\n", i, rep("[", depth), rep("]", depth)) })
		}},
		{"pnpm: long keys with every separator", "pnpm-lock.yaml", func(n int) string {
			return fill(n, "packages:\n", "", func(i int) string {
				return f(" '%s/@s/p%d@1.0.0_%s(%s)': {}\n", rep("/@a", 300), i, rep("@_", 300), rep("(", 300))
			})
		}},
		{"pubspec: keys as short as they come", "pubspec.lock", func(n int) string {
			return fill(n, "packages:\n", "", func(i int) string { return f(" a%d: {version: '1'}\n", i) })
		}},
		{"stack: a list of nothing", "stack.yaml.lock", func(n int) string {
			return fill(n, "packages:\n", "", func(i int) string { return f("- {completed: {hackage: 'p-%d@x', a%d: 1}}\n", i, i) })
		}},
		{"stack: one item with every key", "stack.yaml.lock", func(n int) string {
			return fill(n, "packages:\n- completed:\n", "", func(i int) string { return f("    k%d: 1\n", i) })
		}},
		{"stack: names of dashes and digits", "stack.yaml.lock", func(n int) string {
			return fill(n, "packages:\n", "", func(i int) string { return f("- completed:\n    hackage: %s\n", rep("-1.2", 500)) })
		}},
		{"bun.lock: comments that never end", "bun.lock", func(n int) string { return rep("/* ", n/3) }},
		{"bun.lock: commas and space", "bun.lock", func(n int) string { return "{" + rep(",   \n\t ", n/7) + "}" }},
		{"bun.lock: slashes", "bun.lock", func(n int) string { return rep("/ /", n/3) }},
		{"bun.lock: packages that are not tuples", "bun.lock", func(n int) string {
			return fill(n, `{"packages":{"p":[]`, "}}", func(i int) string {
				return f(`,"p%d":{"a":[[[[[[[[1]]]]]]]]},"q%d":["@@@","",{"dependencies":7}]`, i, i)
			})
		}},
		{"poetry: table headers", "poetry.lock", func(n int) string { return rep("[[package]]\n[package.dependencies]\n[x\n", n/40) }},
		{"poetry: inline tables in dependencies", "poetry.lock", func(n int) string {
			return fill(n, "[[package]]\nname = \"p\"\nversion = \"1\"\n[package.dependencies]\n", "", func(i int) string {
				return f("c%d = {{{%s version version = = \"\n", i, rep("version ", 20))
			})
		}},
		{"poetry: one long line", "poetry.lock", func(n int) string {
			return "[[package]]\nname = \"p\"\nversion = \"1\"\n[package.dependencies]\n" + rep("{version = }", n/12)
		}},
		{"Cargo: an array that never closes", "Cargo.lock", func(n int) string {
			return fill(n, "[[package]]\nname = \"p\"\nversion = \"1\"\ndependencies = [\n", "", func(i int) string { return f(" \"c%d  1  2\",,,\n", i) })
		}},
		{"Gemfile.lock: nearly a spec", "Gemfile.lock", func(n int) string {
			return fill(n, "GEM\n  specs:\n", "", func(i int) string { return f("    p%d (%s\n      c (%s\n", i, rep("(", 200), rep(" (", 100)) })
		}},
		{"Gemfile.lock: one long line", "Gemfile.lock", func(n int) string { return "GEM\n    p (" + rep("1 ", n/2) }},
		{"Gemfile.lock: sections", "Gemfile.lock", func(n int) string {
			return fill(n, "", "", func(i int) string { return f("GEM\nDEPENDENCIES\n  p%d!   \n", i) })
		}},
		{"mix.lock: nearly a hex tuple", "mix.lock", func(n int) string {
			return fill(n, "", "", func(i int) string { return f("\"p%d\": {:hex, :p, %s\n", i, rep("{:a, \"", 30)) })
		}},
		{"mix.lock: one line of tuples", "mix.lock", func(n int) string {
			return "\"p\": {:hex, :p, \"1\", " + rep("{:c, \"1\"}, {:d,   ", n/20)
		}},
		{"mix.lock: one line of quotes", "mix.lock", func(n int) string { return rep("\"a\":  ", n/6) }},
		{"gradle.lockfile: colons", "gradle.lockfile", func(n int) string {
			return fill(n, "", "", func(i int) string { return f("%s=%s\n", rep("a:", 100), rep("=,", 50)) })
		}},
		{"gradle.lockfile: one line", "gradle.lockfile", func(n int) string { return rep("g:a:1=", n/6) }},
		{"composer.lock: one package requiring everything", "composer.lock", func(n int) string {
			return fill(n, `{"packages":[{"name":"v/p","version":"v1","require":{"a/b":"1"`, "}}]}", func(i int) string { return f(`,"v/c%d":"^1"`, i) })
		}},
		{"Package.resolved: locations that are nearly URLs", "Package.resolved", func(n int) string {
			return fill(n, `{"pins":[{"identity":"p"}`, "]}", func(i int) string {
				return f(`,{"identity":"p%d","location":"git@github.com:%s.git","state":{"version":"1"}}`, i, rep("o/", 100))
			})
		}},
		{"packages.lock.json: frameworks", "packages.lock.json", func(n int) string {
			return fill(n, `{"dependencies":{"f":{}`, "}}", func(i int) string { return f(`,"f%d":{"P":{"type":"Direct","resolved":"1"}}`, i) })
		}},
		{"renv.lock: requirement fields of every kind", "renv.lock", func(n int) string {
			return fill(n, `{"Packages":{"p":{}`, "}}", func(i int) string {
				return f(`,"p%d":{"Version":"1","Source":"Repository","Repository":"https://%s","Depends":["R (>= 3)","a%d (>= 1)"],"Imports":"b, c (>= 2), d","LinkingTo":{"x":[1]}}`, i, rep("x/", 50), i)
			})
		}},
		{"renv.lock: Bioconductor and edges to it", "renv.lock", func(n int) string {
			return fill(n, `{"Packages":{"b":{"Version":"1","Source":"Bioconductor"}`, "}}", func(i int) string {
				return f(`,"p%d":{"Version":"1","Source":"Repository","Repository":"CRAN","Requirements":["b","p%d"]},"b%d":{"Version":"1","Source":"Bioconductor","Requirements":["p%d"]}`, i, i+1, i, i)
			})
		}},
		{"Pipfile.lock: versions of spaces", "Pipfile.lock", func(n int) string {
			return fill(n, `{"default":{"p":{}`, "}}", func(i int) string { return f(`,"p%d":{"version":"%s==1"}`, i, rep(" ", 200)) })
		}},
		{"JSON: as deep as it goes", "composer.lock", func(n int) string { return rep(`{"a":[`, n/6) }},
	}
}

func lockScalingMeasure(kind, text string) (alloc uint64, took time.Duration) {
	data := []byte(text)
	alloc, took = ^uint64(0), time.Duration(1<<62)
	for run := 0; run < 3; run++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		began := time.Now()
		_, _ = ParseLockfile(kind, data) // a refused file costs what it costs
		d := time.Since(began)
		runtime.ReadMemStats(&after)
		alloc, took = min(alloc, after.TotalAlloc-before.TotalAlloc), min(took, d)
	}
	return alloc, took
}

// lockScalingNotLinear are the shapes this gate knows are not linear, each
// with what it costs. There are none: an entry needs its numbers.
var lockScalingNotLinear = map[string]string{}

func TestLockfileParsersScaleLinearly(t *testing.T) {
	seen := map[string]bool{}
	kinds := map[string]bool{}
	for _, shape := range lockShapes() {
		if seen[shape.name] {
			t.Fatalf("two shapes are named %q", shape.name)
		}
		seen[shape.name] = true
		kinds[shape.kind] = true
		if _, ok := lockfileKinds[shape.kind]; !ok {
			t.Fatalf("%s: %q is not a lockfile kind", shape.name, shape.kind)
		}
		n := lockScalingN
		a1, _ := lockScalingMeasure(shape.kind, shape.gen(n))
		a4, t4 := lockScalingMeasure(shape.kind, shape.gen(4*n))
		worst := float64(a4) / float64(max(a1, 1))
		what := fmt.Sprintf("allocation %.1f", worst)
		if a4 < 1<<20 {
			worst = 0 // a megabyte allocated at 4N: the ratio is the allocator's rounding
		}
		if !manifestRaceEnabled {
			// Time: where the larger parse can be timed.
			for step := 0; t4 < lockScalingTimeFloor && step < lockScalingEscalations; step++ {
				n *= 4
				_, t4 = lockScalingMeasure(shape.kind, shape.gen(4*n))
			}
			if t4 >= lockScalingTimeFloor {
				_, t1 := lockScalingMeasure(shape.kind, shape.gen(n))
				ratio := float64(t4) / float64(max(t1, time.Microsecond))
				if ratio > lockScalingLimit {
					// Measured again before it is believed: time is the
					// one measure the machine's load can move.
					_, t1 = lockScalingMeasure(shape.kind, shape.gen(n))
					_, t4 = lockScalingMeasure(shape.kind, shape.gen(4*n))
					ratio = float64(t4) / float64(max(t1, time.Microsecond))
				}
				what += fmt.Sprintf(", time %.1f (%s at %d KB)", ratio, t4.Round(time.Millisecond), 4*n>>10)
				worst = max(worst, ratio)
			} else {
				what += fmt.Sprintf(", too fast to time (%s at %d KB)", t4.Round(time.Microsecond), 4*n>>10)
			}
		}
		known, excused := lockScalingNotLinear[shape.name]
		switch {
		case worst > lockScalingLimit && !excused:
			t.Errorf("%s: at four times the size it costs more than %.0f times as much: %s", shape.name, lockScalingLimit, what)
		case worst <= lockScalingLimit && excused:
			t.Errorf("%s is listed as not linear (%s) and is linear now (%s): take it off the list", shape.name, known, what)
		default:
			t.Logf("%-55s %s", shape.name, what)
		}
	}
	for name := range lockScalingNotLinear {
		if !seen[name] {
			t.Errorf("lockScalingNotLinear names %q, which is not a shape", name)
		}
	}
	// Every format the roster parses has a shape: a new parser is gated
	// from the day it is added.
	for kind, spec := range lockfileKinds {
		if !spec.binaryOnly && !kinds[kind] {
			t.Errorf("no scaling shape for %s", kind)
		}
	}
}

// THE SEARCH (round 7), the lockfile readers' half of
// internal/manifest's TestNoRepeatedUnitIsReadInSuperLinearTime: no list of
// shapes, but every unit of one of the words lockfiles are written in, a
// seeded part of the units of two, and a seeded sample of longer ones, each
// repeated to a size and given to every lockfile kind's reader. A unit that
// is slow at the small size is read at four times the size: linear is four
// times as long, quadratic sixteen.
var lockSearchWords = []string{
	"{", "}", "[", "]", ":", ",", `"`, "'", `\`, "\n", " ", "  ", "\t", "-", "- ", "#", "=", "==", "@", "/", "a", "1.0.0", `"a"`, `"a":`, `"a":"1"`, `"a":{`, `"a":[`,
	`"version":"1.0.0"`, `"dependencies":{`, `"packages":{`, `"node_modules/a":{`, `"lockfileVersion":1`, `"requires":{`, `"resolved":"x"`, `"dev":true`,
	"packages:", "snapshots:", "importers:", "dependencies:", "devDependencies:", "version: 1.0.0", "a@1.0.0:", "/a/1.0.0:", "'@s/a@1.0.0':", "resolution: {integrity: x}",
	"a:", "a: 1", "dependency: direct main", "source: hosted", "description:", "sdks:", "&a", "*a", "<<:", "!!str", "? ", "---", "~", "null",
	"[[package]]", `name = "a"`, `version = "1.0.0"`, "dependencies = [", `"a 1.0.0",`, "[metadata]", "[package.dependencies]", "a = \"1\"", "[[package]]\nname = \"a\"\nversion = \"1\"",
	"a@^1.0.0:", "  version \"1.0.0\"", "  dependencies:", "    b \"^2\"", "__metadata:", "\"a@npm:^1\":", "  resolution: \"a@npm:1.0.0\"", "  checksum: x",
	"GEM", "  remote: https://rubygems.org/", "  specs:", "    a (1.0.0)", "      b (>= 1)", "PLATFORMS", "DEPENDENCIES", "BUNDLED WITH", "a!",
	`"a": {:hex, :a, "1.0.0", "x", [:mix], [{:b, "~> 1.0", [hex: :b]}], "hexpm", "y"},`, "%{", "{:hex,", "g:a:1.0.0=compileClasspath", "empty=",
	"a v1.0.0 h1:x=", "a v1.0.0/go.mod h1:x=", `"pins" : [`, `"identity" : "a"`, `"state" : {`, `"R": {`, `"Packages": {`, `"Package": "a"`, `"Requirements": [`,
	"completed:", "  hackage: a-1.0@sha256:x,1", "original:", "\r", "/*", "*/", "//",
}

func TestNoRepeatedUnitOfALockfileIsReadInSuperLinearTime(t *testing.T) {
	if manifestRaceEnabled {
		t.Log("the detector's slowdown is not uniform: times say nothing under it (the plain run judges)")
		return
	}
	const small, slow, limit = 32 << 10, 40 * time.Millisecond, 8.0
	rng := rand.New(rand.NewSource(7))
	var units []string
	for _, a := range lockSearchWords {
		units = append(units, a)
		for _, b := range lockSearchWords {
			if rng.Intn(8) == 0 {
				units = append(units, a+b, a+" "+b)
			}
		}
	}
	for k := 0; k < 200; k++ {
		var b strings.Builder
		for n := 3 + rng.Intn(3); n > 0; n-- {
			b.WriteString(lockSearchWords[rng.Intn(len(lockSearchWords))])
			if rng.Intn(2) == 0 {
				b.WriteString(" ")
			}
		}
		units = append(units, b.String())
	}
	kinds := make([]string, 0, len(lockfileKinds))
	for kind := range lockfileKinds {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	read := func(kind, unit, prefix string, n int) time.Duration {
		data := []byte(prefix + strings.Repeat(unit, n/len(unit)))
		began := time.Now()
		_, _ = ParseLockfile(kind, data)
		return time.Since(began)
	}
	tried, slowUnits := 0, 0
	for _, kind := range kinds {
		for k, unit := range units {
			if strings.TrimSpace(unit) == "" && len(unit) > 1 {
				continue
			}
			prefix := ""
			if k%3 == 1 {
				prefix = lockSearchWords[rng.Intn(len(lockSearchWords))] + "\n"
			}
			tried++
			t1 := read(kind, unit, prefix, small)
			if t1 < slow {
				continue
			}
			slowUnits++
			if t4 := read(kind, unit, prefix, 4*small); float64(t4) > limit*float64(t1) {
				t1, t4 = read(kind, unit, prefix, small), read(kind, unit, prefix, 4*small)
				if t1 >= slow/2 && float64(t4) > limit*float64(t1) {
					t.Errorf("%s: %q after %q, repeated: %d bytes in %v, %d bytes in %v (x%.1f); a linear reader takes about four times",
						kind, unit, prefix, small, t1, 4*small, t4, float64(t4)/float64(t1))
				}
			}
		}
	}
	t.Logf("%d units read over %d lockfile kinds, %d slow enough at %d bytes to be read at four times the size", tried, len(kinds), slowUnits, small)
	if tried < 20000 {
		t.Errorf("%d units tried: the search has lost its units", tried)
	}
}
