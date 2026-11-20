// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// Review round 6: the Gradle reader finds a statement from an index of the
// file's tokens, made once (gradleIndex), where it read to the statement's
// end again at every restart. These are the two readings compared: the index
// against gradleStatement, the reading token by token that it replaced, for
// EVERY position of token streams of every kind of disorder, since a
// statement can begin anywhere the walk restarts.

// r6Stream is a random stream of tokens: brackets and braces that do not
// balance, line ends after and not after the punctuation that continues a
// statement, words and strings.
func r6Stream(rng *rand.Rand, n int) []gradleTok {
	puncts := []string{"(", ")", "[", "]", "{", "}", ",", ":", "+", "=", "+=", ".", "-", "?"}
	toks := make([]gradleTok, n)
	for i := range toks {
		switch k := rng.Intn(10); {
		case k < 2:
			toks[i] = gradleTok{kind: 'n'}
		case k < 6:
			toks[i] = gradleTok{kind: 'p', text: puncts[rng.Intn(len(puncts))]}
		case k < 8:
			toks[i] = gradleTok{kind: 'i', text: []string{"a", "implementation", "if", "ext.v", "from", "apply", "version", "id"}[rng.Intn(8)]}
		default:
			toks[i] = gradleTok{kind: 's', text: []string{"g:a:1", "", "x"}[rng.Intn(3)]}
		}
	}
	return toks
}

func r6SameParts(a, b [][]gradleTok) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) || (len(a[i]) > 0 && !reflect.DeepEqual(a[i], b[i])) {
			return false
		}
	}
	return true
}

func TestTheStatementIndexIsTheStatementReader(t *testing.T) {
	rng := rand.New(rand.NewSource(20261120))
	compared := 0
	check := func(toks []gradleTok) {
		b := &gradleBuild{idx: newGradleIndex(toks)}
		for i := range toks {
			want, wantNext := gradleStatement(toks, i)
			got, gotNext := b.statement(toks, i)
			compared++
			if gotNext != wantNext || len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
				t.Fatalf("the statement at %d of %v:\n the index gives %v, next %d\n the reader gives %v, next %d", i, toks, got, gotNext, want, wantNext)
			}
			// What the index hands out, and any slice of it, says where it
			// is in the file: that is how its readers ask the index about it.
			if len(got) > 1 {
				if at, ok := b.idx.place(got[1:]); !ok || at != int(b.idx.pos[i])+1 {
					t.Fatalf("the statement at %d: a slice of it is placed at %d (%v), want %d", i, at, ok, int(b.idx.pos[i])+1)
				}
			}
			// And its readers answer from the index as they did by reading it.
			if u, w := b.unparen(got), gradleUnparen(want); len(u) != len(w) || (len(w) > 0 && !reflect.DeepEqual(u, w)) {
				t.Fatalf("unparen of the statement at %d of %v: %v, want %v", i, toks, u, w)
			}
			gp, _ := b.parts(got, 0)
			if wp := gradleSplit(want); !r6SameParts(gp, wp) {
				t.Fatalf("the parts of the statement at %d of %v: %v, want %v", i, toks, gp, wp)
			}
			if two, more := b.parts(got, 2); more != (len(gp) > 2) || (!more && !r6SameParts(two, gp)) || (more && !r6SameParts(two, gp[:2])) {
				t.Fatalf("the first two parts of the statement at %d of %v: %v (more %v), of %v", i, toks, two, more, gp)
			}
			// A line of a plugins block: its id, its version, whether it is
			// applied.
			with := &gradleBuild{idx: b.idx, own: map[string]string{}, conflicted: map[string]bool{}, reads: map[string]gradleRead{}}
			without := &gradleBuild{own: map[string]string{}, conflicted: map[string]bool{}, reads: map[string]gradleRead{}}
			with.plugin(toks, i)
			without.plugin(toks, i)
			if !reflect.DeepEqual(with.plugins, without.plugins) {
				t.Fatalf("the plugin read at %d of %v: %v from the index, %v by reading", i, toks, with.plugins, without.plugins)
			}
			var wf [][]gradleTok
			for _, part := range gradleSplit(want) {
				if len(part) > 0 && part[0].kind == 'i' && part[0].text == "from" {
					wf = append(wf, part)
				}
			}
			if gf := b.fromParts(got, nil); !r6SameParts(gf, wf) {
				t.Fatalf("the from-parts of the statement at %d of %v: %v, want %v", i, toks, gf, wf)
			}
		}
	}
	for n := 0; n <= 12; n++ {
		for k := 0; k < 300; k++ {
			check(r6Stream(rng, n))
		}
	}
	for k := 0; k < 300; k++ {
		check(r6Stream(rng, 40+rng.Intn(200)))
	}
	// And streams from text, the hostile shapes among them.
	for _, text := range []string{
		strings.Repeat("{y(", 200), strings.Repeat("}y(", 200), strings.Repeat("a,\n", 200), strings.Repeat("a = 'x' +\n", 100),
		strings.Repeat("} a (", 150) + strings.Repeat(")", 150), strings.Repeat("x {\n", 200), "dependencies {\n" + strings.Repeat("if ( {\n", 200),
		"dependencies {\n  implementation 'g:a:1',\n    'g:b:2'\n  api(\"g:c:3\") {\n    exclude group: 'x'\n  }\n}\next {\n  v = [a: '1',\n    b: '2']\n}\n",
		")))]]] a = 1\n((( b = 2\n", "\n\n\n", "",
	} {
		check(gradleTokens(text))
	}
	if compared < 50000 {
		t.Errorf("only %d statements compared", compared)
	}
	// Tokens that are not the file's are not placed in it, though they are
	// as many and have as much room as a part of it has.
	own := gradleTokens("a = b\nc = d\ne = f\n")
	x := newGradleIndex(own)
	for at := 0; at < len(x.flat); at++ {
		foreign := make([]gradleTok, 1, cap(x.flat)-at)
		foreign[0] = x.flat[at]
		if _, ok := x.place(foreign); ok {
			t.Fatalf("tokens that are a copy were placed at %d of the file's", at)
		}
		if got, ok := x.place(x.flat[at : at+1]); !ok || got != at {
			t.Fatalf("the file's token %d was placed at %d (%v)", at, got, ok)
		}
	}
	// set(name, value) takes two arguments and no more.
	three := gradleTokens("ext {\n  set('n', '5', '6')\n  set('m', '7')\n}\n")
	if b := readGradleWith(three, newGradleIndex(three), nil, nil); b.own["n"] != "" || b.own["m"] != "7" {
		t.Errorf("set with three arguments assigned %q, and with two %q; want nothing and 7", b.own["n"], b.own["m"])
	}
	// A slice that is not the file's own tokens is read the slow way, and
	// rightly: the index is of the whole file.
	toks := gradleTokens("a = 1\nb = 2\nc = 3\n")
	b := &gradleBuild{idx: newGradleIndex(toks)}
	if got, next := b.statement(toks[4:], 0); len(got) != 3 || got[0].text != "b" || next != 3 {
		t.Errorf("a statement of a part of the tokens: %v, next %d", got, next)
	}
}

// The walk's answer for a block (is a statement here a declaration, an
// entry of a repositories block; what do the blocks around make of a
// repository) is worked out when the block is entered. It is what asking the
// whole stack gave: these are the two questions as they were asked.
func r6InDependencies(stack []string) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		name := stack[i]
		if gradleControlFlow[name] {
			continue
		}
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		return name == "dependencies"
	}
	return false
}

func r6InRepositories(stack []string) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		name := stack[i]
		if gradleControlFlow[name] {
			continue
		}
		return name == "repositories" || name == "forRepository"
	}
	return false
}

func TestABlocksLevelIsWhatTheStackAnswered(t *testing.T) {
	names := []string{"", "dependencies", "project.dependencies", "repositories", "forRepository", "exclusiveContent", "if", "else", "for", "try",
		"pluginManagement", "buildscript", "allprojects", "subprojects", "dependencyResolutionManagement", "constraints", "android", "x.dependencies.y"}
	rng := rand.New(rand.NewSource(7))
	for k := 0; k < 20000; k++ {
		var stack []string
		var level gradleLevel
		for d, depth := 0, rng.Intn(9); d < depth; d++ {
			name := names[rng.Intn(len(names))]
			stack = append(stack, name)
			level = level.enter(name)
		}
		plugins, shared := false, false
		for _, name := range stack {
			switch name {
			case "pluginManagement", "buildscript":
				plugins = true
			case "allprojects", "subprojects", "dependencyResolutionManagement":
				shared = true
			}
		}
		want := gradleLevel{inDeps: r6InDependencies(stack), inRepos: r6InRepositories(stack), forPlugins: plugins, repoShared: shared}
		if level != want {
			t.Fatalf("in %v: the level is %+v, the stack answers %+v", stack, level, want)
		}
	}
}

// r6GradleFile is a random build file: mostly lines a build has, some of
// them broken (braces and brackets that do not balance, braces in strings
// and comments, calls over several lines, lines that end in a comma, a
// colon or an operator), and now and then the shapes that made the reader
// read a statement many times over.
func r6GradleFile(rng *rand.Rand) string {
	pick := func(s []string) string { return s[rng.Intn(len(s))] }
	lines := []string{
		"dependencies {", "}", "{", "ext {", "plugins {", "repositories {", "subprojects {", "allprojects {", "buildscript {", "pluginManagement {",
		"dependencyManagement {", "imports {", "constraints {", "if (x) {", "} else {", "exclusiveContent {", "forRepository {", "object V {", "android {",
		"  implementation 'g:a:1.0'", "  implementation \"g:b:$v\"", "  api group: 'g', name: 'c', version: v", "  implementation('g:d:2') {", "    exclude group: 'x'",
		"  testImplementation platform('g:bom:1')", "  implementation libs.x", "  implementation project(':p')", "  add('implementation', 'g:e:3')",
		"  implementation 'g:f:1',", "    'g:h:2'", "  implementation(", "    'g:i:4'", "  )", "  runtimeOnly files('a.jar')",
		"v = '1.2'", "ext.v = '2.0'", "def w = \"${v}.1\"", "val k by extra(\"3\")", "ext['m'] = '4'", "set('n', '5')", "extra.set(\"o\", v)", "w = v +", "  '-x'",
		"ext.vs = [a: '1',", "  b: '2']", "ext.mm = mapOf(\"a\" to \"1\", \"b\" to v)", "const val c = \"6\"", "v = '9'",
		"apply from: 'a.gradle'", "apply plugin: 'java', from: \"$rootDir/b.gradle\"", "apply from: rootProject.file('c.gradle')", "apply(from = \"d.gradle\")", "apply from: 'e.gradle',",
		"  id 'java'", "  id 'org.x' version '1.0'", "  id(\"org.y\") version \"2.0\" apply false", "  id 'org.z' version \"$v\"", "  id 'org.w',",
		"  mavenCentral()", "  maven { url 'https://r.example/m2' }", "  maven {", "    url = uri(\"https://s.example/m2\")", "  google()", "  maven(url = \"https://t.example\")", "  mavenLocal()",
		"  mavenBom 'g:bom:5'", "  mavenBom \"g:bom2:$v\"",
		"// a comment with { and (", "/* { ( */", "println '{ ( [ } ) ]'", "x = \"}{)(\"", "task t(type: Copy) {", "foo(bar { baz() })", "a.b.c { d = 1 }",
		")", "(", "]", "[", "},", "{(", ")}", ",", ":", "=", "+", "", "", "",
		// Whole blocks, so that most files declare something.
		"dependencies {\n  implementation 'g:a:1.0'\n  api \"g:b:$v\"\n  implementation 'g:c:1',\n    'g:d:2'\n  testImplementation platform('g:bom:1')\n  implementation libs.x\n}",
		"dependencies {\n  implementation('g:e:2') {\n    exclude group: 'x'\n  }\n  if (x) {\n    runtimeOnly 'g:f:3'\n  }\n  constraints {\n    implementation 'g:k:9'\n  }\n}",
		"subprojects {\n  repositories {\n    mavenCentral()\n    maven { url \"https://u.example/$v\" }\n  }\n  dependencies {\n    implementation platform(\"g:p:$v\")\n  }\n}",
		"plugins {\n  id 'java'\n  id 'org.springframework.boot' version '3.2.0'\n  id 'io.spring.dependency-management' version \"$v\"\n}",
		"buildscript {\n  ext.kv = '1.9'\n  repositories { google() }\n  dependencies {\n    classpath \"g:plugin:$kv\"\n  }\n}",
		"dependencyManagement {\n  imports {\n    mavenBom 'g:bom:5'\n    mavenBom \"g:bom2:$v\"\n  }\n}",
		"foo(bar {\n  dependencies {\n    implementation 'g:inparens:1'\n  }\n  ext.inparens = '1'\n})",
	}
	var sb strings.Builder
	switch rng.Intn(12) {
	case 0:
		sb.WriteString(strings.Repeat(pick([]string{"{y(", "}y(", "a,\n", "} a (", "{apply(", "{set('a',b,", "apply from: 'a', from: 'b',\n", "{a=[a:b,", "id 'x',\n"}), 20+rng.Intn(60)))
	case 1:
		sb.WriteString("plugins {\n" + strings.Repeat("id 'x' version 'y',\n", 5+rng.Intn(20)))
	}
	for i, n := 0, rng.Intn(40); i < n; i++ {
		sb.WriteString(pick(lines))
		if rng.Intn(12) != 0 {
			sb.WriteString("\n")
		} else {
			sb.WriteString(" ")
		}
	}
	return sb.String()
}

func r6InOrderOnce(list []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range list {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// A build file is read to the same result with its index as without: every
// declaration, platform, repository, plugin and assignment, and the scripts
// it applies in the order it first names each. (Without the index every
// statement is read token by token to its end, as before; a script named
// again by a statement that begins inside another was listed again, which
// applying skips.)
func TestAGradleFileIsReadTheSameWithAndWithoutItsIndex(t *testing.T) {
	rng := rand.New(rand.NewSource(20261121))
	outer := (*gradleVars)(nil).push(map[string]string{"v": "7.7", "libs.versions.k": "1"}, nil)
	libs := &gradleLibs{own: map[string][]gradleDecl{"libs.x": {{group: "g", artifact: "x", version: "1", written: "g:x:1"}}}}
	const files = 4000
	decls, statements := 0, 0
	for k := 0; k < files; k++ {
		content := r6GradleFile(rng)
		toks := gradleTokens(content)
		with := readGradleWith(toks, newGradleIndex(toks), outer, libs)
		without := readGradleWith(toks, nil, outer, libs)
		decls += len(with.decls)
		statements += len(toks)
		wOwn, wGone := with.result()
		oOwn, oGone := without.result()
		for what, pair := range map[string][2]any{
			"declarations": {with.decls, without.decls}, "platforms": {with.boms, without.boms}, "repositories": {with.repos, without.repos},
			"plugins": {with.plugins, without.plugins}, "assignments": {wOwn, oOwn}, "names assigned twice": {wGone, oGone},
			"applied scripts": {r6InOrderOnce(with.applies), r6InOrderOnce(without.applies)},
			"dependencies":    {with.libyearDeps(), without.libyearDeps()},
		} {
			if !reflect.DeepEqual(pair[0], pair[1]) {
				t.Fatalf("file %d: the %s differ\n with the index:    %+v\n without the index: %+v\n%s", k, what, pair[0], pair[1], content)
			}
		}
		// What the reading took from outside: nothing the other did not.
		for name, r := range with.reads {
			if o, ok := without.reads[name]; !ok || o != r {
				t.Fatalf("file %d: with the index the reading looked up %q (%+v); without, %+v (%v)\n%s", k, name, r, o, ok, content)
			}
		}
	}
	if decls < files || statements < 50*files {
		t.Errorf("%d declarations and %d tokens over %d files: too few to compare anything", decls, statements, files)
	}
}

// What the linear reading is for: a file of a shape that made the reader
// read every statement to the end of the file, at a size where that took
// minutes, is read in a time that says it did not. Counted, where a clock
// would be a matter of the machine: the tokens handed to the statement
// readers are no more than a small multiple of the file's.
func TestAStatementThatBeginsAgainInsideItselfIsNotReadAgain(t *testing.T) {
	for name, unit := range map[string]string{
		"an opening brace and an unclosed call": "{y(",
		"a line that ends in a comma":           "a,\n",
		"a call inside a closing brace":         "} a (",
		"set( inside braces":                    "{set('a',b,",
		"apply( inside braces":                  "{apply(",
		"apply with two from parts, per line":   "apply from: 'a', from: 'b',\n",
		"a plugin line that ends in a comma":    "id 'x' version 'y',\n",
	} {
		content := strings.Repeat(unit, 20000)
		if name == "a plugin line that ends in a comma" {
			content = "plugins {\n" + content
		}
		toks := gradleTokens(content)
		b := readGradleWith(toks, newGradleIndex(toks), nil, nil)
		// With two from parts a line, every line named 'b' again for every
		// line after it: four hundred million names. Each is listed for the
		// statement that first reads it, and once more per line for the
		// line's own.
		if len(b.applies) > 3*20000 {
			t.Errorf("%s: %d applied scripts listed for 20,000 lines", name, len(b.applies))
		}
	}
}
