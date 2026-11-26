// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readFileWithin reads one build file through the roster and fails the test if
// that takes longer than limit: a reader that has gone quadratic fails here
// in seconds and does not hang the suite.
func readFileWithin(t *testing.T, name, file, content string, limit time.Duration) Declarations {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, file)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	done := make(chan Declarations, 1)
	go func() { done <- NewWalk(root, nil).Read(path, Options{}) }()
	select {
	case d := <-done:
		return d
	case <-time.After(limit):
		t.Fatalf("%s: %d bytes of %s were not read in %v", name, len(content), file, limit)
		return Declarations{}
	}
}

// A statement that does not end (a line that ends in +, a bracket never
// closed) is begun again at every line of it. Each is read in constant time
// once it is longer than a value, a coordinate or a map can be
// (gradleValueTokens, gradleMapTokens). Round 7: 573 KB of the first shape
// took 269 s.
func TestAStatementThatDoesNotEndIsNotReadAgainForEveryLineOfIt(t *testing.T) {
	const size = 400 << 10
	limit := 10 * time.Second
	if raceEnabled {
		limit = 60 * time.Second
	}
	// The two map shapes cost less a line and are four times the size: in
	// the square they are a minute, read as they should be a fraction of a
	// second.
	for name, c := range map[string]struct {
		file, head, unit, tail string
		size                   int
	}{
		"a declaration by extra continued by +": {"build.gradle.kts", "", "val x by extra a +\n", "", size},
		"a value continued by +":                {"build.gradle", "", "ext.x = a +\n", "", size},
		"blocks that each open a list":          {"build.gradle", "", "{def x=[", "]", 4 * size},
		"blocks that each open a mapOf":         {"build.gradle", "", "{def x=mapOf(", "", 4 * size},
		"mavenBom lines continued by +":         {"build.gradle", "dependencyManagement { imports {\n", "mavenBom 'g:a:1' +\n", "", size},
	} {
		readFileWithin(t, name, c.file, c.head+strings.Repeat(c.unit, c.size/len(c.unit))+c.tail, limit)
	}
	// A value that doubles at every line is not built: it is longer than a
	// value can be at the twelfth, and the last is not known. (Unbounded,
	// the thirtieth is a gibibyte.)
	var doubling strings.Builder
	doubling.WriteString("def x0 = '1'\n")
	for i := 1; i <= 30; i++ {
		doubling.WriteString("def x" + strconv.Itoa(i) + " = x" + strconv.Itoa(i-1) + " + x" + strconv.Itoa(i-1) + "\n")
	}
	doubling.WriteString("dependencies {\n  implementation \"g:small:$x3\"\n  implementation \"g:huge:$x30\"\n}\n")
	got := map[string]string{}
	for _, d := range readFileWithin(t, "a value that doubles at every line", "build.gradle", doubling.String(), limit).Deps {
		got[d.Name] = d.Version
	}
	if got["g:small"] != "11111111" || got["g:huge"] != "" {
		t.Errorf("g:small at %q (want eight ones) and g:huge at a version of %d bytes (want none)", got["g:small"], len(got["g:huge"]))
	}
}

// The bounds are on what is ONE value: an honest value, map and coordinate
// at the size a build writes them are read as before.
func TestTheStatementBoundsLeaveHonestValuesAlone(t *testing.T) {
	var parts, entries []string
	for i := 0; i < 400; i++ {
		parts = append(parts, `"1"`)
		entries = append(entries, "k"+strconv.Itoa(i)+": '"+strconv.Itoa(i)+".0'")
	}
	build := "ext.long = " + strings.Join(parts, " + ") + "\n" +
		"ext.versions = [\n  " + strings.Join(entries, ",\n  ") + "\n]\n" +
		"dependencies {\n  implementation \"g:a:$long\"\n  implementation \"g:b:${versions.k399}\"\n  implementation 'g:' + 'c' + ':' + '3.0'\n}\n"
	got := map[string]string{}
	for _, d := range readFileWithin(t, "an honest build", "build.gradle", build, 10*time.Second).Deps {
		got[d.Name] = d.Version
	}
	if got["g:a"] != strings.Repeat("1", 400) || got["g:b"] != "399.0" || got["g:c"] != "3.0" {
		t.Errorf("read g:a %q (want 400 ones), g:b %q (want 399.0), g:c %q (want 3.0)", got["g:a"], got["g:b"], got["g:c"])
	}
	// One token past the bound on a value's parts is not a value.
	var over []string
	for i := 0; i <= gradleValueTokens/2; i++ {
		over = append(over, `""`)
	}
	build = "ext.v = " + strings.Join(over, " + ") + " + \"7\"\ndependencies {\n  implementation \"g:a:$v\"\n}\n"
	for _, d := range readFileWithin(t, "a value of more parts than its bound", "build.gradle", build, 10*time.Second).Deps {
		if d.Name == "g:a" && d.Version != "" {
			t.Errorf("a value of %d parts was read as %q; gradleValueTokens is %d", len(over)+1, d.Version, gradleValueTokens)
		}
	}
}
