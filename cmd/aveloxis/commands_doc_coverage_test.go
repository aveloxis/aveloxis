// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.35 tripwire: docs/guide/commands.md calls itself the "complete
// reference for every Aveloxis CLI command" — the 2026-07-08 audit
// found 11 registered commands (including `api` and `sbom`) with zero
// mentions. This test walks every cobra `Use:` declaration in the
// package and requires a `## aveloxis <name>` section in commands.md,
// and conversely that every documented command still exists — so the
// reference can neither silently fall behind nor advertise removed
// commands.

package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func registeredCommandNames(t *testing.T) map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	useRe := regexp.MustCompile(`Use:\s*"([a-z-]+)`)
	names := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range useRe.FindAllStringSubmatch(string(src), -1) {
			if m[1] == "aveloxis" { // root command
				continue
			}
			names[m[1]] = true
		}
	}
	if len(names) < 20 {
		t.Fatalf("command scan found only %d commands — scanner broke?", len(names))
	}
	return names
}

func TestCommandsDocCoversEveryRegisteredCommand(t *testing.T) {
	doc, err := os.ReadFile("../../docs/guide/commands.md")
	if err != nil {
		t.Fatalf("read commands.md: %v", err)
	}
	docStr := string(doc)

	for name := range registeredCommandNames(t) {
		header := "## `aveloxis " + name + "`"
		if !strings.Contains(docStr, header) {
			t.Errorf("docs/guide/commands.md is missing a %q section — it claims to be "+
				"the complete reference for every CLI command.", header)
		}
	}
}

func TestCommandsDocHasNoGhostCommands(t *testing.T) {
	doc, err := os.ReadFile("../../docs/guide/commands.md")
	if err != nil {
		t.Fatalf("read commands.md: %v", err)
	}
	headerRe := regexp.MustCompile("(?m)^## `aveloxis ([a-z-]+)`")
	registered := registeredCommandNames(t)
	for _, m := range headerRe.FindAllStringSubmatch(string(doc), -1) {
		if !registered[m[1]] {
			t.Errorf("commands.md documents `aveloxis %s`, which is not a registered "+
				"command — remove the section or fix the name.", m[1])
		}
	}
}
