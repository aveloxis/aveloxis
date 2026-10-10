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
	"regexp"
	"strings"
	"testing"
)

// registeredCommandNames is every top-level command, read from the cobra
// tree itself (closing review r3 F8: a scan of Use: strings could not tell
// a subcommand from a top-level command, and a name-keyed parent map
// excused a future top-level command of the same name).
func registeredCommandNames(t *testing.T) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	for _, c := range newRootCmd().Commands() {
		names[c.Name()] = true
	}
	if len(names) < 20 {
		t.Fatalf("command tree has only %d commands — the root builder changed?", len(names))
	}
	return names
}

// registeredSubcommands is every subcommand as "parent sub", from the tree.
func registeredSubcommands() []string {
	var out []string
	for _, c := range newRootCmd().Commands() {
		for _, sub := range c.Commands() {
			out = append(out, c.Name()+" "+sub.Name())
		}
	}
	return out
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
	for _, full := range registeredSubcommands() {
		header := "### `aveloxis " + full + "`"
		if !strings.Contains(docStr, header) {
			t.Errorf("docs/guide/commands.md is missing a %q section under its parent's.", header)
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
