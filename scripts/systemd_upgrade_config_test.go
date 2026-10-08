// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The systemd upgrade block runs from the source checkout, which has no
// aveloxis.json: an aveloxis command there without -c runs on the built-in
// defaults and migrates, stamps or acknowledges some other database (final
// whole-PR review D1). Every aveloxis command in the block except
// `aveloxis version` must pass the units' config.
func TestSystemdUpgradeBlockPassesTheUnitsConfig(t *testing.T) {
	doc := srctest.Read(t, "docs/guide/running-as-a-service.md")
	_, after, ok := strings.Cut(doc, "## Upgrading under systemd")
	if !ok {
		t.Fatal(`running-as-a-service.md has no "## Upgrading under systemd" section`)
	}
	_, block, ok := strings.Cut(after, "```bash\n")
	if !ok {
		t.Fatal("the upgrade section has no bash block")
	}
	block, _, _ = strings.Cut(block, "```")
	seen := 0
	for _, part := range aveloxisCommands(block) {
		seen++
		if !strings.Contains(part, `-c "$CFG"`) {
			t.Errorf("%q must pass the units' config (-c \"$CFG\")", strings.TrimSpace(part))
		}
	}
	if seen < 3 {
		t.Fatalf("found %d aveloxis commands in the block; want at least deploy-checklist, migrate and ack-deploy", seen)
	}
	if !strings.Contains(block, "CFG=/etc/aveloxis/aveloxis.json") {
		t.Error("the block must set CFG to the units' config path")
	}
}

// aveloxisCommands returns each `aveloxis` command in a bash block except
// `aveloxis version`, split on `&&`, with shell comments removed by the
// shell's own rule (PR #226 Copilot review 5458877691): a `#` inside quotes
// or glued to a word is literal, so cutting at any `#` could truncate a
// valid command and report a missing -c.
func aveloxisCommands(block string) []string {
	var cmds []string
	for _, line := range strings.Split(block, "\n") {
		for _, part := range strings.Split(srctest.StripShellComment(line), "&&") {
			f := strings.Fields(part)
			if len(f) < 2 || f[0] != "aveloxis" || f[1] == "version" {
				continue
			}
			cmds = append(cmds, part)
		}
	}
	return cmds
}

// The pin's own parser keeps a quoted `#` inside the command, so a `-c`
// after it is still seen (L10 round 1 on 0.29.80: the self-check now runs
// the function the pin uses, not the helper beside it).
func TestSystemdUpgradeBlockCommentRuleIsTheShells(t *testing.T) {
	line := `aveloxis deploy-checklist --note "step #3" -c "$CFG"   # read it`
	if got := aveloxisCommands(line); len(got) != 1 || !strings.Contains(got[0], `-c "$CFG"`) {
		t.Errorf("a quoted # must not end the command: %q", got)
	}
	if cut, _, _ := strings.Cut(line, "#"); strings.Contains(cut, `-c "$CFG"`) {
		t.Error("the corpus line must defeat a plain cut at the first #, or this case proves nothing")
	}
}
