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
	for _, line := range strings.Split(block, "\n") {
		cmd, _, _ := strings.Cut(line, "#")
		for _, part := range strings.Split(cmd, "&&") {
			f := strings.Fields(part)
			if len(f) < 2 || f[0] != "aveloxis" || f[1] == "version" {
				continue
			}
			seen++
			if !strings.Contains(part, `-c "$CFG"`) {
				t.Errorf("%q must pass the units' config (-c \"$CFG\")", strings.TrimSpace(part))
			}
		}
	}
	if seen < 3 {
		t.Fatalf("found %d aveloxis commands in the block; want at least deploy-checklist, migrate and ack-deploy", seen)
	}
	if !strings.Contains(block, "CFG=/etc/aveloxis/aveloxis.json") {
		t.Error("the block must set CFG to the units' config path")
	}
}
