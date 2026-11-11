// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// fleetSummaryLead opens the sentence of a release note that tells a fleet on
// the previous release which steps it needs: the one an operator reads instead
// of the whole note.
const fleetSummaryLead = "A fleet already running "

// fleetSummarySentence returns text's sentence that begins with
// fleetSummaryLead, up to its first ";" or ". ".
func fleetSummarySentence(t *testing.T, where, text string) string {
	t.Helper()
	i := strings.Index(text, fleetSummaryLead)
	if i < 0 {
		t.Fatalf("%s: no sentence starting %q", where, fleetSummaryLead)
	}
	s := text[i:]
	if j := strings.Index(s, ";"); j >= 0 {
		s = s[:j]
	}
	if j := strings.Index(s, ". "); j >= 0 {
		s = s[:j]
	}
	return s
}

// ownStartNote is version's own note on its ladder's start step: the start
// step minus its predecessor's (withStartNote prepends each note to the
// previous ladder's start step), the predecessor being the highest version
// below it in deployChecklists. Read from the map and cut by length, not by
// a version string, so the rechain onto 0.29.143 at the PR #257 merge, an
// alias entry (0.29.103 -> the 0.29.102 ladder) and the "0.29.NN (...):"
// note form keep the bound.
func ownStartNote(t *testing.T, version string) string {
	t.Helper()
	var pred string
	for v := range deployChecklists {
		if db.CompareVersionish(v, version) < 0 && (pred == "" || db.CompareVersionish(v, pred) > 0) {
			pred = v
		}
	}
	startOf := func(v string) string {
		for _, s := range deployChecklists[v] {
			if s.cmd == "aveloxis start all" {
				return s.desc
			}
		}
		return ""
	}
	start, prev := startOf(version), startOf(pred)
	if pred == "" || prev == "" || len(start) <= len(prev) || !strings.HasSuffix(start, prev) {
		t.Fatalf("the %s ladder's start step is not its own note prepended to %s's start step: %.80q", version, pred, start)
	}
	own := start[:len(start)-len(prev)]
	if !strings.HasPrefix(own, version+":") && !strings.HasPrefix(own, version+" (") {
		t.Fatalf("the %s ladder's own note does not begin with its version: %.80q", version, own)
	}
	return own
}

// TestV029144SummaryNamesUpgradeTools (Copilot's review of PR #258): 0.29.144
// requires `aveloxis upgrade-tools` while the processes are stopped, or serve
// keeps the old scc until the monthly check is due. The note's closing
// summary said a 0.29.142 fleet needs "only the stop, the migrate and this
// start", and the upgrading guide's entry the same; an operator reading the
// summary skipped the one step this release exists for. Both summaries name
// it, and so does the guide's table of operator-run heals (the L10 round on
// that fix): its rule is "run every row whose Since names a release newer than
// yours", and the upgrade-tools row said v0.23.6, so a fleet coming from
// 0.29.142 skipped it.
func TestV029144SummaryNamesUpgradeTools(t *testing.T) {
	doc := srctest.Read(t, "docs/getting-started/upgrading.md")
	entry := doc
	i := strings.Index(entry, "**0.29.144 ")
	if i < 0 {
		t.Fatal("docs/getting-started/upgrading.md has no 0.29.144 entry")
	}
	entry = entry[i:]
	if j := strings.Index(entry, "\n\n"); j >= 0 {
		entry = entry[:j]
	}
	for where, text := range map[string]string{
		"the 0.29.144 deploy checklist note":   ownStartNote(t, "0.29.144"),
		"the upgrading guide's 0.29.144 entry": entry,
	} {
		s := fleetSummarySentence(t, where, text)
		if !strings.Contains(s, "upgrade-tools") {
			t.Errorf("%s: its summary for a fleet on the previous release leaves out `aveloxis upgrade-tools`: %q", where, s)
		}
		if strings.Contains(s, "only") {
			t.Errorf("%s: its summary still says the fleet needs \"only\" some steps: %q", where, s)
		}
	}
	const row = "| 1 | `aveloxis upgrade-tools` |"
	i = strings.Index(doc, row)
	if i < 0 {
		t.Fatalf("the operator-run heals table has no row %q", row)
	}
	cells := strings.Split(strings.SplitN(doc[i:], "\n", 2)[0], "|")
	if len(cells) < 6 {
		t.Fatalf("the upgrade-tools heal row has %d cells, want Order, Command, Since, Repairs, When", len(cells)-2)
	}
	if since := cells[3]; !strings.Contains(since, "0.29.144") {
		t.Errorf("the upgrade-tools heal row's Since %q does not name 0.29.144: a fleet coming from 0.29.142 skips it by the table's rule", strings.TrimSpace(since))
	}
}
