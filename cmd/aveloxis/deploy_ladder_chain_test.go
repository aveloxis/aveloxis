// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// ladderChainFrom is the first release from which every ladder carries its
// numeric predecessor's notes (0.29.58–0.29.67 were written apart, before
// withStartNote; their digests are frozen).
const ladderChainFrom = "0.29.68"

// TestEachLadderCarriesItsPredecessorsNotes (0.29.144 review F5): a fleet
// skipping releases reads only the newest ladder, so each must carry every
// earlier note — withStartNote(prev, note) prepends to the previous ladder's
// start step. Two releases cut from the same base (0.29.143 on PR #257,
// which superseded #255, and 0.29.144 on PR #258, both chained from 0.29.142)
// passed apart and broke together: once both were in the map, the later one
// had to be rechained onto the earlier (done at the merge of main into PR
// #258), or a fleet going from 0.29.142 straight to 0.29.144 never reads
// 0.29.143's note.
func TestEachLadderCarriesItsPredecessorsNotes(t *testing.T) {
	versions := make([]string, 0, len(deployChecklists))
	for v := range deployChecklists {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return db.CompareVersionish(versions[i], versions[j]) < 0 })
	startNote := func(v string) string {
		t.Helper()
		var desc string
		n := 0
		for _, s := range deployChecklists[v] {
			if s.cmd == "aveloxis start all" {
				desc = s.desc
				n++
			}
		}
		if n != 1 {
			t.Fatalf("ladder %s has %d start steps, want 1", v, n)
		}
		return desc
	}
	checked := 0
	for i := 1; i < len(versions); i++ {
		v, prev := versions[i], versions[i-1]
		if db.CompareVersionish(v, ladderChainFrom) < 0 {
			continue
		}
		checked++
		if !strings.HasSuffix(startNote(v), startNote(prev)) {
			t.Errorf("ladder %s does not carry ladder %s's notes — build it as withStartNote(the %s ladder, note)", v, prev, prev)
		}
	}
	// Guard the denominator: 78 ladders at 0.29.145 (every release from
	// 0.29.68 to 0.29.145), so a broken ordering or filter cannot pass by
	// checking nothing.
	srctest.MinCount(t, "ladders checked for their chain", checked, 78)
}
