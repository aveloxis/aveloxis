// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"sort"
	"testing"

	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// What one parsed lockfile becomes in the snapshot (rowsOfLockfile), under
// the vuln_scan_transitive knob. This is the behaviour the source pins on
// scanLockfiles' text held until 0.29.215 (TestScanLockfilesTransitiveGate,
// TestEdgesGatedOnTransitiveKnobAndSameSnapshot), stated as what comes out:
//
//   - knob off: only the resolutions of declared dependencies, each
//     Direct, and no edges (the pre-C1 and pre-C2 write set);
//   - knob on: every entry once per (name, version), Direct exactly for the
//     declared ones, and every edge once;
//   - the inventory row's direct_count is the format's own flags where it
//     has them, else the declared matches.
func TestRowsOfALockfileUnderTheTransitiveKnob(t *testing.T) {
	pl := parsedLockfile{Path: "web/yarn.lock", Result: &LockfileResult{
		Kind: "yarn.lock", Ecosystem: "npm",
		Entries: []LockfileEntry{
			{Name: "express", Version: "4.18.2"},
			{Name: "express", Version: "4.18.2"}, // the same resolution twice
			{Name: "express", Version: "5.0.0"},  // another version of a declared name
			{Name: "qs", Version: "6.11.0"},
			{Name: "body-parser", Version: "1.20.1", Scope: "dev"},
		},
		Edges: []LockfileEdge{
			{ParentName: "express", ParentVersion: "4.18.2", ChildName: "qs", Constraint: "6.11.0"},
			{ParentName: "express", ParentVersion: "4.18.2", ChildName: "qs", Constraint: "6.11.0"},
			{ParentName: "express", ParentVersion: "4.18.2", ChildName: "body-parser", Constraint: "^1"},
		},
	}}
	declared := map[string]bool{pkgid.MatchKey("npm", "express"): true}
	names := func(direct bool, rows []string) []string { sort.Strings(rows); return rows }

	off := rowsOfLockfile(pl, declared, false)
	var offPkgs []string
	for _, p := range off.Packages {
		if !p.Direct || p.LockfilePath != "web/yarn.lock" || p.Ecosystem != "npm" {
			t.Errorf("knob off: %+v is not a direct row of this lockfile", p)
		}
		offPkgs = append(offPkgs, p.PackageName+"@"+p.ResolvedVersion)
	}
	if got := names(true, offPkgs); len(got) != 2 || got[0] != "express@4.18.2" || got[1] != "express@5.0.0" {
		t.Errorf("knob off: packages %v; want the two resolutions of the declared dependency", got)
	}
	if len(off.Edges) != 0 {
		t.Errorf("knob off: %d edges; want none", len(off.Edges))
	}
	if len(off.Inventory) != 1 || off.Inventory[0].EntryCount != 5 || off.Inventory[0].DirectCount != 2 || off.Inventory[0].LockfileKind != "yarn.lock" {
		t.Errorf("knob off: inventory %+v", off.Inventory)
	}

	on := rowsOfLockfile(pl, declared, true)
	got := map[string]bool{}
	for _, p := range on.Packages {
		key := p.PackageName + "@" + p.ResolvedVersion
		if _, dup := got[key]; dup {
			t.Errorf("knob on: %s twice", key)
		}
		got[key] = p.Direct
		if p.PackageName == "body-parser" && p.Scope != "dev" {
			t.Errorf("knob on: the scope of %s is %q", key, p.Scope)
		}
	}
	want := map[string]bool{"express@4.18.2": true, "express@5.0.0": true, "qs@6.11.0": false, "body-parser@1.20.1": false}
	if len(got) != len(want) {
		t.Errorf("knob on: packages %v; want %v", got, want)
	}
	for key, direct := range want {
		if d, ok := got[key]; !ok || d != direct {
			t.Errorf("knob on: %s present %v direct %v; want direct %v", key, ok, d, direct)
		}
	}
	if len(on.Edges) != 2 {
		t.Errorf("knob on: %d edges; want the two distinct ones", len(on.Edges))
	}

	// A format that flags its own direct entries: its count is the flags'.
	pl.Result.DirectKnown = true
	pl.Result.Entries[3].Direct = true
	if inv := rowsOfLockfile(pl, declared, true).Inventory[0]; inv.DirectCount != 1 {
		t.Errorf("a format that flags direct entries: direct_count %d; want 1", inv.DirectCount)
	}
}
