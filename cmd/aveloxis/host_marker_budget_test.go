// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// db.MaxHostMarkerBytes is derived from the LONGEST component prefix,
// because the marker is prefix-independent (checkBlockersFrom matches
// `application_name LIKE 'aveloxis-%'` across every prefix while
// passing ONE marker parameter). That derivation lives in internal/db,
// which cannot see the component list — so the arithmetic is proved
// here, where the list lives.
//
// A future component whose name pushes its prefix past the budget must
// fail the build rather than silently shorten every host's marker: the
// marker is what the this-host verdict rests on, and changing it
// fleet-wide on a name change is exactly the kind of silent
// reclassification round 14 exists to remove.
func TestEveryComponentPrefixFitsTheHostMarkerBudget(t *testing.T) {
	const pgCeiling = 63 // NAMEDATALEN-1, PostgreSQL's application_name ceiling
	for _, comp := range append(append([]string{}, allComponents...), scancodeWorkerComponent) {
		prefix := componentAppNamePrefix(comp)
		need := len(prefix) + len(db.AppNameHostSep) + db.MaxHostMarkerBytes
		if need > pgCeiling {
			t.Errorf("component %q tags as %q (%d bytes); with the %d-byte host-marker budget that needs %d bytes, "+
				"over PostgreSQL's %d ceiling. Either shorten the component name or lower db.MaxHostMarkerBytes — "+
				"but lowering it changes the marker of every host whose name exceeds the new budget, so the fleet "+
				"must be restarted together.", comp, prefix, len(prefix), db.MaxHostMarkerBytes, need, pgCeiling)
		}
	}
}
