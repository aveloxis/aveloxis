// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import "testing"

// TestProjectionPolicyPerSystem pins the §2 gate values: Apache is clean_fit
// (project issue_event→issues etc.), the kernel public-inbox is forge-less
// (none → Layer-1 only, never synthesize).
func TestProjectionPolicyPerSystem(t *testing.T) {
	systems, err := LoadSystems()
	if err != nil {
		t.Fatal(err)
	}
	if a := systems["apache_ponymail"]; a == nil || !a.ProjectionClean() {
		t.Errorf("apache_ponymail must be clean_fit (ProjectionClean=true); got %+v", a)
	}
	if k := systems["lore_public_inbox"]; k == nil || k.ProjectionClean() {
		t.Errorf("lore_public_inbox must NOT project (forge-less); ProjectionClean must be false")
	}
}
