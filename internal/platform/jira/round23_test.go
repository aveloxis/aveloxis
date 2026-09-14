// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package jira

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestJira400OnlyDisablesOnProjectNotFound (Copilot round 23): a
// "project does not exist" 400 is a dead key (ClassSkip → the worker
// disables); any OTHER 400 (a JQL field/compat error) must NOT disable
// the whole fleet — it classifies ClassFatal (recorded, not disabled).
func TestJira400OnlyDisablesOnProjectNotFound(t *testing.T) {
	dead := &invalidQueryError{detail: "The value 'ZZZ' does not exist for the field 'project'.", deadKey: jiraProjectNotFound("The value 'ZZZ' does not exist for the field 'project'.")}
	if dead.Class() != platform.ClassSkip {
		t.Error("a project-not-found 400 must classify ClassSkip (dead key → disable)")
	}
	field := &invalidQueryError{detail: "Field 'customfield_1' does not exist or you do not have permission to view it.", deadKey: jiraProjectNotFound("Field 'customfield_1' does not exist or you do not have permission to view it.")}
	if field.Class() != platform.ClassFatal {
		t.Error("a non-project 400 (JQL field error) must NOT be ClassSkip — it must not disable every registered project (round 23)")
	}
	if jiraProjectNotFound("some unrelated 400") {
		t.Error("jiraProjectNotFound must only match the project-does-not-exist message")
	}
}
