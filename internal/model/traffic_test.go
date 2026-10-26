// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "testing"

// TestTrafficKindValues pins the stored spelling of the two traffic
// kinds. The traffic_kind column of repo_traffic_windows and
// repo_traffic_weeks accepts exactly these strings (a CHECK
// constraint), and the REST endpoint returns them as written, so a
// rename here would break writes and API consumers.
func TestTrafficKindValues(t *testing.T) {
	cases := []struct {
		name string
		kind TrafficKind
		want string
	}{
		{"views", TrafficKindViews, "views"},
		{"clones", TrafficKindClones, "clones"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(tc.kind); got != tc.want {
				t.Errorf("TrafficKind = %q, want %q", got, tc.want)
			}
		})
	}
}
