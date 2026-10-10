// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package capacity

import "testing"

// The aveloxis.json word decides where a quota comes from (operator
// 2026-10-10): WEB → the stored row (the shipped default until an
// administrator saves one); DEFAULT → the shipped value, enforced;
// SHADOW → the shipped value, observed; OFF → not applied.
func TestEffectiveQuota(t *testing.T) {
	stored := &Stored{Allowed: 2500, Mode: Enforce}
	shipped := Shipped[QuotaRequestsPerHour]
	cases := []struct {
		src       Source
		stored    *Stored
		wantAllow int
		wantMode  Mode
		wantEdit  bool
	}{
		{SourceWeb, stored, 2500, Enforce, true},
		{SourceWeb, nil, shipped.Allowed, shipped.Mode, true},
		{SourceDefault, stored, shipped.Allowed, Enforce, false},
		{SourceShadow, stored, shipped.Allowed, Shadow, false},
		{SourceOff, stored, shipped.Allowed, Off, false},
	}
	for _, tc := range cases {
		e := Effective(QuotaRequestsPerHour, tc.src, tc.stored)
		if e.Allowed != tc.wantAllow || e.Mode != tc.wantMode || e.Editable != tc.wantEdit || e.Source != tc.src {
			t.Errorf("%s with stored %v = %+v; want %d %s editable %v", tc.src, tc.stored, e, tc.wantAllow, tc.wantMode, tc.wantEdit)
		}
	}
}

// Every quota has a shipped default; the new ones start in shadow (the
// operator's one-week dark launch), the API-token hour stays enforced (it
// has been since 0.29.82).
func TestShippedDefaults(t *testing.T) {
	want := map[string]Stored{
		QuotaRequestsPerHour:         {5000, Shadow},
		QuotaRequestsPerDay:          {10000, Shadow},
		QuotaTokenRequestsPerHour:    {1000, Enforce},
		QuotaTokenRequestsPerDay:     {10000, Shadow},
		QuotaReposPerAccount:         {1000, Shadow},
		QuotaRepoLinksPerDay:         {1000, Shadow}, // operator 2026-10-10: a quota on adds
		QuotaSignupsPerAddressPerDay: {3, Shadow},
	}
	if len(Shipped) != len(want) {
		t.Fatalf("Shipped has %d quotas, want %d", len(Shipped), len(want))
	}
	for name, w := range want {
		if Shipped[name] != w {
			t.Errorf("%s shipped %+v, want %+v", name, Shipped[name], w)
		}
	}
	if len(QuotaNames()) != len(want) || QuotaNames()[0] > QuotaNames()[1] {
		t.Errorf("QuotaNames must list every quota, sorted: %v", QuotaNames())
	}
}
