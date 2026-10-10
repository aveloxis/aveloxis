// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package capacity

import "sort"

// Stored is a quota's value and mode (a row of aveloxis_ops.capacity_quotas,
// or a shipped default).
type Stored struct {
	Allowed int
	Mode    Mode
}

// Shipped are the quotas' defaults (operator decisions 2026-10-10,
// summary/53 §7 and §9). The new quotas start in shadow for the one-week
// dark launch; the API-token hour has been enforced since 0.29.82.
var Shipped = map[string]Stored{
	QuotaRequestsPerHour:         {Allowed: 5000, Mode: Shadow},
	QuotaRequestsPerDay:          {Allowed: 10000, Mode: Shadow},
	QuotaTokenRequestsPerHour:    {Allowed: 1000, Mode: Enforce},
	QuotaTokenRequestsPerDay:     {Allowed: 10000, Mode: Shadow},
	QuotaReposPerAccount:         {Allowed: 1000, Mode: Shadow},
	QuotaRepoLinksPerDay:         {Allowed: 1000, Mode: Shadow}, // one full refill of the allocation a day; set after the shadow week
	QuotaSignupsPerAddressPerDay: {Allowed: 3, Mode: Shadow},
}

// QuotaNames lists every quota, sorted.
func QuotaNames() []string {
	out := make([]string, 0, len(Shipped))
	for n := range Shipped {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// EffectiveQuota is a quota as applied: its value, its mode, where they
// came from, and whether the Capacity page may change them.
type EffectiveQuota struct {
	Name     string `json:"name"`
	Allowed  int    `json:"allowed"`
	Mode     Mode   `json:"mode"`
	Source   Source `json:"source"`
	Editable bool   `json:"editable"`
}

// Effective applies the aveloxis.json word to a quota: WEB takes the
// stored row (the shipped default when there is none) and is editable;
// DEFAULT, SHADOW and OFF take the shipped value with that mode and are
// read-only on the page.
func Effective(name string, src Source, stored *Stored) EffectiveQuota {
	shipped := Shipped[name]
	e := EffectiveQuota{Name: name, Allowed: shipped.Allowed, Source: src}
	switch src {
	case SourceDefault:
		e.Mode = Enforce
	case SourceShadow:
		e.Mode = Shadow
	case SourceOff:
		e.Mode = Off
	default:
		e.Source, e.Editable, e.Mode = SourceWeb, true, shipped.Mode
		if stored != nil {
			e.Allowed, e.Mode = stored.Allowed, stored.Mode
		}
	}
	return e
}
