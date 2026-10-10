// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package capacity

import (
	"fmt"
	"strconv"
	"strings"
)

// Message is the text a person reads when a quota refuses them: kind, the
// numbers, that the limit comes from hardware and infrastructure, and whom
// to write for more (operator 2026-10-10). One wording for every surface —
// the API, the web sign-up page, the GUI banner.
func (e *Exceeded) Message() string {
	var b strings.Builder
	switch {
	case e.Kind == KindAllocation:
		fmt.Fprintf(&b, "Your groups can hold up to %s repositories, and they hold %s. ", formatCount(e.Allowed), formatCount(e.Used))
		b.WriteString("Aveloxis runs on limited hardware and infrastructure, so each account has a fair-use limit. ")
		b.WriteString("To add more, remove repositories you no longer need from your groups")
		if e.Contact != "" {
			fmt.Fprintf(&b, ", or email %s with a short description of what you need and we can raise your limit", e.Contact)
		}
		b.WriteString(".")
	case e.Quota == QuotaRepoLinksPerDay:
		fmt.Fprintf(&b, "This account has added %s repositories to its groups today and can add up to %s a day", formatCount(e.Used), formatCount(e.Allowed))
		if e.Wanted > 0 {
			fmt.Fprintf(&b, "; adding %s more would pass that", formatCount(e.Wanted))
		}
		b.WriteString(". ")
		b.WriteString("Aveloxis runs on limited hardware and infrastructure, so additions are paced")
		if !e.ResetAt.IsZero() {
			fmt.Fprintf(&b, "; it resets at %s", e.ResetAt.UTC().Format("15:04 UTC on 2 January"))
		}
		b.WriteString(". Repositories already in your groups are unaffected.")
		if e.Contact != "" {
			fmt.Fprintf(&b, " If your work needs more, email %s with a short description of what you need, and we can raise your limit.", e.Contact)
		}
	case e.Quota == QuotaSharedWithMeAddsPerHour:
		fmt.Fprintf(&b, "This account has added %s repositories to its groups by viewing them this hour, the most it can. ", formatCount(e.Allowed))
		b.WriteString("Aveloxis runs on limited hardware and infrastructure, so these additions are paced")
		if !e.ResetAt.IsZero() {
			fmt.Fprintf(&b, "; it resets at %s", e.ResetAt.UTC().Format("15:04 UTC on 2 January"))
		}
		b.WriteString(". Repositories already in your groups still open normally.")
		if e.Contact != "" {
			fmt.Fprintf(&b, " If your work needs more, email %s.", e.Contact)
		}
	case e.Quota == QuotaSignupsPerAddressPerDay:
		fmt.Fprintf(&b, "At most %s new accounts can be created from one network address per day, and that number has been reached. ", formatCount(e.Allowed))
		b.WriteString("Aveloxis runs on limited hardware and infrastructure, so sign-ups are paced. Please try again tomorrow")
		if e.Contact != "" {
			fmt.Fprintf(&b, ", or email %s if your organization needs more accounts", e.Contact)
		}
		b.WriteString(".")
	default:
		who, what := "This account", "each account"
		if e.Subject.Kind == KindToken {
			who, what = "This API token", "each API token"
		}
		fmt.Fprintf(&b, "%s has reached its limit of %s requests per %s. ", who, formatCount(e.Allowed), e.Window)
		fmt.Fprintf(&b, "Aveloxis runs on limited hardware and infrastructure, so %s has a fair-use limit", what)
		if !e.ResetAt.IsZero() {
			fmt.Fprintf(&b, "; it resets at %s", e.ResetAt.UTC().Format("15:04 UTC on 2 January"))
		}
		b.WriteString(".")
		if e.Contact != "" {
			fmt.Fprintf(&b, " If your work needs more, email %s with a short description of what you need, and we can raise your limit.", e.Contact)
		}
	}
	return b.String()
}

// The quota names (the rows of aveloxis_ops.capacity_quotas and the keys
// of aveloxis.json's "capacity" section).
const (
	QuotaRequestsPerHour      = "requests_per_hour"
	QuotaRequestsPerDay       = "requests_per_day"
	QuotaTokenRequestsPerHour = "token_requests_per_hour"
	QuotaTokenRequestsPerDay  = "token_requests_per_day"
	QuotaReposPerAccount      = "repos_per_account"
	// QuotaRepoLinksPerDay caps repositories NEWLY added to one account's
	// groups per UTC day (operator 2026-10-10: the allocation bounds what is
	// held at once; this bounds add-read-remove churn). Organization scans
	// an administrator approved are not counted.
	QuotaRepoLinksPerDay         = "repo_links_per_day"
	QuotaSignupsPerAddressPerDay = "signups_per_address_per_day"
	// QuotaSharedWithMeAddsPerHour is the fixed cap on repositories one
	// account adds to its groups by viewing them (0.29.82, ASVS A2): not
	// configurable, so not in Shipped.
	QuotaSharedWithMeAddsPerHour = "shared_with_me_adds_per_hour"
)

// formatCount writes n with thousands separators (10,000).
func formatCount(n int) string {
	s := strconv.Itoa(n)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var out []byte
	for i := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
