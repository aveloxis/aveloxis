// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

import (
	"reflect"
	"strings"
	"testing"
)

// v0.28.2 (item 1e) — the redacted contributors section's privacy
// contract. The operator's decision is SERVER-SIDE redaction: the
// public pages carry real counts/classes/cross-repo names but never
// forge identities; the guarantee is STRUCTURAL — the row type
// cannot hold an identity.

// The struct-shape pin, WHITELIST form (v0.28.5, Copilot round): a
// banned-substring blacklist can't anticipate every identity spelling
// (Username, Name, Handle, UserID, …), so the pin freezes the EXACT
// field set instead — adding ANY field to ShowcaseContributor fails
// the build until it passes an explicit privacy review and is added
// here. Only non-identifying fields may ever join this list.
func TestShowcaseContributorTypeCarriesNoIdentity(t *testing.T) {
	allowed := []string{
		"Placeholder",    // deterministic FAKE name (never derived from identity)
		"ActivityClass",  // v0.27.57 class label
		"Commits",        // count
		"Issues",         // count
		"PRs",            // count
		"Reviews",        // count
		"Comments",       // count
		"Total",          // count
		"ElsewhereRepos", // repo names (operator-accepted fingerprint tradeoff)
		"HistoryPending", // bool — the v0.27.58 honesty signal
	}
	typ := reflect.TypeOf(ShowcaseContributor{})
	var got []string
	for i := 0; i < typ.NumField(); i++ {
		got = append(got, typ.Field(i).Name)
	}
	if !reflect.DeepEqual(got, allowed) {
		t.Errorf("ShowcaseContributor field set changed:\n  got  %v\n  want %v\nEvery addition is a privacy-contract change: review that the new field cannot carry identity, then add it to the whitelist here.", got, allowed)
	}
	// Belt on top of the whitelist: no field may be identity-shaped
	// even if the whitelist is edited carelessly.
	for _, name := range got {
		lower := strings.ToLower(name)
		for _, banned := range []string{"login", "name", "handle", "email", "avatar", "cntrb", "user", "id"} {
			if name != "Placeholder" && strings.Contains(lower, banned) {
				t.Errorf("ShowcaseContributor.%s is identity-shaped — the redaction contract is that this type CANNOT hold one", name)
			}
		}
	}
}

// Behavioral render: the section shows blur-styled placeholders +
// real counts + the sign-in CTA; a real-login-shaped string in the
// fixture's OTHER fields must be the only place logins could appear —
// and since the contributor rows can't carry one, the page source is
// login-free by construction.
func TestRepoPageRendersRedactedContributors(t *testing.T) {
	d := hostileRepoPage()
	d.Contributors = []ShowcaseContributor{
		{Placeholder: "Contributor #1", ActivityClass: "public-active",
			Commits: 1200, Issues: 34, PRs: 56, Reviews: 78, Comments: 90, Total: 1458,
			ElsewhereRepos: []string{"other-org/other-repo", "second/repo"}},
		// v0.28.5 (Copilot round): a nil backfill stamp renders as
		// "history pending" — never as an em-dash implying "active
		// nowhere else" (the v0.27.58 honesty rule).
		{Placeholder: "Contributor #2", Total: 3, HistoryPending: true},
	}
	html := renderRepoToString(t, d)
	for _, needle := range []string{
		`class="blur-name"`, "Contributor #1", "Contributor #2",
		"1,200", "other-org/other-repo",
		"history pending",
		"Sign in to see contributor identities",
		"showcase-login-cta",
		"Contributor identities are redacted",
	} {
		if !strings.Contains(html, needle) {
			t.Errorf("rendered repo page missing %q", needle)
		}
	}
	// The blur styling must exist in the inline CSS with the
	// select-proofing — a blur without user-select:none is a
	// copy-pasteable "redaction".
	if !strings.Contains(html, "filter: blur(") || !strings.Contains(html, "user-select: none") {
		t.Error("blur-name styling must blur AND disable selection")
	}
}

// Empty section renders the honest pending note, never a fabricated
// empty table.
func TestRepoPageContributorsEmptyState(t *testing.T) {
	d := hostileRepoPage()
	d.Contributors = nil
	html := renderRepoToString(t, d)
	if !strings.Contains(html, "Contributor analytics pending") {
		t.Error("nil contributors must render the pending note")
	}
}

func renderRepoToString(t *testing.T, d RepoPageData) string {
	t.Helper()
	var b strings.Builder
	if err := RenderRepo(&b, d); err != nil {
		t.Fatal(err)
	}
	return b.String()
}
