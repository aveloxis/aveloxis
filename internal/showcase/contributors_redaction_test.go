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

// The struct-shape pin: adding Login/FullName/Email/AvatarURL to
// ShowcaseContributor is a deliberate privacy-contract change and
// must fail the build until this test is consciously revised.
func TestShowcaseContributorTypeCarriesNoIdentity(t *testing.T) {
	typ := reflect.TypeOf(ShowcaseContributor{})
	for i := 0; i < typ.NumField(); i++ {
		name := strings.ToLower(typ.Field(i).Name)
		for _, banned := range []string{"login", "fullname", "full_name", "email", "avatar", "cntrb"} {
			if strings.Contains(name, banned) {
				t.Errorf("ShowcaseContributor.%s is an identity field — the redaction contract is that this type CANNOT hold one", typ.Field(i).Name)
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
		{Placeholder: "Contributor #2", Total: 3},
	}
	html := renderRepoToString(t, d)
	for _, needle := range []string{
		`class="blur-name"`, "Contributor #1", "Contributor #2",
		"1,200", "other-org/other-repo",
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
