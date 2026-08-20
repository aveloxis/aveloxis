// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.103 — Workstream A of the 2026-08-19 fill audit: values the
// GitHub client decoded (or could select for free) but dropped before
// storage.
package github

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func readSrc(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func region(t *testing.T, src, decl string) string {
	t.Helper()
	i := strings.Index(src, decl)
	if i < 0 {
		t.Fatalf("declaration not found: %s", decl)
	}
	rest := src[i+len(decl):]
	if j := strings.Index(rest, "\nfunc "); j > 0 {
		rest = rest[:j]
	}
	return decl + rest
}

// A2 — the REST fallback dropped `archived`: fetchRepoInfoREST set no
// Status, so staged.go's `info.Status == "Archived"` was always false on
// every GraphQL→REST fallback and repos.repo_archived went silently
// false (the v0.27.50 class, reintroduced by transport asymmetry).
func TestRESTFallbackMapsArchivedStatus(t *testing.T) {
	types := readSrc(t, "types.go")
	if !strings.Contains(types, "`json:\"disabled\"`") {
		t.Error("ghRepoInfo must decode `disabled` so statusStr gets both inputs on the REST fallback")
	}
	body := region(t, readSrc(t, "client.go"), "func (c *Client) fetchRepoInfoREST")
	if !strings.Contains(body, "statusStr(raw.Archived") {
		t.Error("fetchRepoInfoREST must set Status via statusStr(raw.Archived, raw.Disabled) — parity with the GraphQL mapping")
	}
}

// A3 — GitHub releases carried no Origin: releases.data_source was empty
// on all 1,051,111 production rows.
func TestListReleasesSetsOrigin(t *testing.T) {
	body := region(t, readSrc(t, "client.go"), "func (c *Client) ListReleases")
	if !strings.Contains(body, `DataSource: "GitHub API"`) {
		t.Error(`ListReleases must set Origin.DataSource "GitHub API" (the GitLab side's convention)`)
	}
}

// A4a — userRefFromGraphQL dropped the already-decoded node ID, leaving
// contributor_identities.node_id dark on the whole GraphQL rail (26%
// fill = REST-era rows only).
func TestUserRefFromGraphQLMapsNodeID(t *testing.T) {
	body := region(t, readSrc(t, "graphql_pr_batch.go"), "func userRefFromGraphQL")
	if !strings.Contains(body, "NodeID:") {
		t.Error("userRefFromGraphQL must map prBatchUser.ID into UserRef.NodeID (REST's ghUserToRef already does)")
	}
}

// A4b — every `... on User {` inline in the batch + listing queries must
// select `id` (the GraphQL node ID) so A4a has data to map. One token
// per fragment, zero extra request cost.
func TestUserInlinesSelectNodeID(t *testing.T) {
	re := regexp.MustCompile(`\.\.\. on User \{([^}]*)\}`)
	for _, f := range []string{"graphql_pr_batch.go", "graphql_listing.go"} {
		src := readSrc(t, f)
		for _, m := range re.FindAllStringSubmatch(src, -1) {
			fields := " " + strings.Join(strings.Fields(m[1]), " ") + " "
			if !strings.Contains(fields, " id ") {
				t.Errorf("%s: User inline %q must select `id`", f, strings.TrimSpace(m[0]))
			}
		}
	}
}

// A4c — FetchIssueClosers selected __typename but decoded neither it nor
// `id`, so every closer identity landed with empty user_type/node_id.
func TestIssueClosersCaptureTypeAndNodeID(t *testing.T) {
	src := readSrc(t, "issue_closers.go")
	if !strings.Contains(src, "`json:\"__typename\"`") {
		t.Error("issue closer actor decode must capture __typename → UserRef.Type")
	}
	if !strings.Contains(src, "`json:\"id\"`") {
		t.Error("issue closer actor decode must capture id → UserRef.NodeID (select it in the User inline)")
	}
	body := region(t, src, "func (c *Client) FetchIssueClosers")
	if !strings.Contains(body, "NodeID:") || !strings.Contains(body, "Type:") {
		t.Error("FetchIssueClosers must map Type and NodeID onto the returned UserRef")
	}
}
