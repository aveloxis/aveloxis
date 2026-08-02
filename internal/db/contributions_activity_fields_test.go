// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// contributions_activity_fields_test.go — v0.27.57 pins: the
// /contributions/identities endpoint rows carry the activity
// classification so the operator's front-end page can render the four
// classes without extra round trips.

import (
	"reflect"
	"strings"
	"testing"
)

func TestRepoContributorCarriesActivityFields(t *testing.T) {
	typ := reflect.TypeOf(RepoContributor{})
	want := map[string]string{
		"ActivityClass":      "activity_class",
		"PublicContribsYear": "public_contribs_year",
		"RestrictedContribs": "restricted_contribs_year",
		"LastContributionYr": "last_contribution_year",
	}
	for field, tag := range want {
		f, ok := typ.FieldByName(field)
		if !ok {
			t.Errorf("RepoContributor must carry %s (frontend activity page contract)", field)
			continue
		}
		if got := f.Tag.Get("json"); got != tag {
			t.Errorf("RepoContributor.%s json tag = %q, want %q — the GUI greps by these names", field, got, tag)
		}
	}
}

func TestGetRepoContributorsSelectsActivityColumns(t *testing.T) {
	src := readSourceFile(t, "contributions.go")
	idx := strings.Index(src, "func (s *PostgresStore) GetRepoContributors(")
	if idx < 0 {
		t.Fatal("cannot find GetRepoContributors")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}
	for _, col := range []string{"gh_activity_class", "gh_public_contribs_year", "gh_restricted_contribs_year", "gh_last_contribution_year"} {
		if !strings.Contains(body, col) {
			t.Errorf("GetRepoContributors must SELECT %s", col)
		}
	}
}
