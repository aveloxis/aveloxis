package github

import (
	"testing"
)

// TestGraphQLRepoInfoQueryIsValid verifies the GraphQL query string is well-formed.
func TestGraphQLRepoInfoQueryIsValid(t *testing.T) {
	q := repoInfoGraphQL("chaoss", "augur")
	if q == "" {
		t.Fatal("repoInfoGraphQL returned empty string")
	}
	// Must contain repository(owner:, name:) selection.
	if !containsAll(q, "repository(", "owner:", "name:", "issues(states: OPEN)", "pullRequests(states: OPEN)", "defaultBranchRef") {
		t.Errorf("GraphQL query missing expected fields:\n%s", q)
	}
}

// TestGraphQLRepoInfoQueryContainsCounts verifies all count fields are requested.
func TestGraphQLRepoInfoQueryContainsCounts(t *testing.T) {
	q := repoInfoGraphQL("o", "r")
	for _, field := range []string{
		"totalIssues: issues", "closedIssues: issues(states: CLOSED)",
		"totalPRs: pullRequests", "closedPRs: pullRequests(states: CLOSED)",
		"mergedPRs: pullRequests(states: MERGED)",
	} {
		if !contains(q, field) {
			t.Errorf("GraphQL query missing count field %q", field)
		}
	}
}

// TestGraphQLRepoInfoQueryContainsCommunityProfile verifies community profile files are requested.
func TestGraphQLRepoInfoQueryContainsCommunityProfile(t *testing.T) {
	q := repoInfoGraphQL("o", "r")
	for _, file := range []string{"codeOfConduct", "contributing", "license"} {
		if !contains(q, file) {
			t.Errorf("GraphQL query missing community profile field %q", file)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && stringContains(s, substr)
}

func containsAll(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if !stringContains(s, sub) {
			return false
		}
	}
	return true
}

func stringContains(s, sub string) bool {
	return len(s) >= len(sub) && searchString(s, sub)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
