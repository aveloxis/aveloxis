package model

import "strings"

// NormalizeRepoName returns the canonical form of a repository slug with
// trailing "/" and ".git" suffixes stripped. The Git clone URL may legitimately
// end in ".git", but the repo slug used in forge API paths (/repos/owner/NAME)
// never does — leaving it on produces 404s for every endpoint that embeds the
// slug (releases, issues, pulls, etc.). Use this at every write boundary so
// repo.Name in the database is always clean.
func NormalizeRepoName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimSuffix(name, "/")
	name = strings.TrimSuffix(name, ".git")
	return name
}
