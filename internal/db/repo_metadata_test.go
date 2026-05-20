// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// v0.23.0 — repos.repo_description and repos.primary_language existed
// in the schema since v0.5.x but were never populated by FetchRepoInfo
// (the natural source). Plus the operator wants the FULL language
// breakdown (not just the top one) since the request was "primary
// languages" (plural). This release adds:
//
//   - repos.languages JSONB DEFAULT '{}' for the full breakdown
//   - model.RepoInfo gains Description, PrimaryLanguage, Languages
//   - FetchRepoInfo populates all three (GitHub via GraphQL languages
//     connection, GitLab via /projects/:id/languages REST endpoint)
//   - Staged collector Phase 0 writes them to the repos row alongside
//     the per-cycle repo_info snapshot
//   - Startup backfill task targets repos with empty description AND
//     empty primary_language so existing installations heal on
//     restart without waiting for the natural collection cycle

func TestReposTableHasLanguagesColumn(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The column is added inside the CREATE TABLE block for repos.
	start := strings.Index(code, "CREATE TABLE IF NOT EXISTS aveloxis_data.repos (")
	if start < 0 {
		t.Fatal("repos table declaration not found")
	}
	end := strings.Index(code[start:], "\n);")
	if end < 0 {
		t.Fatal("repos CREATE TABLE block unterminated")
	}
	block := code[start : start+end]
	if !regexp.MustCompile(`languages\s+JSONB`).MatchString(block) {
		t.Error("aveloxis_data.repos must declare a languages JSONB column " +
			"(v0.23.0 — full language breakdown beyond the top-1 in primary_language).")
	}
}

func TestRepoMetadataMigrationAddsLanguagesColumn(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// addColumnIfMissing on repos.languages must be present so
	// existing installations get the column on next migrate.
	if !regexp.MustCompile(`addColumnIfMissing\([^)]*"aveloxis_data\.repos"[^)]*"languages"`).MatchString(code) {
		t.Error("migrate.go must call addColumnIfMissing for aveloxis_data.repos.languages " +
			"so existing v0.22.x installations pick up the column on next migrate.")
	}
}

func TestRepoInfoModelHasDescriptionAndLanguages(t *testing.T) {
	src, err := os.ReadFile("../model/repoinfo.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, fld := range []string{"Description", "PrimaryLanguage", "Languages"} {
		if !regexp.MustCompile(`\b` + fld + `\s+`).MatchString(code) {
			t.Errorf("model.RepoInfo must declare a %s field (v0.23.0)", fld)
		}
	}
	// Languages is a map[string]int — language name → bytes (GitHub)
	// or percentage * 100 (GitLab, normalized to int).
	if !regexp.MustCompile(`Languages\s+map\[string\]int`).MatchString(code) {
		t.Error("model.RepoInfo.Languages must be declared as map[string]int " +
			"so the JSONB column can serialize cleanly via pgx. " +
			"Float percentages get normalized to int on the GitLab path.")
	}
}

func TestGithubFetchRepoInfoSelectsDescriptionAndLanguages(t *testing.T) {
	src, err := os.ReadFile("../platform/github/client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The GraphQL query must select description and languages.
	if !regexp.MustCompile(`\bdescription\b`).MatchString(code) {
		t.Error("GitHub FetchRepoInfo's GraphQL query must select `description` on the repository node " +
			"so repos.repo_description gets populated for tracked repos.")
	}
	if !regexp.MustCompile(`languages\s*\(\s*first:`).MatchString(code) {
		t.Error("GitHub FetchRepoInfo's GraphQL query must select `languages(first: N, " +
			"orderBy: {field: SIZE, direction: DESC})` so the language breakdown is captured.")
	}
}

func TestUpdateRepoMetadataStoreMethodExists(t *testing.T) {
	for _, fname := range []string{"repo_metadata.go", "postgres.go"} {
		src, err := os.ReadFile(fname)
		if err != nil {
			continue
		}
		if strings.Contains(string(src), "UpdateRepoMetadata") {
			return
		}
	}
	t.Error("v0.23.0 must expose UpdateRepoMetadata(ctx, repoID, description, primaryLanguage, languages) " +
		"so the staged collector and the startup backfill task can write description + language data " +
		"to the repos table without going through the full UpsertRepo path (which overwrites other fields).")
}

func TestStartupBackfillRepoMetadataExists(t *testing.T) {
	for _, fname := range []string{
		"../scheduler/scheduler.go",
		"../scheduler/repo_metadata_backfill.go",
	} {
		src, err := os.ReadFile(fname)
		if err != nil {
			continue
		}
		if strings.Contains(string(src), "runRepoMetadataBackfill") ||
			strings.Contains(string(src), "RepoMetadataBackfill") {
			return
		}
	}
	t.Error("v0.23.0 must spawn a startup task (runRepoMetadataBackfill or similar) " +
		"that iterates repos with empty repo_description AND empty primary_language, " +
		"fetches their metadata via FetchRepoInfo, and writes to repos via " +
		"UpdateRepoMetadata. Per operator direction: 'for those repos already collected, " +
		"we need to go get that information on the next restart.'")
}

func TestStagedCollectorWritesRepoMetadata(t *testing.T) {
	src, err := os.ReadFile("../collector/staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "UpdateRepoMetadata") {
		t.Error("staged collector Phase 0 must call store.UpdateRepoMetadata after FetchRepoInfo " +
			"so description + primary_language + languages flow to the repos row on every cycle. " +
			"Without this, fresh-tracked repos never get the data populated.")
	}
}
