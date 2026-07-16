// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Phase C (v0.27.21) store-layer pins: the C1 schema columns, the
// direct/transitive read split, and the digest's transitive filter.

package db

import (
	"os"
	"strings"
	"testing"
)

func TestPhaseCSchemaColumns(t *testing.T) {
	src, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"direct           BOOLEAN NOT NULL DEFAULT TRUE",
		"idx_lockfile_packages_pkg",
		"dependency_kind  TEXT NOT NULL DEFAULT ''",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("schema.sql missing Phase C1 declaration %q", needle)
		}
	}
	if strings.Count(s, "dependency_scope TEXT NOT NULL DEFAULT ''") < 2 {
		t.Error("dependency_scope must be declared on BOTH repo_lockfile_packages and repo_deps_vulnerabilities")
	}

	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	m := string(mig)
	for _, needle := range []string{
		`"aveloxis_data.repo_deps_vulnerabilities", "dependency_kind"`,
		`"aveloxis_data.repo_lockfile_packages", "direct"`,
		`"idx_lockfile_packages_pkg"`,
	} {
		if !strings.Contains(m, needle) {
			t.Errorf("migrate.go missing Phase C1 step %q", needle)
		}
	}
}

// TestLockedVersionsExcludeTransitives pins the classification
// boundary: with transitive storage on, GetRepoLockedVersions (which
// drives the 'locked' classification of DECLARED deps) must only see
// direct rows, and GetRepoTransitivePackages must only see the rest.
func TestLockedVersionsExcludeTransitives(t *testing.T) {
	src, err := os.ReadFile("lockfile_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "WHERE repo_id = $1 AND COALESCE(direct, TRUE)") {
		t.Error("GetRepoLockedVersions must filter to direct=TRUE — a transitive entry " +
			"must never reclassify a declared dep as 'locked'")
	}
	if !strings.Contains(s, "WHERE repo_id = $1 AND NOT COALESCE(direct, TRUE)") {
		t.Error("GetRepoTransitivePackages must select ONLY direct=FALSE rows")
	}
	// Scope aggregation: 'dev' only when EVERY occurrence is dev — a
	// package pulled in by both a dev tool and a runtime dep is
	// runtime exposure.
	if !strings.Contains(s, "THEN 'dev' ELSE '' END") {
		t.Error("transitive scope aggregation must fold non-dev occurrences to '' " +
			"(any non-dev observation wins)")
	}
}

// TestDigestExcludesTransitiveByDefault pins the v0.27.21 digest
// filter: transitive findings only enter the operator digest when
// mail.vuln_digest_include_transitive is set. Pre-C1 (”) and direct
// rows always pass.
func TestDigestExcludesTransitiveByDefault(t *testing.T) {
	src, err := os.ReadFile("vuln_digest_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, `($3 OR COALESCE(v.dependency_kind, '') IS DISTINCT FROM 'transitive')`) {
		t.Error("GetNewVulnerabilityFindings must gate transitive findings on the " +
			"includeTransitive parameter")
	}
	sched, err := os.ReadFile("../scheduler/vuln_digest.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sched), "VulnDigestIncludeTransitive") {
		t.Error("runVulnDigest must thread mail.vuln_digest_include_transitive into the query")
	}
}
