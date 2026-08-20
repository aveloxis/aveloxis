// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.115 — pins for the 2026-08-20 schema-drift-audit remediation
// (fresh migrate vs aveloxis_large; operator decisions on all four
// findings). See scripts/schema_structure_dump.sql for the audit tool.

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Finding 1 (operator: "I accept your recommendation for the real fix
// class"): on a populated fleet NO path created a newly-added plain
// view — CreateMaterializedViewsIfNotExist probes one sentinel matview
// and skips the whole file; refresh-views + the weekly rebuild only
// REFRESH known names; the deploy recipe is `migrate --skip-views`.
// mailing_list_pr_equivalents was unreachable on production since
// v0.25.7. Fix: base-table plain views live in views.sql and run on
// EVERY migrate (they cost nothing — no storage, no refresh).
func TestBaseTableViewsRunOnEveryMigrate(t *testing.T) {
	views, err := os.ReadFile("views.sql")
	if err != nil {
		t.Fatalf("views.sql must exist — the always-run home for base-table plain views: %v", err)
	}
	if !strings.Contains(string(views), "CREATE OR REPLACE VIEW aveloxis_data.mailing_list_pr_equivalents") {
		t.Error("mailing_list_pr_equivalents must live in views.sql (it was stranded in matviews.sql, unreachable on populated fleets)")
	}
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), `"base-table views", viewsSQL`) {
		t.Error("RunMigrations must exec views.sql unconditionally via execMigrationStep — the matview block is gated/skipped, views must not be")
	}
	mv, err := os.ReadFile("matviews.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mv), "mailing_list_pr_equivalents") {
		t.Error("mailing_list_pr_equivalents must NOT remain in matviews.sql — that file is unreachable on populated fleets")
	}
	// Class guard: the only plain views allowed in matviews.sql are the
	// two MATVIEW ALIASES (they depend on matviews and share their
	// lifecycle). A new base-table view added there would be
	// structurally unreachable on every existing installation — put it
	// in views.sql instead.
	viewRe := regexp.MustCompile(`CREATE OR REPLACE VIEW aveloxis_data\.([a-z_]+)`)
	allowed := map[string]bool{"augur_new_contributors": true, "explorer_libyear_all": true}
	for _, m := range viewRe.FindAllStringSubmatch(string(mv), -1) {
		if !allowed[m[1]] {
			t.Errorf("plain view %q added to matviews.sql — that file never runs on populated fleets; base-table views belong in views.sql", m[1])
		}
	}
}

// Finding 3 (operator: "I don't think history requires the same
// indexes"): repo_labor_history carried two auto-copied parent indexes
// on fleets whose v0.27.7 migration ran after repo_labor had its
// indexes (LIKE INCLUDING ALL copies whatever exists at creation
// time). Audit evidence: the composite (repo_id, rl_analysis_date
// DESC) copy had ZERO scans ever (1.2 GB of pure rotation write
// amplification) — dropped everywhere. The plain repo_id copy had 188
// real scans (dedup-repos' hygiene DELETE ... WHERE repo_id = loser) —
// kept, and now DELIBERATELY declared in schema.sql under the exact
// auto-generated name so fresh installs and fleets converge.
func TestRepoLaborHistoryIndexConvergence(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	s := string(schema)
	if !strings.Contains(s, "CREATE INDEX IF NOT EXISTS repo_labor_history_repo_id_idx") {
		t.Error("schema.sql must declare repo_labor_history_repo_id_idx — the dedup hygiene-delete path's index, now deliberate instead of accidental")
	}
	if strings.Contains(s, "repo_labor_history_repo_id_rl_analysis_date_idx") {
		t.Error("the composite history index is a dropped name (v0.27.115) — it must never be re-created (0 scans ever; pure write amplification)")
	}
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "DROP INDEX IF EXISTS aveloxis_data.repo_labor_history_repo_id_rl_analysis_date_idx") {
		t.Error("migrate must drop the unused composite copy on fleets that have it")
	}
}

// Finding 4 (operator: "that table can be dropped. Its empty and its a
// residual from Augur we don't need"): contributors_old — zero writers
// ever, zero readers, zero rows on production.
func TestContributorsOldDropped(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schema), "contributors_old") {
		t.Error("contributors_old must be gone from schema.sql (operator-confirmed Augur residue)")
	}
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "DROP TABLE IF EXISTS aveloxis_data.contributors_old") {
		t.Error("migrate must drop contributors_old on existing installations")
	}
}
