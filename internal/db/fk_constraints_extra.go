// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// fkExtraConstraint is one of the 50 FKs identified by the
// 2026-05-17 audit that needs ON UPDATE CASCADE ON DELETE
// RESTRICT DEFERRABLE INITIALLY DEFERRED applied. Same
// (child_table, child_column) groupings as fkExtraIndexes.
//
// constraintName is Postgres's auto-generated name for the FK
// from the inline REFERENCES declaration in schema.sql. The
// pattern is `<table>_<column>_fkey`. Same convention v0.22.1
// relied on for the cntrb_id FKs; verified empirically against
// the live production schema before v0.22.7.
//
// Adding a new FK to this set requires extending fkExtraIndexes
// AND schema.sql AND this list. Tests in fk_extra_test.go pin
// all three.
type fkExtraConstraint struct {
	table          string
	column         string
	parent         string
	parentCol      string
	constraintName string
}

var fkExtraConstraints = []fkExtraConstraint{
	// pull_requests(pull_request_id) ← 11 children
	{"pull_request_analysis", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_analysis_pull_request_id_fkey"},
	{"pull_request_assignees", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_assignees_pull_request_id_fkey"},
	{"pull_request_commits", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_commits_pull_request_id_fkey"},
	{"pull_request_events", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_events_pull_request_id_fkey"},
	{"pull_request_files", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_files_pull_request_id_fkey"},
	{"pull_request_labels", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_labels_pull_request_id_fkey"},
	{"pull_request_message_ref", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_message_ref_pull_request_id_fkey"},
	{"pull_request_meta", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_meta_pull_request_id_fkey"},
	{"pull_request_reviewers", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_reviewers_pull_request_id_fkey"},
	{"pull_request_reviews", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_reviews_pull_request_id_fkey"},
	{"pull_request_teams", "pull_request_id", "pull_requests", "pull_request_id", "pull_request_teams_pull_request_id_fkey"},

	// issues(issue_id) ← 4 children
	{"issue_assignees", "issue_id", "issues", "issue_id", "issue_assignees_issue_id_fkey"},
	{"issue_events", "issue_id", "issues", "issue_id", "issue_events_issue_id_fkey"},
	{"issue_labels", "issue_id", "issues", "issue_id", "issue_labels_issue_id_fkey"},
	{"issue_message_ref", "issue_id", "issues", "issue_id", "issue_message_ref_issue_id_fkey"},

	// messages(msg_id) ← 3 children
	{"issue_message_ref", "msg_id", "messages", "msg_id", "issue_message_ref_msg_id_fkey"},
	{"pull_request_message_ref", "msg_id", "messages", "msg_id", "pull_request_message_ref_msg_id_fkey"},
	{"review_comments", "msg_id", "messages", "msg_id", "review_comments_msg_id_fkey"},

	// pull_request_reviews(pr_review_id) ← 2 children
	{"pull_request_review_message_ref", "pr_review_id", "pull_request_reviews", "pr_review_id", "pull_request_review_message_ref_pr_review_id_fkey"},
	{"review_comments", "pr_review_id", "pull_request_reviews", "pr_review_id", "review_comments_pr_review_id_fkey"},

	// repos(repo_id) ← 30 children
	{"commit_comment_ref", "repo_id", "repos", "repo_id", "commit_comment_ref_repo_id_fkey"},
	{"commit_messages", "repo_id", "repos", "repo_id", "commit_messages_repo_id_fkey"},
	{"dei_badging", "repo_id", "repos", "repo_id", "dei_badging_repo_id_fkey"},
	{"issue_assignees", "repo_id", "repos", "repo_id", "issue_assignees_repo_id_fkey"},
	{"issue_labels", "repo_id", "repos", "repo_id", "issue_labels_repo_id_fkey"},
	{"issue_message_ref", "repo_id", "repos", "repo_id", "issue_message_ref_repo_id_fkey"},
	{"libraries", "repo_id", "repos", "repo_id", "libraries_repo_id_fkey"},
	{"lstm_anomaly_results", "repo_id", "repos", "repo_id", "lstm_anomaly_results_repo_id_fkey"},
	{"message_analysis_summary", "repo_id", "repos", "repo_id", "message_analysis_summary_repo_id_fkey"},
	{"message_sentiment_summary", "repo_id", "repos", "repo_id", "message_sentiment_summary_repo_id_fkey"},
	{"pull_request_assignees", "repo_id", "repos", "repo_id", "pull_request_assignees_repo_id_fkey"},
	{"pull_request_commits", "repo_id", "repos", "repo_id", "pull_request_commits_repo_id_fkey"},
	{"pull_request_files", "repo_id", "repos", "repo_id", "pull_request_files_repo_id_fkey"},
	{"pull_request_labels", "repo_id", "repos", "repo_id", "pull_request_labels_repo_id_fkey"},
	{"pull_request_message_ref", "repo_id", "repos", "repo_id", "pull_request_message_ref_repo_id_fkey"},
	{"pull_request_meta", "repo_id", "repos", "repo_id", "pull_request_meta_repo_id_fkey"},
	{"pull_request_review_message_ref", "repo_id", "repos", "repo_id", "pull_request_review_message_ref_repo_id_fkey"},
	{"pull_request_reviewers", "repo_id", "repos", "repo_id", "pull_request_reviewers_repo_id_fkey"},
	{"pull_request_reviews", "repo_id", "repos", "repo_id", "pull_request_reviews_repo_id_fkey"},
	{"repo_badging", "repo_id", "repos", "repo_id", "repo_badging_repo_id_fkey"},
	{"repo_clones", "repo_id", "repos", "repo_id", "repo_clones_repo_id_fkey"},
	{"repo_cluster_messages", "repo_id", "repos", "repo_id", "repo_cluster_messages_repo_id_fkey"},
	{"repo_insights", "repo_id", "repos", "repo_id", "repo_insights_repo_id_fkey"},
	{"repo_insights_records", "repo_id", "repos", "repo_id", "repo_insights_records_repo_id_fkey"},
	{"repo_meta", "repo_id", "repos", "repo_id", "repo_meta_repo_id_fkey"},
	{"repo_sbom_scans", "repo_id", "repos", "repo_id", "repo_sbom_scans_repo_id_fkey"},
	{"repo_stats", "repo_id", "repos", "repo_id", "repo_stats_repo_id_fkey"},
	{"repo_topic", "repo_id", "repos", "repo_id", "repo_topic_repo_id_fkey"},
	{"review_comments", "repo_id", "repos", "repo_id", "review_comments_repo_id_fkey"},
	{"topic_model_meta", "repo_id", "repos", "repo_id", "topic_model_meta_repo_id_fkey"},
}

// constraintStateMatches checks whether a FK already has the
// desired (update_rule, delete_rule, deferrable) state. Used by
// both helpers below for idempotency.
func constraintStateMatches(ctx context.Context, pg *PostgresStore, constraintName, wantUpdate, wantDelete string, wantDeferrable bool) (matches, exists bool, err error) {
	var updateRule, deleteRule string
	var deferrable bool
	err = pg.pool.QueryRow(ctx, `
		SELECT rc.update_rule, rc.delete_rule, con.condeferrable
		FROM information_schema.referential_constraints rc
		JOIN pg_constraint con
		  ON con.conname = rc.constraint_name
		WHERE rc.constraint_schema = 'aveloxis_data'
		  AND rc.constraint_name = $1
	`, constraintName).Scan(&updateRule, &deleteRule, &deferrable)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, nil
		}
		return false, false, err
	}
	return updateRule == wantUpdate && deleteRule == wantDelete && deferrable == wantDeferrable, true, nil
}

// applyFullBehaviorFK is the shared DROP+ADD+VALIDATE step. Uses
// the v0.22.1 NOT VALID + VALIDATE pattern so the validation scan
// takes SHARE UPDATE EXCLUSIVE (concurrent reads + writes permitted)
// instead of ACCESS EXCLUSIVE for the duration. Important on
// production where the largest child tables (pull_request_commits,
// messages) routinely exceed 50M rows.
//
// The full ADD clause is:
//
//	FOREIGN KEY (<col>) REFERENCES aveloxis_data.<parent>(<parentCol>)
//	  ON UPDATE CASCADE
//	  ON DELETE RESTRICT
//	  DEFERRABLE INITIALLY DEFERRED
//	  NOT VALID
//
// DEFERRABLE INITIALLY DEFERRED is orthogonal to NOT VALID in
// PostgreSQL — they combine cleanly in one ADD clause.
func applyFullBehaviorFK(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error, table, column, parent, parentCol, constraintName, version string) {
	addLabel := fmt.Sprintf("%s ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE on aveloxis_data.%s.%s", version, table, column)
	execMigrationStep(ctx, pg, logger, errs, addLabel, fmt.Sprintf(`
		ALTER TABLE aveloxis_data.%s
		DROP CONSTRAINT IF EXISTS %s,
		ADD CONSTRAINT %s
		FOREIGN KEY (%s) REFERENCES aveloxis_data.%s(%s)
		ON UPDATE CASCADE
		ON DELETE RESTRICT
		DEFERRABLE INITIALLY DEFERRED
		NOT VALID
	`, table, constraintName, constraintName, column, parent, parentCol))

	validateLabel := fmt.Sprintf("%s VALIDATE CONSTRAINT %s", version, constraintName)
	execMigrationStep(ctx, pg, logger, errs, validateLabel, fmt.Sprintf(`
		ALTER TABLE aveloxis_data.%s
		VALIDATE CONSTRAINT %s
	`, table, constraintName))
}

// ensureExtraFKConstraints is the v0.22.7 migration step that
// applies ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE
// INITIALLY DEFERRED to the 50 child FKs identified by the
// 2026-05-17 audit.
//
// Idempotent via constraintStateMatches — already-migrated FKs
// (constraint already in the desired state) are skipped. Fresh
// installs that picked up the inline schema.sql declarations
// hit this fast path.
//
// Build the indexes BEFORE this helper runs (ensureExtraFKIndexes
// is called first from RunMigrations). Without the indexes, the
// RESTRICT/CASCADE checks fire against unindexed child tables —
// which is exactly the 17-hour-stall pattern v0.22.6 was built
// to prevent.
func ensureExtraFKConstraints(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, fk := range fkExtraConstraints {
		matches, exists, err := constraintStateMatches(ctx, pg, fk.constraintName, "CASCADE", "RESTRICT", true)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("v0.22.7 introspect %s: %w", fk.constraintName, err))
			logger.Error("v0.22.7 failed to introspect FK constraint",
				"constraint", fk.constraintName, "error", err)
			continue
		}
		if !exists {
			// Constraint doesn't exist yet (fresh install case where
			// schema.sql declared everything inline). Nothing to migrate.
			continue
		}
		if matches {
			// Already in the desired state. Idempotency fast path.
			continue
		}
		applyFullBehaviorFK(ctx, pg, logger, errs, fk.table, fk.column, fk.parent, fk.parentCol, fk.constraintName, "v0.22.7")
	}
}

// ensureCntrbIDFKsFullBehavior is the v0.22.7 follow-up to
// v0.22.1's ensureOnUpdateCascadeOnCntrbIDFKs. The earlier
// version added ON UPDATE CASCADE only; v0.22.7 brings the
// 16 cntrb_id FKs to the same full-behavior posture as the
// 50 new FKs: ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE
// INITIALLY DEFERRED.
//
// Reuses cntrbIDChildFKs (defined in cntrb_id_cascade.go) as
// the source of truth for the 16-FK list, so v0.22.7 cannot
// drift from v0.22.1's exhaustive enumeration.
//
// Idempotent. On a fresh install where schema.sql declared the
// full behavior inline, every check hits the
// already-in-desired-state fast path and the function is a no-op.
// On an existing v0.22.6 → v0.22.7 upgrade, each FK is DROPped
// and re-ADDed with the full clause.
func ensureCntrbIDFKsFullBehavior(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, fk := range cntrbIDChildFKs {
		matches, exists, err := constraintStateMatches(ctx, pg, fk.constraint, "CASCADE", "RESTRICT", true)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("v0.22.7 introspect %s: %w", fk.constraint, err))
			logger.Error("v0.22.7 failed to introspect cntrb_id FK constraint",
				"constraint", fk.constraint, "error", err)
			continue
		}
		if !exists {
			continue
		}
		if matches {
			continue
		}
		applyFullBehaviorFK(ctx, pg, logger, errs, fk.table, fk.column, "contributors", "cntrb_id", fk.constraint, "v0.22.7")
	}
}

// ensureAllFKsDeferrable is the v0.22.7 catch-all that ensures
// EVERY FK in the aveloxis schemas is DEFERRABLE INITIALLY
// DEFERRED. Covers the ~39 small-parent FKs (platforms, users,
// repo_groups, etc.) that the operator did NOT scope into the
// CASCADE/RESTRICT treatment but that should still be deferrable
// for consistency.
//
// Uses ALTER CONSTRAINT (not DROP+ADD) because that's a
// metadata-only change in PostgreSQL — no table scan needed,
// no impact on running queries. Critically, it lets us flip the
// deferral flag WITHOUT redefining the FK's ON UPDATE / ON DELETE
// semantics, which the operator explicitly scoped out of v0.22.7
// for these FKs.
//
// Self-discovering via pg_constraint, so the function covers FKs
// the migration code doesn't enumerate explicitly. Adding a new
// table with a new FK in the future automatically picks up the
// deferrable behavior on the next migrate run.
//
// Idempotent: only FKs with condeferrable=false are touched.
func ensureAllFKsDeferrable(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	rows, err := pg.pool.Query(ctx, `
		SELECT nsp.nspname, cls.relname, con.conname
		FROM pg_constraint con
		JOIN pg_class cls ON cls.oid = con.conrelid
		JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
		WHERE con.contype = 'f'
		  AND con.condeferrable = false
		  AND nsp.nspname IN ('aveloxis_data', 'aveloxis_ops', 'aveloxis_scan', 'aveloxis_augur_data')
		ORDER BY nsp.nspname, cls.relname, con.conname
	`)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("v0.22.7 enumerate non-deferrable FKs: %w", err))
		logger.Error("v0.22.7 failed to enumerate non-deferrable FKs", "error", err)
		return
	}
	defer rows.Close()

	type todo struct {
		schema, table, constraint string
	}
	var pending []todo
	for rows.Next() {
		var t todo
		if err := rows.Scan(&t.schema, &t.table, &t.constraint); err != nil {
			*errs = append(*errs, fmt.Errorf("v0.22.7 scan FK row: %w", err))
			logger.Error("v0.22.7 failed to scan FK row", "error", err)
			continue
		}
		pending = append(pending, t)
	}
	if err := rows.Err(); err != nil {
		*errs = append(*errs, fmt.Errorf("v0.22.7 iterate FK rows: %w", err))
		logger.Error("v0.22.7 failed to iterate FK rows", "error", err)
		return
	}

	for _, t := range pending {
		label := fmt.Sprintf("v0.22.7 ALTER CONSTRAINT DEFERRABLE on %s.%s.%s", t.schema, t.table, t.constraint)
		execMigrationStep(ctx, pg, logger, errs, label, fmt.Sprintf(`
			ALTER TABLE %s.%s
			ALTER CONSTRAINT %s DEFERRABLE INITIALLY DEFERRED
		`, t.schema, t.table, t.constraint))
	}
}
