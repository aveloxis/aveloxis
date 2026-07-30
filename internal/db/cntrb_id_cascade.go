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

// cntrbIDChildFK enumerates every FK column pointing at
// aveloxis_data.contributors(cntrb_id). The list is exhaustive: it
// matches information_schema.referential_constraints on the live
// production schema. Adding a NEW child column with a cntrb_id FK
// in the future requires extending this list AND schema.sql.
//
// Tests in cntrb_id_cascade_test.go pin the same set against both
// schema.sql and migrate.go to keep the three sources of truth in
// lockstep.
type cntrbIDChildFK struct {
	table      string // child table (relative to aveloxis_data schema)
	column     string // FK column
	constraint string // current FK constraint name from information_schema
}

var cntrbIDChildFKs = []cntrbIDChildFK{
	{"contributor_identities", "cntrb_id", "contributor_identities_cntrb_id_fkey"},
	{"contributor_repo", "cntrb_id", "contributor_repo_cntrb_id_fkey"},
	{"contributors_aliases", "cntrb_id", "contributors_aliases_cntrb_id_fkey"},
	{"issue_assignees", "cntrb_id", "issue_assignees_cntrb_id_fkey"},
	{"issue_events", "cntrb_id", "issue_events_cntrb_id_fkey"},
	{"issues", "closed_by_id", "issues_closed_by_id_fkey"},
	{"issues", "reporter_id", "issues_reporter_id_fkey"},
	{"messages", "cntrb_id", "messages_cntrb_id_fkey"},
	{"pull_request_assignees", "cntrb_id", "pull_request_assignees_cntrb_id_fkey"},
	{"pull_request_commits", "author_cntrb_id", "pull_request_commits_author_cntrb_id_fkey"},
	{"pull_request_events", "cntrb_id", "pull_request_events_cntrb_id_fkey"},
	{"pull_request_meta", "cntrb_id", "pull_request_meta_cntrb_id_fkey"},
	{"pull_request_repo", "pr_cntrb_id", "pull_request_repo_pr_cntrb_id_fkey"},
	{"pull_request_reviewers", "cntrb_id", "pull_request_reviewers_cntrb_id_fkey"},
	{"pull_request_reviews", "cntrb_id", "pull_request_reviews_cntrb_id_fkey"},
	{"pull_requests", "author_id", "pull_requests_author_id_fkey"},
	// v0.23.0: contributor_login_history is a new cntrb_id child;
	// the schema declaration already carries ON UPDATE CASCADE, but
	// adding it here ensures the v0.22.1 migration helper covers it
	// on legacy databases where the table is created post-fact.
	{"contributor_login_history", "cntrb_id", "contributor_login_history_cntrb_id_fkey"},
	// v0.27.58: the daily activity-history tables are cntrb_id
	// children; schema declarations carry the full house clause, these
	// entries keep the v0.22.1 migration helper + cascade coverage
	// complete on legacy databases.
	{"contributor_activity_days", "cntrb_id", "contributor_activity_days_cntrb_id_fkey"},
	{"contributor_activity_day_totals", "cntrb_id", "contributor_activity_day_totals_cntrb_id_fkey"},
}

// ensureOnUpdateCascadeOnCntrbIDFKs is the v0.22.1 idempotent
// migration step that adds ON UPDATE CASCADE to every FK pointing
// at aveloxis_data.contributors(cntrb_id). Required prerequisite
// for the v0.22.2 cntrb_id data migration.
//
// For each FK in cntrbIDChildFKs the function:
//
//  1. Queries information_schema.referential_constraints for the
//     current update_rule. If already 'CASCADE', skip — this is
//     a no-op for already-migrated DBs and for fresh schemas
//     (schema.sql declares CASCADE inline).
//
//  2. Otherwise runs:
//     ALTER TABLE aveloxis_data.<table>
//     DROP CONSTRAINT IF EXISTS <constraint_name>,
//     ADD CONSTRAINT <constraint_name>
//     FOREIGN KEY (<column>) REFERENCES aveloxis_data.contributors(cntrb_id)
//     ON UPDATE CASCADE NOT VALID;
//
//     NOT VALID skips the synchronous validation scan that would
//     hold ACCESS EXCLUSIVE on the child table for the full scan
//     duration — on production's 50M+-row messages and
//     pull_request_commits, that's minutes. The combined ALTER
//     TABLE keeps the drop+add atomic.
//
//  3. Then runs:
//     ALTER TABLE aveloxis_data.<table>
//     VALIDATE CONSTRAINT <constraint_name>;
//
//     VALIDATE CONSTRAINT acquires only SHARE UPDATE EXCLUSIVE,
//     permitting concurrent reads and writes on the child table
//     while the validation scan runs. The data is known to be
//     consistent (the dropped constraint already enforced it), so
//     validation always succeeds; the step is there to clear the
//     NOT VALID flag and reach a fully-validated state.
//
// Errors are appended to the shared collector per the v0.19.4
// fail-closed contract — serve startup refuses to proceed if any
// step fails.
func ensureOnUpdateCascadeOnCntrbIDFKs(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, fk := range cntrbIDChildFKs {
		var updateRule string
		err := pg.pool.QueryRow(ctx, `
			SELECT update_rule
			FROM information_schema.referential_constraints
			WHERE constraint_schema = 'aveloxis_data'
			  AND constraint_name = $1
		`, fk.constraint).Scan(&updateRule)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Constraint doesn't exist yet. This is the fresh-
				// install path: schema.sql declared the constraint
				// with CASCADE inline; nothing to migrate. (If the
				// constraint legitimately doesn't exist, the next
				// child-table INSERT would fail — but that's a
				// schema-corruption problem this helper doesn't
				// own.)
				continue
			}
			*errs = append(*errs, fmt.Errorf("v0.22.1 introspect %s: %w", fk.constraint, err))
			logger.Error("v0.22.1 failed to introspect FK constraint",
				"constraint", fk.constraint, "error", err)
			continue
		}
		if updateRule == "CASCADE" {
			// Already migrated (or fresh schema with CASCADE
			// declared inline). Skip — this is the idempotency
			// fast path that makes the helper safe to run on
			// every migrate invocation.
			continue
		}

		label := fmt.Sprintf("v0.22.1 ON UPDATE CASCADE on aveloxis_data.%s.%s", fk.table, fk.column)
		execMigrationStep(ctx, pg, logger, errs, label, fmt.Sprintf(`
			ALTER TABLE aveloxis_data.%s
			DROP CONSTRAINT IF EXISTS %s,
			ADD CONSTRAINT %s
			FOREIGN KEY (%s) REFERENCES aveloxis_data.contributors(cntrb_id)
			ON UPDATE CASCADE NOT VALID
		`, fk.table, fk.constraint, fk.constraint, fk.column))

		validateLabel := fmt.Sprintf("v0.22.1 VALIDATE CONSTRAINT %s", fk.constraint)
		execMigrationStep(ctx, pg, logger, errs, validateLabel, fmt.Sprintf(`
			ALTER TABLE aveloxis_data.%s
			VALIDATE CONSTRAINT %s
		`, fk.table, fk.constraint))
	}
}
