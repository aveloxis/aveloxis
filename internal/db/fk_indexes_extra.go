// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// fkExtraIndex enumerates the 50 child FK columns identified by
// the 2026-05-17 schema audit (post-v0.22.6) as needing btree
// indexes. Grouped by parent table:
//
//   - pull_requests(pull_request_id) → 11 children
//   - issues(issue_id)               →  4 children
//   - messages(msg_id)               →  3 children
//   - pull_request_reviews(pr_review_id) → 2 children
//   - repos(repo_id)                 → 30 children
//
// The repos group includes issue_assignees.repo_id (operator
// decision on 2026-05-17 to be exhaustive across the 30 child
// tables that share that FK). Tier 3 (platforms) and Tier 4
// (libraries, lstm_anomaly_models) were explicitly excluded —
// platforms has 3 rows so an index is pure overhead; libraries
// and lstm_anomaly_models look like dead-code legacy tables that
// deserve audit before indexing.
//
// Companion to v0.22.7's fkExtraConstraints, which adds
// ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE INITIALLY
// DEFERRED to the same 50 FKs. Indexes are built CONCURRENTLY
// BEFORE the constraint flip so the new RESTRICT/CASCADE
// behavior runs against indexed lookups from minute one.
//
// Adding a new FK to one of these 5 parent tables requires
// extending BOTH this list AND fkExtraConstraints AND schema.sql.
// Tests in fk_extra_test.go pin all three sources in lockstep.
type fkExtraIndex struct {
	table     string
	column    string
	indexName string
}

var fkExtraIndexes = []fkExtraIndex{
	// pull_requests(pull_request_id) ← 11 children
	{"pull_request_analysis", "pull_request_id", "idx_pull_request_analysis_pull_request_id"},
	{"pull_request_assignees", "pull_request_id", "idx_pull_request_assignees_pull_request_id"},
	{"pull_request_commits", "pull_request_id", "idx_pull_request_commits_pull_request_id"},
	{"pull_request_events", "pull_request_id", "idx_pull_request_events_pull_request_id"},
	{"pull_request_files", "pull_request_id", "idx_pull_request_files_pull_request_id"},
	{"pull_request_labels", "pull_request_id", "idx_pull_request_labels_pull_request_id"},
	{"pull_request_message_ref", "pull_request_id", "idx_pull_request_message_ref_pull_request_id"},
	{"pull_request_meta", "pull_request_id", "idx_pull_request_meta_pull_request_id"},
	{"pull_request_reviewers", "pull_request_id", "idx_pull_request_reviewers_pull_request_id"},
	{"pull_request_reviews", "pull_request_id", "idx_pull_request_reviews_pull_request_id"},
	{"pull_request_teams", "pull_request_id", "idx_pull_request_teams_pull_request_id"},

	// issues(issue_id) ← 4 children
	{"issue_assignees", "issue_id", "idx_issue_assignees_issue_id"},
	{"issue_events", "issue_id", "idx_issue_events_issue_id"},
	{"issue_labels", "issue_id", "idx_issue_labels_issue_id"},
	{"issue_message_ref", "issue_id", "idx_issue_message_ref_issue_id"},

	// messages(msg_id) ← 3 children
	{"issue_message_ref", "msg_id", "idx_issue_message_ref_msg_id"},
	{"pull_request_message_ref", "msg_id", "idx_pull_request_message_ref_msg_id"},
	{"review_comments", "msg_id", "idx_review_comments_msg_id"},

	// pull_request_reviews(pr_review_id) ← 2 children
	{"pull_request_review_message_ref", "pr_review_id", "idx_pull_request_review_message_ref_pr_review_id"},
	{"review_comments", "pr_review_id", "idx_review_comments_pr_review_id"},

	// repos(repo_id) ← 30 children (includes issue_assignees.repo_id)
	{"commit_comment_ref", "repo_id", "idx_commit_comment_ref_repo_id"},
	{"commit_messages", "repo_id", "idx_commit_messages_repo_id"},
	{"dei_badging", "repo_id", "idx_dei_badging_repo_id"},
	{"issue_assignees", "repo_id", "idx_issue_assignees_repo_id"},
	{"issue_labels", "repo_id", "idx_issue_labels_repo_id"},
	{"issue_message_ref", "repo_id", "idx_issue_message_ref_repo_id"},
	{"libraries", "repo_id", "idx_libraries_repo_id"},
	{"lstm_anomaly_results", "repo_id", "idx_lstm_anomaly_results_repo_id"},
	{"message_analysis_summary", "repo_id", "idx_message_analysis_summary_repo_id"},
	{"message_sentiment_summary", "repo_id", "idx_message_sentiment_summary_repo_id"},
	{"pull_request_assignees", "repo_id", "idx_pull_request_assignees_repo_id"},
	{"pull_request_commits", "repo_id", "idx_pull_request_commits_repo_id"},
	{"pull_request_files", "repo_id", "idx_pull_request_files_repo_id"},
	{"pull_request_labels", "repo_id", "idx_pull_request_labels_repo_id"},
	{"pull_request_message_ref", "repo_id", "idx_pull_request_message_ref_repo_id"},
	{"pull_request_meta", "repo_id", "idx_pull_request_meta_repo_id"},
	{"pull_request_review_message_ref", "repo_id", "idx_pull_request_review_message_ref_repo_id"},
	{"pull_request_reviewers", "repo_id", "idx_pull_request_reviewers_repo_id"},
	{"pull_request_reviews", "repo_id", "idx_pull_request_reviews_repo_id"},
	{"repo_badging", "repo_id", "idx_repo_badging_repo_id"},
	{"repo_clones", "repo_id", "idx_repo_clones_repo_id"},
	{"repo_cluster_messages", "repo_id", "idx_repo_cluster_messages_repo_id"},
	{"repo_insights", "repo_id", "idx_repo_insights_repo_id"},
	{"repo_insights_records", "repo_id", "idx_repo_insights_records_repo_id"},
	{"repo_meta", "repo_id", "idx_repo_meta_repo_id"},
	{"repo_sbom_scans", "repo_id", "idx_repo_sbom_scans_repo_id"},
	{"repo_stats", "repo_id", "idx_repo_stats_repo_id"},
	{"repo_topic", "repo_id", "idx_repo_topic_repo_id"},
	{"review_comments", "repo_id", "idx_review_comments_repo_id"},
	{"topic_model_meta", "repo_id", "idx_topic_model_meta_repo_id"},
}

// ensureExtraFKIndexes is the v0.22.7 migration step that adds
// btree indexes on the 50 unindexed FK columns identified by the
// 2026-05-17 audit. Uses execCreateIndexConcurrently so production
// keeps accepting writes during the build.
//
// The largest tables — pull_request_commits (~42M rows on
// production), messages (~50M), pull_request_events,
// pull_request_files, review_comments (millions each) — dominate
// the wall clock. Build durations vary; operators should expect
// the full pass to take hours on a fleet-scale DB. The
// CONCURRENTLY flag prevents blocking concurrent INSERTs.
//
// Build order matches the declaration order above (pull_requests
// children first, then issues, messages, pull_request_reviews,
// repos). No correctness dependency on order — just makes the
// log easier to follow.
//
// Idempotent via `CREATE INDEX CONCURRENTLY IF NOT EXISTS` plus
// the v0.20.1 helper's INVALID-recovery path.
//
// Errors append to the shared collector per the v0.19.4
// fail-closed contract — serve refuses to start if any index
// build fails.
func ensureExtraFKIndexes(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, idx := range fkExtraIndexes {
		sql := fmt.Sprintf(
			`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON aveloxis_data.%s (%s)`,
			idx.indexName, idx.table, idx.column,
		)
		execCreateIndexConcurrently(ctx, pg, logger, errs,
			"aveloxis_data", idx.indexName, sql)
	}
}
