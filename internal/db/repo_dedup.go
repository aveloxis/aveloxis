// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_dedup.go — store layer for `aveloxis dedup-repos` (v0.25.32).
//
// GitHub and GitLab treat owner/repo paths case-insensitively, but
// repos.repo_git was byte-exact-unique, so bulk-pasted case variants of
// already-tracked repos created full duplicate rows that each collected
// the same repository twice (1,220 pairs on the production fleet). This
// file merges each duplicate group down to one winner:
//
//   - winner = MIN(repo_id) — the oldest row, referenced the longest.
//     A wrong-cased winner is harmless: the Phase 0 case self-heal
//     (HealRepoCaseDrift) corrects its spelling on the next collection.
//   - group/user links (aveloxis_ops.user_repos) are repointed to the
//     winner so every group that referenced either variant keeps seeing
//     the repo.
//   - SHARED-COPY rows are repointed, never deleted. Tables whose unique
//     key is global rather than per-repo (messages: UNIQUE
//     (platform_msg_id, platform_id); commit_comment_ref; email_message)
//     hold ONE row for the pair — whichever variant collected first owns
//     it. Deleting "the loser's" rows there would destroy the only copy
//     or trip the winner's RESTRICT refs. contributor_repo is
//     deliberately NOT touched (v0.25.34): it is the breadth worker's
//     observational record keyed by the numeric gh_repo_id, not a
//     catalog reference.
//   - per-repo duplicated child data (issues, PRs, commits, ... — all
//     keyed UNIQUE (repo_id, platform_*)) is byte-duplicate of the
//     winner's and is hard-deleted leaves-first, then the loser repos
//     row itself is deleted. Nothing is lost: the winner holds the same
//     data.
//
// One transaction per pair; idempotent across runs (resolved pairs drop
// out of the candidate set). Pairs with either side mid-collection
// (status='collecting') are left out of the batch window (the dry-run
// sample and the end-of-run remaining count report them), and the
// LOSER's queue row is re-checked FOR UPDATE inside each pair
// transaction — a loser is never deleted out from under a worker (the
// winner is only ever repointed onto, so a winner claimed mid-pair is
// harmless).

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RepoDupPair is one case-variant duplicate pair: the winner that
// survives and the loser that merges into it.
type RepoDupPair struct {
	LowerGit   string
	WinnerID   int64
	WinnerGit  string
	WinnerName string
	LoserID    int64
	LoserGit   string
	// GroupSize is the number of case variants sharing LowerGit. All
	// production groups are pairs (2); larger groups drain one loser per
	// pass.
	GroupSize int
	// Last-collected timestamps for dry-run visibility (nil = never).
	WinnerLastCollected *time.Time
	LoserLastCollected  *time.Time
	// Collecting is true when either side's queue row is mid-collection;
	// the batch window excludes such pairs (v0.28.18) and the dry-run
	// sample shows them flagged.
	Collecting bool
}

// repoDupCandidatesSQL is the shared candidate query. Winner =
// MIN(repo_id); the LATERAL picks the lowest-id loser so >2-variant
// groups drain deterministically. Scoped to forge platforms — generic
// git hosts (platform 3) may legitimately be case-sensitive, so their
// case variants are NOT duplicates. $2 = TRUE drops mid-collection
// pairs from the window (the batch loop's shape — see
// DedupCaseVariantReposBatch); FALSE keeps them, flagged, for dry-run
// display.
const repoDupCandidatesSQL = `
	WITH dup_groups AS (
		SELECT LOWER(repo_git) AS lower_git,
		       MIN(repo_id)    AS winner_id,
		       COUNT(*)        AS group_size
		FROM aveloxis_data.repos
		WHERE platform_id IN (1, 2)
		GROUP BY LOWER(repo_git)
		HAVING COUNT(*) > 1
	), candidates AS (
		SELECT g.lower_git,
		       g.winner_id,
		       w.repo_git   AS winner_git,
		       w.repo_name  AS winner_name,
		       l.repo_id    AS loser_id,
		       l.repo_git   AS loser_git,
		       g.group_size,
		       qw.last_collected AS winner_last_collected,
		       ql.last_collected AS loser_last_collected,
		       (COALESCE(qw.status, '') = 'collecting'
		        OR COALESCE(ql.status, '') = 'collecting') AS collecting
		FROM dup_groups g
		JOIN aveloxis_data.repos w ON w.repo_id = g.winner_id
		JOIN LATERAL (
			SELECT r.repo_id, r.repo_git
			FROM aveloxis_data.repos r
			WHERE LOWER(r.repo_git) = g.lower_git
			  AND r.platform_id IN (1, 2)
			  AND r.repo_id <> g.winner_id
			ORDER BY r.repo_id
			LIMIT 1
		) l ON TRUE
		LEFT JOIN aveloxis_ops.collection_queue qw ON qw.repo_id = g.winner_id
		LEFT JOIN aveloxis_ops.collection_queue ql ON ql.repo_id = l.repo_id
	)
	SELECT lower_git, winner_id, winner_git, winner_name, loser_id, loser_git,
	       group_size, winner_last_collected, loser_last_collected, collecting
	FROM candidates
	WHERE NOT ($2::boolean AND collecting)
	ORDER BY lower_git
	LIMIT $1`

// CountCaseVariantRepoDups returns the number of unresolved case-variant
// duplicate groups among forge-platform repos.
func CountCaseVariantRepoDups(ctx context.Context, store *PostgresStore) (int, error) {
	var n int
	err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT 1
			FROM aveloxis_data.repos
			WHERE platform_id IN (1, 2)
			GROUP BY LOWER(repo_git)
			HAVING COUNT(*) > 1
		) dup`).Scan(&n)
	return n, err
}

// SampleCaseVariantRepoDups returns up to limit candidate pairs for
// dry-run display — mid-collection pairs included, flagged Collecting.
func SampleCaseVariantRepoDups(ctx context.Context, store *PostgresStore, limit int) ([]RepoDupPair, error) {
	return sampleCaseVariantRepoDups(ctx, store, limit, false)
}

// sampleCaseVariantRepoDups is the candidate read behind both the
// dry-run sample and the batch loop; excludeCollecting = TRUE is the
// batch's window (a mid-collection pair at the head of the lower_git
// order must not occupy a slot every round).
func sampleCaseVariantRepoDups(ctx context.Context, store *PostgresStore, limit int, excludeCollecting bool) ([]RepoDupPair, error) {
	rows, err := store.pool.Query(ctx, repoDupCandidatesSQL, limit, excludeCollecting)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []RepoDupPair
	for rows.Next() {
		var p RepoDupPair
		if err := rows.Scan(&p.LowerGit, &p.WinnerID, &p.WinnerGit, &p.WinnerName,
			&p.LoserID, &p.LoserGit, &p.GroupSize,
			&p.WinnerLastCollected, &p.LoserLastCollected, &p.Collecting); err != nil {
			return pairs, err
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

// errPairCollecting signals that the loser transitioned to 'collecting'
// between the candidate query and the pair transaction — skip, don't
// delete a repo out from under its worker.
var errPairCollecting = errors.New("pair is mid-collection")

// DedupCaseVariantReposBatch merges up to batchSize pairs, one
// transaction each. Returns how many merged and how many were skipped
// because a side was mid-collection. Callers loop until merged == 0;
// resolved pairs drop out of the candidate set, so re-runs are no-ops.
//
// The batch's candidate window EXCLUDES mid-collection pairs (v0.28.18):
// the window is the first batchSize groups in lower_git order, so a
// batchSize-long run of collecting pairs at the head (one 33-hour
// leftover drain per pair is a production shape) used to fill every
// round with skips, the loop read merged == 0 as "done", and no pair
// beyond the head was ever reached — the SR-19 "rerun until 0 pairs"
// contract could not converge. skippedCollecting now counts only the
// in-transaction race (a side claimed between the read and the FOR
// UPDATE); the CLI reports the groups still remaining afterwards.
func DedupCaseVariantReposBatch(ctx context.Context, store *PostgresStore, batchSize int) (merged, skippedCollecting int, err error) {
	// v0.28.18: refuse, naming the fix, rather than sequential-scan
	// email_message per pair (the gate's doc has the shape).
	if err := emailMessageFKIndexesReadyFor(ctx, store.pool, "repos"); err != nil {
		return 0, 0, err
	}
	if batchSize <= 0 {
		return 0, 0, fmt.Errorf("batchSize must be positive, got %d", batchSize)
	}
	pairs, err := sampleCaseVariantRepoDups(ctx, store, batchSize, true)
	if err != nil {
		return 0, 0, fmt.Errorf("load dedup candidates: %w", err)
	}
	for _, pair := range pairs {
		// Belt: the window excludes collecting pairs; kept so a caller
		// handing in a dry-run sample still skips them.
		if pair.Collecting {
			skippedCollecting++
			continue
		}
		// v0.27.114: bounded 40P01 retry per pair — a pair transaction
		// picked as the deadlock victim (concurrent collection writes,
		// or migration DDL beside a live serve) rolls back cleanly, and
		// "re-run" is already the documented recovery, so retry inline
		// instead of failing the whole command run. Non-deadlock errors
		// (incl. the errPairCollecting sentinel) pass through untouched.
		perr := store.withRetry(ctx, func(ctx context.Context) error {
			return dedupOnePair(ctx, store, pair)
		})
		if perr != nil {
			if errors.Is(perr, errPairCollecting) {
				skippedCollecting++
				continue
			}
			return merged, skippedCollecting, fmt.Errorf(
				"dedup %s (loser repo_id=%d -> winner repo_id=%d): %w",
				pair.LowerGit, pair.LoserID, pair.WinnerID, perr)
		}
		merged++

		// Post-merge: a winner that has never been collected gets
		// enqueued immediately. This is what makes the simple
		// MIN(repo_id) winner rule safe when the data-less side wins —
		// it just collects fresh.
		var collected bool
		qerr := store.pool.QueryRow(ctx,
			`SELECT last_collected IS NOT NULL FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
			pair.WinnerID).Scan(&collected)
		if errors.Is(qerr, pgx.ErrNoRows) || (qerr == nil && !collected) {
			if eerr := store.EnqueueRepo(ctx, pair.WinnerID, 100); eerr != nil {
				return merged, skippedCollecting, fmt.Errorf(
					"enqueue never-collected winner repo_id=%d: %w", pair.WinnerID, eerr)
			}
		}
	}
	return merged, skippedCollecting, nil
}

// loserJoinDeletes removes grandchild rows that carry NO repo_id column
// and must go before their parents. Each SQL takes $1 = loser repo_id.
var loserJoinDeletes = []struct {
	label string
	sql   string
}{
	{"pull_request_teams", `DELETE FROM aveloxis_data.pull_request_teams
		WHERE pull_request_id IN (SELECT pull_request_id FROM aveloxis_data.pull_requests WHERE repo_id = $1)`},
	{"pull_request_analysis", `DELETE FROM aveloxis_data.pull_request_analysis
		WHERE pull_request_id IN (SELECT pull_request_id FROM aveloxis_data.pull_requests WHERE repo_id = $1)`},
	{"pull_request_repo", `DELETE FROM aveloxis_data.pull_request_repo
		WHERE pr_repo_meta_id IN (SELECT pr_meta_id FROM aveloxis_data.pull_request_meta WHERE repo_id = $1)`},
	{"commit_parents", `DELETE FROM aveloxis_data.commit_parents
		WHERE cmt_id IN (SELECT cmt_id FROM aveloxis_data.commits WHERE repo_id = $1)`},
	{"library_dependencies", `DELETE FROM aveloxis_data.library_dependencies
		WHERE library_id IN (SELECT library_id FROM aveloxis_data.libraries WHERE repo_id = $1)`},
	{"library_version", `DELETE FROM aveloxis_data.library_version
		WHERE library_id IN (SELECT library_id FROM aveloxis_data.libraries WHERE repo_id = $1)`},
}

// loserRepoIDDeletes are the loser's per-repo duplicated child tables,
// hard-deleted as `DELETE FROM <table> WHERE repo_id = $1` in FK-safe
// leaves-first order: issue children -> review children -> reviews ->
// PR children -> issues/pull_requests -> commits -> repo-level tables.
// Every row here is byte-duplicate of the winner's collection of the
// same repository.
var loserRepoIDDeletes = []string{
	// message-bridge rows (reference the shared messages rows, which are
	// repointed — the loser's bridges duplicate the winner's)
	"aveloxis_data.issue_message_ref",
	"aveloxis_data.pull_request_review_message_ref",
	"aveloxis_data.pull_request_message_ref",
	"aveloxis_data.review_comments",
	// issue children
	"aveloxis_data.issue_labels",
	"aveloxis_data.issue_assignees",
	"aveloxis_data.issue_events",
	// PR children (reviews after their own children above)
	"aveloxis_data.pull_request_reviews",
	"aveloxis_data.pull_request_labels",
	"aveloxis_data.pull_request_assignees",
	"aveloxis_data.pull_request_reviewers",
	"aveloxis_data.pull_request_commits",
	"aveloxis_data.pull_request_files",
	"aveloxis_data.pull_request_events",
	"aveloxis_data.pull_request_meta",
	// parents
	"aveloxis_data.issues",
	"aveloxis_data.pull_requests",
	// git-side
	"aveloxis_data.commit_messages",
	"aveloxis_data.commits",
	// repo-level (no inter-dependencies; records before insights for
	// safety, everything else order-free)
	"aveloxis_data.releases",
	"aveloxis_data.repo_info",
	"aveloxis_data.repo_clones",
	"aveloxis_data.repo_badging",
	"aveloxis_data.dei_badging",
	"aveloxis_data.repo_insights_records",
	"aveloxis_data.repo_insights",
	"aveloxis_data.repo_dependencies",
	"aveloxis_data.repo_deps_libyear",
	"aveloxis_data.repo_deps_scorecard",
	"aveloxis_data.repo_deps_vulnerabilities",
	// v0.27.11 lockfile inventory + direct-dep resolutions: per-repo
	// analysis snapshots, regenerated by the winner's next analysis
	// pass — plain per-repo delete is correct.
	"aveloxis_data.repo_lockfiles",
	"aveloxis_data.repo_lockfile_packages",
	"aveloxis_data.repo_lockfile_edges",
	"aveloxis_data.repo_sbom_scans",
	"aveloxis_data.libraries",
	"aveloxis_data.lstm_anomaly_results",
	"aveloxis_data.topic_model_meta",
	"aveloxis_data.repo_cluster_messages",
	"aveloxis_data.repo_topic",
	"aveloxis_data.repo_labor",
	"aveloxis_data.repo_meta",
	"aveloxis_data.repo_stats",
	"aveloxis_data.message_analysis_summary",
	"aveloxis_data.message_sentiment_summary",
	"aveloxis_data.repo_distribution",
	"aveloxis_data.repo_distribution_manifest",
}

// loserHygieneDeletes covers tables that carry a repo_id column with NO
// foreign key: derived aggregates (rebuilt by RefreshAllRepoAggregates),
// history snapshots, scancode results, and network exports. Deleting the
// loser's rows keeps analytics from double-counting; nothing references
// them.
var loserHygieneDeletes = []string{
	"aveloxis_data.dm_repo_annual",
	"aveloxis_data.dm_repo_monthly",
	"aveloxis_data.dm_repo_weekly",
	"aveloxis_data.repo_info_history",
	"aveloxis_data.repo_deps_libyear_history",
	"aveloxis_data.repo_deps_scorecard_history",
	"aveloxis_data.repo_labor_history", // v0.27.7: rotated scc snapshots (no FK — LIKE doesn't copy them)
	"aveloxis_data.repo_distribution_history",
	"aveloxis_data.repo_distribution_manifest_history",
	"aveloxis_scan.scancode_scans",
	"aveloxis_scan.scancode_file_results",
	"aveloxis_scan.scancode_scans_history",
	"aveloxis_scan.scancode_file_results_history",
	"aveloxis_data.historical_repo_urls",
	"aveloxis_data.topic_model_event",
	"aveloxis_ops.network_weighted_commits",
	"aveloxis_ops.network_weighted_issues",
	"aveloxis_ops.network_weighted_pr_reviews",
	"aveloxis_ops.network_weighted_prs",
}

// Deliberately untouched: aveloxis_ops.worker_history (audit log of what
// each worker did — historical fact, not repo state) and the
// Augur-legacy no-write-path tables (repos_fetch_log, working_commits,
// repo_test_coverage, network_beyond_augur*).

// dedupOnePair merges one loser into its winner inside a single
// transaction. Statement order matters — most child FKs are ON DELETE
// RESTRICT, which checks immediately even though the constraints are
// DEFERRABLE (RESTRICT cannot be deferred; verified empirically in
// v0.22.7).
func dedupOnePair(ctx context.Context, store *PostgresStore, pair RepoDupPair) error {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Re-check the loser's queue status under lock: a worker can claim
	// the repo between the candidate query and this transaction.
	// Deleting a 'collecting' repo's rows would corrupt the in-flight
	// job (its CompleteJob would also resurrect a queue row for a repo
	// we just deleted).
	var loserStatus string
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(status, '') FROM aveloxis_ops.collection_queue
		 WHERE repo_id = $1 FOR UPDATE`, pair.LoserID).Scan(&loserStatus)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock loser queue row: %w", err)
	}
	if loserStatus == "collecting" {
		return errPairCollecting
	}

	exec := func(label, sql string, args ...any) error {
		if _, e := tx.Exec(ctx, sql, args...); e != nil {
			return fmt.Errorf("%s: %w", label, e)
		}
		return nil
	}

	// --- Step 1: ops linkage. user_repos is repointed (PK (group_id,
	// repo_id), no FK on repo_id) so every group that referenced either
	// variant keeps the repo; queue/status/staging rows are loser-scoped
	// bookkeeping and go away.
	steps := []struct {
		label string
		sql   string
		args  []any
	}{
		{"repoint user_repos", `
			INSERT INTO aveloxis_ops.user_repos (group_id, repo_id)
			SELECT group_id, $2 FROM aveloxis_ops.user_repos WHERE repo_id = $1
			ON CONFLICT DO NOTHING`, []any{pair.LoserID, pair.WinnerID}},
		{"delete loser user_repos", `DELETE FROM aveloxis_ops.user_repos WHERE repo_id = $1`, []any{pair.LoserID}},
		// v0.27.4: per-user stars are links like user_repos — repoint so
		// a user who starred either case variant keeps their star.
		{"repoint user_repo_stars", `
			INSERT INTO aveloxis_ops.user_repo_stars (user_id, repo_id)
			SELECT user_id, $2 FROM aveloxis_ops.user_repo_stars WHERE repo_id = $1
			ON CONFLICT DO NOTHING`, []any{pair.LoserID, pair.WinnerID}},
		{"delete loser user_repo_stars", `DELETE FROM aveloxis_ops.user_repo_stars WHERE repo_id = $1`, []any{pair.LoserID}},
		{"delete loser collection_queue", `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, []any{pair.LoserID}},
		{"delete loser collection_status", `DELETE FROM aveloxis_ops.collection_status WHERE repo_id = $1`, []any{pair.LoserID}},
		{"delete loser staging", `DELETE FROM aveloxis_ops.staging WHERE repo_id = $1`, []any{pair.LoserID}},
		{"repoint mailing_list_staging", `UPDATE aveloxis_ops.mailing_list_staging SET repo_id = $2 WHERE repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},

		// --- Step 2: shared-copy repoints (UPDATE, never DELETE). These
		// tables are globally unique — the pair shares ONE row, owned by
		// whichever variant collected first. Unique keys exclude repo_id,
		// so the repoint can't conflict.
		{"repoint messages", `UPDATE aveloxis_data.messages SET repo_id = $2 WHERE repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"repoint email_message.repo_id", `UPDATE aveloxis_data.email_message SET repo_id = $2 WHERE repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"repoint email_message.signaled_repo_id", `UPDATE aveloxis_data.email_message SET signaled_repo_id = $2 WHERE signaled_repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"repoint commit_comment_ref", `UPDATE aveloxis_data.commit_comment_ref SET repo_id = $2 WHERE repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		// contributor_repo is DELIBERATELY not touched (v0.25.34). It is
		// the breadth worker's observational record of each contributor's
		// GitHub-WIDE event stream: its repo_git values overwhelmingly
		// reference repos Aveloxis does not track, it has no FK to repos,
		// and its stable repo key is the numeric gh_repo_id (case-immune).
		// The v0.25.32 repoint rewrote observational history AND
		// sequential-scanned the 51M-row table once per pair.
		{"migrate foundation_membership", `
			INSERT INTO aveloxis_ops.foundation_membership (foundation, status, project_name, homepage_url, repo_url)
			SELECT foundation, status, project_name, homepage_url, $2
			FROM aveloxis_ops.foundation_membership WHERE repo_url = $1
			ON CONFLICT DO NOTHING`, []any{pair.LoserGit, pair.WinnerGit}},
		{"delete loser foundation_membership", `DELETE FROM aveloxis_ops.foundation_membership WHERE repo_url = $1`, []any{pair.LoserGit}},

		// --- Step 2b: email_message links into the loser's issue/PR/
		// review trees (mailing-list projection, v0.25.7). Remap each
		// link to the winner's equivalent row by platform ID so the
		// bridge survives the merge; clear any leftover links whose
		// winner equivalent doesn't exist (their NO-ACTION-deferred FKs
		// would otherwise fail the transaction at COMMIT once the loser
		// rows are deleted).
		{"remap email_message issue links", `
			UPDATE aveloxis_data.email_message em
			SET linked_issue_id = wi.issue_id
			FROM aveloxis_data.issues li
			JOIN aveloxis_data.issues wi
			  ON wi.repo_id = $2 AND wi.platform_issue_id = li.platform_issue_id
			WHERE em.linked_issue_id = li.issue_id AND li.repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"clear unmapped email_message issue links", `
			UPDATE aveloxis_data.email_message SET linked_issue_id = NULL
			WHERE linked_issue_id IN (SELECT issue_id FROM aveloxis_data.issues WHERE repo_id = $1)`, []any{pair.LoserID}},
		{"remap email_message PR links", `
			UPDATE aveloxis_data.email_message em
			SET linked_pull_request_id = wp.pull_request_id
			FROM aveloxis_data.pull_requests lp
			JOIN aveloxis_data.pull_requests wp
			  ON wp.repo_id = $2 AND wp.platform_pr_id = lp.platform_pr_id
			WHERE em.linked_pull_request_id = lp.pull_request_id AND lp.repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"clear unmapped email_message PR links", `
			UPDATE aveloxis_data.email_message SET linked_pull_request_id = NULL
			WHERE linked_pull_request_id IN (SELECT pull_request_id FROM aveloxis_data.pull_requests WHERE repo_id = $1)`, []any{pair.LoserID}},
		{"remap email_message review links", `
			UPDATE aveloxis_data.email_message em
			SET linked_pr_review_id = wr.pr_review_id
			FROM aveloxis_data.pull_request_reviews lr
			JOIN aveloxis_data.pull_request_reviews wr
			  ON wr.repo_id = $2 AND wr.platform_review_id = lr.platform_review_id
			WHERE em.linked_pr_review_id = lr.pr_review_id AND lr.repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"clear unmapped email_message review links", `
			UPDATE aveloxis_data.email_message SET linked_pr_review_id = NULL
			WHERE linked_pr_review_id IN (SELECT pr_review_id FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1)`, []any{pair.LoserID}},

		// --- Step 2c: cross-repo review links (v0.25.33). Review
		// comments used to resolve their parent review via a
		// globally-scoped FindReviewDBID (repo-scoped since v0.25.33),
		// so WINNER-owned bridge rows can point at LOSER-owned
		// pull_request_reviews rows — the 2026-07-08 production failure
		// (SQLSTATE 23503 on review_comments_pr_review_id_fkey). Remap
		// every bridge row (ANY repo_id) from a loser review to the
		// winner's copy of the same review; delete the leftovers with no
		// winner equivalent. No bridge may still reference a loser
		// review when Step 4 deletes them.
		//
		// v0.27.49 COLLISION GUARD: the original v0.25.33 note said
		// "neither table has a unique key involving pr_review_id, so
		// the remaps cannot conflict" — TRUE then, FALSE since
		// v0.27.15 created uq_pr_review_msg_ref (pr_review_id,
		// msg_id). Both copies of a duplicated repo share the SAME
		// messages rows (UNIQUE platform_msg_id/platform_id/msg_kind),
		// so the winner usually ALREADY holds the (winner_review,
		// msg) link the remap would create → 23505 (the 2026-07-22
		// reconcile-repos failure wave). The msg_ref remap now skips
		// rows whose winner-equivalent link exists; the blanket
		// delete below removes them (the winner already has that
		// link — dropping the loser's copy loses nothing).
		// review_comments' uniques don't involve pr_review_id, so its
		// remap stays unguarded.
		{"remap cross-repo review_comments", `
			UPDATE aveloxis_data.review_comments rc SET pr_review_id = wr.pr_review_id
			FROM aveloxis_data.pull_request_reviews lr
			JOIN aveloxis_data.pull_request_reviews wr
			  ON wr.repo_id = $2 AND wr.platform_review_id = lr.platform_review_id
			WHERE rc.pr_review_id = lr.pr_review_id AND lr.repo_id = $1`, []any{pair.LoserID, pair.WinnerID}},
		{"delete unmappable review_comments cross-links", `
			DELETE FROM aveloxis_data.review_comments WHERE pr_review_id IN
			(SELECT pr_review_id FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1)`, []any{pair.LoserID}},
		{"remap cross-repo pull_request_review_message_ref", `
			UPDATE aveloxis_data.pull_request_review_message_ref mr SET pr_review_id = wr.pr_review_id
			FROM aveloxis_data.pull_request_reviews lr
			JOIN aveloxis_data.pull_request_reviews wr
			  ON wr.repo_id = $2 AND wr.platform_review_id = lr.platform_review_id
			WHERE mr.pr_review_id = lr.pr_review_id AND lr.repo_id = $1
			  AND NOT EXISTS (
			      SELECT 1 FROM aveloxis_data.pull_request_review_message_ref x
			      WHERE x.pr_review_id = wr.pr_review_id AND x.msg_id = mr.msg_id)`, []any{pair.LoserID, pair.WinnerID}},
		{"delete unmappable pull_request_review_message_ref cross-links", `
			DELETE FROM aveloxis_data.pull_request_review_message_ref WHERE pr_review_id IN
			(SELECT pr_review_id FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1)`, []any{pair.LoserID}},
	}
	for _, st := range steps {
		if err := exec(st.label, st.sql, st.args...); err != nil {
			return err
		}
	}

	// --- Step 3: grandchildren without a repo_id column, join-deleted
	// before their parents.
	for _, jd := range loserJoinDeletes {
		if err := exec("join-delete "+jd.label, jd.sql, pair.LoserID); err != nil {
			return err
		}
	}

	// --- Step 4: the loser's per-repo duplicated child data,
	// leaves-first.
	for _, table := range loserRepoIDDeletes {
		if err := exec("delete "+table,
			fmt.Sprintf(`DELETE FROM %s WHERE repo_id = $1`, table), pair.LoserID); err != nil {
			return err
		}
	}

	// --- Step 5: no-FK hygiene deletes (aggregates, history snapshots,
	// scancode results, network exports).
	for _, table := range loserHygieneDeletes {
		if err := exec("delete "+table,
			fmt.Sprintf(`DELETE FROM %s WHERE repo_id = $1`, table), pair.LoserID); err != nil {
			return err
		}
	}

	// --- Step 6: the loser row itself.
	if err := exec("delete loser repos row",
		`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, pair.LoserID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
