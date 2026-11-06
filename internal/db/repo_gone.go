// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_gone.go — the distinct "gone" state for repos that no longer
// resolve on their forge (v0.28.1, item 9 — the
// department-of-veterans-affairs incident: the org privatized all
// repos after collection; prelim's 404 sideline archived + dequeued
// 477 of them, and with no queue row the GUI misread the state as
// "queued for first collection" over real collected data).
//
// repos.repo_gone_at is the marker: NULL = reachable (or never
// probed since the column landed); a timestamp = prelim's probe got
// a DEFINITIVE 404/410/451. repo_archived alone cannot express this —
// it conflates "GitHub says archived" (still public, still cycling)
// with "unreachable" (privatized or deleted).
//
// 0.29.140 — the gone HISTORY. The marker is the current state only; it is
// cleared when the repository comes back, so the page could not say "this
// repository was unavailable from the 4th to the 5th" (microsoft/bobsql,
// re-created upstream on 2026-10-29, probed while still 404 on 11-04, back
// on 11-05 — operator decision 2026-11-05: keep and show every such
// stretch). aveloxis_data.repo_gone_history holds one row per EPISODE:
// opened by MarkRepoGone in the same statement as the stamp (the probe's
// status and the writer that saw it), closed by whichever clearer lifts the
// stamp (its source recorded) — or by the dedup merge, when a loser's open
// episode arrives on a reachable winner — at most one open episode per repository
// (uq_repo_gone_history_open). Every writer here also stamps the
// repository's cache state (data_changed_at) in that same statement: the
// page shows the stamp and the history, and the clearers run outside any
// collection job (the recheck ticker, the CLIs, a forge-ID adoption).
//
// Resurrection (0.29.140): a DEFINITIVE sign of life from the forge lifts a
// stale stamp. Before, only prelim's next probe (which never comes for a
// dequeued repository) or the 28-day recheck did; bobsql's org scan listed it
// under its new forge ID and the operator adopted that change, and neither
// brought it back. Now AdoptForgeID resurrects a gone-stamped row in its own
// transaction (the operator's approval that the re-created repository is this
// row's continuation — and it exists), and the org scan resurrects a
// gone-stamped repository it lists under the forge ID the row already stores
// (ResurrectIfListed; a listing under ANOTHER id is the delete-and-recreate
// signal and stays a pending change, observation-only).

package db

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// The writers that open or close a gone episode, as recorded in
// repo_gone_history.gone_source / restored_source. One spelling each; the
// page shows them as they are.
const (
	GoneSourcePrelim        = "prelim"
	GoneSourceRecheck       = "gone-recheck"
	GoneSourceMarkGoneRepos = "mark-gone-repos"
	GoneSourceReconcile     = "reconcile-repos"
	GoneSourceAdoption      = "forge-id-adoption"
	GoneSourceOrgScan       = "org-scan"
	GoneSourceBackfill      = "backfill-0.29.140"
	GoneSourceDedup         = "dedup-repos" // closes a merged loser's open episode on a reachable winner
	GoneSourceRenameHeal    = "rename-heal" // the same, for a dataless duplicate the heal removes
)

// GoneEpisode is one stretch during which a repository did not resolve on
// its forge: from the probe that saw it gone to the moment it was seen
// again (RestoredAt nil = still gone, the row's current repo_gone_at).
// Status is the probe's HTTP status (404, 410, 451; 0 when unknown — the
// episodes backfilled from a bare stamp at 0.29.140).
type GoneEpisode struct {
	GoneAt         time.Time  `json:"gone_at"`
	Status         int        `json:"status,omitempty"`
	Source         string     `json:"source"`
	RestoredAt     *time.Time `json:"restored_at,omitempty"`
	RestoredSource string     `json:"restored_source,omitempty"`
}

// MarkRepoGone stamps the gone state in ONE statement — both
// repo_archived (so every archived-filtered surface keeps excluding
// the row) and repo_gone_at. Single-statement on purpose: prelim's
// sideline contract (v0.27.39) is archive-must-succeed-before-
// dequeue, and splitting the two column writes would mint a partial
// state the recovery reasoning can't classify. v0.29.7: the sideline
// probe IS a check, so repo_gone_checked_at lands in the same
// statement — a freshly-gone repo waits a full recheck cadence before
// the scheduler probes it again. 0.29.140: the same statement opens the
// repository's gone episode (status = the probe's HTTP status, source =
// the writer) when none is open — a repository marked again while gone
// keeps its first observation — and stamps the cache state.
func (s *PostgresStore) MarkRepoGone(ctx context.Context, repoID int64, status int, source string) error {
	_, err := s.pool.Exec(ctx, `WITH `+markGoneCTEs+`
		SELECT COUNT(*) FROM episode`, repoID, status, source)
	return err
}

// markGoneCTEs is the gone stamp as data-modifying CTEs — `marked` (the
// repos row: archived, stamped, checked, cache state) and `episode` (the
// history row, opened when none is open) — shared by MarkRepoGone and
// SidelineRepo ($1 repo, $2 status, $3 source). One spelling (SR-17).
const markGoneCTEs = `
		marked AS (
			UPDATE aveloxis_data.repos
			SET repo_archived = TRUE, repo_gone_at = NOW(), repo_gone_checked_at = NOW(),
			    data_collection_date = NOW(), data_changed_at = NOW()
			WHERE repo_id = $1
			RETURNING repo_id, repo_gone_at
		),
		episode AS (
			INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source)
			SELECT repo_id, repo_gone_at, $2, $3 FROM marked
			ON CONFLICT (repo_id) WHERE restored_at IS NULL DO NOTHING
			RETURNING repo_id
		)`

// SidelineRepo is prelim's sideline as ONE transaction (Copilot PR #252, second
// review): the gone stamp with its episode, then the queue row's removal,
// committed together. As two separate statements (MarkRepoGone, then
// DequeueRepo) an org scan's ResurrectIfListed could land between them —
// clear the stamp and re-enqueue — and the dequeue then left the repository
// unstamped AND queueless: the stranded shape nothing but reconcile-repos
// reaches. Here the stamp statement takes the repos row's lock, so the two
// writers serialise on it, and the dequeue runs as a SECOND statement on
// purpose: a statement's snapshot is taken before its lock wait, so a single
// statement waiting on a resurrection could not see the queue row that
// resurrection inserted (the fresh review's two-session probe); the second
// statement's snapshot is taken after the wait and sees it. Either order
// then ends consistent — resurrected then sidelined is gone and queueless
// (prelim saw the 404 last), sidelined then resurrected is clear and queued
// — and a failure of either statement commits nothing (the next cycle
// retries); a 40P01 victim retries here.
func (s *PostgresStore) SidelineRepo(ctx context.Context, repoID int64, status int, source string) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `WITH `+markGoneCTEs+`
			SELECT COUNT(*) FROM episode`, repoID, status, source); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

// carryGoneHistorySQL moves a merged duplicate's gone episodes onto the
// repository that stays ($1 loser, $2 winner, $3 the merge's source): closed
// episodes as they are, and the loser's OPEN one closed by the merge when the
// winner is reachable (its stamp NULL — the invariant "open episode ⇔ current
// gone state" holds across the merge) or dropped when the winner is gone
// itself (uq_repo_gone_history_open takes the DO NOTHING). Provenance travels
// with the row, and the winner's cache state is stamped in the same statement
// when anything was carried (its page's gone history changed; Copilot PR
// #252, third review). The caller holds BOTH repos rows FOR UPDATE
// (lockRepoRowsSQL), so the loser can gain no episode after the copy and the
// winner's state read here is the state the merge commits against. Shared by
// the dedup merge and the dataless rename heal (one spelling).
const carryGoneHistorySQL = `
			WITH carried AS (
				INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source, restored_at, restored_source,
				                                             tool_source, tool_version, data_source, data_collection_date)
				SELECT $2, l.gone_at, l.gone_status, l.gone_source,
				       CASE WHEN l.restored_at IS NULL AND w.repo_gone_at IS NULL THEN NOW() ELSE l.restored_at END,
				       CASE WHEN l.restored_at IS NULL AND w.repo_gone_at IS NULL THEN $3 ELSE l.restored_source END,
				       l.tool_source, l.tool_version, l.data_source, l.data_collection_date
				  FROM aveloxis_data.repo_gone_history l
				  JOIN aveloxis_data.repos w ON w.repo_id = $2
				 WHERE l.repo_id = $1
				ON CONFLICT (repo_id) WHERE restored_at IS NULL DO NOTHING
				RETURNING repo_id
			)
			UPDATE aveloxis_data.repos SET data_changed_at = NOW()
			WHERE repo_id = $2 AND EXISTS (SELECT 1 FROM carried)`

// lockRepoRowsSQL takes a merge's two repos rows FOR NO KEY UPDATE, in id
// order ($1 = both ids) and BEFORE any queue-row lock — the same order as the
// gone writers (SidelineRepo: the repos row, then the queue row), so a merge
// and a concurrent sideline or resurrection never deadlock, and a loser can
// gain no episode between the carry and its delete (Copilot PR #252, third
// review). NO KEY UPDATE, not FOR UPDATE (fifth review): every writer this
// must exclude UPDATEs non-key columns of the row and so takes NO KEY UPDATE
// itself, which conflicts; a foreign-key check from a child table
// (user_repo_stars.repo_id — a user starring the winner) takes KEY SHARE,
// which conflicts with FOR UPDATE but not with this, so the star commits
// instead of waiting on the merge while the merge's own ON CONFLICT insert
// of that star waits on it — the deadlock whose heal-side victim left the
// duplicate dequeued with its links unrepaired. The one reverse edge left is
// the daily-table writers (heal-commit-daily: its advisory lock and the
// loser's daily rows, THEN the repos row); a merge beside such a fill can be
// a deadlock victim, and every merge caller retries on 40P01 (the batch,
// DedupRenamedRepoPair and HealRenamedDuplicate), as the fill does.
const lockRepoRowsSQL = `SELECT 1 FROM aveloxis_data.repos WHERE repo_id = ANY($1::bigint[]) ORDER BY repo_id FOR NO KEY UPDATE`

// MarkRepoGoneChecked records that the gone state was re-verified
// against the forge and the repo is STILL gone (or the probe was not
// definitive — the caller decides; see runGoneRecheck). Guarded to
// gone-stamped rows, and BOTH clearers of the gone state (ClearRepoGone
// on prelim's healthy path, ResurrectRepo on the recheck/CLI path —
// review round 2, L11) null the check stamp with it, so a reachable
// repo carries no check stamp and a later gone→recheck cycle starts
// from MarkRepoGone's own stamp.
func (s *PostgresStore) MarkRepoGoneChecked(ctx context.Context, repoID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repos
		SET repo_gone_checked_at = NOW()
		WHERE repo_id = $1 AND repo_gone_at IS NOT NULL`, repoID)
	return err
}

// GetGoneRecheckCandidates returns up to limit gone-stamped repos
// whose last verification is older than olderThan (never-checked rows
// first, then oldest check first). olderThan is the operator's
// cadence (CollectionConfig.GoneRepoRecheckInterval) — the store
// carries no default of its own (SR-10: one default layer).
func (s *PostgresStore) GetGoneRecheckCandidates(ctx context.Context, olderThan time.Duration, limit int) ([]GoneProbeCandidate, error) {
	// Only repo_id + repo_git are read (review round 1: the commits
	// EXISTS the queueless candidate query needs is 500 index probes
	// per tick for nothing here); GoneStamped is true by construction.
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_git
		FROM aveloxis_data.repos r
		WHERE r.repo_gone_at IS NOT NULL
		  AND (r.repo_gone_checked_at IS NULL OR r.repo_gone_checked_at < NOW() - $1::interval)
		ORDER BY r.repo_gone_checked_at NULLS FIRST, r.repo_id
		LIMIT $2`, olderThan, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GoneProbeCandidate
	for rows.Next() {
		c := GoneProbeCandidate{GoneStamped: true}
		if err := rows.Scan(&c.RepoID, &c.GitURL); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// clearGoneCTE is the clear of the gone state as a data-modifying CTE named
// `cleared`, shared by every clearer (one spelling, SR-17): the stamp, the
// check stamp and the forge's unavailability notice go together, the cache
// state is stamped, and only a gone-stamped row is touched ($1 = repo_id;
// %s = an extra guard, or nothing).
const clearGoneCTE = `
		cleared AS (
			UPDATE aveloxis_data.repos
			SET repo_gone_at = NULL, repo_gone_checked_at = NULL,
			    repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
			    data_changed_at = NOW()
			WHERE repo_id = $1 AND repo_gone_at IS NOT NULL%s
			RETURNING repo_id
		)`

// closeEpisodeCTE closes the repository's open episode for the rows
// `cleared` named ($2 = the source).
const closeEpisodeCTE = `
		closed AS (
			UPDATE aveloxis_data.repo_gone_history h
			SET restored_at = NOW(), restored_source = $2
			FROM cleared
			WHERE h.repo_id = cleared.repo_id AND h.restored_at IS NULL
			RETURNING h.repo_id
		)`

// ClearRepoGone clears a stale gone stamp (and, since v0.29.7, the
// check stamp with it). Called by the SAME probe that sets it —
// prelim's healthy path; mark-gone-repos and the recheck ticker go
// through ResurrectRepo instead (v0.28.6) — so resurrection is
// symmetric: an org that re-publicizes
// gets its repos back the moment they're probed again. The
// IS NOT NULL guard makes it a 0-row no-op for the normal fleet.
// 0.29.140: closes the open episode with source in the same statement.
func (s *PostgresStore) ClearRepoGone(ctx context.Context, repoID int64, source string) error {
	_, err := s.pool.Exec(ctx, `WITH `+fmt.Sprintf(clearGoneCTE, "")+`,`+closeEpisodeCTE+`
		SELECT COUNT(*) FROM closed`, repoID, source)
	return err
}

// resurrectSQL is the clear + close + re-enqueue as ONE statement ($1 repo,
// $2 source, $3 priority, $4 alwaysEnqueue; guard = an extra predicate on the
// clear). The queue upsert mirrors EnqueueRepo verbatim (collecting rows are
// left alone); it runs for a row the clear touched, or always when the
// caller asks (reconcile-repos' lost-queue-row arm enqueues a repository
// that was never gone).
func resurrectSQL(guard string) string {
	return `WITH ` + fmt.Sprintf(clearGoneCTE, guard) + `,` + closeEpisodeCTE + `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		SELECT $1, $3, 'queued', NOW()
		WHERE $4::boolean OR EXISTS (SELECT 1 FROM cleared)
		ON CONFLICT (repo_id) DO UPDATE SET
			priority = LEAST(collection_queue.priority, EXCLUDED.priority),
			status = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.status ELSE 'queued' END,
			due_at = CASE WHEN collection_queue.status = 'collecting' THEN collection_queue.due_at ELSE NOW() END,
			updated_at = NOW()`
}

// ResurrectRepo is mark-gone-repos' 2xx recovery as ONE statement
// (v0.28.6, Copilot round; SR-18 — the atomicity lives in the owning
// layer so a wrong caller cannot half-apply): clear the gone stamp AND
// re-enqueue for collection together. The two-statement sequence had
// no recoverable ordering — clear-then-failed-enqueue stranded the
// repo un-stamped and queueless (the rerun's !GoneStamped check never
// retried), while enqueue-then-failed-clear dropped it out of
// GetGoneProbeCandidates' queueless candidate set (only prelim's next
// probe would heal it). Atomic: either both commit or the rerun sees
// the untouched gone-stamped queueless state and retries cleanly.
// The repository is enqueued whether or not it was gone-stamped
// (reconcile-repos' lost-queue-row arm, 0.29.140); source names the
// writer in the episode it closes.
func (s *PostgresStore) ResurrectRepo(ctx context.Context, repoID int64, priority int, source string) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, resurrectSQL(""), repoID, source, priority, true)
		return err
	})
}

// listedGuardSQL is ResurrectIfListed's extra predicate on the clear: the
// row stores the listed forge id ($5), and the repository's LATEST closed
// episode was not the org scan's own restoration — a repository the forge
// lists but whose page an unauthenticated probe cannot see (a blocked one
// the listing still carries) would otherwise flap at every org refresh, an
// episode per cycle (round 1 F1). Once per gone stretch: the scan's
// restoration reversed by prelim's next mark is not repeated, while a later
// stretch whose last restoration was the recheck's, an adoption's or
// prelim's is the scan's to end again (round 2 F2: a predicate over EVERY
// earlier org-scan restoration refused for the repository's lifetime). The
// recheck, an adoption and prelim's own 2xx are not bounded by it.
const listedGuardSQL = ` AND platform_repo_id = $5
			  AND COALESCE((SELECT h.restored_source FROM aveloxis_data.repo_gone_history h
			                WHERE h.repo_id = $1 AND h.restored_at IS NOT NULL
			                ORDER BY h.restored_at DESC, h.episode_id DESC LIMIT 1), '') <> '` + GoneSourceOrgScan + `'`

// ResurrectIfListed is the org scan's resurrection (0.29.140): the forge
// listed the repository (publicly — the caller checks) under forgeID, so a
// gone-stamped row that STORES that id is reachable again — cleared, its
// episode closed, re-enqueued, in one statement, and true is returned. A
// row whose stored id differs is left as it is (the delete-and-recreate
// signal SetPlatformRepoIDIfEmptySeen records; the operator's adoption
// resurrects it), as is a row that is not gone (false), as is a row whose
// earlier org-scan resurrection a later gone mark reversed (listedGuardSQL).
// An empty forgeID matches nothing: the many empty-column rows must never
// match an absent listing field.
func (s *PostgresStore) ResurrectIfListed(ctx context.Context, repoID int64, forgeID string, priority int) (bool, error) {
	if strings.TrimSpace(forgeID) == "" {
		return false, nil
	}
	var resurrected bool
	err := s.withRetry(ctx, func(ctx context.Context) error {
		tag, err := s.pool.Exec(ctx, resurrectSQL(listedGuardSQL), repoID, GoneSourceOrgScan, priority, false, forgeID)
		resurrected = err == nil && tag.RowsAffected() > 0
		return err
	})
	return resurrected, err
}

// resurrectInTx is the same recovery inside a caller's transaction
// (AdoptForgeID: the adoption and the resurrection commit together, or
// neither does). Only a gone-stamped row is enqueued.
func resurrectInTx(ctx context.Context, tx rotationExecer, repoID int64, priority int, source string) error {
	_, err := tx.Exec(ctx, resurrectSQL(""), repoID, source, priority, false)
	return err
}

// resurrectPriority is the queue priority a resurrected repository takes
// (the recheck ticker's and mark-gone-repos' since v0.28.6: ahead of the
// fleet's routine 100, behind an operator's add).
const resurrectPriority = 10

// repoGoneHistoryBatch reads every episode of each repository, oldest first,
// for the repository page.
func (s *PostgresStore) repoGoneHistoryBatch(ctx context.Context, repoIDs []int64) (map[int64][]GoneEpisode, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, gone_at, gone_status, gone_source, restored_at, restored_source
		FROM aveloxis_data.repo_gone_history
		WHERE repo_id = ANY($1)
		ORDER BY repo_id, gone_at, episode_id`, repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64][]GoneEpisode{}
	for rows.Next() {
		var id int64
		var e GoneEpisode
		if err := rows.Scan(&id, &e.GoneAt, &e.Status, &e.Source, &e.RestoredAt, &e.RestoredSource); err != nil {
			return nil, err
		}
		out[id] = append(out[id], e)
	}
	return out, rows.Err()
}

// The two migrate steps of this state live in migrate.go as literal SQL (the
// step-order analyzer reads them there): the v0.27.50 mirror of the forge's
// archived flag, which since 0.29.140 leaves gone-stamped rows alone, and the
// ledgered 0.29.140 backfill that opens an episode for every repository
// stamped before this history existed ("v0.29.140 open a gone episode for
// every gone-stamped repository").

// GoneProbeCandidate is one row of the mark-gone-repos work list.
type GoneProbeCandidate struct {
	RepoID      int64
	GitURL      string
	GoneStamped bool
	HasData     bool
}

// GetGoneProbeCandidates returns the repos worth probing for the
// gone state: QUEUELESS rows (prelim dequeues gone repos; genuinely
// archived repos keep their queue rows and keep cycling) that either
// carry the archived sideline flag, already carry a gone stamp
// (so a 200 can resurrect them), or hold collected commit data.
// Dataless unarchived queueless rows — the pre-v0.27.39
// stranded-row residue — fall out naturally: there is nothing to
// display for them either way (reconcile-repos territory).
func (s *PostgresStore) GetGoneProbeCandidates(ctx context.Context, limit int) ([]GoneProbeCandidate, error) {
	// v0.28.11 (Copilot round 10): limit 0 = ALL, expressed as a NULL
	// SQL LIMIT — the old 1,000,000 substitution was a silent cap
	// that would let a larger fleet report "complete" while leaving
	// higher-id candidates unprobed.
	if limit < 0 {
		limit = 0
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_git,
		       r.repo_gone_at IS NOT NULL,
		       EXISTS (SELECT 1 FROM aveloxis_data.commits c WHERE c.repo_id = r.repo_id)
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE q.repo_id IS NULL
		  AND (COALESCE(r.repo_archived, FALSE)
		       OR r.repo_gone_at IS NOT NULL
		       OR EXISTS (SELECT 1 FROM aveloxis_data.commits c2 WHERE c2.repo_id = r.repo_id))
		ORDER BY r.repo_id
		LIMIT NULLIF($1, 0)`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GoneProbeCandidate
	for rows.Next() {
		var c GoneProbeCandidate
		if err := rows.Scan(&c.RepoID, &c.GitURL, &c.GoneStamped, &c.HasData); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
