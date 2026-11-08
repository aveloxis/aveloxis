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
// opened by SidelineRepo in the same statement as the stamp (the probe's
// status and the writer that saw it), closed by whichever clearer lifts the
// stamp (its source recorded) — or by the dedup merge or the rename heal,
// when a merged duplicate's open episode arrives (carryGoneHistorySQL) — at
// most one open episode per repository
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

	"github.com/jackc/pgx/v5/pgconn"
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
	GoneSourceDedup         = "dedup-repos" // closes a merged loser's open episode on a reachable winner with no evidence of when it came back (an inference)
	GoneSourceRenameHeal    = "rename-heal" // the same, for a dataless duplicate the heal removes
	// GoneSourceCollection names a metadata snapshot (repo_info) as what saw
	// a repository back: a merge closes a stale episode at the first
	// snapshot after it (carryGoneHistorySQL).
	GoneSourceCollection = "collection"
)

// GoneEpisode is one stretch during which a repository did not resolve on
// its forge: from the probe that saw it gone to the moment it was seen
// again (RestoredAt nil = still gone, the row's current repo_gone_at).
// Status is the probe's HTTP status (404, 410, 451; 0 when unknown — the
// episodes backfilled from a bare stamp at 0.29.140, and a current stretch a
// merge restarted at the repository's latest gone mark, whose Source is
// empty too: the gone marker keeps only an episode's first observation).
type GoneEpisode struct {
	GoneAt         time.Time  `json:"gone_at"`
	Status         int        `json:"status,omitempty"`
	Source         string     `json:"source"`
	RestoredAt     *time.Time `json:"restored_at,omitempty"`
	RestoredSource string     `json:"restored_source,omitempty"`
}

// markGoneCTEs is the gone stamp as data-modifying CTEs — `marked` (the
// repos row: archived — so every archived-filtered surface keeps excluding
// the row — stamped, checked, cache state) and `episode` (the history row,
// opened when none is open; a repository marked again while gone keeps its
// first observation) — SidelineRepo's one statement ($1 repo, $2 status, $3
// source). v0.29.7: the sideline probe IS a check, so repo_gone_checked_at
// lands here too — a freshly-gone repo waits a full recheck cadence before
// the scheduler probes it again. One spelling (SR-17).
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

// SidelineRepo is THE gone marker (Copilot PR #252, second and sixth
// reviews): the gone stamp with its episode, then the queue row's removal,
// committed together — prelim's sideline, mark-gone-repos' and
// reconcile-repos' dead arms all go through it, so "gone" always means
// "and out of the queue" in the same transaction (SR-18). As two separate
// statements (a stamp, then DequeueRepo) an org scan's ResurrectIfListed
// could land between them — clear the stamp and re-enqueue — and the dequeue
// then left the repository unstamped AND queueless: the stranded shape
// nothing but reconcile-repos reaches; and a stamp WITHOUT a dequeue (the two
// CLIs, before the sixth review) left a resurrected repository gone and
// queued when the resurrection landed between the candidate read and the
// stamp. Here the stamp statement takes the repos row's lock, so the writers
// serialise on it, and the dequeue runs as a SECOND statement on purpose: a
// statement's snapshot is taken before its lock wait, so a single statement
// waiting on a resurrection could not see the queue row that resurrection
// inserted (the fresh review's two-session probe); the second statement's
// snapshot is taken after the wait and sees it. Either order then ends
// consistent — resurrected then sidelined is gone and queueless (the probe
// saw the 404 last), sidelined then resurrected is clear and queued — and a
// failure of either statement commits nothing (the next cycle retries); a
// 40P01 victim retries here. A queued gone-stamped row exists only as a
// pending re-probe: a user's re-add (EnqueueRepo) of a gone repository,
// which prelim's next probe clears or sidelines.
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
// repository that stays ($1 loser, $2 winner, $3 the merge's source). The two
// rows are one upstream, so their episodes are observations of one history:
//   - closed episodes are carried as they are;
//   - the loser's OPEN episode ends at the earliest evidence that the
//     upstream was reachable after it began (seen_back: a restoration in
//     either row's history, or a metadata snapshot in repo_info or
//     repo_info_history, the evidence listedGuardSQL reads), restored then
//     by the evidence's own source (seen_src: the restoring writer, or
//     collection for a snapshot), so a later merge reads it as evidence too
//     (the fifth L10 pass). A gone row is out of the queue and rechecked
//     about every 28 days, so it can be weeks stale behind the winner's own
//     observations (the second L10 pass on Copilot PR #252's sixteenth
//     review: a fold read "not reachable from -30 to -20" beside "not
//     reachable since -30 — current");
//   - with no such evidence, on a reachable winner it arrives closed by the
//     merge (restored now by $3, the one inferred closure), keeping "open
//     episode ⇔ current gone state";
//   - the same evidence, applied to a gone winner's open episode (the
//     mirror, the third and fourth L10 passes): the merge never clears the
//     winner's stamp, so that episode cannot simply end; when a definitive
//     gone observation came after the evidence — the loser's open episode
//     (without evidence of its own) or the winner's stamp time, which only
//     the gone marker refreshes — the history SPLITS: the winner's old
//     stretch inserted closed at the first evidence, by its source (split),
//     and the open episode restarted at the earlier of those observations
//     after the LATEST evidence (restart, folded; the fifth L10 pass: the
//     first let a restart at the mark contain a later restoration). A
//     restart at the mark records status 0 and no writer: the gone marker
//     keeps an episode's first observation, so the mark's are unknown. With
//     neither, the stamp is staler than the evidence and the episode is left
//     for the recheck; the merge never invents a restart. A merge's own
//     closures (dedup-repos, rename-heal: inferred from an unstamped winner,
//     never probed) are not evidence;
//   - with none, on a gone winner the two open episodes are one outage seen
//     twice, so they FOLD (folded): the winner's open episode starts at the
//     earlier observation (its time, status and source), the rule a
//     repository marked again while gone already follows, and a later loser
//     episode lies inside it and adds nothing. Closing it instead printed
//     "back via dedup-repos" for an outage that never ended (the L10 pass on
//     Copilot PR #252's sixteenth review, which found the loser's outage
//     dropped there);
//   - with none, a stamped winner with no open episode (the shape the
//     0.29.140 backfill removes at migrate) takes the loser's open episode
//     as its own, open.
//
// On a gone winner the loser's open row is inserted open and the partial
// unique arbiter (uq_repo_gone_history_open) drops it when the winner has
// its own open episode, the fold having carried its start; with none, the
// row stays as the winner's open episode. Provenance travels with a carried
// row (a folded row keeps its own), and the winner's cache state is stamped
// in the same statement when anything was carried, split or folded (its
// page's gone history changed; Copilot PR #252, third review). The caller
// holds BOTH repos rows FOR NO KEY UPDATE (lockRepoRowsSQL), so the loser
// can gain no episode after the copy and the winner's gone state read here
// (winner) is the state the merge commits against. A snapshot's
// data_collection_date is when it was processed (InsertRepoInfo leaves it to
// the column default), not when the forge answered: one fetched while
// reachable, left unprocessed in staging across a sideline and drained
// later, reads as later evidence. Declined here (the third L10 pass):
// listedGuardSQL trusts the same date, and dating snapshots by their fetch
// time changes every repo_info row's date, an Augur-parity column, beyond
// this merge. A winner row missing at the read inserts a closed row whose
// deferred foreign key fails the commit: the merge rolls back, never
// deleting the loser's history unseen. Shared by the dedup merge and the
// dataless rename heal (one spelling).
const carryGoneHistorySQL = `
			WITH winner AS (
				SELECT r.repo_gone_at IS NOT NULL AS gone, r.repo_gone_at AS last_marked
				  FROM aveloxis_data.repos r WHERE r.repo_id = $2
			),
			evidence AS (
				SELECT h.restored_at AS at, h.restored_source AS src FROM aveloxis_data.repo_gone_history h
				 WHERE h.repo_id IN ($1, $2) AND h.restored_at IS NOT NULL
				   AND h.restored_source NOT IN ('` + GoneSourceDedup + `', '` + GoneSourceRenameHeal + `')
				UNION ALL
				SELECT ri.data_collection_date, '` + GoneSourceCollection + `' FROM aveloxis_data.repo_info ri
				 WHERE ri.repo_id IN ($1, $2) AND ri.data_collection_date IS NOT NULL
				UNION ALL
				SELECT rh.data_collection_date, '` + GoneSourceCollection + `' FROM aveloxis_data.repo_info_history rh
				 WHERE rh.repo_id IN ($1, $2) AND rh.data_collection_date IS NOT NULL
			),
			loser AS (
				SELECT l.*, sb.at AS seen_back, sb.src AS seen_src
				  FROM aveloxis_data.repo_gone_history l
				  LEFT JOIN LATERAL (SELECT e.at, e.src FROM evidence e
				                      WHERE l.restored_at IS NULL AND e.at > l.gone_at
				                      ORDER BY e.at, e.src LIMIT 1) sb ON TRUE
				 WHERE l.repo_id = $1
			),
			loser_open AS (
				SELECT * FROM loser WHERE restored_at IS NULL AND seen_back IS NULL
			),
			winner_open AS (
				SELECT x.*,
				       CASE WHEN x.seen_back IS NOT NULL THEN LEAST(
				           (SELECT lo.gone_at FROM loser_open lo),
				           CASE WHEN x.last_marked > (SELECT MAX(e.at) FROM evidence e) THEN x.last_marked END)
				       END AS restart
				  FROM (SELECT o.*, w.last_marked, sb.at AS seen_back, sb.src AS seen_src
				          FROM aveloxis_data.repo_gone_history o
				          JOIN winner w ON w.gone
				          LEFT JOIN LATERAL (SELECT e.at, e.src FROM evidence e WHERE e.at > o.gone_at
				                              ORDER BY e.at, e.src LIMIT 1) sb ON TRUE
				         WHERE o.repo_id = $2 AND o.restored_at IS NULL) x
			),
			carried AS (
				INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source, restored_at, restored_source,
				                                             tool_source, tool_version, data_source, data_collection_date)
				SELECT $2, l.gone_at, l.gone_status, l.gone_source,
				       CASE WHEN l.restored_at IS NOT NULL THEN l.restored_at
				            WHEN l.seen_back IS NOT NULL THEN l.seen_back
				            WHEN COALESCE(w.gone, FALSE) THEN NULL
				            ELSE NOW() END,
				       CASE WHEN l.restored_at IS NOT NULL THEN l.restored_source
				            WHEN l.seen_back IS NOT NULL THEN l.seen_src
				            WHEN COALESCE(w.gone, FALSE) THEN ''
				            ELSE $3 END,
				       l.tool_source, l.tool_version, l.data_source, l.data_collection_date
				  FROM loser l
				  LEFT JOIN winner w ON TRUE
				ON CONFLICT (repo_id) WHERE restored_at IS NULL DO NOTHING
				RETURNING repo_id
			),
			split AS (
				INSERT INTO aveloxis_data.repo_gone_history (repo_id, gone_at, gone_status, gone_source, restored_at, restored_source,
				                                             tool_source, tool_version, data_source, data_collection_date)
				SELECT $2, o.gone_at, o.gone_status, o.gone_source, o.seen_back, o.seen_src,
				       o.tool_source, o.tool_version, o.data_source, o.data_collection_date
				  FROM winner_open o
				 WHERE o.restart IS NOT NULL
				ON CONFLICT (repo_id) WHERE restored_at IS NULL DO NOTHING
				RETURNING repo_id
			),
			folded AS (
				UPDATE aveloxis_data.repo_gone_history o
				   SET gone_at     = COALESCE(wo.restart, l.gone_at),
				       gone_status = CASE WHEN wo.restart IS NOT NULL AND wo.restart IS DISTINCT FROM l.gone_at THEN 0 ELSE l.gone_status END,
				       gone_source = CASE WHEN wo.restart IS NOT NULL AND wo.restart IS DISTINCT FROM l.gone_at THEN '' ELSE l.gone_source END
				  FROM winner_open wo
				  LEFT JOIN loser_open l ON TRUE
				 WHERE o.episode_id = wo.episode_id
				   AND (wo.restart IS NOT NULL OR (wo.seen_back IS NULL AND l.gone_at < wo.gone_at))
				RETURNING o.repo_id
			)
			UPDATE aveloxis_data.repos SET data_changed_at = NOW()
			WHERE repo_id = $2
			  AND (EXISTS (SELECT 1 FROM carried) OR EXISTS (SELECT 1 FROM split) OR EXISTS (SELECT 1 FROM folded))`

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
// from the sideline's own stamp.
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

// clearGoneCTE is the clear of a sideline as a data-modifying CTE named
// `cleared`, shared by every clearer (one spelling, SR-17): the stamp, the
// check stamp and the forge's unavailability notice go together, the cache
// state is stamped, and only a sidelined row is touched ($1 = repo_id; %s =
// the sideline predicate — goneSidelineSQL, with an extra guard appended
// when the caller has one, or archivedSidelineSQL). The archived flag the stamp set
// (markGoneCTEs: repo_archived = TRUE, so every archived-filtered surface
// kept excluding the row) goes back to the forge's LAST WORD — the latest
// repo_info snapshot's status, FALSE when there is none (Copilot PR #252,
// eighth review: the clearers left it TRUE, so a recovered repository kept
// the archived collection cadence, archivedStretchCaseSQL, and the archived
// badge until its next metadata collection rewrote the flag). The same
// rule as the v0.27.50 migrate step's, which skips gone-stamped rows and
// so never reaches these; the latest snapshot is picked the fleet's way
// (data_collection_date DESC NULLS LAST, repo_info_id DESC).
const clearGoneCTE = `
		cleared AS (
			UPDATE aveloxis_data.repos
			SET repo_gone_at = NULL, repo_gone_checked_at = NULL,
			    repo_unavailable_reason = NULL, repo_unavailable_url = NULL,
			    repo_archived = COALESCE((SELECT ri.status = 'Archived' FROM aveloxis_data.repo_info ri
			                              WHERE ri.repo_id = $1
			                              ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
			                              LIMIT 1), FALSE),
			    data_changed_at = NOW()
			WHERE repo_id = $1 AND %s
			RETURNING repo_id
		)`

// goneSidelineSQL is the gone sideline: the stamp prelim, mark-gone-repos and
// reconcile-repos write (SidelineRepo). Every gone clearer lifts it.
const goneSidelineSQL = "repo_gone_at IS NOT NULL"

// archivedSidelineSQL is the OLD dead-arm sideline (0.29.140, operator
// decision 2026-11-07): before this release reconcile-repos set repo_archived
// ALONE on a definitive 404 — no gone stamp, no episode — and the row sat
// archived and queueless (49 on kate). A repository the forge archived keeps
// its queue row and cycles, so archived + unstamped + queueless is that
// residue and nothing else; a queueless row that is not archived (a
// data-bearing rename duplicate prelim dequeued on purpose) never matches.
// ResurrectArchivedSideline lifts it on a definitive 2xx.
const archivedSidelineSQL = `repo_gone_at IS NULL AND COALESCE(repo_archived, FALSE)
			  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = $1)`

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
	_, err := s.clearUnderRowLock(ctx, repoID, goneClear{sideline: goneSidelineSQL, build: clearOnlySQL}, repoID, source)
	return err
}

// clearOnlySQL is the clear + close without a re-enqueue (prelim's: the
// probe that cleared it is collecting the repository) — $1 repo, $2 source.
func clearOnlySQL(predicate string) string {
	return `WITH ` + fmt.Sprintf(clearGoneCTE, predicate) + `,` + closeEpisodeCTE + `
		SELECT COUNT(*) FROM closed`
}

// goneClear is one clear, as the lock-first helpers run it: the sideline
// (the predicate the row lock gates on and the clear acts on; it may name $1
// only, the lock's one parameter), an optional guard the clear alone adds
// (it may name later parameters: listedGuardSQL's $5), the statement builder
// (resurrectSQL, clearOnlySQL), and lockAlways for ResurrectRepo, which
// enqueues a repository that was never gone (reconcile-repos' lost-queue-row
// arm) and so locks whether or not the row is sidelined. Both statements are
// built from the one sideline, so the gate is never narrower than the clear
// (the L10 pass on the gate).
type goneClear struct {
	sideline   string
	guard      string
	build      func(predicate string) string
	lockAlways bool
}

// clearUnderRowLock runs a clear (clearGoneCTE with its episode close, and
// resurrectSQL's queue upsert) after taking the repos row's lock in a
// statement of its own, in one transaction — the one way every clearer runs
// (SR-18; Copilot PR #252, seventeenth review). As a single statement the
// clear's snapshot was taken BEFORE its lock wait: a waiting UPDATE
// re-checks only the row it locked, so a clearer queued behind a sideline
// cleared the sideline's new stamp but could not see the episode the
// sideline committed beside it, and left it open on a reachable repository
// (no later clear reaches it: the stamp is NULL). The statement after the
// lock takes its snapshot after the wait, and sees it. The lock first also
// keeps one order, repos row then queue row (SidelineRepo's, the merge's):
// resurrectSQL's queue upsert could take the queue row before the repos row
// and deadlock with a sideline's dequeue (40P01, a deadlock_timeout stall,
// one side retried). It opens its own transaction; clearInTxUnderRowLock is
// the same inside a caller's (an adoption).
//
// gate is the sideline predicate the clear acts on (it may name $1, the
// repository): the lock takes only a row that matches it, and the clear is
// skipped when none does, so a healthy repository costs no lock and waits for
// nothing (the L10 pass on the fix: prelim clears on every healthy probe, and
// an unconditional lock waited behind any holder of the row, an adoption's era
// restore for minutes). A locker that waits re-checks the row's newest
// version, so the race above stays closed; a row whose stamp is still being
// written by an uncommitted sideline is skipped, the clear ordered before the
// sideline. lockAlways locks unconditionally (goneClear).
//
// Declined in the same pass, with the reason: a healthy probe still pays
// BEGIN, the gated SELECT and COMMIT, two round trips more than the old
// autocommit statement, on a connection the probe holds anyway and beside
// the forge request it just made; and ResurrectRepo's unconditional lock
// can wait behind a holder of a never-gone row, which only the command-line
// runs (reconcile-repos, mark-gone-repos) and the recheck of gone rows reach.
func (s *PostgresStore) clearUnderRowLock(ctx context.Context, repoID int64, c goneClear, args ...any) (pgconn.CommandTag, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := clearInTxUnderRowLock(ctx, tx, repoID, c, args...)
	if err != nil {
		return tag, err
	}
	return tag, tx.Commit(ctx)
}

// clearInTxUnderRowLock is clearUnderRowLock inside a caller's transaction.
func clearInTxUnderRowLock(ctx context.Context, tx rotationExecer, repoID int64, c goneClear, args ...any) (pgconn.CommandTag, error) {
	sql := c.build(c.sideline + c.guard)
	if c.lockAlways {
		if _, err := tx.Exec(ctx, lockRepoRowsSQL, []int64{repoID}); err != nil {
			return pgconn.CommandTag{}, fmt.Errorf("lock repository %d: %w", repoID, err)
		}
		return tx.Exec(ctx, sql, args...)
	}
	locked, err := tx.Exec(ctx, `SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1 AND (`+c.sideline+`) FOR NO KEY UPDATE`, repoID)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("lock repository %d: %w", repoID, err)
	}
	if locked.RowsAffected() == 0 {
		return pgconn.CommandTag{}, nil // not sidelined: nothing to clear
	}
	return tx.Exec(ctx, sql, args...)
}

// resurrectSQL is the clear + close + re-enqueue as ONE statement ($1 repo,
// $2 source, $3 priority, $4 alwaysEnqueue; sideline = the clear's
// predicate: goneSidelineSQL, plus a guard, or archivedSidelineSQL). The
// queue upsert mirrors EnqueueRepo verbatim (collecting rows are left
// alone); it runs for a row the clear touched, or always when the caller
// asks (reconcile-repos' lost-queue-row arm enqueues a repository that was
// never gone).
func resurrectSQL(sideline string) string {
	return `WITH ` + fmt.Sprintf(clearGoneCTE, sideline) + `,` + closeEpisodeCTE + `
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
		_, err := s.clearUnderRowLock(ctx, repoID, goneClear{sideline: goneSidelineSQL, build: resurrectSQL, lockAlways: true}, repoID, source, priority, true)
		return err
	})
}

// ResurrectArchivedSideline lifts the old dead-arm sideline (archivedSidelineSQL)
// on a definitive 2xx: the same statement as every resurrection — the flag
// back to the forge's last word, the cache state stamped, the row re-queued —
// under the residue's predicate, and true when the row was lifted. Nothing
// for a gone-stamped row (ResurrectRepo's), a row with a queue row, or a
// queueless row that is not archived. No episode is opened or closed: that
// arm recorded no stretch, and none is fabricated (the gone time is
// unknown). mark-gone-repos is the caller.
func (s *PostgresStore) ResurrectArchivedSideline(ctx context.Context, repoID int64, priority int, source string) (bool, error) {
	var lifted bool
	err := s.withRetry(ctx, func(ctx context.Context) error {
		tag, err := s.clearUnderRowLock(ctx, repoID, goneClear{sideline: archivedSidelineSQL, build: resurrectSQL}, repoID, source, priority, false)
		lifted = err == nil && tag.RowsAffected() > 0
		return err
	})
	return lifted, err
}

// listedGuardSQL is ResurrectIfListed's extra predicate on the clear: the
// row stores the listed forge id ($5), and the org scan's own last
// restoration, if the repository's latest closed episode is one, was not
// REVERSED — a repository the forge lists but whose page an unauthenticated
// probe cannot see (a blocked one the listing still carries) would otherwise
// flap at every org refresh, an episode per cycle (round 1 F1). Reversed
// means no collection succeeded after that restoration: no metadata snapshot
// (repo_info, written by a collection's metadata phase, which runs only
// after prelim's probe answered; or repo_info_history, where the rotation
// moves it — it runs in its own transaction before the insert, so a failed
// insert leaves the stretch's snapshot only there) is dated after it — prelim's next mark
// undid it. A restoration followed by a collected stretch is not reversed,
// so a later, unrelated outage is the scan's to end again (Copilot PR #252,
// thirteenth review: "the latest closer was the org scan" refused it until
// the 28-day recheck); so is a stretch whose last restoration was the
// recheck's, an adoption's or prelim's (round 2 F2). The recheck, an
// adoption and prelim's own 2xx are not bounded by it.
const listedGuardSQL = ` AND platform_repo_id = $5
			  AND NOT EXISTS (
			      SELECT 1 FROM (SELECT h.restored_source, h.restored_at FROM aveloxis_data.repo_gone_history h
			                      WHERE h.repo_id = $1 AND h.restored_at IS NOT NULL
			                      ORDER BY h.restored_at DESC, h.episode_id DESC LIMIT 1) last
			      WHERE last.restored_source = '` + GoneSourceOrgScan + `'
			        AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_info ri
			                        WHERE ri.repo_id = $1 AND ri.data_collection_date > last.restored_at
			                        UNION ALL
			                        SELECT 1 FROM aveloxis_data.repo_info_history rh
			                        WHERE rh.repo_id = $1 AND rh.data_collection_date > last.restored_at))`

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
		tag, err := s.clearUnderRowLock(ctx, repoID, goneClear{sideline: goneSidelineSQL, guard: listedGuardSQL, build: resurrectSQL}, repoID, GoneSourceOrgScan, priority, false, forgeID)
		resurrected = err == nil && tag.RowsAffected() > 0
		return err
	})
	return resurrected, err
}

// resurrectInTx is the same recovery inside a caller's transaction
// (AdoptForgeID: the adoption and the resurrection commit together, or
// neither does). Only a gone-stamped row is enqueued.
func resurrectInTx(ctx context.Context, tx rotationExecer, repoID int64, priority int, source string) error {
	_, err := clearInTxUnderRowLock(ctx, tx, repoID, goneClear{sideline: goneSidelineSQL, build: resurrectSQL}, repoID, source, priority, false)
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
	// Archived (0.29.140): repos.repo_archived. On an unstamped queueless
	// row it marks the old dead-arm sideline mark-gone-repos lifts on a 2xx
	// (archivedSidelineSQL).
	Archived bool
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
		       EXISTS (SELECT 1 FROM aveloxis_data.commits c WHERE c.repo_id = r.repo_id),
		       COALESCE(r.repo_archived, FALSE)
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
		if err := rows.Scan(&c.RepoID, &c.GitURL, &c.GoneStamped, &c.HasData, &c.Archived); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
