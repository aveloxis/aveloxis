// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_store.go — the Jira subsystem's store layer (C2/C3 of the email
// attribution program). Three concerns:
//
//   - registration/claim/checkpoint over aveloxis_ops.jira_project_serve
//     (the repo_groups_list_serve pattern: per-source claim lock +
//     resume cursor, independent of repos);
//   - staging over aveloxis_ops.jira_staging (the mailing_list_staging
//     pattern — natural-key UNIQUE so re-staging a replayed window is a
//     no-op; nullable un-FK'd repo_id);
//   - the C3a provider-precedence writers: one logical ticket = one
//     issues row keyed (repo_id, external_key), converging at write
//     time through the deterministic negative syntheticIssueID; field
//     ownership forge > Jira API > mail enforced HERE (SR-18), so a
//     wrong caller cannot violate it.

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// JiraAPIDataSource is THE data_source value the Jira API writers stamp
// (SR-17): trackerActionEventGuardSQL keys provider RANK on it — a
// drifted spelling would silently degrade every API-owned row to the
// mail-owned <= arm (re-opening the Part G 53-flip tie bug), so the
// writers, the guard, and the rank-guard test all share this symbol.
const JiraAPIDataSource = "JIRA API"

// jiraCommentFreshSQL is the comment-level stale-replay guard (round
// 14): true when the incoming snapshot is equal-or-newer than the
// stored provider edit time (NULL stored = pre-guard row, always
// upgradable; NULL incoming never beats a known stored time).
const jiraCommentFreshSQL = `(aveloxis_data.messages.msg_updated IS NULL
			                OR (EXCLUDED.msg_updated IS NOT NULL
			                    AND EXCLUDED.msg_updated >= aveloxis_data.messages.msg_updated))`

// JiraPlatformID stamps messages rows holding NATIVE Jira comment
// bodies (platforms row 4). The platform-6 precedent: platform_id on
// messages identifies the message's SOURCE system, never the repo's.
const JiraPlatformID int16 = 4

// JiraProjectJob is one claimed project sync. LockedAt is the claim's
// OWN jps_locked_at stamp — the ownership key ReleaseJiraClaim
// qualifies on (the pass-39 mailing-list rule: a scan that outlived
// the stale window and was re-claimed elsewhere cannot clear the new
// holder's lock).
type JiraProjectJob struct {
	JpsID       int64
	ProjectKey  string
	BaseURL     string
	RepoID      *int64
	LastUpdated *time.Time // the incremental checkpoint; nil = full history
	LockedAt    time.Time
}

// RegisterJiraProject registers (or re-registers) a project for
// collection. Idempotent on project_key; repoID fills only when
// currently NULL (re-registration never clobbers an operator fix).
func (s *PostgresStore) RegisterJiraProject(ctx context.Context, projectKey, baseURL string, repoID *int64) error {
	// Review 2026-08-30 #10: jira_identities (UNIQUE jira_name) and the
	// comment arbiter (platform_msg_id = Jira comment id) are
	// INSTANCE-BLIND — usernames and comment ids are unique only per
	// Jira instance. Until those keys are instance-scoped, a second
	// distinct base_url is refused outright. NOTE (L10 #3): with two or
	// more projects registered, this also refuses a per-project URL
	// correction — the probe cannot tell "new instance" from "the same
	// instance moved". A whole-instance address change (same Jira, new
	// URL — identity keys unaffected) is one hand UPDATE over every
	// row; the error below says so. The probe and the insert run in ONE
	// transaction under a registration-scoped advisory xact lock
	// (Copilot round 13 on PR #193): the earlier check-then-act shape
	// was documented as acceptable for a sequential operator CLI, but
	// two concurrent register-jira-projects processes could both pass
	// the probe and insert projects for DIFFERENT instances — after
	// which the instance-blind username and comment-id collisions the
	// guard exists to prevent become real. The xact lock serializes
	// registrations only (never collection) and releases at
	// commit/rollback; no CONCURRENTLY DDL runs under it, so the
	// v0.27.20 blocking-lock/CIC deadlock class does not apply.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("register jira project %q: begin: %w", projectKey, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, JiraRegistrationAdvisoryLockID); err != nil {
		return fmt.Errorf("register jira project %q: registration lock: %w", projectKey, err)
	}
	var other string
	err = tx.QueryRow(ctx, `
		SELECT base_url FROM aveloxis_ops.jira_project_serve
		WHERE base_url <> $1 AND project_key <> $2 LIMIT 1`, baseURL, projectKey).Scan(&other)
	if err == nil {
		return fmt.Errorf("register jira project %q: a different Jira instance %q is already registered — identity and comment keys are instance-blind, one instance per deployment until they are scoped (same instance at a new address: UPDATE aveloxis_ops.jira_project_serve SET base_url = ... for every row by hand)", projectKey, other)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("register jira project %q: instance probe: %w", projectKey, err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO aveloxis_ops.jira_project_serve (project_key, base_url, repo_id, tool_version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (project_key) DO UPDATE SET
			base_url = EXCLUDED.base_url,
			repo_id = COALESCE(aveloxis_ops.jira_project_serve.repo_id, EXCLUDED.repo_id)`,
		projectKey, baseURL, repoID, ToolVersion)
	if err != nil {
		return fmt.Errorf("register jira project %q: %w", projectKey, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("register jira project %q: commit: %w", projectKey, err)
	}
	return nil
}

// JiraRegistrationAdvisoryLockID serializes RegisterJiraProject's
// one-instance probe with its insert ("AVLXJIRA" packed into 63 bits;
// distinct from MigrateAdvisoryLockID). Transaction-scoped: released
// automatically at commit/rollback.
const JiraRegistrationAdvisoryLockID int64 = 0x41564C584A495241

// ClaimNextJiraProject claims one enabled, cadence-due project under
// FOR UPDATE SKIP LOCKED. keyFilter narrows to one project ("" = any) —
// tests and the targeted CLI use it. The quadratic failure backoff
// mirrors the distribution worker's (v0.21.4 family).
func (s *PostgresStore) ClaimNextJiraProject(ctx context.Context, cadence time.Duration, keyFilter string) (*JiraProjectJob, error) {
	var job JiraProjectJob
	err := s.pool.QueryRow(ctx, `
		UPDATE aveloxis_ops.jira_project_serve j
		SET jps_locked_at = NOW()
		WHERE j.jps_id = (
			SELECT jps_id FROM aveloxis_ops.jira_project_serve
			WHERE jps_enabled
			  AND ($2 = '' OR project_key = $2)
			  AND (jps_locked_at IS NULL OR jps_locked_at < NOW() - INTERVAL '2 hours')
			  AND (jps_last_run IS NULL
			       OR jps_last_run < NOW() - make_interval(secs => $1))
			  AND (jps_last_failed_at IS NULL
			       OR jps_last_failed_at < NOW() - make_interval(secs =>
			            120 * GREATEST(COALESCE(jps_failed_attempts, 0), 1)
			                * GREATEST(COALESCE(jps_failed_attempts, 0), 1)))
			ORDER BY jps_last_run NULLS FIRST, jps_id
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING j.jps_id, j.project_key, j.base_url, j.repo_id, j.jps_last_updated, j.jps_locked_at`,
		cadence.Seconds(), keyFilter).Scan(&job.JpsID, &job.ProjectKey, &job.BaseURL, &job.RepoID, &job.LastUpdated, &job.LockedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim jira project: %w", err)
	}
	return &job, nil
}

// CheckpointJiraProject advances the incremental cursor to the max
// issue-updated timestamp STAGED so far (SR-3: the marker stamps only
// over work proven staged — callers checkpoint after StageJiraIssue
// succeeds for the page).
func (s *PostgresStore) CheckpointJiraProject(ctx context.Context, jpsID int64, lastUpdated time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_last_updated = GREATEST(COALESCE(jps_last_updated, '-infinity'), $2)
		WHERE jps_id = $1`, jpsID, lastUpdated)
	if err != nil {
		return fmt.Errorf("checkpoint jira project %d: %w", jpsID, err)
	}
	return nil
}

// ErrJiraClaimLost means a claim-owned write matched zero rows: the
// scan outlived the 2-hour stale window and another worker re-claimed
// the project. The outcome belongs to the NEW holder — the stale
// owner must record nothing (Copilot round 6 on PR #193).
var ErrJiraClaimLost = errors.New("jira claim ownership lost (re-claimed after the stale window)")

// CompleteJiraScan releases the claim and stamps the run,
// OWNERSHIP-QUALIFIED by the claim's own jps_locked_at (round 6): an
// unqualified write from a >2h stale owner would clear the
// replacement worker's lock and stamp a run the new holder is still
// earning. CheckpointJiraProject stays deliberately UNQUALIFIED (a
// monotonic GREATEST over staged work — valid from any holder;
// qualifying it would discard the stale owner's genuinely-staged
// pages), as does DisableJiraProject (a dead key is a property of the
// project, not of the claim).
func (s *PostgresStore) CompleteJiraScan(ctx context.Context, jpsID int64, lockedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NULL, jps_last_run = NOW(),
		    jps_failed_attempts = 0, jps_last_failed_at = NULL
		WHERE jps_id = $1 AND jps_locked_at = $2`, jpsID, lockedAt)
	if err != nil {
		return fmt.Errorf("complete jira scan %d: %w", jpsID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJiraClaimLost
	}
	return nil
}

// RecordJiraFailure releases the claim and advances the quadratic
// backoff counter — ownership-qualified like CompleteJiraScan (a
// stale owner must neither clear the new holder's lock nor strike
// the project for a scan it no longer owns).
func (s *PostgresStore) RecordJiraFailure(ctx context.Context, jpsID int64, lockedAt time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NULL,
		    jps_failed_attempts = COALESCE(jps_failed_attempts, 0) + 1,
		    jps_last_failed_at = NOW()
		WHERE jps_id = $1 AND jps_locked_at = $2`, jpsID, lockedAt)
	if err != nil {
		return fmt.Errorf("record jira failure %d: %w", jpsID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrJiraClaimLost
	}
	return nil
}

// ReleaseJiraClaim clears a claim WITHOUT recording a failure or a
// completed run — the shutdown rollback (Copilot round 4 on PR #193:
// every context.Canceled exit in syncProject left jps_locked_at held,
// stranding the project for the claim query's 2-hour stale window on
// every restart — the pass-37 mailing-list ReleaseListLock class).
// Ownership-qualified by the claim's own jps_locked_at stamp
// (JiraProjectJob.LockedAt): a scan that outlived the stale window
// and was re-claimed elsewhere cannot clear the new holder's lock.
// Checkpoints and backoff counters are untouched — the per-page
// checkpoint (SR-3) is the resume state.
func (s *PostgresStore) ReleaseJiraClaim(ctx context.Context, jpsID int64, lockedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NULL
		WHERE jps_id = $1 AND jps_locked_at = $2`, jpsID, lockedAt)
	if err != nil {
		return fmt.Errorf("release jira claim %d: %w", jpsID, err)
	}
	return nil
}

// DisableJiraProject turns a project off permanently (a 400 = dead
// key — 5 of the 191 pilot keys; retrying cannot revive it).
func (s *PostgresStore) DisableJiraProject(ctx context.Context, jpsID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_enabled = FALSE, jps_locked_at = NULL
		WHERE jps_id = $1`, jpsID)
	if err != nil {
		return fmt.Errorf("disable jira project %d: %w", jpsID, err)
	}
	return nil
}

// JiraProjectCandidate is one project key derivable from the
// synthetic issues the mailing-list projection already minted.
type JiraProjectCandidate struct {
	ProjectKey string
	RepoID     int64
}

// DeriveJiraProjectsFromSynthetics enumerates the distinct Jira
// project keys already present as synthetic issues (data_source =
// 'JIRA', keyed external_key) with each key's repo. 191 keys on the
// production aveloxis DB; 5 of them are dead upstream — the worker
// disables those on their first 400.
func (s *PostgresStore) DeriveJiraProjectsFromSynthetics(ctx context.Context) ([]JiraProjectCandidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT split_part(external_key, '-', 1) AS proj, min(repo_id)
		FROM aveloxis_data.issues
		WHERE data_source = 'JIRA' AND external_key <> ''
		GROUP BY 1 ORDER BY count(*) DESC`)
	if err != nil {
		return nil, fmt.Errorf("derive jira projects: %w", err)
	}
	defer rows.Close()
	var out []JiraProjectCandidate
	for rows.Next() {
		var c JiraProjectCandidate
		if err := rows.Scan(&c.ProjectKey, &c.RepoID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// JiraProjectRegistration is one enabled jira_project_serve row — the
// operator-correctable mapping (the registration's repo_id and
// base_url WIN over anything derivable from synthetics; the L10 round
// on the C3 build made GetJiraStagingBatch prefer the registration for
// the same reason).
type JiraProjectRegistration struct {
	JpsID      int64
	ProjectKey string
	BaseURL    string
	RepoID     *int64
}

// ListJiraProjectRegistrations returns every ENABLED registration.
// Consumers that walk projects (the identity backfill CLI) must use
// THIS — never re-derive projects from synthetics, which ignores
// operator corrections to repo_id/base_url (Copilot round 2 on
// PR #193, #1).
func (s *PostgresStore) ListJiraProjectRegistrations(ctx context.Context) ([]JiraProjectRegistration, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT jps_id, project_key, base_url, repo_id
		FROM aveloxis_ops.jira_project_serve
		WHERE jps_enabled
		ORDER BY project_key`)
	if err != nil {
		return nil, fmt.Errorf("list jira registrations: %w", err)
	}
	defer rows.Close()
	var out []JiraProjectRegistration
	for rows.Next() {
		var r JiraProjectRegistration
		if err := rows.Scan(&r.JpsID, &r.ProjectKey, &r.BaseURL, &r.RepoID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// JiraStagingRow is one staged issue envelope.
type JiraStagingRow struct {
	JsID     int64
	IssueKey string
	RepoID   *int64
	Envelope []byte
}

// StageJiraIssue stages one issue envelope. The natural-key UNIQUE
// (project_key, issue_key, issue_updated) makes a replayed sync window
// a true no-op.
func (s *PostgresStore) StageJiraIssue(ctx context.Context, jpsID int64, projectKey, issueKey string, issueUpdated time.Time, repoID *int64, envelope []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.jira_staging (jps_id, project_key, issue_key, issue_updated, repo_id, envelope)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (project_key, issue_key, issue_updated) DO NOTHING`,
		jpsID, projectKey, issueKey, issueUpdated, repoID, envelope)
	if err != nil {
		return fmt.Errorf("stage jira issue %s: %w", issueKey, err)
	}
	return nil
}

// JiraProjectsWithStaging lists jps_ids holding unprocessed staging,
// oldest backlog first.
// JiraProjectsWithStaging pages staged projects by jps_id KEYSET
// (Copilot round 13 on PR #193): the old oldest-first window meant
// projects that never drain (nil-repo rows awaiting the operator's
// registration heal, persistently failing envelopes) kept their old
// min(created_at) and permanently occupied the head — once more than
// one window's worth existed, newer healthy projects starved. The
// drain rotates a cursor through id order instead (fairness beats age
// priority); afterID 0 starts from the top.
func (s *PostgresStore) JiraProjectsWithStaging(ctx context.Context, afterID int64, limit int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT jps_id FROM aveloxis_ops.jira_staging
		WHERE NOT processed AND jps_id > $2
		GROUP BY jps_id ORDER BY jps_id LIMIT $1`, limit, afterID)
	if err != nil {
		return nil, fmt.Errorf("jira projects with staging: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// GetJiraStagingBatch reads unprocessed rows for one project. The repo
// is read through the LIVE registration first (review 2026-08-30 #2,
// L10 #4): the staged copy is only a snapshot of jira_project_serve's
// repo_id at stage time (the worker's sole staging call passes
// job.RepoID), so the registration wins — an operator fixing a NULL or
// wrong registration heals every undrained row on the next drain, and
// nil-repo rows can no longer head-block the project's drain. The
// staged copy is the fallback only for rows whose registration row
// somehow lost its mapping.
// GetJiraStagingBatch pages a project's staged rows by js_id keyset
// (fresh-context round 2026-09-02 #4 — the round-13 rotation one
// level down): rows that never drain (persistent envelope failures,
// the refusing-to-LINK anomaly) stay unprocessed, and a bare
// `ORDER BY js_id LIMIT n` re-served the same failing window forever,
// head-blocking the project's own tail. afterID 0 starts from the
// top; failed rows retry on the NEXT drain instead of every batch.
func (s *PostgresStore) GetJiraStagingBatch(ctx context.Context, jpsID, afterID int64, limit int) ([]JiraStagingRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT js.js_id, js.issue_key,
		       COALESCE(jps.repo_id, js.repo_id) AS repo_id,
		       js.envelope
		FROM aveloxis_ops.jira_staging js
		JOIN aveloxis_ops.jira_project_serve jps ON jps.jps_id = js.jps_id
		WHERE js.jps_id = $1 AND NOT js.processed AND js.js_id > $3
		ORDER BY js.js_id LIMIT $2`, jpsID, limit, afterID)
	if err != nil {
		return nil, fmt.Errorf("jira staging batch: %w", err)
	}
	defer rows.Close()
	var out []JiraStagingRow
	for rows.Next() {
		var r JiraStagingRow
		if err := rows.Scan(&r.JsID, &r.IssueKey, &r.RepoID, &r.Envelope); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkJiraStagingProcessed stamps drained rows.
func (s *PostgresStore) MarkJiraStagingProcessed(ctx context.Context, jsIDs []int64) error {
	if len(jsIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_staging SET processed = TRUE WHERE js_id = ANY($1)`, jsIDs)
	if err != nil {
		return fmt.Errorf("mark jira staging processed: %w", err)
	}
	return nil
}

// PurgeJiraStagingProcessed removes drained tombstones past retention
// (shares staging_retention_hours with the other staging tables).
func (s *PostgresStore) PurgeJiraStagingProcessed(ctx context.Context, retention time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.jira_staging
		WHERE processed AND created_at < NOW() - make_interval(secs => $1)`, retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("purge jira staging: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ResolveJiraIdentity upserts the raw identity row and attempts an
// UNAMBIGUOUS link (SR-6): jira_name against gh_login/cntrb_login
// first (pilot: 49.2% of issues), then display_name against
// cntrb_full_name (+17.6%); anything ambiguous or unmatched stays
// NULL with the raw identity preserved. Returns the linked cntrb_id
// ("" when unlinked), the match method, and whether the miss was
// AMBIGUOUS (candidates existed but not uniquely — the caller must
// NOT mint a new contributor for those; minting is for pure
// Jira-only identities with zero candidates).
func (s *PostgresStore) ResolveJiraIdentity(ctx context.Context, jiraName, jiraUserKey, displayName string) (string, string, bool, error) {
	// Upsert-first so the raw identity survives regardless of matching.
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.jira_identities (jira_name, jira_user_key, display_name, tool_version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (jira_name) DO UPDATE SET
			jira_user_key = COALESCE(NULLIF(EXCLUDED.jira_user_key, ''), aveloxis_data.jira_identities.jira_user_key),
			display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), aveloxis_data.jira_identities.display_name),
			last_seen = NOW()`,
		jiraName, jiraUserKey, displayName, ToolVersion)
	if err != nil {
		return "", "", false, fmt.Errorf("upsert jira identity %q: %w", jiraName, err)
	}
	// Already linked?
	var existing *string
	var method string
	err = s.pool.QueryRow(ctx, `
		SELECT cntrb_id::text, match_method FROM aveloxis_data.jira_identities WHERE jira_name = $1`,
		jiraName).Scan(&existing, &method)
	if err != nil {
		return "", "", false, fmt.Errorf("read jira identity %q: %w", jiraName, err)
	}
	if existing != nil {
		return *existing, method, false, nil
	}
	// Login arm: unambiguous match against gh_login OR cntrb_login.
	var cntrb string
	var n int
	err = s.pool.QueryRow(ctx, `
		SELECT count(DISTINCT cntrb_id), COALESCE(min(cntrb_id::text), '')
		FROM aveloxis_data.contributors
		WHERE COALESCE(cntrb_deleted, 0) = 0
		  AND ((gh_login <> '' AND lower(gh_login) = lower($1))
		       OR (cntrb_login <> '' AND lower(cntrb_login) = lower($1)))`,
		jiraName).Scan(&n, &cntrb)
	if err != nil {
		return "", "", false, fmt.Errorf("jira login match %q: %w", jiraName, err)
	}
	if n == 1 {
		winner, wmethod, lerr := s.linkJiraIdentity(ctx, jiraName, cntrb, "login")
		if lerr != nil {
			return "", "", false, lerr
		}
		return winner, wmethod, false, nil
	}
	if n > 1 {
		return "", "", true, nil // ambiguous stays NULL (SR-6)
	}
	// Display arm.
	if displayName == "" {
		return "", "", false, nil
	}
	err = s.pool.QueryRow(ctx, `
		SELECT count(DISTINCT cntrb_id), COALESCE(min(cntrb_id::text), '')
		FROM aveloxis_data.contributors
		WHERE COALESCE(cntrb_deleted, 0) = 0
		  AND cntrb_full_name <> '' AND lower(cntrb_full_name) = lower($1)`,
		displayName).Scan(&n, &cntrb)
	if err != nil {
		return "", "", false, fmt.Errorf("jira display match %q: %w", displayName, err)
	}
	if n == 1 {
		winner, wmethod, lerr := s.linkJiraIdentity(ctx, jiraName, cntrb, "display")
		if lerr != nil {
			return "", "", false, lerr
		}
		return winner, wmethod, false, nil
	}
	return "", "", n > 1, nil
}

// linkJiraIdentity persists a match and returns the PERSISTED
// (cntrb_id, match_method) pair — never the caller's locally selected
// candidate (Copilot round 11 on PR #193). The fill-empty guard means
// a concurrent resolver (live drain vs the identity backfill, or a
// racing mint) can link first; the pre-fix void return then let the
// LOSER attribute its issue/comment to its own candidate while the
// table held the winner's, splitting attribution for one jira_name.
// UPDATE ... RETURNING with a read fallback on zero rows is the
// MintJiraContributor conflict pattern applied to the link path: the
// persisted row is the single source of the returned pair.
func (s *PostgresStore) linkJiraIdentity(ctx context.Context, jiraName, cntrbID, method string) (string, string, error) {
	var winner, wmethod string
	err := s.pool.QueryRow(ctx, `
		UPDATE aveloxis_data.jira_identities
		SET cntrb_id = $2::uuid, match_method = $3
		WHERE jira_name = $1 AND cntrb_id IS NULL
		RETURNING cntrb_id::text, match_method`, jiraName, cntrbID, method).Scan(&winner, &wmethod)
	if err == nil {
		return winner, wmethod, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("link jira identity %q: %w", jiraName, err)
	}
	// Zero rows: the fill-empty guard held because a concurrent
	// resolver already linked. Read the persisted winner and hand THAT
	// to the caller.
	var got *string
	if rerr := s.pool.QueryRow(ctx, `
		SELECT cntrb_id::text, match_method
		FROM aveloxis_data.jira_identities WHERE jira_name = $1`,
		jiraName).Scan(&got, &wmethod); rerr != nil {
		return "", "", fmt.Errorf("link jira identity %q (winner read): %w", jiraName, rerr)
	}
	if got == nil {
		// The guard only refuses when cntrb_id is non-NULL, and nothing
		// ever unlinks — a NULL here is a state this code cannot
		// explain. Fail rather than fabricate (SR-6).
		return "", "", fmt.Errorf("link jira identity %q: update matched no row yet no winner is persisted", jiraName)
	}
	return *got, wmethod, nil
}

// MintJiraContributor creates a contributor row for an unambiguous
// Jira-only identity and links it in jira_identities — ATOMICALLY
// (Copilot round 9 on PR #193): the pre-fix three-statement shape
// committed the contributor before the identity write, so an identity
// failure left an ACTIVE orphan (and every retry could mint another),
// and the concurrent-double-mint loser's row was deliberately
// abandoned. Those rows are NOT invisible — GetPublicFleetStats
// publishes COUNT(*) of contributors on the landing page. Now one
// transaction: an identity failure rolls the contributor back, and
// the loser DELETEs its own row in-tx (safe — nothing can reference a
// row that was never handed out) before returning the winner's id.
func (s *PostgresStore) MintJiraContributor(ctx context.Context, jiraName, displayName string) (string, error) {
	var existing *string
	err := s.pool.QueryRow(ctx, `
		SELECT cntrb_id::text FROM aveloxis_data.jira_identities WHERE jira_name = $1`,
		jiraName).Scan(&existing)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("mint jira contributor read %q: %w", jiraName, err)
	}
	if existing != nil {
		return *existing, nil
	}
	var winner string
	err = s.withRetry(ctx, func(ctx context.Context) error {
		tx, terr := s.pool.Begin(ctx)
		if terr != nil {
			return terr
		}
		defer func() { _ = tx.Rollback(ctx) }()
		var cntrb string
		if qerr := tx.QueryRow(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_full_name, tool_source, data_source)
			VALUES ('', $1, 'Aveloxis Jira Collector', '`+JiraAPIDataSource+`')
			RETURNING cntrb_id::text`, displayName).Scan(&cntrb); qerr != nil {
			return qerr
		}
		// RETURNING makes the identity upsert the single source of the
		// returned id: under a concurrent double-mint the ON CONFLICT
		// keeps the winner's link and the loser hands the WINNER's
		// cntrb_id to its caller (review 2026-08-30 #9).
		if qerr := tx.QueryRow(ctx, `
			INSERT INTO aveloxis_data.jira_identities (jira_name, display_name, cntrb_id, match_method, tool_version)
			VALUES ($1, $2, $3::uuid, 'minted', $4)
			ON CONFLICT (jira_name) DO UPDATE SET
				cntrb_id = COALESCE(aveloxis_data.jira_identities.cntrb_id, EXCLUDED.cntrb_id),
				match_method = CASE WHEN aveloxis_data.jira_identities.cntrb_id IS NULL THEN 'minted'
				                    ELSE aveloxis_data.jira_identities.match_method END
			RETURNING cntrb_id::text`,
			jiraName, displayName, cntrb, ToolVersion).Scan(&winner); qerr != nil {
			return qerr
		}
		if winner != cntrb {
			// The loser of a concurrent double-mint: its just-created
			// contributor row was never handed out, so deleting it here
			// is safe and keeps the published contributor count honest.
			if _, derr := tx.Exec(ctx, `
				DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, cntrb); derr != nil {
				return derr
			}
		}
		return tx.Commit(ctx)
	})
	if err != nil {
		return "", fmt.Errorf("mint jira contributor %q: %w", jiraName, err)
	}
	return winner, nil
}

// JiraAPIIssue is the typed input to the provider-precedence issue
// writer (decoded from the staging envelope by the collector).
type JiraAPIIssue struct {
	RepoID         int64
	ExternalKey    string
	JiraIssueID    int64
	Title          string
	Status         string
	Resolution     string // resolution NAME ("" = unresolved); any non-empty value means closed
	ResolutionDate time.Time
	Created        time.Time
	Updated        time.Time
	ReporterCntrb  string // linked cntrb_id, "" = unresolved
}

// jiraAPISnapshotFreshSQL is the API-over-API freshness predicate used
// by every state-writing arm of UpsertJiraIssueFromAPI's conflict
// update (SR-17: one spelling). TRUE when the stored row may take this
// snapshot's state: mail-owned rows always upgrade (rank 2 over 3);
// API-owned rows advance only on equal-or-newer snapshots (equal keeps
// idempotent same-snapshot replays; strictly-older replays are the C3
// stale-regression shape). Deliberately NOT shared with
// trackerActionEventGuardSQL: that guard arbitrates MAIL-rank writers
// (strict < against API rows so minute-rounded ties cannot flip
// API-owned state); this one arbitrates the API writer itself, where
// equality must pass.
//
// Known skew (L10 review, F4): a mail-owned row's updated_at is the
// notification's sent_at (Pony Mail minute-rounds it, and relay
// latency runs ahead of Jira's own clock), so it can EXCEED the API
// snapshot's `updated` for the very change the mail announced. The
// mail-owned arm above deliberately ignores updated_at entirely
// (data_source <> 'JIRA API' upgrades unconditionally), so the skew
// costs nothing here — it only means updated_at on a mail-owned row is
// a sent_at stamp, not a Jira timestamp, until the first API snapshot
// rewrites it.
const jiraAPISnapshotFreshSQL = `(aveloxis_data.issues.data_source <> '` +
	JiraAPIDataSource + `'
			                    OR aveloxis_data.issues.updated_at IS NULL
			                    OR aveloxis_data.issues.updated_at <= EXCLUDED.updated_at
			                    OR aveloxis_data.issues.last_mail_event_id IS NOT NULL)`

// UpsertJiraIssueFromAPI is the C3a rank-2 writer. One logical ticket
// = one issues row keyed (repo_id, external_key):
//
//   - an existing row (mail-minted synthetic OR native forge) is
//     LINKed, never duplicated — the deterministic negative
//     syntheticIssueID means a jira-first mint and a mail-first mint
//     are the SAME row in either order;
//   - the API OVERWRITES mail-derived state/reporter on SYNTHETIC rows
//     (rank 2 > rank 3), event-time guarded by Updated;
//   - a NATIVE forge row (positive platform_issue_id — the Jira→GitHub
//     migration case) keeps its forge-owned state/title/reporter
//     untouchable (rank 1 > rank 2); only jira_issue_id enrichment
//     lands.
//
// data_source records the highest-grade provider that has written the
// row ('JIRA' → 'JIRA API'); mail lineage stays in issue_message_ref +
// email_message forever.
func (s *PostgresStore) UpsertJiraIssueFromAPI(ctx context.Context, in JiraAPIIssue) (int64, error) {
	state := "open"
	var closedAt *time.Time
	// Review 2026-08-30 #5: ASF Jira workflows carry per-project custom
	// terminal status names — a set resolution (or resolutiondate) means
	// CLOSED regardless of the status vocabulary. The three canonical
	// names stay as the secondary signal for resolution-less closes.
	closed := in.Resolution != "" || !in.ResolutionDate.IsZero()
	switch in.Status {
	case "Resolved", "Closed", "Done":
		closed = true
	}
	if closed {
		state = "closed"
		if !in.ResolutionDate.IsZero() {
			closedAt = &in.ResolutionDate
		}
	}
	var reporter *string
	if in.ReporterCntrb != "" {
		reporter = &in.ReporterCntrb
	}
	pid := syntheticIssueID(in.ExternalKey)
	num := issueNumberFromKey(in.ExternalKey)

	// LINK-first probes (Copilot round 12 on PR #193): the 23505 arm
	// below only converges with a native forge row that ALREADY
	// carries external_key. A migrated repo's imported native issue
	// (the ARROW shape) can hold the key ONLY in its title until
	// backfill-issue-external-keys runs — the insert then succeeded
	// under the synthetic id and minted exactly the missed-LINK
	// duplicate LinkOrCreateIssueFromEmail prevents. Mirror its
	// key-then-title lookup: the exact-key probe is an indexed point
	// lookup on every envelope; the title probe (leading-wildcard
	// LIKE, trgm-assisted) runs only when NO row holds the key — the
	// first-ever sight of a ticket. A probe ERROR surfaces (SR-5):
	// falling through to mint on bad information is the duplicate.
	var probeID, probePID int64
	perr := s.pool.QueryRow(ctx, `
		SELECT issue_id, platform_issue_id FROM aveloxis_data.issues
		WHERE repo_id = $1 AND external_key = $2 AND external_key <> ''
		LIMIT 1`, in.RepoID, in.ExternalKey).Scan(&probeID, &probePID)
	switch {
	case perr == nil && probePID >= 0:
		// Native row already keyed: LINK (rank-1 protection — the
		// forge owns its state; we only enrich jira_issue_id).
		return s.enrichNativeJiraIssue(ctx, probeID, in.JiraIssueID, "")
	case perr == nil:
		// The synthetic row: fall through — the upsert converges on
		// (repo_id, platform_issue_id), and the 23505 arm still guards
		// the anomalous foreign-negative-id case.
	case errors.Is(perr, pgx.ErrNoRows):
		// No row holds the key: first sight. Title fallback — a native
		// row (positive id only; synthetics always carry their key, so
		// a negative-id hit here is noise, never a LINK target) whose
		// title embeds "[KEY]".
		terr := s.pool.QueryRow(ctx, `
			SELECT issue_id, platform_issue_id FROM aveloxis_data.issues
			WHERE repo_id = $1 AND COALESCE(external_key, '') = ''
			  AND platform_issue_id >= 0
			  AND issue_title LIKE '%[' || $2 || ']%'
			ORDER BY issue_id LIMIT 1`, in.RepoID, in.ExternalKey).Scan(&probeID, &probePID)
		if terr == nil {
			// Enrich the native row with the key it was imported
			// without, plus the Jira internal id — then LINK.
			return s.enrichNativeJiraIssue(ctx, probeID, in.JiraIssueID, in.ExternalKey)
		}
		if !errors.Is(terr, pgx.ErrNoRows) {
			return 0, fmt.Errorf("upsert jira issue %q: title probe: %w", in.ExternalKey, terr)
		}
	default:
		return 0, fmt.Errorf("upsert jira issue %q: key probe: %w", in.ExternalKey, perr)
	}

	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues
			(repo_id, platform_issue_id, issue_number, issue_title, issue_state,
			 closed_at, external_key, jira_issue_id, reporter_id, created_at, updated_at,
			 data_source, tool_source, tool_version, data_collection_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, 0), $9, $10, $11,
			'`+JiraAPIDataSource+`', 'Aveloxis Jira Collector', $12, NOW())
		ON CONFLICT (repo_id, platform_issue_id) DO UPDATE SET
			-- rank 2 overwrites rank 3 — but ONLY on synthetic rows;
			-- guarded per column below. (This arm only ever fires for the
			-- synthetic id: a native forge row has a different
			-- platform_issue_id and is LINKed by the key probe instead.)
			--
			-- FRESHNESS guard on the state trio (Copilot round on
			-- PR #193, C3): the drain continues past one failed envelope,
			-- so a STALE staged snapshot can replay AFTER a newer one
			-- already applied — an API-owned row only advances on
			-- equal-or-newer API snapshots (equal keeps same-snapshot
			-- replays idempotent); a mail-owned row is always upgraded.
			issue_title = CASE WHEN `+jiraAPISnapshotFreshSQL+`
			                    AND NULLIF(EXCLUDED.issue_title, '') IS NOT NULL
			                   THEN EXCLUDED.issue_title ELSE aveloxis_data.issues.issue_title END,
			issue_state = CASE WHEN `+jiraAPISnapshotFreshSQL+`
			                   THEN EXCLUDED.issue_state ELSE aveloxis_data.issues.issue_state END,
			closed_at = CASE WHEN `+jiraAPISnapshotFreshSQL+`
			                 THEN EXCLUDED.closed_at ELSE aveloxis_data.issues.closed_at END,
			jira_issue_id = COALESCE(EXCLUDED.jira_issue_id, aveloxis_data.issues.jira_issue_id),
			-- Copilot round 2 on PR #193 (#5): reporter rides the SAME
			-- freshness predicate as the state trio — a stale replayed
			-- snapshot naming a different (or since-fixed) reporter must
			-- not regress attribution. Equality passes the predicate, so
			-- an equal-timestamp snapshot can still FILL an unresolved
			-- reporter; an incoming NULL (identity unmatched) never
			-- clobbers a resolved one.
			reporter_id = CASE WHEN `+jiraAPISnapshotFreshSQL+`
			                    AND EXCLUDED.reporter_id IS NOT NULL
			                   THEN EXCLUDED.reporter_id ELSE aveloxis_data.issues.reporter_id END,
			updated_at = CASE WHEN `+jiraAPISnapshotFreshSQL+`
			                  THEN EXCLUDED.updated_at ELSE aveloxis_data.issues.updated_at END,
			-- Fresh-context round 2026-09-02 #1: an applying API write
			-- re-establishes the API clock domain — clear the
			-- mail-authored marker so the tracker guard's API arm goes
			-- back to strict-< against this genuine API stamp.
			last_mail_event_id = CASE WHEN `+jiraAPISnapshotFreshSQL+`
			                          THEN NULL ELSE aveloxis_data.issues.last_mail_event_id END,
			data_source = '`+JiraAPIDataSource+`',
			data_collection_date = NOW()
		RETURNING issue_id`,
			in.RepoID, pid, num, SanitizeText(in.Title), state, closedAt,
			in.ExternalKey, in.JiraIssueID, reporter,
			NullTime(in.Created), NullTime(in.Updated), ToolVersion).Scan(&id)
	})
	if err == nil {
		return id, nil
	}
	// The synthetic slot may be shadowed by a NATIVE row carrying the
	// key (LINK case): the INSERT then trips 23505 on the partial
	// unique (repo_id, external_key). ONLY that error routes to the
	// LINK path — any other failure (transient, encode, constraint)
	// must surface, or the caller marks the staging row processed
	// while the API's state/title write was silently dropped and the
	// incremental JQL never re-fetches it (review 2026-08-30 #1).
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return 0, fmt.Errorf("upsert jira issue %q: %w", in.ExternalKey, err)
	}
	var linkedID int64
	var nativePID int64
	lerr := s.pool.QueryRow(ctx, `
		SELECT issue_id, platform_issue_id FROM aveloxis_data.issues
		WHERE repo_id = $1 AND external_key = $2 AND external_key <> ''
		LIMIT 1`, in.RepoID, in.ExternalKey).Scan(&linkedID, &nativePID)
	if lerr != nil {
		return 0, fmt.Errorf("upsert jira issue %q: %w (link probe: %w)", in.ExternalKey, err, lerr)
	}
	// Rank 1 protection, ENFORCED: the LINK path may only attach to a
	// native forge row. A key-collision row with a negative id under a
	// different synthetic value is an anomaly (hand-inserted or a
	// hashing change) — never silently succeed onto it.
	if nativePID < 0 {
		return 0, fmt.Errorf("upsert jira issue %q: key held by non-native row issue_id=%d platform_issue_id=%d — refusing to LINK (%w)",
			in.ExternalKey, linkedID, nativePID, err)
	}
	return s.enrichNativeJiraIssue(ctx, linkedID, in.JiraIssueID, "")
}

// enrichNativeJiraIssue is the ONE LINK tail (SR-17) for a native
// forge row that a Jira envelope resolved to: stamp jira_issue_id
// (fill-empty) and — for the round-12 title-fallback case — the
// external_key the row was imported without (fill-empty guard, so a
// concurrent enrich or the key backfill is never overwritten). It
// touches NOTHING forge-owned (rank-1 protection): state, title,
// closed_at all stay the forge's.
func (s *PostgresStore) enrichNativeJiraIssue(ctx context.Context, issueID, jiraIssueID int64, externalKey string) (int64, error) {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.issues
		SET jira_issue_id = COALESCE(jira_issue_id, NULLIF($2, 0)),
		    external_key = CASE WHEN COALESCE(external_key, '') = '' AND $3 <> ''
		                        THEN $3 ELSE external_key END
		WHERE issue_id = $1`, issueID, jiraIssueID, externalKey)
	if err != nil {
		return 0, fmt.Errorf("enrich native issue %d: %w", issueID, err)
	}
	return issueID, nil
}

// JiraAPIComment is the typed input to the native-comment writer.
type JiraAPIComment struct {
	RepoID        int64
	IssueID       int64
	ExternalKey   string
	CommentID     int64
	Body          string
	AuthorCntrbID string // linked cntrb_id, "" = unresolved
	Created       time.Time
	Updated       time.Time
}

// UpsertJiraComment stores a NATIVE Jira comment (platform 4,
// MsgKindComment, platform_msg_id = Jira's comment id), bridges it via
// issue_message_ref, and stamps the matching [Commented] notification's
// email_message.linked_msg_id — collection-time linking (the v0.28.20
// GitBox mirror-link precedent; pilot-validated (issue, ±2 min) match,
// 94.4% within 2 minutes, median 37 s). Read-time precedence
// (native > notification) keys off that stamp, never off fuzzy matching
// at query time. The mail row itself is kept verbatim.
func (s *PostgresStore) UpsertJiraComment(ctx context.Context, in JiraAPIComment) (int64, error) {
	var author *string
	if in.AuthorCntrbID != "" {
		author = &in.AuthorCntrbID
	}
	var msgID int64
	// Freshness guard on the conflict arm (Copilot round 14 on
	// PR #193): the drain deliberately continues past a failed
	// envelope, so an OLDER staged snapshot can replay AFTER a newer
	// edited comment already landed — the bare EXCLUDED overwrite
	// regressed msg_text while the issue-level jiraAPISnapshotFreshSQL
	// guard never covered comments. messages.msg_updated persists the
	// provider's edit timestamp; text advances only on equal-or-newer
	// snapshots (equality keeps same-snapshot replays idempotent; a
	// NULL stored value — pre-fix rows — always upgrades). The author
	// keeps fill-from-any semantics: a Jira comment's author is
	// immutable across edits, so even a stale snapshot's author is
	// correct, and a resolved author is preferred on fresh replays
	// (the round-2 reporter rule's shape, immutability carve-out
	// reasoned here). Deliberately NOT shared with
	// jiraAPISnapshotFreshSQL: that predicate arbitrates data_source
	// PROVIDER ranks on issues; comments are always Jira-owned.
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, msg_text, msg_timestamp,
			 msg_updated, cntrb_id, tool_source, tool_version, data_source)
		VALUES ($1, $2, $3, $4, $5, $6, $9, $7::uuid, 'Aveloxis Jira Collector', $8, '`+JiraAPIDataSource+`')
		ON CONFLICT (platform_msg_id, platform_id, msg_kind) DO UPDATE SET
			msg_text = CASE WHEN `+jiraCommentFreshSQL+`
			                THEN EXCLUDED.msg_text ELSE aveloxis_data.messages.msg_text END,
			cntrb_id = CASE WHEN `+jiraCommentFreshSQL+` AND EXCLUDED.cntrb_id IS NOT NULL
			                THEN EXCLUDED.cntrb_id
			                ELSE COALESCE(aveloxis_data.messages.cntrb_id, EXCLUDED.cntrb_id) END,
			msg_updated = GREATEST(
				COALESCE(EXCLUDED.msg_updated, aveloxis_data.messages.msg_updated),
				COALESCE(aveloxis_data.messages.msg_updated, EXCLUDED.msg_updated)),
			data_collection_date = NOW()
		RETURNING msg_id`,
			in.RepoID, in.CommentID, JiraPlatformID, MsgKindComment,
			SanitizeText(in.Body), NullTime(in.Created), author, ToolVersion,
			NullTime(in.Updated)).Scan(&msgID)
	})
	if err != nil {
		return 0, fmt.Errorf("upsert jira comment %d: %w", in.CommentID, err)
	}
	// Collection-time notification link RUNS BEFORE the bridge
	// (Copilot round 6, suppressed #1): the bridge recounts
	// comment_count with superseded notifications excluded, so the
	// link must land first or the count reads one high until the
	// issue's next recount. Nearest unlinked [Commented] notification
	// for the same issue within ±2 minutes.
	// Replay-idempotent (Copilot round on PR #193, C5): once ANY
	// notification is linked to this comment's msg_id, the step is a
	// no-op — a re-drained envelope must not consume a NEIGHBORING
	// unlinked notification in the same two-minute window.
	_, lerr := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.email_message SET linked_msg_id = $1
		WHERE email_message_id = (
			SELECT email_message_id FROM aveloxis_data.email_message
			WHERE repo_id = $2 AND linked_external_key = $3
			  AND msg_class = 'issue_event'
			  AND subject LIKE '[jira] [Commented]%'
			  AND linked_msg_id IS NULL
			  AND sent_at BETWEEN $4::timestamptz - INTERVAL '2 minutes'
			                  AND $4::timestamptz + INTERVAL '2 minutes'
			ORDER BY abs(extract(epoch FROM (sent_at - $4::timestamptz)))
			LIMIT 1)
		  AND NOT EXISTS (
			SELECT 1 FROM aveloxis_data.email_message linkedem
			WHERE linkedem.linked_msg_id = $1)`,
		msgID, in.RepoID, in.ExternalKey, in.Created)
	if lerr != nil {
		return 0, fmt.Errorf("link notification for comment %d: %w", in.CommentID, lerr)
	}
	// Round 17 (suppressed #2): the bridge here is recount-FREE — the
	// processor writes a whole comment block per envelope and recounts
	// the issue ONCE after its loop (the per-comment recount was
	// quadratic in the block size and repeated on every later issue
	// update).
	if err := s.bridgeEmailToIssueNoRecount(ctx, in.IssueID, in.RepoID, msgID); err != nil {
		return 0, fmt.Errorf("bridge jira comment %d: %w", in.CommentID, err)
	}
	return msgID, nil
}

// LinkCommentNotificationToNative is the REVERSE arrival order of
// UpsertJiraComment's collection-time link (Copilot round 6 on
// PR #193, suppressed #2): when a [Commented] notification is
// projected AFTER Jira collection already stored the native comment,
// nothing used to revisit the pair — linked_msg_id stayed NULL
// forever and both records counted. This stamps the notification's
// linked_msg_id from the nearest UNCLAIMED native Jira comment on the
// same issue within ±2 minutes, then recounts the issue so the
// superseded notification leaves comment_count. Both orders now
// converge (C3a's promise at the comment level). Idempotent: fires
// only while the notification is unlinked, and never claims a native
// comment another notification already linked.
func (s *PostgresStore) LinkCommentNotificationToNative(ctx context.Context, emailMessageID, issueID int64, sentAt time.Time) error {
	var tag pgconn.CommandTag
	err := s.withRetry(ctx, func(ctx context.Context) error {
		var werr error
		tag, werr = s.pool.Exec(ctx, `
		UPDATE aveloxis_data.email_message em SET linked_msg_id = native.msg_id
		FROM (
			SELECT m.msg_id FROM aveloxis_data.messages m
			JOIN aveloxis_data.issue_message_ref imr
			  ON imr.msg_id = m.msg_id AND imr.issue_id = $2
			WHERE m.platform_id = $4
			  AND m.msg_kind = $5
			  AND m.msg_timestamp BETWEEN $3::timestamptz - INTERVAL '2 minutes'
			                          AND $3::timestamptz + INTERVAL '2 minutes'
			  AND NOT EXISTS (
				SELECT 1 FROM aveloxis_data.email_message claimed
				WHERE claimed.linked_msg_id = m.msg_id)
			ORDER BY abs(extract(epoch FROM (m.msg_timestamp - $3::timestamptz)))
			LIMIT 1) native
		WHERE em.email_message_id = $1 AND em.linked_msg_id IS NULL`,
			emailMessageID, issueID, sentAt, JiraPlatformID, MsgKindComment)
		return werr
	})
	if err != nil {
		return fmt.Errorf("reverse-link comment notification %d: %w", emailMessageID, err)
	}
	if tag.RowsAffected() > 0 {
		return s.recountIssueComments(ctx, issueID)
	}
	return nil
}
