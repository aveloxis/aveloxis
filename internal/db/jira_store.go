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

// JiraPlatformID stamps messages rows holding NATIVE Jira comment
// bodies (platforms row 4). The platform-6 precedent: platform_id on
// messages identifies the message's SOURCE system, never the repo's.
const JiraPlatformID int16 = 4

// JiraProjectJob is one claimed project sync.
type JiraProjectJob struct {
	JpsID       int64
	ProjectKey  string
	BaseURL     string
	RepoID      *int64
	LastUpdated *time.Time // the incremental checkpoint; nil = full history
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
	// row; the error below says so. Check-then-act: concurrent
	// registrations of distinct instances can both pass the probe —
	// acceptable for the sequential operator CLI that owns this path.
	var other string
	err := s.pool.QueryRow(ctx, `
		SELECT base_url FROM aveloxis_ops.jira_project_serve
		WHERE base_url <> $1 AND project_key <> $2 LIMIT 1`, baseURL, projectKey).Scan(&other)
	if err == nil {
		return fmt.Errorf("register jira project %q: a different Jira instance %q is already registered — identity and comment keys are instance-blind, one instance per deployment until they are scoped (same instance at a new address: UPDATE aveloxis_ops.jira_project_serve SET base_url = ... for every row by hand)", projectKey, other)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("register jira project %q: instance probe: %w", projectKey, err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.jira_project_serve (project_key, base_url, repo_id, tool_version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (project_key) DO UPDATE SET
			base_url = EXCLUDED.base_url,
			repo_id = COALESCE(aveloxis_ops.jira_project_serve.repo_id, EXCLUDED.repo_id)`,
		projectKey, baseURL, repoID, ToolVersion)
	if err != nil {
		return fmt.Errorf("register jira project %q: %w", projectKey, err)
	}
	return nil
}

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
		RETURNING j.jps_id, j.project_key, j.base_url, j.repo_id, j.jps_last_updated`,
		cadence.Seconds(), keyFilter).Scan(&job.JpsID, &job.ProjectKey, &job.BaseURL, &job.RepoID, &job.LastUpdated)
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

// CompleteJiraScan releases the claim and stamps the run.
func (s *PostgresStore) CompleteJiraScan(ctx context.Context, jpsID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NULL, jps_last_run = NOW(),
		    jps_failed_attempts = 0, jps_last_failed_at = NULL
		WHERE jps_id = $1`, jpsID)
	if err != nil {
		return fmt.Errorf("complete jira scan %d: %w", jpsID, err)
	}
	return nil
}

// RecordJiraFailure releases the claim and advances the quadratic
// backoff counter.
func (s *PostgresStore) RecordJiraFailure(ctx context.Context, jpsID int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NULL,
		    jps_failed_attempts = COALESCE(jps_failed_attempts, 0) + 1,
		    jps_last_failed_at = NOW()
		WHERE jps_id = $1`, jpsID)
	if err != nil {
		return fmt.Errorf("record jira failure %d: %w", jpsID, err)
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
func (s *PostgresStore) JiraProjectsWithStaging(ctx context.Context, limit int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT jps_id FROM aveloxis_ops.jira_staging
		WHERE NOT processed
		GROUP BY jps_id ORDER BY min(created_at) LIMIT $1`, limit)
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
func (s *PostgresStore) GetJiraStagingBatch(ctx context.Context, jpsID int64, limit int) ([]JiraStagingRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT js.js_id, js.issue_key,
		       COALESCE(jps.repo_id, js.repo_id) AS repo_id,
		       js.envelope
		FROM aveloxis_ops.jira_staging js
		JOIN aveloxis_ops.jira_project_serve jps ON jps.jps_id = js.jps_id
		WHERE js.jps_id = $1 AND NOT js.processed
		ORDER BY js.js_id LIMIT $2`, jpsID, limit)
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
		return cntrb, "login", false, s.linkJiraIdentity(ctx, jiraName, cntrb, "login")
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
		return cntrb, "display", false, s.linkJiraIdentity(ctx, jiraName, cntrb, "display")
	}
	return "", "", n > 1, nil
}

func (s *PostgresStore) linkJiraIdentity(ctx context.Context, jiraName, cntrbID, method string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.jira_identities
		SET cntrb_id = $2::uuid, match_method = $3
		WHERE jira_name = $1 AND cntrb_id IS NULL`, jiraName, cntrbID, method)
	if err != nil {
		return fmt.Errorf("link jira identity %q: %w", jiraName, err)
	}
	return nil
}

// MintJiraContributor creates a contributor row for a Jira-only person
// (no forge identity anywhere — 32.3% of the pilot's issue-weighted
// sample) so networks include them: the CreateEmailOnlyContributor
// precedent, keyed here by the jira_identities row. Idempotent — an
// already-linked identity returns its existing cntrb_id.
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
	var cntrb string
	err = s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_full_name, tool_source, data_source)
		VALUES ('', $1, 'Aveloxis Jira Collector', 'JIRA API')
		RETURNING cntrb_id::text`, displayName).Scan(&cntrb)
	if err != nil {
		return "", fmt.Errorf("mint jira contributor %q: %w", jiraName, err)
	}
	// RETURNING makes the identity upsert the single source of the
	// returned id: under a concurrent double-mint the ON CONFLICT keeps
	// the winner's link, and the loser must hand the WINNER's cntrb_id
	// to its batch — never its own orphan contributor row (review
	// 2026-08-30 #9; the orphan row stays, empty-login rows are legal
	// under the partial unique and invisible to every read path).
	var winner string
	err = s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.jira_identities (jira_name, display_name, cntrb_id, match_method, tool_version)
		VALUES ($1, $2, $3::uuid, 'minted', $4)
		ON CONFLICT (jira_name) DO UPDATE SET
			cntrb_id = COALESCE(aveloxis_data.jira_identities.cntrb_id, EXCLUDED.cntrb_id),
			match_method = CASE WHEN aveloxis_data.jira_identities.cntrb_id IS NULL THEN 'minted'
			                    ELSE aveloxis_data.jira_identities.match_method END
		RETURNING cntrb_id::text`,
		jiraName, displayName, cntrb, ToolVersion).Scan(&winner)
	if err != nil {
		return "", fmt.Errorf("mint jira identity link %q: %w", jiraName, err)
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
	var id int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues
			(repo_id, platform_issue_id, issue_number, issue_title, issue_state,
			 closed_at, external_key, jira_issue_id, reporter_id, created_at, updated_at,
			 data_source, tool_source, tool_version, data_collection_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, 0), $9, $10, $11,
			'JIRA API', 'Aveloxis Jira Collector', $12, NOW())
		ON CONFLICT (repo_id, platform_issue_id) DO UPDATE SET
			-- rank 2 overwrites rank 3 — but ONLY on synthetic rows;
			-- guarded per column below. (This arm only ever fires for the
			-- synthetic id: a native forge row has a different
			-- platform_issue_id and is LINKed by the key probe instead.)
			issue_title = CASE WHEN NULLIF(EXCLUDED.issue_title, '') IS NOT NULL
			                   THEN EXCLUDED.issue_title ELSE aveloxis_data.issues.issue_title END,
			issue_state = EXCLUDED.issue_state,
			closed_at = EXCLUDED.closed_at,
			jira_issue_id = COALESCE(EXCLUDED.jira_issue_id, aveloxis_data.issues.jira_issue_id),
			reporter_id = COALESCE(EXCLUDED.reporter_id, aveloxis_data.issues.reporter_id),
			updated_at = EXCLUDED.updated_at,
			data_source = 'JIRA API',
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
	_, uerr := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.issues
		SET jira_issue_id = COALESCE(jira_issue_id, NULLIF($2, 0))
		WHERE issue_id = $1`, linkedID, in.JiraIssueID)
	if uerr != nil {
		return 0, fmt.Errorf("enrich native issue %d: %w", linkedID, uerr)
	}
	return linkedID, nil
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
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, msg_text, msg_timestamp,
			 cntrb_id, tool_source, tool_version, data_source)
		VALUES ($1, $2, $3, $4, $5, $6, $7::uuid, 'Aveloxis Jira Collector', $8, 'JIRA API')
		ON CONFLICT (platform_msg_id, platform_id, msg_kind) DO UPDATE SET
			msg_text = EXCLUDED.msg_text,
			cntrb_id = COALESCE(EXCLUDED.cntrb_id, aveloxis_data.messages.cntrb_id),
			data_collection_date = NOW()
		RETURNING msg_id`,
			in.RepoID, in.CommentID, JiraPlatformID, MsgKindComment,
			SanitizeText(in.Body), NullTime(in.Created), author, ToolVersion).Scan(&msgID)
	})
	if err != nil {
		return 0, fmt.Errorf("upsert jira comment %d: %w", in.CommentID, err)
	}
	if err := s.BridgeEmailToIssue(ctx, in.IssueID, in.RepoID, msgID); err != nil {
		return 0, fmt.Errorf("bridge jira comment %d: %w", in.CommentID, err)
	}
	// Collection-time notification link: nearest unlinked [Commented]
	// notification for the same issue within ±2 minutes.
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
			LIMIT 1)`,
		msgID, in.RepoID, in.ExternalKey, in.Created)
	if lerr != nil {
		return 0, fmt.Errorf("link notification for comment %d: %w", in.CommentID, lerr)
	}
	return msgID, nil
}
