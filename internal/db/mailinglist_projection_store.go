// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// mailinglist_projection_store.go is Phase 3 / Phase A of the mailing-list
// subsystem (summary/12 §3, §10b): project an `issue_event` mailing-list
// message onto a canonical `issues` row — LINK an existing issue carrying the
// same external_key, else CREATE a synthetic one — and bridge the originating
// email as a comment via issue_message_ref.
//
// Analytical intent (operator 2026-06-04): before the mailing-list ingestor,
// Apache projects' issue data was ABSENT. A projected Jira/Bugzilla issue is
// written under the PMC's GitHub repo_id (NOT segregated by platform — `issues`
// has no platform_id column; the repo carries the platform), so it appears in
// that repo's standard issue analytics exactly like a native issue. Provenance
// lives in external_key + data_source ('JIRA'/'Bugzilla') + tool_source.

var issueKeyTrailingNum = regexp.MustCompile(`-(\d+)$`)

// syntheticIssueID derives a deterministic, NEGATIVE platform_issue_id from an
// external key. Negative so it can never collide with a real (positive) GitHub
// or GitLab platform_issue_id under the same repo — a Jira-only issue and a
// native GitHub issue coexist cleanly. Stable across re-collection, so the
// CREATE is an idempotent upsert and thread replies converge on the same row.
func syntheticIssueID(externalKey string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(externalKey))
	v := int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF) // mask to a positive 63-bit value
	if v == 0 {
		v = 1
	}
	return -v
}

// issueNumberFromKey extracts the numeric suffix of a tracker key
// (KAFKA-123 → 123) for the NOT-NULL issue_number column. 0 when there's no
// trailing number.
func issueNumberFromKey(externalKey string) int {
	if m := issueKeyTrailingNum.FindStringSubmatch(externalKey); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return 0
}

// LinkOrCreateIssueFromEmail implements the §3 link-or-create rule for an
// issue_event message with a parsed external_key. Returns the issue_id and
// whether a new row was created.
//
//   - LINK: an issue with this external_key already exists in the repo (the
//     Jira→GitHub migration case, OR a prior synthetic create) → return it.
//   - CREATE: no such issue → insert a synthetic issues row (negative
//     platform_issue_id from the key), idempotent on (repo_id,
//     platform_issue_id).
//
// reporterID is the resolved real actor (NEVER the jira@ bot — the caller
// gates that); NULL when unresolved (attribution integrity over a wrong guess).
func (s *PostgresStore) LinkOrCreateIssueFromEmail(ctx context.Context, repoID int64, externalKey, title, body, dataSource string, reporterID *string, createdAt time.Time) (issueID int64, created bool, err error) {
	if externalKey == "" {
		return 0, false, fmt.Errorf("link-or-create issue: empty external_key")
	}

	// LINK: an existing issue for this key. Done as TWO queries so the common
	// case is an indexed point-lookup, not a sequential scan:
	//
	//  1. EXACT external_key match (idx_issues_external_key) — fast. Covers
	//     issues that already carry the key (backfill-issue-external-keys ran,
	//     or a prior synthetic).
	//  2. FALLBACK title match — a native GitHub issue whose external_key is
	//     still EMPTY but whose title carries the bracketed key (e.g.
	//     "lock files [LUCENE-1]"). This is a leading-wildcard LIKE the btree
	//     can't serve, so it runs ONLY when the exact match misses (and is
	//     index-assisted by idx_issues_title_trgm). It PREVENTS the missed-LINK
	//     duplicate (#2): without it, projecting before the key backfill would
	//     mint a synthetic that squats the key.
	var existing int64
	lerr := s.pool.QueryRow(ctx,
		`SELECT issue_id FROM aveloxis_data.issues
		 WHERE repo_id = $1 AND external_key = $2 AND external_key <> '' LIMIT 1`,
		repoID, externalKey).Scan(&existing)
	if lerr == nil && existing > 0 {
		return existing, false, nil
	}
	if lerr != nil && !errors.Is(lerr, pgx.ErrNoRows) {
		return 0, false, fmt.Errorf("link-or-create issue: lookup by external_key: %w", lerr)
	}
	// Fallback: native issue carrying the key only in its title.
	ferr := s.pool.QueryRow(ctx,
		`SELECT issue_id FROM aveloxis_data.issues
		 WHERE repo_id = $1 AND COALESCE(external_key, '') = ''
		   AND issue_title LIKE '%[' || $2 || ']%'
		 ORDER BY issue_id LIMIT 1`,
		repoID, externalKey).Scan(&existing)
	if ferr == nil && existing > 0 {
		return existing, false, nil
	}
	if ferr != nil && !errors.Is(ferr, pgx.ErrNoRows) {
		return 0, false, fmt.Errorf("link-or-create issue: lookup by title: %w", ferr)
	}

	// CREATE: synthetic issue, idempotent on (repo_id, platform_issue_id).
	// v0.27.114: wrapped in withRetry (40P01) — the processor's
	// drop-for-progress policy turns any unretried deadlock into a
	// DROPPED message (the v0.25.36 pre-decision; the v0.27.112 wave
	// covered the email_message writers, this projection writer was the
	// missed half — the counter trended again 2026-08-20).
	pid := syntheticIssueID(externalKey)
	num := issueNumberFromKey(externalKey)
	cerr := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues
			(repo_id, platform_issue_id, issue_number, issue_title, issue_body, issue_state,
			 external_key, reporter_id, created_at,
			 data_source, tool_source, tool_version, data_collection_date)
		VALUES ($1, $2, $3, $4, $5, 'open', $6, $7, $8, $9,
			'Aveloxis Mailing List Collector', $10, NOW())
		ON CONFLICT (repo_id, platform_issue_id) DO UPDATE SET
			issue_title  = COALESCE(NULLIF(EXCLUDED.issue_title, ''), aveloxis_data.issues.issue_title),
			external_key = EXCLUDED.external_key,
			reporter_id  = COALESCE(aveloxis_data.issues.reporter_id, EXCLUDED.reporter_id),
			data_collection_date = NOW()
		RETURNING issue_id`,
			repoID, pid, num, SanitizeText(title), SanitizeText(body),
			externalKey, reporterID, NullTime(createdAt), dataSource, ToolVersion).Scan(&issueID)
	})
	if cerr != nil {
		return 0, false, fmt.Errorf("link-or-create issue: create %q: %w", externalKey, cerr)
	}
	return issueID, true, nil
}

// ProjectionDuplicate is a synthetic ML-projected issue that SHADOWS a native
// (API-collected) GitHub issue — the missed-LINK signal (#2). The
// `idx_issues_external_key` UNIQUE constraint means they can't share the
// external_key column; the shadow presents as: the synthetic holds
// external_key=K (negative platform_issue_id), while a native issue
// (non-negative platform_issue_id, external_key still empty) carries the
// bracketed key `[K]` in its title. The synthetic squatting K is what blocked
// backfill-issue-external-keys from setting it on the native issue. The
// v0.25.x LINK-by-title fix prevents NEW shadows; this surfaces any that
// predate it (remediation = merge the synthetic into the native issue).
type ProjectionDuplicate struct {
	RepoID         int64
	ExternalKey    string
	SyntheticIssue int64
	NativeIssue    int64
}

// MailingListProjectionDuplicates returns synthetic ML issues (negative
// platform_issue_id, external_key set) that shadow a native issue
// (non-negative platform_issue_id) whose TITLE carries the same bracketed key.
// Capped at limit. Empty result = no shadowing (LINK-by-title is doing its job).
func (s *PostgresStore) MailingListProjectionDuplicates(ctx context.Context, limit int) ([]ProjectionDuplicate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT syn.repo_id, syn.external_key, syn.issue_id, nat.issue_id
		FROM aveloxis_data.issues syn
		JOIN aveloxis_data.issues nat
		  ON nat.repo_id = syn.repo_id
		 AND nat.platform_issue_id >= 0
		 AND nat.issue_id <> syn.issue_id
		 AND nat.issue_title LIKE '%[' || syn.external_key || ']%'
		WHERE syn.external_key <> ''
		  AND syn.platform_issue_id < 0
		ORDER BY syn.repo_id, syn.external_key
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("projection duplicates: %w", err)
	}
	defer rows.Close()
	var out []ProjectionDuplicate
	for rows.Next() {
		var d ProjectionDuplicate
		if err := rows.Scan(&d.RepoID, &d.ExternalKey, &d.SyntheticIssue, &d.NativeIssue); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// FindIssueForThread returns the issue a thread is already projected onto, by
// finding any email_message in the same thread (sharing thread_root_id, or
// whose message_id_header IS the thread root) that carries a linked_issue_id.
// This is the thread-inheritance lookup (#1): a reply/discussion email that
// doesn't itself carry an external_key inherits the issue of a keyed sibling,
// so the FULL thread — human discussion, Re: replies — attaches to the issue,
// not just the Jira-notification stream. Scoped to repoID so it can't bleed
// across repos. Returns (0, false) when no sibling is projected yet.
func (s *PostgresStore) FindIssueForThread(ctx context.Context, threadRoot string, repoID int64) (int64, bool, error) {
	if threadRoot == "" {
		return 0, false, nil
	}
	var id int64
	// `thread_root_id <> ''` in the first OR branch is load-bearing (not redundant):
	// idx_email_message_thread_root is PARTIAL (WHERE thread_root_id <> ''), and under a
	// parameterized/generic plan Postgres can't prove `$1 <> ''`, so without this literal
	// the branch seq-scans email_message. threadRoot is always non-empty here (caller
	// guards), so it changes no results. message_id_header uses its own UNIQUE index.
	err := s.pool.QueryRow(ctx, `
		SELECT linked_issue_id FROM aveloxis_data.email_message
		WHERE linked_issue_id IS NOT NULL
		  AND repo_id = $2
		  AND ((thread_root_id = $1 AND thread_root_id <> '') OR message_id_header = $1)
		LIMIT 1`, threadRoot, repoID).Scan(&id)
	if err != nil {
		return 0, false, nil //nolint:nilerr // no projected sibling yet is not an error
	}
	return id, id > 0, nil
}

// BridgeEmailToIssue records a mailing-list email's body row as a comment on a
// projected issue (issue_message_ref) and recomputes the issue's comment_count
// so per-repo issue analytics see the thread. Idempotent on (issue_id, msg_id).
func (s *PostgresStore) BridgeEmailToIssue(ctx context.Context, issueID, repoID, msgID int64) error {
	// v0.27.114: both statements ride withRetry (40P01) — the
	// processor's drop-for-progress policy turns an unretried deadlock
	// into a DROPPED message (observed live 2026-08-20: "bridge email
	// to issue: deadlock detected" → dropped=1). Both statements are
	// idempotent (ON CONFLICT DO NOTHING; recount is a pure recompute).
	if err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id, data_source)
			VALUES ($1, $2, $3,
			        COALESCE((SELECT m.data_source FROM aveloxis_data.messages m WHERE m.msg_id = $3), ''))
			ON CONFLICT (issue_id, msg_id) DO NOTHING`, issueID, repoID, msgID)
		return err
	}); err != nil {
		return fmt.Errorf("bridge email to issue: %w", err)
	}
	if err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_data.issues
			SET comment_count = (SELECT count(*) FROM aveloxis_data.issue_message_ref WHERE issue_id = $1)
			WHERE issue_id = $1`, issueID)
		return err
	}); err != nil {
		return fmt.Errorf("bridge email to issue: recount: %w", err)
	}
	return nil
}

// trackerCloseActions is the SQL-side spelling of "this action closes
// the ticket" — Resolved and Closed; Reopened reopens. Everything else
// (Created, Commented, Work logged, ...) is state-neutral. The subject
// regex below is the third spelling of the action vocabulary
// (systems.yaml capture + mailinglist.TrackerActionFromSubject are the
// others) — TestTrackerActionParityWithSystemsYAML and the fixtures in
// tracker_state_test.go pin them together (SR-17).
const trackerActionSubjectSQL = `substring(subject from '^\[jira\] \[([\w ]+)\] \([A-Z][A-Z0-9]+-[0-9]+\)')`

// trackerActionRe is internal/db's Go spelling of the same extraction
// (this package cannot import internal/mailinglist — no sibling
// feature-package edges). TestTrackerActionDBSpellingParity pins it to
// mailinglist.TrackerActionFromSubject on the shared fixtures.
var trackerActionRe = regexp.MustCompile(`^\[jira\] \[([\w ]+)\] \([A-Z][A-Z0-9]+-[0-9]+\)`)

func trackerActionFromSubject(subject string) string {
	if m := trackerActionRe.FindStringSubmatch(subject); m != nil {
		return m[1]
	}
	return ""
}

// ApplyTrackerAction applies a Jira-notification action to a projected
// issue: Resolved/Closed -> closed (+closed_at = the notification's
// sent_at), Reopened -> open. Two hard rules (C3a provider
// precedence, SR-18 — enforced HERE, not at call sites):
//
//   - SYNTHETIC rows only (platform_issue_id < 0). The LINK path can
//     resolve a native GitHub issue (the Jira→GitHub migration case)
//     whose state is API-owned; a notification must never touch it.
//   - EVENT-TIME guarded: the write stamps updated_at with the
//     action's sent_at and only applies when the stored updated_at is
//     older — so a replayed old archive month can never regress a
//     newer state, regardless of drain order. Pilot-measured (2026-08-31,
//     6,000 keys): mail-derived state falsely closes 0.04%; the guard
//     is what keeps replays from adding to that.
//
// State-neutral actions ("Commented", "Work logged", "") are no-ops.
func (s *PostgresStore) ApplyTrackerAction(ctx context.Context, issueID int64, action string, sentAt time.Time) error {
	var newState string
	switch action {
	case "Resolved", "Closed":
		newState = "closed"
	case "Reopened":
		newState = "open"
	default:
		return nil
	}
	err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.issues
		SET issue_state = $2,
		    closed_at = CASE WHEN $2 = 'closed' THEN $3 ELSE NULL END,
		    updated_at = $3
		WHERE issue_id = $1
		  AND platform_issue_id < 0
		  AND (updated_at IS NULL OR updated_at <= $3)`,
			issueID, newState, NullTime(sentAt))
		return err
	})
	if err != nil {
		return fmt.Errorf("apply tracker action %q on issue %d: %w", action, issueID, err)
	}
	return nil
}

// BackfillSyntheticJiraState is the ledgered history walk closing the
// permanently-open synthetics: every keyed issue_event notification
// already in email_message carries its action in the subject; the walk
// derives each (repo, key)'s LATEST state-relevant action per window
// and applies it under the same synthetic gate + event-time guard as
// ApplyTrackerAction (window order cannot matter — the newest sent_at
// wins whichever window lands last). Keyset windows over
// email_message_id per the house rule; found 485,892 open synthetics
// against 358,384 Resolved notifications on the aveloxis DB.
// Idempotent: reruns re-derive the same latest actions and the guard's
// <= keeps the result stable.
func (s *PostgresStore) BackfillSyntheticJiraState(ctx context.Context, logger *slog.Logger) error {
	return runKeysetWindows(ctx, s, logger,
		"v0.29.0 synthetic Jira issue state from notification subjects",
		`SELECT COALESCE(MAX(email_message_id), 0) FROM aveloxis_data.email_message`,
		`
		UPDATE aveloxis_data.issues i
		SET issue_state = CASE WHEN l.action IN ('Resolved','Closed') THEN 'closed' ELSE 'open' END,
		    closed_at = CASE WHEN l.action IN ('Resolved','Closed') THEN l.at ELSE NULL END,
		    updated_at = l.at
		FROM (
			SELECT em.repo_id, em.linked_external_key AS key,
			       (array_agg(`+trackerActionSubjectSQL+` ORDER BY em.sent_at DESC))[1] AS action,
			       (array_agg(em.sent_at ORDER BY em.sent_at DESC))[1] AS at
			FROM aveloxis_data.email_message em
			WHERE em.email_message_id > $1 AND em.email_message_id <= $2
			  AND em.msg_class = 'issue_event'
			  AND em.repo_id IS NOT NULL
			  AND em.linked_external_key <> ''
			  AND em.sent_at IS NOT NULL
			  AND `+trackerActionSubjectSQL+` IN ('Resolved','Closed','Reopened')
			GROUP BY em.repo_id, em.linked_external_key
		) l
		WHERE i.repo_id = l.repo_id
		  AND i.external_key = l.key
		  AND i.external_key <> ''
		  AND i.platform_issue_id < 0
		  AND (i.updated_at IS NULL OR i.updated_at <= l.at)`)
}
