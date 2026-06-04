// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
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

	// LINK: existing issue carrying this external_key (native-migrated or prior synthetic).
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

	// CREATE: synthetic issue, idempotent on (repo_id, platform_issue_id).
	pid := syntheticIssueID(externalKey)
	num := issueNumberFromKey(externalKey)
	cerr := s.pool.QueryRow(ctx, `
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
	if cerr != nil {
		return 0, false, fmt.Errorf("link-or-create issue: create %q: %w", externalKey, cerr)
	}
	return issueID, true, nil
}

// BridgeEmailToIssue records a mailing-list email's body row as a comment on a
// projected issue (issue_message_ref) and recomputes the issue's comment_count
// so per-repo issue analytics see the thread. Idempotent on (issue_id, msg_id).
func (s *PostgresStore) BridgeEmailToIssue(ctx context.Context, issueID, repoID, msgID int64) error {
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (issue_id, msg_id) DO NOTHING`, issueID, repoID, msgID); err != nil {
		return fmt.Errorf("bridge email to issue: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.issues
		SET comment_count = (SELECT count(*) FROM aveloxis_data.issue_message_ref WHERE issue_id = $1)
		WHERE issue_id = $1`, issueID); err != nil {
		return fmt.Errorf("bridge email to issue: recount: %w", err)
	}
	return nil
}
