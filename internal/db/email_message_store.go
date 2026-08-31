// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/model"
)

// MailingListPlatformID is the platforms row for mailing-list-sourced rows.
const MailingListPlatformID = 6

// MailingListToolSource is the tool_source stamped on mailing-list message
// bodies + email_message rows (operator-mandated convention, §0).
const MailingListToolSource = "Aveloxis Mailing List Collector"

// messageIDToPlatformMsgID derives a stable, collision-resistant
// platform_msg_id from an RFC-822 Message-ID. The messages table dedups on
// (platform_msg_id, platform_id); mailing-list messages have no numeric ID,
// so without this every platform-6 body would collide on (0, 6). FNV-1a
// folded into a positive 63-bit int is deterministic (idempotent
// re-collection) and astronomically unlikely to collide.
func messageIDToPlatformMsgID(messageID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(messageID))
	return int64(h.Sum64() & math.MaxInt64)
}

// UpsertEmailMessage writes (or refreshes) an email_message entity row,
// keyed on the RFC-822 Message-ID. Re-collecting the same message is a
// no-op-equivalent UPSERT — the dedup key makes retry-after-a-wait safe
// (v0.25.7 defensive-collection contract). A previously-resolved
// signaled_repo_id is preserved across re-collection.
func (s *PostgresStore) UpsertEmailMessage(ctx context.Context, em *model.EmailMessage) (int64, error) {
	var id int64
	// v0.27.112: routed through withRetry (bounded 40P01 retry). The
	// processor's drop-for-progress policy dropped a message on a
	// deadlock in CI (2026-08-20, TestMailingListPipelineEndToEnd) —
	// the v0.25.36 note pre-decided this fix: "if the counter trends
	// non-zero, the fix is a bounded retry".
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.email_message (
			repo_id, repo_group_id, rgls_id, platform_id, ml_system,
			message_id_header, list_address, list_id_header, subject, sender_email,
			sent_at, in_reply_to, references_chain, thread_root_id, has_patch,
			msg_class, classification_source, is_mirror, mirrors_url,
			signaled_repo_url, signaled_repo_id,
			linked_issue_id, linked_pull_request_id, linked_pr_review_id, linked_external_key, linked_commit_hash,
			projected_kind, data_source, tool_version
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			$16, $17, $18, $19,
			$20, $21,
			$22, $23, $24, $25, $26,
			$27, $28, $29
		)
		ON CONFLICT (message_id_header) DO UPDATE SET
			repo_id            = COALESCE(aveloxis_data.email_message.repo_id, EXCLUDED.repo_id),
			repo_group_id      = COALESCE(aveloxis_data.email_message.repo_group_id, EXCLUDED.repo_group_id),
			rgls_id            = COALESCE(aveloxis_data.email_message.rgls_id, EXCLUDED.rgls_id),
			subject            = EXCLUDED.subject,
			msg_class          = EXCLUDED.msg_class,
			classification_source = EXCLUDED.classification_source,
			is_mirror          = EXCLUDED.is_mirror,
			mirrors_url        = EXCLUDED.mirrors_url,
			signaled_repo_url  = EXCLUDED.signaled_repo_url,
			-- preserve a resolved FK; never wipe it with a re-collect's NULL
			signaled_repo_id   = COALESCE(aveloxis_data.email_message.signaled_repo_id, EXCLUDED.signaled_repo_id),
			linked_issue_id        = COALESCE(EXCLUDED.linked_issue_id, aveloxis_data.email_message.linked_issue_id),
			linked_pull_request_id = COALESCE(EXCLUDED.linked_pull_request_id, aveloxis_data.email_message.linked_pull_request_id),
			linked_pr_review_id    = COALESCE(EXCLUDED.linked_pr_review_id, aveloxis_data.email_message.linked_pr_review_id),
			linked_external_key    = EXCLUDED.linked_external_key,
			linked_commit_hash     = EXCLUDED.linked_commit_hash,
			-- v0.28.20: derive the kind from the POST-COALESCE links, not from
			-- EXCLUDED alone. The three link columns above are preserve-on-
			-- conflict, so a caller that failed to resolve (e.g. a transient
			-- ResolveMirrorLinkByNodeID error) sends NULLs and would otherwise
			-- assert 'mailing_list_only' over a link the row already holds --
			-- permanently, since the heal only considers rows whose links are
			-- both NULL. The owning layer enforces the invariant (SR-18) so a
			-- caller cannot desynchronise kind from links.
			projected_kind     = CASE
				WHEN COALESCE(EXCLUDED.linked_pull_request_id, aveloxis_data.email_message.linked_pull_request_id) IS NOT NULL THEN 'pr'
				WHEN COALESCE(EXCLUDED.linked_pr_review_id, aveloxis_data.email_message.linked_pr_review_id) IS NOT NULL THEN 'review'
				WHEN COALESCE(EXCLUDED.linked_issue_id, aveloxis_data.email_message.linked_issue_id) IS NOT NULL THEN 'issue'
				ELSE EXCLUDED.projected_kind END,
			data_source        = EXCLUDED.data_source,
			tool_version       = EXCLUDED.tool_version
		RETURNING email_message_id`,
			em.RepoID, em.RepoGroupID, em.RglsID, int16(em.PlatformID), em.MLSystem,
			em.MessageIDHeader, em.ListAddress, em.ListIDHeader, em.Subject, em.SenderEmail,
			NullTime(em.SentAt), em.InReplyTo, em.ReferencesChain, em.ThreadRootID, em.HasPatch,
			em.MsgClass, em.ClassificationSource, em.IsMirror, em.MirrorsURL,
			em.SignaledRepoURL, em.SignaledRepoID,
			em.LinkedIssueID, em.LinkedPullRequestID, em.LinkedReviewID, em.LinkedExternalKey, em.LinkedCommitHash,
			em.ProjectedKind, em.DataSource, ToolVersion,
		).Scan(&id)
	})
	if err != nil {
		return 0, fmt.Errorf("upsert email_message %q: %w", em.MessageIDHeader, err)
	}
	return id, nil
}

// UpsertMailingListMessageBody writes a mailing-list message body to the
// shared messages table with the operator-mandated metadata convention
// (platform_id=6, data_source=the specific list, tool_source="Aveloxis
// Mailing List Collector", tool_version=release). Dedup key is the
// Message-ID-derived synthetic platform_msg_id, so re-collecting a month is
// idempotent. cntrbID may be nil (unresolved sender — sender_email is
// retained for the §5d backfill). Returns the msg_id.
// (Part B) cleanBody/cleanRule carry the quote-stripped variant + the
// pattern-library version that produced it; both ALWAYS refresh on
// conflict (recomputed from the incoming body — the AlwaysRefresh
// policy), matching msg_text's own refresh.
func (s *PostgresStore) UpsertMailingListMessageBody(ctx context.Context, repoID int64, messageID, listAddress, senderEmail, body string, sentAt time.Time, cntrbID *string, cleanBody, cleanRule string) (int64, error) {
	var id int64
	// v0.27.112: withRetry (40P01) — see UpsertEmailMessage.
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, node_id, cntrb_id,
			 msg_text, msg_text_clean, msg_text_clean_rule,
			 msg_timestamp, msg_sender_email, tool_source, tool_version, data_source)
		VALUES ($1, $2, $3, $12, $4, $5, $6, $13, $14, $7, $8, $9, $10, $11)
		ON CONFLICT (platform_msg_id, platform_id, msg_kind) DO UPDATE SET
			msg_text = EXCLUDED.msg_text,
			msg_text_clean = EXCLUDED.msg_text_clean,
			msg_text_clean_rule = EXCLUDED.msg_text_clean_rule,
			cntrb_id = COALESCE(EXCLUDED.cntrb_id, aveloxis_data.messages.cntrb_id),
			data_collection_date = NOW()
		RETURNING msg_id`,
			repoID, messageIDToPlatformMsgID(messageID), MailingListPlatformID, messageID, cntrbID,
			body, NullTime(sentAt), senderEmail, MailingListToolSource, ToolVersion, listAddress,
			MsgKindEmail, cleanBody, cleanRule,
		).Scan(&id)
	})
	if err != nil {
		return 0, fmt.Errorf("upsert mailing-list message body %q: %w", messageID, err)
	}
	return id, nil
}

// SetRepoGroup assigns a repo to a legacy repo_group (repos.group_id) — the
// per-PMC bridge (§11). This is what makes GetPrimaryRepoForGroup resolve a
// list's repo_group to a concrete repo so mailing-list bodies have a
// (NOT NULL) repo_id. Used by load-apache-lists / DOAP-enrichment.
func (s *PostgresStore) SetRepoGroup(ctx context.Context, repoID, repoGroupID int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_data.repos SET repo_group_id = $2 WHERE repo_id = $1`, repoID, repoGroupID)
	if err != nil {
		return fmt.Errorf("set repo %d group %d: %w", repoID, repoGroupID, err)
	}
	return nil
}

// RegisterMailingList records a list in repo_groups_list_serve and tags it
// with the mailing-list system definition so the MailingListWorker can claim
// it. Idempotent on (repo_group_id, rgls_email).
func (s *PostgresStore) RegisterMailingList(ctx context.Context, repoGroupID int64, listEmail, system string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_groups_list_serve (repo_group_id, rgls_email, rgls_name, mlls_system)
		VALUES ($1, $2, $2, $3)
		ON CONFLICT (repo_group_id, rgls_email) DO UPDATE SET mlls_system = EXCLUDED.mlls_system`,
		repoGroupID, listEmail, system)
	if err != nil {
		return fmt.Errorf("register mailing list %q in group %d: %w", listEmail, repoGroupID, err)
	}
	return nil
}

// GetPrimaryRepoForGroup returns the default repo a list's discussion links
// to — the lowest repo_id in the list's repo_group (per §5c, discussion
// default-links to the PMC's primary repo). Returns (0, false, nil) when the
// group has no repos yet (DOAP-enrichment / load-foundation-orgs must run
// first to populate the per-PMC group).
func (s *PostgresStore) GetPrimaryRepoForGroup(ctx context.Context, repoGroupID int64) (int64, bool, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`SELECT repo_id FROM aveloxis_data.repos WHERE repo_group_id = $1 ORDER BY repo_id LIMIT 1`,
		repoGroupID).Scan(&id)
	if err != nil {
		return 0, false, nil //nolint:nilerr // no-repo is a clean "not yet populated", not an error
	}
	return id, true, nil
}

// ResolveMirrorLinkByNodeID resolves a GitHub-mirror email to the issue or
// pull request it mirrors, using the GitHub GraphQL node ID recovered from the
// GitBox Message-ID (mailinglist.NodeIDFromMessageID).
//
// This is the PRIMARY mirror-link path. ResolveMirrorLink (below) resolves by
// owner/repo/kind/number from the body URL and stays as the fallback for
// systems whose mail carries one; on Apache it never fires, because the body
// captures are absent (2026-08-29 production audit) AND the default
// mirror_handling="metadata_only" does not retain bodies at all.
//
// Node IDs are globally unique on GitHub and type-prefixed, so the prefix
// selects the table and no repo scoping is needed. Returns (nil, nil, nil)
// for a clean miss — an entity we have not collected. A real query failure
// propagates: a lookup ERROR is not "not found" (SR-5), and silently
// treating one as a miss would leave the row permanently unlinked while
// reporting success.
func (s *PostgresStore) ResolveMirrorLinkByNodeID(ctx context.Context, nodeID string) (issueID, prID *int64, err error) {
	if nodeID == "" {
		return nil, nil, nil
	}
	switch {
	case strings.HasPrefix(nodeID, "PR_"):
		var id int64
		e := s.pool.QueryRow(ctx,
			`SELECT pull_request_id FROM aveloxis_data.pull_requests WHERE node_id = $1 LIMIT 1`,
			nodeID).Scan(&id)
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, nil, nil // not collected — clean miss
		}
		if e != nil {
			return nil, nil, fmt.Errorf("resolve mirror PR by node_id %q: %w", nodeID, e)
		}
		return nil, &id, nil
	case strings.HasPrefix(nodeID, "I_"):
		var id int64
		e := s.pool.QueryRow(ctx,
			`SELECT issue_id FROM aveloxis_data.issues WHERE node_id = $1 LIMIT 1`,
			nodeID).Scan(&id)
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, nil, nil
		}
		if e != nil {
			return nil, nil, fmt.Errorf("resolve mirror issue by node_id %q: %w", nodeID, e)
		}
		return &id, nil, nil
	}
	return nil, nil, nil
}

// ResolveMirrorLink resolves a GitHub-mirror email's body URL
// (owner/repo/kind/number) to the existing issue or pull_request row we
// already collected, so a mirror message links to its parent instead of
// duplicating it (§5b). kind is "pull" or "issues". Returns (nil, nil, nil)
// when the repo or the issue/PR isn't in the catalog yet.
func (s *PostgresStore) ResolveMirrorLink(ctx context.Context, owner, repo, kind string, number int) (issueID, prID *int64, err error) {
	// SR-5 throughout: ErrNoRows is a clean miss (we have not collected the
	// entity); any other failure is an ERROR. Before v0.28.20 every failure
	// here collapsed to a miss, which became load-bearing once the caller
	// started branching on error-vs-miss to decide whether to fall back to
	// this guessed-owner path at all.
	var repoID int64
	e := s.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE lower(repo_owner) = lower($1) AND lower(repo_name) = lower($2)
		LIMIT 1`, owner, repo).Scan(&repoID)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, nil, nil // repo not collected — clean miss
	}
	if e != nil {
		return nil, nil, fmt.Errorf("resolve mirror repo %s/%s: %w", owner, repo, e)
	}
	switch kind {
	case "pull":
		var id int64
		e := s.pool.QueryRow(ctx,
			`SELECT pull_request_id FROM aveloxis_data.pull_requests WHERE repo_id = $1 AND pr_number = $2 LIMIT 1`,
			repoID, number).Scan(&id)
		if e == nil {
			return nil, &id, nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return nil, nil, fmt.Errorf("resolve mirror PR %s/%s#%d: %w", owner, repo, number, e)
		}
	case "issues":
		var id int64
		e := s.pool.QueryRow(ctx,
			`SELECT issue_id FROM aveloxis_data.issues WHERE repo_id = $1 AND issue_number = $2 LIMIT 1`,
			repoID, number).Scan(&id)
		if e == nil {
			return &id, nil, nil
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return nil, nil, fmt.Errorf("resolve mirror issue %s/%s#%d: %w", owner, repo, number, e)
		}
	}
	return nil, nil, nil
}

// BackfillIssueExternalKeys populates issues.external_key from issue titles
// carrying a bracketed tracker key (e.g. "... [LUCENE-1]") — the §6
// Pattern-A signal where Apache bulk-imported Jira history into GitHub
// issues. Idempotent (only fills empty keys). Returns rows updated.
//
// Conflict-safe against BOTH collision shapes the UNIQUE idx_issues_external_key
// (repo_id, external_key) can hit:
//
//  1. Two empty-key issues in the same repo whose titles derive the SAME key
//     (common in Apache Jira→GitHub imports — renamed/duplicate titles). A
//     blind UPDATE would set both to the same value and fail 23505 WITHIN the
//     statement. `DISTINCT ON (repo_id, key)` picks exactly one winner (lowest
//     issue_id) per (repo, key) so only one row is keyed.
//  2. A sibling that ALREADY holds the key (e.g. a synthetic issue minted by
//     mailing-list projection that squats it — the missed-LINK shadow surfaced
//     by `mailing-list-stats`). The NOT EXISTS guard skips those.
//
// In both cases the loser/shadowed issue is left key-less rather than erroring.
// Idempotent. Returns the number of issues keyed.
func (s *PostgresStore) BackfillIssueExternalKeys(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH derived AS (
		    SELECT issue_id, repo_id,
		           substring(issue_title from '\[([A-Z][A-Z0-9]+-[0-9]+)\]') AS k
		    FROM aveloxis_data.issues
		    WHERE COALESCE(external_key, '') = ''
		      AND issue_title ~ '\[[A-Z][A-Z0-9]+-[0-9]+\]'
		),
		pick AS (
		    SELECT DISTINCT ON (repo_id, k) issue_id, repo_id, k
		    FROM derived
		    ORDER BY repo_id, k, issue_id
		)
		UPDATE aveloxis_data.issues i
		SET external_key = p.k
		FROM pick p
		WHERE i.issue_id = p.issue_id
		  AND NOT EXISTS (
		      SELECT 1 FROM aveloxis_data.issues j
		      WHERE j.repo_id = p.repo_id AND j.issue_id <> p.issue_id AND j.external_key = p.k
		  )`)
	if err != nil {
		return 0, fmt.Errorf("backfill issue external keys: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ResolveContributorIDByEmail resolves a sender email to an existing
// contributor cntrb_id via the SAME chain commit-author resolution uses —
// contributors.cntrb_email / cntrb_canonical first, then
// contributors_aliases.alias_email — filtering soft-deleted (v0.20.2)
// rows. Returns ("", false, nil) on a clean miss. This is the read half of
// mailing-list sender identity resolution (v0.25.7): a list sender who is
// also a committer to the project already has their commit-email alias in
// the DB, so coverage rises as the contributors table fills over time. On
// a miss the worker keeps cntrb_id NULL and retains sender_email; a periodic
// backfill re-resolves later. We deliberately do NOT synthesize an
// email-only contributor per sender (one-off posters would flood the table;
// the email on email_message is enough to reconcile later).
func (s *PostgresStore) ResolveContributorIDByEmail(ctx context.Context, email string) (string, bool, error) {
	if email == "" {
		return "", false, nil
	}
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT cntrb_id::text FROM aveloxis_data.contributors
		WHERE (cntrb_email = $1 OR cntrb_canonical = $1)
		  AND COALESCE(cntrb_deleted, 0) = 0
		LIMIT 1`, email).Scan(&id)
	if err == nil && id != "" {
		return id, true, nil
	}
	err = s.pool.QueryRow(ctx, `
		SELECT c.cntrb_id::text
		FROM aveloxis_data.contributors_aliases a
		JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id
		WHERE a.alias_email = $1
		  AND COALESCE(c.cntrb_deleted, 0) = 0
		LIMIT 1`, email).Scan(&id)
	if err == nil && id != "" {
		return id, true, nil
	}
	return "", false, nil
}

// BackfillMailingListSenderIDs re-resolves the cntrb_id of mailing-list
// message bodies whose sender was unresolved at write time, over ONE
// keyset window (afterMsgID, afterMsgID+window] of messages.msg_id.
// Two statements, deliberately: the email/canonical arm first (measured
// 15,445 rows/s on the production aveloxis DB), then the
// contributors_aliases arm over rows STILL NULL in the same window
// (measured 87,848 rows/1.0s via the alias_email UNIQUE probe). The
// split gives email-beats-alias priority for free — matching
// ResolveContributorIDByEmail's own probe order — and keeps both index
// plans (a 3-arm OR through a LEFT JOIN defeats the BitmapOr).
//
// Keyset windows, never LIMIT: the pre-Part-A LIMIT-rescan form re-paid
// the full join per batch (measured 8.5:1 scan waste, ~31 days to
// converge on 9.9M unattributed rows). Callers walk windows from
// MailingListMsgIDFloor to MailingListMsgIDCeiling; sparse windows
// legally resolve 0, so termination is cursor >= ceiling, NEVER
// rows-affected. "cntrb_id IS NULL is the resume state": rerun until the
// cursor clears the ceiling and only still-unresolvable rows remain.
//
// Both statements ride withRetry (40P01): the drain loop's
// UpsertMailingListMessageBody DO-UPDATEs the same messages rows — the
// deadlock class between these writers is observed history
// (v0.27.112/114).
func (s *PostgresStore) BackfillMailingListSenderIDs(ctx context.Context, afterMsgID, window int64) (int64, error) {
	var total int64
	// Arm 1: direct cntrb_email / cntrb_canonical match. DISTINCT ON
	// makes a multi-contributor email (two rows sharing cntrb_email —
	// none observed, but the schema permits it) a deterministic pick
	// instead of a planner-dependent one.
	err := s.withRetry(ctx, func(ctx context.Context) error {
		tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.messages m
		SET cntrb_id = sub.cntrb_id
		FROM (
			SELECT DISTINCT ON (m2.msg_id) m2.msg_id, c.cntrb_id
			FROM aveloxis_data.messages m2
			JOIN aveloxis_data.email_message em ON em.message_id_header = m2.node_id
			JOIN aveloxis_data.contributors c
			  ON (c.cntrb_email = em.sender_email OR c.cntrb_canonical = em.sender_email)
			 AND COALESCE(c.cntrb_deleted, 0) = 0
			WHERE m2.platform_id = 6
			  AND m2.cntrb_id IS NULL
			  AND em.sender_email <> ''
			  AND NOT aveloxis_data.is_automation_email(em.sender_email)
			  AND m2.msg_id > $1 AND m2.msg_id <= $1 + $2
			ORDER BY m2.msg_id, c.cntrb_id
		) sub
		WHERE m.msg_id = sub.msg_id`, afterMsgID, window)
		if err != nil {
			return err
		}
		total += tag.RowsAffected()
		return nil
	})
	if err != nil {
		return total, fmt.Errorf("backfill mailing-list sender ids (email arm, after %d): %w", afterMsgID, err)
	}
	// Arm 2: the contributors_aliases bridge — a list sender who is a
	// committer has their commit-email alias here even when no
	// contributors row carries the address directly (163,236 messages on
	// the production aveloxis DB were reachable ONLY this way). Runs
	// over rows arm 1 left NULL, so a both-ways match keeps arm 1's
	// answer. alias_email is UNIQUE, so there is no fan-out to pick from.
	err = s.withRetry(ctx, func(ctx context.Context) error {
		tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.messages m
		SET cntrb_id = sub.cntrb_id
		FROM (
			SELECT m2.msg_id, ca.cntrb_id
			FROM aveloxis_data.messages m2
			JOIN aveloxis_data.email_message em ON em.message_id_header = m2.node_id
			JOIN aveloxis_data.contributors_aliases ca ON ca.alias_email = em.sender_email
			JOIN aveloxis_data.contributors c
			  ON c.cntrb_id = ca.cntrb_id AND COALESCE(c.cntrb_deleted, 0) = 0
			WHERE m2.platform_id = 6
			  AND m2.cntrb_id IS NULL
			  AND em.sender_email <> ''
			  AND NOT aveloxis_data.is_automation_email(em.sender_email)
			  AND m2.msg_id > $1 AND m2.msg_id <= $1 + $2
		) sub
		WHERE m.msg_id = sub.msg_id`, afterMsgID, window)
		if err != nil {
			return err
		}
		total += tag.RowsAffected()
		return nil
	})
	if err != nil {
		return total, fmt.Errorf("backfill mailing-list sender ids (alias arm, after %d): %w", afterMsgID, err)
	}
	return total, nil
}

// CountResolvableMailingListSenders reports how many unattributed
// mailing-list bodies the backfill could resolve RIGHT NOW through
// either identity arm — the resolve-email-identities --dry-run number.
// Read-only; seconds-scale (one pass over the unattributed set with
// indexed EXISTS probes).
func (s *PostgresStore) CountResolvableMailingListSenders(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM aveloxis_data.messages m
		JOIN aveloxis_data.email_message em ON em.message_id_header = m.node_id
		WHERE m.platform_id = 6 AND m.cntrb_id IS NULL AND em.sender_email <> ''
		  AND NOT aveloxis_data.is_automation_email(em.sender_email)
		  AND (EXISTS (SELECT 1 FROM aveloxis_data.contributors c
				WHERE (c.cntrb_email = em.sender_email OR c.cntrb_canonical = em.sender_email)
				  AND COALESCE(c.cntrb_deleted, 0) = 0)
		    OR EXISTS (SELECT 1 FROM aveloxis_data.contributors_aliases ca
				JOIN aveloxis_data.contributors c2 ON c2.cntrb_id = ca.cntrb_id
				WHERE ca.alias_email = em.sender_email AND COALESCE(c2.cntrb_deleted, 0) = 0))`,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count resolvable mailing-list senders: %w", err)
	}
	return n, nil
}

// MailingListMsgIDFloor returns the smallest messages.msg_id carrying a
// mailing-list body (platform 6), or 0 when none exist. EXPENSIVE
// (~17.5s measured: the MIN side walks messages_pkey forward through
// every lower-id non-email row — there is no (platform_id, msg_id)
// index, deliberately: one 17.5s read per process beats a permanent
// index on a 68M-row table). Callers cache it for the process lifetime;
// the floor never moves down.
func (s *PostgresStore) MailingListMsgIDFloor(ctx context.Context) (int64, error) {
	var floor int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MIN(msg_id), 0) FROM aveloxis_data.messages WHERE platform_id = 6`).Scan(&floor)
	if err != nil {
		return 0, fmt.Errorf("mailing-list msg_id floor: %w", err)
	}
	return floor, nil
}

// MailingListMsgIDCeiling returns the largest messages.msg_id carrying a
// mailing-list body, or 0 when none exist. Cheap (~26ms measured:
// backward pkey scan — email rows are recent). Refresh per pass so rows
// staged since the last pass are covered.
func (s *PostgresStore) MailingListMsgIDCeiling(ctx context.Context) (int64, error) {
	var ceil int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(msg_id), 0) FROM aveloxis_data.messages WHERE platform_id = 6`).Scan(&ceil)
	if err != nil {
		return 0, fmt.Errorf("mailing-list msg_id ceiling: %w", err)
	}
	return ceil, nil
}

// StripBatchRow is one unstripped (or stale-rule) mailing-list body
// for the strip-quoted-history walker.
type StripBatchRow struct {
	MsgID int64
	Text  string
}

// GetMailingListBodiesForStrip pages mailing-list bodies needing a
// quote-strip, by keyset cursor. ruleRerun == "" selects never-stripped
// rows (msg_text_clean IS NULL — the resume state); a non-empty
// ruleRerun selects rows whose stored rule differs from it (the
// --rule-rerun mode after a pattern-library bump).
func (s *PostgresStore) GetMailingListBodiesForStrip(ctx context.Context, afterMsgID int64, limit int, ruleRerun string) ([]StripBatchRow, error) {
	predicate := `msg_text_clean IS NULL`
	args := []any{afterMsgID, limit}
	if ruleRerun != "" {
		predicate = `COALESCE(msg_text_clean_rule, '') <> $3`
		args = append(args, ruleRerun)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT msg_id, msg_text FROM aveloxis_data.messages
		WHERE platform_id = 6 AND msg_text <> '' AND msg_id > $1 AND `+predicate+`
		ORDER BY msg_id LIMIT $2`, args...)
	if err != nil {
		return nil, fmt.Errorf("strip batch after %d: %w", afterMsgID, err)
	}
	defer rows.Close()
	var out []StripBatchRow
	for rows.Next() {
		var r StripBatchRow
		if err := rows.Scan(&r.MsgID, &r.Text); err != nil {
			return nil, fmt.Errorf("strip batch scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("strip batch rows: %w", err)
	}
	return out, nil
}

// UpdateMessageCleanBatch stamps stripped bodies + the rule that
// produced them, one unnest UPDATE per batch. withRetry: the drain
// loop's body upsert DO-UPDATEs the same rows (the 40P01 class,
// v0.27.112/114).
func (s *PostgresStore) UpdateMessageCleanBatch(ctx context.Context, msgIDs []int64, cleans []string, rule string) error {
	if len(msgIDs) == 0 {
		return nil
	}
	if len(msgIDs) != len(cleans) {
		return fmt.Errorf("update clean batch: %d ids vs %d cleans", len(msgIDs), len(cleans))
	}
	err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.messages m
		SET msg_text_clean = sub.clean, msg_text_clean_rule = $3
		FROM (SELECT unnest($1::bigint[]) AS msg_id, unnest($2::text[]) AS clean) sub
		WHERE m.msg_id = sub.msg_id`, msgIDs, cleans, rule)
		return err
	})
	if err != nil {
		return fmt.Errorf("update clean batch (%d rows): %w", len(msgIDs), err)
	}
	return nil
}

// HealAutomationPhantomContributors repairs the 2026-08-31 identity
// fabrication: email-only contributor rows minted for AUTOMATION
// addresses (jira@apache.org, gitbox@, list addresses) by the
// pre-guard sender-resolve ticker. Phantoms are identified as rows
// whose email classifies as automation AND that carry no platform
// identity (empty cntrb_login/gh_login/gl_username — a real person who
// merely also matches an automation pattern cannot look like this).
// Repair: NULL every messages.cntrb_id and issues.reporter_id they
// hold, drop their relay alias rows, and SOFT-delete the rows
// (cntrb_deleted = 1 — R10: FK targets are never physically deleted).
// Idempotent; ledgered in migrate.
func (s *PostgresStore) HealAutomationPhantomContributors(ctx context.Context) error {
	const phantoms = `
		SELECT cntrb_id FROM aveloxis_data.contributors
		WHERE aveloxis_data.is_automation_email(cntrb_email)
		  AND COALESCE(cntrb_login, '') = ''
		  AND COALESCE(gh_login, '') = ''
		  AND COALESCE(gl_username, '') = ''`
	steps := []struct{ label, sql string }{
		{"null message attributions",
			`UPDATE aveloxis_data.messages SET cntrb_id = NULL WHERE cntrb_id IN (` + phantoms + `)`},
		{"null issue reporters",
			`UPDATE aveloxis_data.issues SET reporter_id = NULL WHERE reporter_id IN (` + phantoms + `)`},
		{"drop relay aliases",
			`DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id IN (` + phantoms + `)`},
		{"soft-delete phantoms",
			`UPDATE aveloxis_data.contributors SET cntrb_deleted = 1
			 WHERE cntrb_id IN (` + phantoms + `) AND COALESCE(cntrb_deleted, 0) = 0`},
	}
	for _, st := range steps {
		if _, err := s.pool.Exec(ctx, st.sql); err != nil {
			return fmt.Errorf("heal automation phantoms (%s): %w", st.label, err)
		}
	}
	return nil
}

// InsertEmailMessageRef links an email_message entity to its body row in
// messages. Idempotent on (email_message_id, msg_id).
func (s *PostgresStore) InsertEmailMessageRef(ctx context.Context, emailMessageID, msgID int64, repoGroupID *int64) error {
	// v0.27.112: withRetry (40P01) — see UpsertEmailMessage.
	err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.email_message_ref (email_message_id, msg_id, repo_group_id, tool_version)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (email_message_id, msg_id) DO NOTHING`,
			emailMessageID, msgID, repoGroupID, ToolVersion)
		return err
	})
	if err != nil {
		return fmt.Errorf("insert email_message_ref (%d,%d): %w", emailMessageID, msgID, err)
	}
	return nil
}

// ResolveSignaledRepoForURL backfills email_message.signaled_repo_id for
// every row whose signaled_repo_url matches the given (newly-created) repo.
// Called on repo creation by load-foundation-orgs / refreshUserOrgs — the
// repo-side half of the §5c bidirectional resolution. Returns the number of
// rows resolved. Match is case-insensitive and trailing-slash-insensitive;
// repoURL should be the repo's canonical git URL without a .git suffix.
func (s *PostgresStore) ResolveSignaledRepoForURL(ctx context.Context, repoID int64, repoURL string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.email_message
		SET signaled_repo_id = $1
		WHERE signaled_repo_id IS NULL
		  AND signaled_repo_url <> ''
		  AND lower(rtrim(signaled_repo_url, '/')) = lower(rtrim($2, '/'))`,
		repoID, repoURL)
	if err != nil {
		return 0, fmt.Errorf("resolve signaled_repo for %q: %w", repoURL, err)
	}
	return tag.RowsAffected(), nil
}
