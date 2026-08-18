// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — add_requests.go: the v0.27.20 per-add approval layer
// (summary/15, Option A). The approval unit is the ADDITION of
// not-yet-tracked content: known repos link into any group instantly
// (approval gates collection load, never visibility — the v0.27.4
// rule); unknown repo URLs and org registrations by non-admins create
// a pending collection_add_requests row that an admin decides.
// Request items store URLs, not repos rows — UpsertRepo runs at
// APPROVAL, so a rejected request leaves zero catalog residue.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// AddOutcome reports what AddReposToGroup did with a batch of URLs so
// handlers can tell the user "N added, M awaiting approval".
type AddOutcome struct {
	Linked    int   // known repos linked into the group (no new collection)
	Enqueued  int   // new repos created + enqueued (admin or auto-approved path)
	Pending   int   // URLs parked on a pending add-request
	RequestID int64 // non-zero when a pending request was created
}

// OrgAddOutcome reports what AddOrgToGroup did.
type OrgAddOutcome struct {
	Registered bool  // true = org tracking registered (admin path)
	RequestID  int64 // non-zero when a pending request was created instead
}

// AddRequest is one pending (or decided) addition request, joined to
// its requester and group for the admin queue view.
type AddRequest struct {
	RequestID  int64
	UserID     int
	UserLogin  string
	UserEmail  string
	GroupID    int64
	GroupName  string
	Kind       string // 'repos' | 'org'
	OrgURL     string
	Status     string
	ItemCount  int
	CreatedAt  time.Time
	SampleURLs []string // first few item URLs for the admin list
}

// repoTracked reports whether the repo participates in collection —
// i.e. it has a collection_queue row (queued or collected). A repos
// row WITHOUT a queue row (e.g. legacy pending-group residue) gives a
// linking user nothing and never will, so it does NOT count as
// "already collected" for the per-add approval rule.
func (s *PostgresStore) repoTracked(ctx context.Context, repoID int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue WHERE repo_id = $1)`,
		repoID).Scan(&exists)
	return exists, err
}

// GetGroupStatus returns the group's approval status (” rows default
// to 'approved' like every other reader of the column).
func (s *PostgresStore) GetGroupStatus(ctx context.Context, groupID int64) (string, error) {
	var status string
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(status, 'approved') FROM aveloxis_ops.user_groups WHERE group_id = $1`,
		groupID).Scan(&status)
	return status, err
}

// ensureRepoCollectedInGroup is the single "make this URL exist,
// collect, and belong to the group" helper — shared by the admin
// direct-add path and the approval processor so both behave
// identically. Resolves case-insensitively; creates the repos row
// only when absent (never routes existing repos through UpsertRepo's
// DO UPDATE, which would clobber collected metadata); always ensures
// a queue row; always links user_repos.
func (s *PostgresStore) ensureRepoCollectedInGroup(ctx context.Context, groupID int64, repoURL string) (int64, error) {
	repoID, err := s.FindRepoByURL(ctx, repoURL)
	if err != nil {
		return 0, fmt.Errorf("resolve repo URL: %w", err)
	}
	if repoID == 0 {
		newRepo := &model.Repo{GitURL: repoURL, Platform: model.PlatformGenericGit}
		if ru, perr := platform.ParseAnyRepoURL(repoURL); perr == nil {
			newRepo.Platform = ru.Platform
			newRepo.Owner = ru.Owner
			newRepo.Name = ru.Repo
		}
		repoID, err = s.UpsertRepo(ctx, newRepo)
		if err != nil {
			return 0, err
		}
	}
	if err := s.EnqueueRepo(ctx, repoID, 100); err != nil {
		return 0, fmt.Errorf("enqueue repo: %w", err)
	}
	if _, err := s.AddRepoToGroupByID(ctx, groupID, repoID); err != nil {
		return 0, err
	}
	return repoID, nil
}

// AddReposToGroup adds a batch of repo URLs to a user group under the
// v0.27.20 per-add approval rule:
//
//   - tracked repos (a collection_queue row exists) link instantly for
//     everyone — no approval, no new collection load;
//   - unknown URLs from an ADMIN are created + enqueued immediately;
//   - unknown URLs from a non-admin are parked on ONE pending
//     add-request (the whole batch is one approval unit), UNLESS
//     autoApproveLimit > 0 and the batch fits under it, in which case
//     they are added directly with an 'approved' audit request row
//     (decided_by = 0 marks auto-approval).
//
// autoApproveLimit comes from web.auto_approve_add_limit; 0 (the
// default) means every non-admin new-repo add requires approval.
func (s *PostgresStore) AddReposToGroup(ctx context.Context, userID int, groupID int64, repoURLs []string, autoApproveLimit int) (AddOutcome, error) {
	var out AddOutcome
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return out, err
	}
	status, err := s.GetGroupStatus(ctx, groupID)
	if err != nil {
		return out, fmt.Errorf("look up group status: %w", err)
	}
	if status == "rejected" {
		return out, fmt.Errorf("group has been rejected by an administrator")
	}
	isAdmin, _ := s.IsUserAdmin(ctx, userID)

	var unknown []string
	seen := make(map[string]bool, len(repoURLs))
	for _, raw := range repoURLs {
		repoURL := strings.TrimSpace(raw)
		if repoURL == "" || seen[repoURL] {
			continue
		}
		seen[repoURL] = true

		repoID, err := s.FindRepoByURL(ctx, repoURL)
		if err != nil {
			return out, fmt.Errorf("resolve repo URL: %w", err)
		}
		tracked := false
		if repoID > 0 {
			tracked, err = s.repoTracked(ctx, repoID)
			if err != nil {
				return out, err
			}
		}
		switch {
		case tracked:
			// Known repo: instant link, zero collection load added.
			if _, err := s.AddRepoToGroupByID(ctx, groupID, repoID); err != nil {
				return out, err
			}
			out.Linked++
		case isAdmin:
			if _, err := s.ensureRepoCollectedInGroup(ctx, groupID, repoURL); err != nil {
				return out, err
			}
			out.Enqueued++
		default:
			unknown = append(unknown, repoURL)
		}
	}

	if len(unknown) == 0 {
		return out, nil
	}

	// Non-admin with genuinely new content: auto-approve small batches
	// when the operator opted in, otherwise park on a pending request.
	if autoApproveLimit > 0 && len(unknown) <= autoApproveLimit {
		reqID, err := s.createAddRequest(ctx, userID, groupID, "repos", "", unknown, "approved")
		if err != nil {
			return out, err
		}
		out.RequestID = reqID
		if _, err := s.ProcessApprovedAddRequest(ctx, reqID); err != nil {
			return out, err
		}
		out.Enqueued += len(unknown)
		return out, nil
	}

	reqID, err := s.createAddRequest(ctx, userID, groupID, "repos", "", unknown, "pending")
	if err != nil {
		return out, err
	}
	out.RequestID = reqID
	out.Pending = len(unknown)
	return out, nil
}

// createAddRequest inserts the request row + its items in one
// transaction. status is 'pending' for the normal flow or 'approved'
// for the auto-approve audit path (decided_by = 0, decided_at = NOW()).
func (s *PostgresStore) createAddRequest(ctx context.Context, userID int, groupID int64, kind, orgURL string, urls []string, status string) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var requestID int64
	if status == "approved" {
		err = tx.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.collection_add_requests
				(user_id, group_id, kind, org_url, status, item_count, decided_by, decided_at)
			VALUES ($1, $2, $3, $4, 'approved', $5, 0, NOW())
			RETURNING request_id`,
			userID, groupID, kind, orgURL, len(urls)).Scan(&requestID)
	} else {
		err = tx.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.collection_add_requests
				(user_id, group_id, kind, org_url, status, item_count)
			VALUES ($1, $2, $3, $4, 'pending', $5)
			RETURNING request_id`,
			userID, groupID, kind, orgURL, len(urls)).Scan(&requestID)
	}
	if err != nil {
		return 0, fmt.Errorf("create add request: %w", err)
	}
	for _, u := range urls {
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_add_request_items (request_id, repo_url)
			VALUES ($1, $2) ON CONFLICT (request_id, repo_url) DO NOTHING`,
			requestID, u); err != nil {
			return 0, fmt.Errorf("create add request item: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return requestID, nil
}

// ListPendingAddRequests returns the admin approval queue,
// oldest-first, each with a small sample of item URLs.
func (s *PostgresStore) ListPendingAddRequests(ctx context.Context) ([]AddRequest, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ar.request_id, ar.user_id, u.login_name, COALESCE(u.email, ''),
		       ar.group_id, g.name, ar.kind, ar.org_url, ar.status, ar.item_count, ar.created_at
		FROM aveloxis_ops.collection_add_requests ar
		JOIN aveloxis_ops.users u ON u.user_id = ar.user_id
		JOIN aveloxis_ops.user_groups g ON g.group_id = ar.group_id
		WHERE ar.status = 'pending'
		ORDER BY ar.request_id`)
	if err != nil {
		return nil, fmt.Errorf("list pending add requests: %w", err)
	}
	defer rows.Close()
	var out []AddRequest
	for rows.Next() {
		var a AddRequest
		if err := rows.Scan(&a.RequestID, &a.UserID, &a.UserLogin, &a.UserEmail,
			&a.GroupID, &a.GroupName, &a.Kind, &a.OrgURL, &a.Status, &a.ItemCount, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		sample, err := s.pool.Query(ctx, `
			SELECT repo_url FROM aveloxis_ops.collection_add_request_items
			WHERE request_id = $1 ORDER BY item_id LIMIT 10`, out[i].RequestID)
		if err != nil {
			continue
		}
		for sample.Next() {
			var u string
			if err := sample.Scan(&u); err != nil {
				sample.Close()
				return nil, err
			}
			out[i].SampleURLs = append(out[i].SampleURLs, u)
		}
		sample.Close()
	}
	return out, nil
}

// DecideAddRequest flips a pending request to approved/rejected and
// returns the request (joined to requester + group for notification).
// changed=false means the request was already decided (double click) —
// callers skip processing and emails. For kind='org' approvals the org
// registration happens HERE (INSERT into user_org_requests): presence
// in that table means "approved to scan", which is what keeps the
// scheduler's org tickers gate-free by construction.
func (s *PostgresStore) DecideAddRequest(ctx context.Context, requestID int64, adminID int, approve bool) (AddRequest, bool, error) {
	var req AddRequest
	err := s.pool.QueryRow(ctx, `
		SELECT ar.request_id, ar.user_id, u.login_name, COALESCE(u.email, ''),
		       ar.group_id, g.name, ar.kind, ar.org_url, ar.status, ar.item_count, ar.created_at
		FROM aveloxis_ops.collection_add_requests ar
		JOIN aveloxis_ops.users u ON u.user_id = ar.user_id
		JOIN aveloxis_ops.user_groups g ON g.group_id = ar.group_id
		WHERE ar.request_id = $1`, requestID).Scan(
		&req.RequestID, &req.UserID, &req.UserLogin, &req.UserEmail,
		&req.GroupID, &req.GroupName, &req.Kind, &req.OrgURL, &req.Status, &req.ItemCount, &req.CreatedAt)
	if err != nil {
		return req, false, fmt.Errorf("load add request: %w", err)
	}

	newStatus := "rejected"
	if approve {
		newStatus = "approved"
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.collection_add_requests
		SET status = $2, decided_by = $3, decided_at = NOW()
		WHERE request_id = $1 AND status = 'pending'`,
		requestID, newStatus, adminID)
	if err != nil {
		return req, false, fmt.Errorf("decide add request: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return req, false, nil // already decided
	}
	req.Status = newStatus

	if approve && req.Kind == "org" {
		orgName, platformName := parseOrgURLMeta(req.OrgURL)
		if _, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.user_org_requests
				(user_id, group_id, org_url, org_name, platform)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (group_id, org_url) DO NOTHING`,
			req.UserID, req.GroupID, req.OrgURL, orgName, platformName); err != nil {
			return req, true, fmt.Errorf("register approved org: %w", err)
		}
	}
	return req, true, nil
}

// ProcessApprovedAddRequest walks the request's unprocessed items and
// runs the shared add machinery for each. Idempotent + resumable:
// items with repo_id already stamped are skipped, so an interrupted
// pass (or a double approval) picks up where it left off. An
// unresolvable URL stamps repo_id = -1 (processed-with-error) so it
// can't wedge the request forever; the failure is logged.
func (s *PostgresStore) ProcessApprovedAddRequest(ctx context.Context, requestID int64) (int, error) {
	var groupID int64
	var status string
	if err := s.pool.QueryRow(ctx, `
		SELECT group_id, status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`,
		requestID).Scan(&groupID, &status); err != nil {
		return 0, fmt.Errorf("load add request: %w", err)
	}
	if status != "approved" {
		return 0, fmt.Errorf("request %d is %s, not approved", requestID, status)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT item_id, repo_url FROM aveloxis_ops.collection_add_request_items
		WHERE request_id = $1 AND repo_id IS NULL ORDER BY item_id`, requestID)
	if err != nil {
		return 0, err
	}
	type item struct {
		id  int64
		url string
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.url); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, it)
	}
	rows.Close()

	processed := 0
	for _, it := range items {
		repoID, err := s.ensureRepoCollectedInGroup(ctx, groupID, it.url)
		if err != nil {
			s.logger.Warn("add-request item failed — marking processed-with-error",
				"request_id", requestID, "url", it.url, "error", err)
			repoID = -1
		}
		if _, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_ops.collection_add_request_items
			SET repo_id = $2 WHERE item_id = $1`, it.id, repoID); err != nil {
			return processed, err
		}
		if repoID > 0 {
			processed++
		}
	}
	return processed, nil
}

// PendingAddItem is one awaiting-approval URL for the group page's
// "pending" strip.
type PendingAddItem struct {
	RequestID int64
	Kind      string
	URL       string
	CreatedAt time.Time
}

// GetPendingAddItemsForGroup returns the group's awaiting-approval
// content (repo URLs from pending 'repos' requests plus pending org
// registrations) so the group page can show what's still in review.
// Callers own the ownership check (portal: verifyGroupOwned semantics
// via GetPendingAddItemsForUser; admins may read any group).
func (s *PostgresStore) GetPendingAddItemsForGroup(ctx context.Context, groupID int64) ([]PendingAddItem, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ar.request_id, ar.kind,
		       CASE WHEN ar.kind = 'org' THEN ar.org_url ELSE i.repo_url END,
		       ar.created_at
		FROM aveloxis_ops.collection_add_requests ar
		LEFT JOIN aveloxis_ops.collection_add_request_items i ON i.request_id = ar.request_id
		WHERE ar.group_id = $1 AND ar.status = 'pending'
		  AND (ar.kind = 'org' OR i.item_id IS NOT NULL)
		ORDER BY ar.request_id, i.item_id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingAddItem
	for rows.Next() {
		var p PendingAddItem
		if err := rows.Scan(&p.RequestID, &p.Kind, &p.URL, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetPendingAddItemsForUser is the ownership-checked wrapper the
// portal uses: non-admins may only read their own group's pending
// items; admins bypass.
func (s *PostgresStore) GetPendingAddItemsForUser(ctx context.Context, userID int, groupID int64, isAdmin bool) ([]PendingAddItem, error) {
	if !isAdmin {
		if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
			return nil, err
		}
	}
	return s.GetPendingAddItemsForGroup(ctx, groupID)
}

// migrateLegacyPendingGroups converts every pre-v0.27.20 'pending'
// group into the per-add model: the group's linked-but-never-enqueued
// repos (exactly the set the legacy ApproveGroup bulk-enqueue would
// have collected) become ONE pending collection_add_requests row, and
// the group flips to 'approved'. Nothing is lost — yesterday's
// pending group appears as today's pending request in the same admin
// queue. Idempotent: after the first run no 'pending' groups remain;
// the per-group NOT EXISTS guard also protects a crash between
// request creation and the group flip from duplicating the request.
func migrateLegacyPendingGroups(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	rows, err := pg.pool.Query(ctx, `
		SELECT group_id, user_id FROM aveloxis_ops.user_groups WHERE status = 'pending'`)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: list: %w", err))
		return
	}
	type pg2 struct {
		groupID int64
		userID  int
	}
	var pending []pg2
	for rows.Next() {
		var p pg2
		if err := rows.Scan(&p.groupID, &p.userID); err != nil {
			rows.Close()
			*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: scan: %w", err))
			return
		}
		pending = append(pending, p)
	}
	rows.Close()
	if len(pending) == 0 {
		return
	}

	converted := 0
	for _, p := range pending {
		// The un-enqueued repos are what group approval would have
		// collected; they become the request's items (URLs — the
		// approval processor re-resolves and enqueues them).
		urlRows, err := pg.pool.Query(ctx, `
			SELECT r.repo_git
			FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_data.repos r ON r.repo_id = ur.repo_id
			WHERE ur.group_id = $1
			  AND NOT EXISTS (SELECT 1 FROM aveloxis_ops.collection_queue q WHERE q.repo_id = ur.repo_id)`,
			p.groupID)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: group %d items: %w", p.groupID, err))
			return
		}
		var urls []string
		for urlRows.Next() {
			var u string
			if err := urlRows.Scan(&u); err != nil {
				urlRows.Close()
				*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: group %d url scan: %w", p.groupID, err))
				return
			}
			if u != "" {
				urls = append(urls, u)
			}
		}
		if err := urlRows.Err(); err != nil {
			urlRows.Close()
			*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: group %d url iteration: %w", p.groupID, err))
			return
		}
		urlRows.Close()

		// v0.27.36: a failed EXISTS probe must not read as "not yet
		// converted" — that re-converted groups into duplicate pending
		// requests on the next migrate run (summary/18 Phase 0c).
		var alreadyConverted bool
		if err := pg.pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM aveloxis_ops.collection_add_requests WHERE group_id = $1)`,
			p.groupID).Scan(&alreadyConverted); err != nil {
			*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: group %d converted-probe: %w", p.groupID, err))
			return
		}
		if len(urls) > 0 && !alreadyConverted {
			if _, err := pg.createAddRequest(ctx, p.userID, p.groupID, "repos", "", urls, "pending"); err != nil {
				*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: convert group %d: %w", p.groupID, err))
				return
			}
			converted++
		}
		if _, err := pg.pool.Exec(ctx, `
			UPDATE aveloxis_ops.user_groups SET status = 'approved'
			WHERE group_id = $1 AND status = 'pending'`, p.groupID); err != nil {
			*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: flip group %d: %w", p.groupID, err))
			return
		}
	}
	logger.Info("v0.27.20 converted legacy pending groups to add-requests",
		"pending_groups", len(pending), "requests_created", converted)
}

// parseOrgURLMeta extracts the org name + platform label from an org
// URL, keeping the historical host-contains-"gitlab" heuristic and
// the permissive empty-name fallback (shared by AddOrgToGroup and the
// org-approval path so both register identically).
func parseOrgURLMeta(orgURL string) (orgName, platformName string) {
	platformName = "github"
	if host, org, err := platform.ParseOrgURL(orgURL); err == nil {
		orgName = org
		if strings.Contains(host, "gitlab") {
			platformName = "gitlab"
		}
	} else if strings.Contains(orgURL, "gitlab") {
		platformName = "gitlab"
	}
	return orgName, platformName
}
