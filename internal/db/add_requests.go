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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// AddOutcome reports what AddReposToGroup did with a batch of URLs so
// handlers can tell the user "N added, M awaiting approval".
type AddOutcome struct {
	Linked    int   // known repos newly linked into the group (no new collection; an existing link is not counted)
	Enqueued  int   // new repos created + enqueued (admin or auto-approved path)
	Pending   int   // URLs parked on a pending add-request
	Failed    int   // auto-approved repos whose add failed (their items are stamped -1)
	RequestID int64 // non-zero when a pending request was created
}

// OrgAddOutcome reports what AddOrgToGroup did.
type OrgAddOutcome struct {
	Registered bool  // true = org tracking registered: an admin's add, or a non-admin's add of an org already registered elsewhere (v0.27.84) — including a re-add that inserted nothing
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

// GetGroupStatus returns the group's approval status ("" rows default
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
// identically. Resolves the URL by its stored spelling and case; creates
// the repos row only when absent (an existing repo is linked, not
// re-upserted); always ensures a queue row; always links user_repos.
// glWebBase is the configured GitLab instance's web base: a new row under
// it is a GitLab repository whatever its host name says (round-3 review of
// 0.29.100), as the org scan of that instance records its projects.
func (s *PostgresStore) ensureRepoCollectedInGroup(ctx context.Context, groupID int64, repoURL, glWebBase string) (int64, error) {
	repoID, err := s.FindRepoByURL(ctx, repoURL)
	if err != nil {
		return 0, fmt.Errorf("resolve repo URL: %w", err)
	}
	if repoID == 0 {
		// A URL that does not parse as a repository is refused (0.29.146,
		// SR-18: the layer that writes the row enforces it). Until then it
		// was stored anyway, as a git-only repository with no owner and no
		// name ("historical permissiveness"), on the grounds that the web
		// path had validated it: an organization page stored that way
		// failed at the clone on every collection. The service still
		// validates first; an item an older release saved reaches this
		// function at its approval with no other check.
		ru, perr := platform.ParseAnyRepoURLWithHints(repoURL, platform.GitLabWebHints(glWebBase))
		if perr != nil {
			return 0, fmt.Errorf("not a repository URL: %w", perr)
		}
		repoID, err = s.UpsertRepo(ctx, &model.Repo{GitURL: repoURL, Platform: ru.Platform, Owner: ru.Owner, Name: ru.Repo})
		if err != nil {
			return 0, err
		}
	}
	if err := s.EnqueueRepo(ctx, repoID, 100); err != nil {
		return 0, fmt.Errorf("enqueue repo: %w", err)
	}
	// The repository's place is already held (v0.29.89): by the approved
	// item this pass stamps next (reachSQL counts it until then), or the
	// caller is an administrator's own session, which the quota exempts.
	n, err := s.linkWithinCap(ctx, groupID, []int64{repoID}, linkHeld)
	if err != nil {
		return 0, err
	}
	if err := s.goneIfUnlinked(ctx, n, repoID); err != nil {
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
func (s *PostgresStore) AddReposToGroup(ctx context.Context, userID int, groupID int64, repoURLs []string, autoApproveLimit int, glWebBase string) (AddOutcome, error) {
	var out AddOutcome
	if err := s.verifyGroupOwned(ctx, userID, groupID); err != nil {
		return out, err
	}
	status, err := s.GetGroupStatus(ctx, groupID)
	if err != nil {
		return out, fmt.Errorf("look up group status: %w", err)
	}
	if status == "rejected" {
		return out, ErrGroupRejected
	}
	// Checked before anything is written, so a refused add changes nothing.
	// A URL carrying credentials is refused here too (v0.29.57, Copilot
	// review 5261384568): the portal API reaches this writer without the web
	// validator, and a pending item is stored and shown to the admin before
	// UpsertRepo would refuse it at approval.
	for _, raw := range repoURLs {
		if len(strings.TrimSpace(raw)) > MaxAddURLBytes {
			return out, ErrURLTooLong
		}
		if err := platform.RefuseURLUserinfo(raw); err != nil {
			return out, err
		}
	}
	// A lookup ERROR is not "not an admin" (SR-5; worklist follow-up 6): it
	// sent an admin's paste to the approval queue, silently.
	isAdmin, err := s.IsUserAdmin(ctx, userID)
	if err != nil {
		return out, fmt.Errorf("look up admin flag: %w", err)
	}

	// Classify every URL first, decide the repository allocation once for
	// the whole paste (v0.29.89, summary/53: all or nothing — the user
	// decides what to drop), then write.
	type item struct {
		url     string
		repoID  int64
		tracked bool
	}
	var items []item
	var unknown []string
	var trackedIDs []int64
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
		items = append(items, item{url: repoURL, repoID: repoID, tracked: tracked})
		switch {
		case tracked:
			trackedIDs = append(trackedIDs, repoID)
		case !isAdmin:
			unknown = append(unknown, repoURL)
		}
	}
	if !isAdmin {
		// One transaction under the owner's lock decides the whole paste
		// and writes it, or nothing (Copilot review 5479554271: an unlocked
		// precheck then one transaction per link let a concurrent add turn
		// a refused paste into a partial one). Small batches auto-approve
		// when the operator opted in; otherwise the items wait for an
		// administrator.
		autoApprove := len(unknown) > 0 && autoApproveLimit > 0 && len(unknown) <= autoApproveLimit
		status := "pending"
		if autoApprove {
			status = "approved"
		}
		linked, reqID, err := s.addPasteWithinCap(ctx, userID, groupID, trackedIDs, unknown, status)
		if err != nil {
			return out, err
		}
		// Only a link this call inserted counts: a repository already in
		// the group changed no one's scope, and the API drops its whole
		// token cache on Linked > 0 (PR #218 review C2).
		out.Linked = linked
		if len(unknown) == 0 {
			return out, nil
		}
		out.RequestID = reqID
		if !autoApprove {
			out.Pending = len(unknown)
			return out, nil
		}
		return s.processAutoApproved(ctx, out, userID, groupID, reqID, len(unknown), glWebBase)
	}
	for _, it := range items {
		if it.tracked {
			// Known repo: instant link, zero collection load added.
			inserted, err := s.AddRepoToGroupByID(ctx, groupID, it.repoID)
			if err != nil {
				return out, err
			}
			if inserted {
				out.Linked++
			}
			continue
		}
		if _, err := s.ensureRepoCollectedInGroup(ctx, groupID, it.url, glWebBase); err != nil {
			return out, err
		}
		out.Enqueued++
	}
	return out, nil
}

// processAutoApproved runs an auto-approved add request's items.
func (s *PostgresStore) processAutoApproved(ctx context.Context, out AddOutcome, userID int, groupID, reqID int64, items int, glWebBase string) (AddOutcome, error) {
	// The request is committed as approved and no admin will see it,
	// so its processing must not stop with the caller's request context
	// (a client disconnect used to leave its items unprocessed for good;
	// round-14 review), and a failed item is final for it: the pass marks
	// it processed-with-error and goes on, and the add reports the count
	// (round-23 review: a retryable failure left it and every later item
	// unprocessed for good).
	// detached cannot be cancelled — not by the request, not by a stop —
	// so the pass's failure below is never a shutdown and needs no
	// context.Canceled arm (the shutdown-classification ratchet's
	// producer rule).
	detached := context.WithoutCancel(ctx)
	processed, failed, err := s.processAddRequest(detached, reqID, true, glWebBase)
	out.Enqueued += processed
	out.Failed = failed
	if err != nil {
		// Logged HERE, the layer that detached the pass (SR-18; NET-6
		// review r8 F1): its failure can never be the caller's request
		// ending, but the caller logs through httpserver.LogFailure,
		// which is Debug once nginx or http_timeout_seconds has ended
		// the request — and an approved request with unprocessed items
		// is listed for no admin and re-run by nothing (old problem O17
		// in the operator's to-do list).
		s.logger.Warn("auto-approved add request: processing failed — its unprocessed items are not retried automatically; the user can add those repositories again",
			"request_id", reqID, "user_id", userID, "group_id", groupID, "processed", processed, "failed", failed, "error", err)
		return out, fmt.Errorf("add request %d: %w", reqID, err)
	}
	if failed > 0 {
		return out, fmt.Errorf("add request %d: %d of %d %w", reqID, failed, items, ErrAddItemsFailed)
	}
	return out, nil
}

// createAddRequest inserts the request row + its items in one
// transaction. status is 'pending' for the normal flow or 'approved'
// for the auto-approve audit path (decided_by = 0, decided_at = NOW()).
func (s *PostgresStore) createAddRequest(ctx context.Context, userID int, groupID int64, kind, orgURL string, urls []string, status string) (int64, error) {
	// Each repository item holds a place in the account's allocation from
	// here until the approval pass links it; decided under the per-owner
	// lock (v0.29.89; AddReposToGroup writes its pastes through
	// addPasteWithinCap, which decides the same way). The
	// allocation is read before the transaction begins (no pool read inside
	// it: review round 1 R2-2).
	var quota capacity.EffectiveQuota
	var alloc capacity.Allocation
	if kind == "repos" && len(urls) > 0 {
		var err error
		if quota, alloc, err = s.reposAllocation(ctx, userID); err != nil {
			return 0, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if kind == "repos" {
		if err := s.holdRepoPlaces(ctx, tx, userID, urls, quota, alloc); err != nil {
			return 0, err
		}
	}
	requestID, err := insertAddRequest(ctx, tx, userID, groupID, kind, orgURL, urls, status)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return requestID, nil
}

// insertAddRequest writes a request and its items inside the caller's
// transaction; createAddRequest is the stand-alone form. AddOrgToGroup's
// auto-approve uses it to commit the audit request with the registration.
func insertAddRequest(ctx context.Context, tx pgx.Tx, userID int, groupID int64, kind, orgURL string, urls []string, status string) (int64, error) {
	var requestID int64
	var err error
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
	return requestID, nil
}

// addRequestSelectSQL is the one SELECT behind every AddRequest reader (the
// queue listing, the single-row read, the decision): one column order, one
// scanner. Append a WHERE and an ORDER BY.
const addRequestSelectSQL = `
		SELECT ar.request_id, ar.user_id, u.login_name, COALESCE(u.email, ''),
		       ar.group_id, g.name, ar.kind, ar.org_url, ar.status, ar.item_count, ar.created_at
		FROM aveloxis_ops.collection_add_requests ar
		JOIN aveloxis_ops.users u ON u.user_id = ar.user_id
		JOIN aveloxis_ops.user_groups g ON g.group_id = ar.group_id`

// scanAddRequest fills one AddRequest from an addRequestSelectSQL row.
func scanAddRequest(row pgx.Row, a *AddRequest) error {
	return row.Scan(&a.RequestID, &a.UserID, &a.UserLogin, &a.UserEmail,
		&a.GroupID, &a.GroupName, &a.Kind, &a.OrgURL, &a.Status, &a.ItemCount, &a.CreatedAt)
}

// addRequestSampleLimit is how many item URLs a request carries for the
// admin queue and the operator's notification: enough to recognise the
// paste, never the whole request.
const addRequestSampleLimit = 10

// addRequestSample reads the first item URLs of one request.
func (s *PostgresStore) addRequestSample(ctx context.Context, requestID int64) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_url FROM aveloxis_ops.collection_add_request_items
		WHERE request_id = $1 ORDER BY item_id LIMIT $2`, requestID, addRequestSampleLimit)
	if err != nil {
		return nil, fmt.Errorf("add request %d sample urls: %w", requestID, err)
	}
	defer rows.Close()
	var sample []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("add request %d sample urls: %w", requestID, err)
		}
		sample = append(sample, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("add request %d sample urls: %w", requestID, err)
	}
	return sample, nil
}

// GetAddRequest reads one request, decided or pending, with its sample of
// item URLs: the single-row read the submission notification needs (stage
// 1 of summary/59, eval-2 item 1 — both notifiers listed the whole queue to
// find one row). ErrAddRequestNotFound for no such request.
func (s *PostgresStore) GetAddRequest(ctx context.Context, requestID int64) (AddRequest, error) {
	var a AddRequest
	err := scanAddRequest(s.pool.QueryRow(ctx, addRequestSelectSQL+` WHERE ar.request_id = $1`, requestID), &a)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, fmt.Errorf("add request %d: %w", requestID, ErrAddRequestNotFound)
	}
	if err != nil {
		return a, fmt.Errorf("load add request %d: %w", requestID, err)
	}
	if a.SampleURLs, err = s.addRequestSample(ctx, requestID); err != nil {
		return a, err
	}
	return a, nil
}

// ListPendingAddRequests returns the admin approval queue,
// oldest-first, each with a small sample of item URLs.
func (s *PostgresStore) ListPendingAddRequests(ctx context.Context) ([]AddRequest, error) {
	rows, err := s.pool.Query(ctx, addRequestSelectSQL+`
		WHERE ar.status = 'pending'
		ORDER BY ar.request_id`)
	if err != nil {
		return nil, fmt.Errorf("list pending add requests: %w", err)
	}
	defer rows.Close()
	var out []AddRequest
	for rows.Next() {
		var a AddRequest
		if err := scanAddRequest(rows, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		// A failed sample read is the listing's failure (it was skipped
		// silently: the admin saw a request with no URLs and nothing said
		// why; stage 1 of summary/59).
		if out[i].SampleURLs, err = s.addRequestSample(ctx, out[i].RequestID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// DecideAddRequest flips a pending request to approved/rejected and
// returns the request (joined to requester + group for notification).
// changed=true means this call made the decision (or completed an
// approval, below): callers process, scan and notify. changed=false means
// the request was already decided (double click): callers notify nobody,
// but re-approving an approved repos request resumes its processing pass. For kind='org' approvals the org
// registration happens HERE (INSERT into user_org_requests): presence
// in that table means "approved to scan", which is what keeps the
// scheduler's org tickers gate-free by construction.
func (s *PostgresStore) DecideAddRequest(ctx context.Context, requestID int64, adminID int, approve bool, ghAPIBase, glWebBase string) (AddRequest, bool, error) {
	var req AddRequest
	err := scanAddRequest(s.pool.QueryRow(ctx, addRequestSelectSQL+` WHERE ar.request_id = $1`, requestID), &req)
	if errors.Is(err, pgx.ErrNoRows) {
		return req, false, fmt.Errorf("add request %d: %w", requestID, ErrAddRequestNotFound)
	}
	if err != nil {
		return req, false, fmt.Errorf("load add request: %w", err)
	}

	newStatus := "rejected"
	if approve {
		newStatus = "approved"
	}
	// The flip and an org approval's registration are one transaction: a
	// failed registration approves nothing, so the admin's retry approves
	// and notifies normally (round-12 review: registering after the flip
	// could leave an approved org untracked, its requester never told).
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return req, false, fmt.Errorf("decide add request: %w", err)
	}
	defer tx.Rollback(ctx)
	// A rejected group takes no approval (v0.29.71 review round 2): the pass
	// would refuse its items, so approving would tell the requester
	// "approved" and leave the request approved and unprocessed for good.
	// Checked INSIDE the flip's transaction with the group row share-locked
	// (review round 3): a RejectGroup committing between a separate check
	// and the flip approved anyway; now it waits for the flip to commit.
	if approve {
		var groupStatus string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(status, '') FROM aveloxis_ops.user_groups WHERE group_id = $1 FOR SHARE`,
			req.GroupID).Scan(&groupStatus); err != nil {
			return req, false, fmt.Errorf("decide add request: read group %d: %w", req.GroupID, err)
		}
		if groupStatus == "rejected" {
			return req, false, fmt.Errorf("add request %d: %w", requestID, ErrGroupRejected)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE aveloxis_ops.collection_add_requests
		SET status = $2, decided_by = $3, decided_at = NOW()
		WHERE request_id = $1 AND status = 'pending'`,
		requestID, newStatus, adminID)
	if err != nil {
		return req, false, fmt.Errorf("decide add request: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Already decided. An approved org request can still lack its
		// registration if a release before v0.29.39 lost it (this function
		// registered after the flip until v0.29.38, AddOrgToGroup's
		// auto-approve until v0.29.39); re-approving registers it, and that
		// completes the approval, so it reports changed=true and the caller
		// notifies the requester then.
		if approve && req.Status == "approved" && req.Kind == "org" {
			inserted, err := registerApprovedOrg(ctx, tx, req, ghAPIBase, glWebBase)
			if err != nil {
				return req, false, err
			}
			if err := tx.Commit(ctx); err != nil {
				return req, false, fmt.Errorf("register approved org: %w", err)
			}
			return req, inserted, nil
		}
		return req, false, nil
	}
	if approve && req.Kind == "org" {
		if _, err := registerApprovedOrg(ctx, tx, req, ghAPIBase, glWebBase); err != nil {
			return req, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return req, false, fmt.Errorf("decide add request: %w", err)
	}
	req.Status = newStatus
	return req, true, nil
}

// ErrOrgOffGitHubHost refuses a GitHub org URL whose host is not this
// deployment's GitHub host (github.base_url's web host). Every consumer
// enumerates the org NAME on the configured base, so a same-named org on
// another host is a different org; the store refuses the registration in
// every writer — the web and portal adds, the CLI loaders and an admin's
// approval — rather than leaving a row the refreshes must skip forever
// (v0.29.57, round 2 on the Copilot 5260961848 fixes).
var ErrOrgOffGitHubHost = errors.New("organization is not on this deployment's GitHub host")

// ErrInvalidOrgURL is an organization URL no parser accepts (a scheme
// other than http(s), no host, no name): refused at registration, because
// a row the scan could never enumerate would sit unstamped forever
// (0.29.106 review round 1: `ftp://github.com/chaoss` registered with an
// empty org_name once ParseOrgURL refused the scheme).
var ErrInvalidOrgURL = errors.New("not an organization URL")

// ErrOrgOffGitLabInstance is ErrOrgOffGitHubHost's GitLab twin (round-3
// review of 0.29.100): a GitLab group not on the configured instance
// (gitlab.base_url) — or any GitLab group when none is configured — can
// never be enumerated (keys are sent to that instance alone), so it is
// refused at every writer like a GitHub org off the host, instead of
// registering and then being skipped by every scan with a WARN forever.
var ErrOrgOffGitLabInstance = errors.New("organization is not on this deployment's GitLab instance")

// OrgApprovalRefusalAdvice is the 409 body both admin surfaces (web page,
// portal) send when an org approval is refused — ONE spelling (SR-17). The
// host parenthetical belongs to the host refusal only. A credentialed
// request can be rejected while pending; one already approved (the
// half-state re-approve) cannot be rejected, and nothing enumerates it, so
// the advice says exactly that — there is no delete path to point at.
func OrgApprovalRefusalAdvice(err error, ghAPIBase, glWebBase string) string {
	if errors.Is(err, ErrURLTooLong) {
		// PR #218 fix review r1: its own text ("longer than 1342 bytes") is
		// the add limit; this refusal is the registration index's bound.
		// PR #218 fix review r2 F6: a request approved before v0.29.39
		// that lost its registration reaches this on re-approve, and a
		// reject of an approved row changes nothing — say so, as the
		// credentials twin below does.
		return fmt.Sprintf("the organization URL is longer than %d bytes, the most the registration index holds; reject the request — it can never be registered, and one that is already approved cannot be registered and nothing enumerates it", maxIndexedURLBytes)
	}
	if errors.Is(err, platform.ErrURLUserinfo) {
		return "the organization URL carries credentials; reject the request — one that is already approved cannot be registered and nothing enumerates it"
	}
	if errors.Is(err, ErrOrgOffGitLabInstance) {
		return err.Error() + " (" + GitLabInstanceLabel(glWebBase) + "); reject the request instead — one that is already approved cannot be registered and nothing enumerates it"
	}
	// PR #218 fix review r3 F3: an org approved before v0.29.57's host gate
	// reaches this on re-approve too (registerApprovedOrg → orgRegistrable).
	return err.Error() + " (" + platform.GitHubWebHost(ghAPIBase) + "); reject the request instead — one that is already approved cannot be registered and nothing enumerates it"
}

// GitLabInstanceLabel names the configured GitLab instance in a refusal:
// its web base, or the fact that none is configured.
func GitLabInstanceLabel(glWebBase string) string {
	if glWebBase == "" {
		return "no GitLab instance is configured: gitlab.base_url"
	}
	return glWebBase
}

// orgRegistrable is the registration gate, the registration-time twin of
// OrgScanEligible (SR-18: what the scan would never enumerate is refused
// where it is written): a "github"-labelled org must be on the deployment's
// GitHub host; a "gitlab"-labelled group must be on the configured
// instance (glWebBase; none configured → refused). A URL that does not
// PARSE is refused as ErrInvalidOrgURL (0.29.106): no later check names a
// wrong scheme, and the registration would never be enumerated. The
// service classifies it as the caller's input with the same words as the
// database's value rejection, so a NUL byte reads as before.
func orgRegistrable(platformName, orgURL, ghAPIBase, glWebBase string) error {
	host, _, err := platform.ParseOrgURL(orgURL)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOrgURL, err)
	}
	switch platformName {
	case "github":
		if !platform.IsGitHubHost(host, ghAPIBase) {
			return ErrOrgOffGitHubHost
		}
	case "gitlab":
		if glWebBase == "" || !platform.OnGitLabInstance(orgURL, glWebBase) {
			return ErrOrgOffGitLabInstance
		}
	}
	return nil
}

// OrgScanEligible reports whether a registered org row is one the periodic
// refresh will ENUMERATE: a "github" row on the deployment's GitHub host,
// or a "gitlab" row on the configured instance (glWebBase; the probe
// passes "" while no GitLab key exists). The never-scanned probe counts
// only these: a row a gate skips is never stamped and would otherwise
// re-fire the demand scan on every poll tick (v0.29.57 round 2; the
// rejected-group exclusion has the same reason). Since stage 1e of
// summary/59 the pass enumerates GitLab groups by full path on that
// instance.
func OrgScanEligible(platformName, orgURL, ghAPIBase, glWebBase string) bool {
	switch platformName {
	case "github":
		return platform.OrgOnGitHubHost(orgURL, ghAPIBase)
	case "gitlab":
		// A GitLab group is enumerable only on the configured instance
		// (gitlab.base_url): its keys are sent to that host alone. The
		// probe passes "" while no GitLab key exists, so an unenumerable
		// row never re-fires the demand scan (stage 1e of summary/59, C4).
		return glWebBase != "" && platform.OnGitLabInstance(orgURL, glWebBase)
	}
	return false
}

// registerApprovedOrg records an approved org request in user_org_requests,
// which is what lets the scheduler scan it, and reports whether it added the
// row (false: it was already registered). Idempotent. It takes a
// transaction, never the pool: a caller that writes the approval justifying
// the registration (DecideAddRequest's flip, AddOrgToGroup's auto-approve
// audit request) must register in that same transaction, and a pool argument
// used to compile there (round-14 review). The type does not stop a caller
// from opening a second transaction, so keep the registration in the
// approval's transaction; TestDecideAddRequestOrgRegistrationFailures and
// TestAddOrgToGroupAutoApproveIsAtomic inject failures into both writes. The
// admin add and the half-state re-approve pass a transaction that holds only
// the registration.
func registerApprovedOrg(ctx context.Context, tx pgx.Tx, req AddRequest, ghAPIBase, glWebBase string) (bool, error) {
	// A pending request written before the store refused credentialed URLs
	// reaches this writer without AddOrgToGroup; refused here, so the
	// approval's transaction never commits (Copilot review 5267193512). The
	// admin is told to reject the request.
	if err := platform.RefuseURLUserinfo(req.OrgURL); err != nil {
		return false, fmt.Errorf("register approved org: %w", err)
	}
	// A request created before v0.29.54's limit can carry a URL the
	// registration's unique index refuses (SQLSTATE 54000, forever, at every
	// approval): refuse it here so the admin is told to reject the request
	// (worklist follow-up 14). The bound is the index's own — the stored
	// form — so the 1,343–2,684-byte requests that always approved still do.
	if len(req.OrgURL) > maxIndexedURLBytes {
		return false, fmt.Errorf("register approved org: %w", ErrURLTooLong)
	}
	orgName, platformName := parseOrgURLMeta(req.OrgURL, glWebBase)
	if err := orgRegistrable(platformName, req.OrgURL, ghAPIBase, glWebBase); err != nil {
		return false, err
	}
	// The arbiter below is the exact key, and rows written before v0.29.68
	// keep their case (SR-1: a LOWER() unique needs a dedup migration
	// first), so the "already registered in this group" question is asked
	// here case-insensitively (batch 5 review round 1: a legacy mixed-case
	// row plus an identical re-paste made a second, lowercase row where
	// the exact-key conflict used to insert nothing).
	var registered bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM aveloxis_ops.user_org_requests
			WHERE group_id = $1 AND LOWER(org_url) = LOWER($2))`,
		req.GroupID, req.OrgURL).Scan(&registered); err != nil {
		return false, fmt.Errorf("register approved org: %w", err)
	}
	if registered {
		return false, nil
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_org_requests
			(user_id, group_id, org_url, org_name, platform)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (group_id, org_url) DO NOTHING`,
		req.UserID, req.GroupID, req.OrgURL, orgName, platformName)
	if err != nil {
		return false, fmt.Errorf("register approved org: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MaxAddURLBytes is the longest URL, in bytes, an add accepts. Every index an
// add writes a URL into is a btree, whose rows hold at most 2704 bytes on 8 KB
// pages; a row carries up to 20 bytes before the URL (the tuple header, a
// bigint key, the text length), and the rest is halved because
// lower(repo_git) lengthens some characters: U+023A by half in en_US.UTF-8,
// and ASCII 'I' to the 2-byte dotless i under an ICU Turkish collation. Longer
// URLs reached the database, which refused them with SQLSTATE 54000 — also the
// code for a server-wide stop, so the API could not tell the caller's mistake
// from an outage (round-27 review).
const MaxAddURLBytes = maxIndexedURLBytes / 2

// maxIndexedURLBytes is the bytes a btree index row can hold for a URL after
// the tuple header, a bigint key and the text length: the bound on a STORED
// value. MaxAddURLBytes halves it for the entry checks because the stored
// or indexed form may be lower() of the input; a value already in that form
// (a stored org URL — in CanonicalOrgURL's form when pasted since v0.29.68,
// in the registrant's case when pended before) is bounded by this.
const maxIndexedURLBytes = 2704 - 20

// ErrURLTooLong means a URL in an add is longer than MaxAddURLBytes.
var ErrURLTooLong = fmt.Errorf("a URL is longer than %d bytes", MaxAddURLBytes)

// ErrAddRequestNotFound is DecideAddRequest's answer for a request id that
// does not exist: the caller's input, a 404, not a store failure (PR #218
// fix review r1 — it read as a logged generic 500 after review C6).
var ErrAddRequestNotFound = errors.New("no such add request")

// ErrAddItemsFailed means some repositories of an auto-approved add could not
// be added; the rest were.
var ErrAddItemsFailed = errors.New("repositories could not be added")

// ErrAddRequestInProgress is ProcessApprovedAddRequest's answer when this
// process is already processing the request: the running pass finishes the
// batch, so the caller has nothing to do.
var ErrAddRequestInProgress = errors.New("add request is already being processed")

// ProcessApprovedAddRequest walks the request's unprocessed items and
// runs the shared add machinery for each. Idempotent + resumable:
// items with repo_id already stamped are skipped, so an interrupted
// pass picks up where it left off. An item the database rejects for its own
// data (addItemFailurePermanent) is stamped repo_id = -1
// (processed-with-error) so it can't wedge the request forever, and the pass
// continues; any other failure stops the pass with the item unprocessed, so
// re-approving retries it (Copilot review of PR #207: every failure used to
// be stamped -1, and a transient database error dropped the repository).
//
// One pass per request at a time in this process: a second call while one
// runs (an admin's second approve click) returns ErrAddRequestInProgress
// instead of walking the batch again (Copilot review of PR #207). The pass
// holds no database connection or transaction across items: v0.29.46 held
// one per pass while the add machinery took a second connection, and as many
// passes as the pool has connections deadlocked the pool (round-21 review).
// Passes in two processes (web and api) can still overlap; each step is
// idempotent and a stamp only fills an unstamped item.
// It returns the items processed and the items stamped processed-with-error
// (-1: a permanent failure of the item's own values), so the caller's log
// line reports both (worklist follow-up 9; v0.29.50 counted the failed
// items on the auto-approve path only).
func (s *PostgresStore) ProcessApprovedAddRequest(ctx context.Context, requestID int64, glWebBase string) (processed, failed int, err error) {
	return s.processAddRequest(ctx, requestID, false, glWebBase)
}

// groupRejected reports whether an administrator has rejected the group.
func (s *PostgresStore) groupRejected(ctx context.Context, groupID int64) (bool, error) {
	var rejected bool
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(status, '') = 'rejected' FROM aveloxis_ops.user_groups WHERE group_id = $1`, groupID).Scan(&rejected); err != nil {
		return false, fmt.Errorf("read group %d status: %w", groupID, err)
	}
	return rejected, nil
}

// ApprovedAddRequestsToRetry lists the approved 'repos' requests that still
// have an unprocessed item and were decided more than olderThan ago (O17,
// v0.29.71). Such a request's pass stopped early — a failed stamp or read,
// a process stopped mid-pass — and nothing re-ran it: the admin page lists
// pending requests only, so no administrator sees an approved request to
// re-approve (re-approving would resume it). A rejected group's requests
// are left out. serve retries them through ProcessApprovedAddRequest. The age
// keeps a pass still running in web or api from being raced (overlapping
// passes are safe, only wasted).
func (s *PostgresStore) ApprovedAddRequestsToRetry(ctx context.Context, olderThan time.Duration) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.request_id FROM aveloxis_ops.collection_add_requests r
		JOIN aveloxis_ops.user_groups g ON g.group_id = r.group_id
		WHERE r.status = 'approved' AND r.kind = 'repos'
		  AND g.status IS DISTINCT FROM 'rejected'
		  AND COALESCE(r.decided_at, r.created_at) < NOW() - $1::interval
		  AND EXISTS (SELECT 1 FROM aveloxis_ops.collection_add_request_items i
		              WHERE i.request_id = r.request_id AND i.repo_id IS NULL)
		ORDER BY r.request_id`, olderThan.String())
	if err != nil {
		return nil, fmt.Errorf("list approved add requests to retry: %w", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("list approved add requests to retry: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list approved add requests to retry: %w", err)
	}
	return ids, nil
}

// processAddRequest is the processing pass. With everyFailureFinal, used for
// an auto-approved add that nobody re-approves, every failed item is stamped
// processed-with-error and the pass goes on; failed counts those items.
func (s *PostgresStore) processAddRequest(ctx context.Context, requestID int64, everyFailureFinal bool, glWebBase string) (int, int, error) {
	if !s.startAddRequestPass(requestID) {
		return 0, 0, ErrAddRequestInProgress
	}
	defer s.finishAddRequestPass(requestID)

	var groupID int64
	var status, groupStatus string
	if err := s.pool.QueryRow(ctx, `
		SELECT r.group_id, r.status, COALESCE(g.status, '')
		FROM aveloxis_ops.collection_add_requests r
		LEFT JOIN aveloxis_ops.user_groups g ON g.group_id = r.group_id
		WHERE r.request_id = $1`,
		requestID).Scan(&groupID, &status, &groupStatus); err != nil {
		return 0, 0, fmt.Errorf("load add request: %w", err)
	}
	if status != "approved" {
		return 0, 0, fmt.Errorf("request %d is %s, not approved", requestID, status)
	}
	// A group an administrator rejected after the approval takes no more
	// collection (v0.29.71 review round 1): the pass refuses, whoever calls
	// it — serve's hourly retry or an admin's re-approve (SR-18).
	if groupStatus == "rejected" {
		return 0, 0, fmt.Errorf("request %d: %w", requestID, ErrGroupRejected)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT item_id, repo_url FROM aveloxis_ops.collection_add_request_items
		WHERE request_id = $1 AND repo_id IS NULL ORDER BY item_id`, requestID)
	if err != nil {
		return 0, 0, fmt.Errorf("read add-request items: %w", err)
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
			return 0, 0, fmt.Errorf("read add-request items: %w", err)
		}
		items = append(items, it)
	}
	rows.Close()
	// A read that fails partway must not look like a shorter batch.
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("read add-request items: %w", err)
	}

	processed, failed := 0, 0
	for _, it := range items {
		// A group rejected while the pass runs takes no further item
		// (v0.29.71 review round 2): re-read before each one. A failed read
		// is not "not rejected" (SR-5); it stops the pass like any error.
		rejected, err := s.groupRejected(ctx, groupID)
		if err != nil {
			return processed, failed, err
		}
		if rejected {
			return processed, failed, fmt.Errorf("request %d: %w", requestID, ErrGroupRejected)
		}
		repoID, err := s.ensureRepoCollectedInGroup(ctx, groupID, it.url, glWebBase)
		if errors.Is(err, context.Canceled) {
			// A stopped process, not a failure: leave the item unprocessed.
			return processed, failed, err
		}
		if err != nil && !everyFailureFinal && !addItemFailurePermanent(err) {
			// Leave this item and the rest unprocessed for a later pass. The
			// URL is redacted: an item that predates the store's refusal can
			// carry a credential, and this message reaches the server log.
			return processed, failed, fmt.Errorf("add-request item %q: %w", platform.RedactURLUserinfo(it.url), err)
		}
		if err != nil {
			s.logger.Warn("add-request item failed — marking processed-with-error",
				"request_id", requestID, "url", platform.RedactURLUserinfo(it.url), "error", err)
			repoID = -1
		}
		if _, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_ops.collection_add_request_items
			SET repo_id = $2 WHERE item_id = $1 AND repo_id IS NULL`, it.id, repoID); err != nil {
			return processed, failed, err
		}
		if repoID > 0 {
			processed++
		} else {
			failed++
		}
	}
	return processed, failed, nil
}

// addItemFailurePermanent reports whether an add-machinery error is a property
// of the item's own values, which retrying cannot fix: a data exception
// (SQLSTATE class 22), a not-null violation (23502) or a check violation
// (23514). Anything else may succeed later — a lost connection, a timeout, a
// serialization failure, a cancelled context, and the constraint violations a
// concurrent delete or dedup can cause (a foreign key, 23503; a unique index,
// 23505; an exclusion, 23P01).
func addItemFailurePermanent(err error) bool {
	// A URL the store refuses for its own content (credentials, over-long)
	// can never succeed on retry: a legacy pending item carrying one would
	// otherwise block its request's approval forever (round 3).
	if errors.Is(err, platform.ErrURLUserinfo) || errors.Is(err, ErrURLTooLong) {
		return true
	}
	// Nor can a URL that is not a repository's (an organization page, a
	// scheme git cannot clone) become one.
	if errors.Is(err, platform.ErrInvalidRepoURL) {
		return true
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "22") || pgErr.Code == "23502" || pgErr.Code == "23514"
}

// IsRejectedValue reports whether the database refused a value it was given
// rather than failing: a data exception (SQLSTATE class 22, such as a NUL
// byte). An add's only values are the caller's URLs, so the API answers these
// with a 400 (round-25 review). A URL too long for an index never reaches the
// database (ErrURLTooLong), so 54000 is not matched: Postgres also uses it when
// it stops assigning transaction IDs to avoid wraparound (round 26), and one of
// its two index-size errors names no index (round 27).
func IsRejectedValue(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "22")
}

// isUniqueViolation reports SQLSTATE 23505: the unique index is the
// arbiter of "that name is taken" (a precheck would race a concurrent
// insert). Unlike class 54 (memory: validate input, do not classify
// SQLSTATEs), 23505 has one meaning.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// startAddRequestPass records that this process is processing requestID, and
// reports false when a pass for it is already running here.
func (s *PostgresStore) startAddRequestPass(requestID int64) bool {
	s.addPassMu.Lock()
	defer s.addPassMu.Unlock()
	if _, running := s.addPasses[requestID]; running {
		return false
	}
	if s.addPasses == nil {
		s.addPasses = map[int64]struct{}{}
	}
	s.addPasses[requestID] = struct{}{}
	return true
}

func (s *PostgresStore) finishAddRequestPass(requestID int64) {
	s.addPassMu.Lock()
	defer s.addPassMu.Unlock()
	delete(s.addPasses, requestID)
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
// through admin.Service.GroupAccess; an administrator's session may read any group).
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
	if err := rows.Err(); err != nil {
		*errs = append(*errs, fmt.Errorf("v0.27.20 legacy pending groups: read: %w", err))
		return
	}
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
// URL through the one classifier (ParseOrgRef with the configured
// instance's hint), keeping the historical host-contains-"gitlab"
// heuristic and the permissive empty-name fallback only for a URL the
// classifier cannot place (shared by AddOrgToGroup and the org-approval
// path so both register identically).
func parseOrgURLMeta(orgURL, glWebBase string) (orgName, platformName string) {
	// The one classifier (stage 1e of summary/59): a GitLab group is named
	// by its FULL path (N14), GitHub by its account. glWebBase is the
	// configured instance's web base (the caller's gitlab.base_url): a
	// group there is GitLab whatever its host name says (round-2 review of
	// 0.29.100: code.example.org was labelled github and refused off-host).
	// Without one, a self-hosted instance whose host does not say "gitlab"
	// takes the fallback below, as before.
	if ref, err := platform.ParseOrgRef(orgURL, platform.GitLabWebHints(glWebBase)); err == nil {
		platformName = "github"
		if ref.Platform == model.PlatformGitLab {
			platformName = "gitlab"
		}
		return ref.Path, platformName
	}
	// Not a forge this deployment classifies, or unparseable: the label
	// and the first-segment name as they always were (DBS-11: observed,
	// never rewritten); the scan's gates decide what is enumerated.
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
