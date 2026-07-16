// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package web — admin.go provides the v0.19.0 admin pages:
//   - /admin/groups/pending  list + approve/reject of pending submissions
//   - /admin/users           list + toggle admin role
//
// All routes here are gated by Server.requireAdmin (defined in server.go),
// which checks Session.IsAdmin. Non-admin authenticated users get a 403;
// unauthenticated users get redirected to /login.

package web

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/aveloxis/aveloxis/internal/safego"
)

// notifyAddRequestSubmitted emails the operator that a new add-request
// awaits review (v0.27.20). Best-effort in a goroutine — the user's
// redirect never waits on SMTP. No-op without a mailer or operator
// email.
func (s *Server) notifyAddRequestSubmitted(requestID int64) {
	if s.mailer == nil || s.mailer.OperatorEmail() == "" || requestID == 0 {
		return
	}
	safego.Go(s.logger, "add-request-submitted-email", func() {
		ctx := context.Background()
		pending, err := s.store.ListPendingAddRequests(ctx)
		if err != nil {
			s.logger.Warn("add-request email: list failed", "error", err)
			return
		}
		for _, req := range pending {
			if req.RequestID != requestID {
				continue
			}
			sample := req.SampleURLs
			if req.Kind == "org" {
				sample = []string{req.OrgURL}
			}
			if err := s.mailer.SendAddRequestSubmitted(s.mailer.OperatorEmail(),
				req.UserLogin, req.GroupName, req.Kind, req.ItemCount, sample, req.RequestID); err != nil {
				s.logger.Warn("failed to send add-request email", "request_id", requestID, "error", err)
			}
			return
		}
	})
}

// decideAddRequest is the shared web-side decision flow: flip the
// request, notify the requester, and (on approval) run the item
// processing / org scan in the background so a 50K-item approval
// doesn't block the admin's redirect. Idempotent — a double click
// finds the request already decided and does nothing.
func (s *Server) decideAddRequest(ctx context.Context, requestID int64, adminID int, approve bool) error {
	req, changed, err := s.store.DecideAddRequest(ctx, requestID, adminID, approve)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if approve {
		if req.Kind == "org" {
			// Registration happened in DecideAddRequest; scan now,
			// exactly like an admin's own org add.
			safego.Go(s.logger, "approved-org-scan", func() {
				s.scanOrgRepos(context.Background(), req.GroupID, req.OrgURL)
			})
		} else {
			safego.Go(s.logger, "approved-add-request", func() {
				n, err := s.store.ProcessApprovedAddRequest(context.Background(), req.RequestID)
				if err != nil {
					s.logger.Warn("processing approved add-request failed — re-approving resumes it",
						"request_id", req.RequestID, "processed", n, "error", err)
					return
				}
				s.logger.Info("approved add-request processed", "request_id", req.RequestID, "repos", n)
			})
		}
	}
	if s.mailer != nil && req.UserEmail != "" {
		req := req
		safego.Go(s.logger, "add-request-decided-email", func() {
			if err := s.mailer.SendAddRequestDecided(req.UserEmail, req.UserLogin,
				req.GroupName, req.Kind, approve, req.ItemCount); err != nil {
				s.logger.Warn("failed to send add-request decision email",
					"request_id", req.RequestID, "error", err)
			}
		})
	}
	return nil
}

func (s *Server) handleApproveAddRequest(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	requestID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid request id", http.StatusBadRequest)
		return
	}
	if err := s.decideAddRequest(r.Context(), requestID, sess.UserID, true); err != nil {
		s.logger.Warn("failed to approve add-request", "request_id", requestID, "error", err)
		http.Error(w, "Failed to approve request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/groups/pending", http.StatusFound)
}

func (s *Server) handleRejectAddRequest(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	requestID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid request id", http.StatusBadRequest)
		return
	}
	if err := s.decideAddRequest(r.Context(), requestID, sess.UserID, false); err != nil {
		s.logger.Warn("failed to reject add-request", "request_id", requestID, "error", err)
		http.Error(w, "Failed to reject request", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/groups/pending", http.StatusFound)
}

func (s *Server) handleAdminPendingGroups(w http.ResponseWriter, r *http.Request) {
	pending, err := s.store.ListPendingGroups(r.Context())
	if err != nil {
		s.logger.Warn("failed to list pending groups", "error", err)
		http.Error(w, "Failed to list pending groups", http.StatusInternalServerError)
		return
	}
	// v0.27.20: the per-add approval queue shares this page. Legacy
	// pending groups empty out after the migration; the additions
	// table is the ongoing queue.
	requests, err := s.store.ListPendingAddRequests(r.Context())
	if err != nil {
		s.logger.Warn("failed to list pending add-requests", "error", err)
		http.Error(w, "Failed to list pending additions", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html><html><head><title>Approvals</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #f5f5f5; color: #333; }
  h1 { margin-bottom: 0.5rem; }
  h2 { margin-top: 2rem; }
  .sub { color: #666; margin-bottom: 1.5rem; }
  table { border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  th, td { padding: 0.6rem 0.8rem; text-align: left; border-bottom: 1px solid #eee; font-size: 0.9rem; }
  th { background: #f8f8f8; font-weight: 600; }
  .btn { padding: 0.3rem 0.8rem; border: 1px solid #ddd; border-radius: 4px; cursor: pointer; font-size: 0.85rem; margin-right: 0.4rem; }
  .approve { background: #059669; color: white; border-color: #047857; }
  .reject { background: #dc2626; color: white; border-color: #b91c1c; }
  .empty { color: #6b7280; font-style: italic; padding: 2rem; text-align: center; }
  .navlinks a { margin-right: 1rem; }
  .urls { color: #6b7280; font-size: 0.8rem; max-width: 28rem; word-break: break-all; }
</style></head><body>
<h1>Approvals</h1>
<div class="sub navlinks">
  <a href="/dashboard">Back to dashboard</a>
  <a href="/admin/users">Users</a>
</div>
<h2>Pending additions</h2>
`)

	if len(requests) == 0 {
		fmt.Fprint(w, `<div class="empty">No pending additions awaiting review.</div>`)
	} else {
		fmt.Fprint(w, `<table><tr><th>#</th><th>Requested by</th><th>Group</th><th>Kind</th><th>Items</th><th>Sample</th><th>Action</th></tr>`)
		for _, q := range requests {
			var sample string
			count := q.ItemCount
			if q.Kind == "org" {
				sample = template.HTMLEscapeString(q.OrgURL)
				count = 1
			} else {
				escaped := make([]string, 0, len(q.SampleURLs))
				for _, u := range q.SampleURLs {
					escaped = append(escaped, template.HTMLEscapeString(u))
				}
				sample = strings.Join(escaped, "<br>")
			}
			fmt.Fprintf(w, `<tr><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td class="urls">%s</td><td>
<form method="POST" action="/admin/add-requests/%d/approve" style="display:inline"><button class="btn approve" type="submit">Approve</button></form>
<form method="POST" action="/admin/add-requests/%d/reject" style="display:inline"><button class="btn reject" type="submit">Reject</button></form>
</td></tr>`,
				q.RequestID,
				template.HTMLEscapeString(q.UserLogin),
				template.HTMLEscapeString(q.GroupName),
				template.HTMLEscapeString(q.Kind),
				count, sample,
				q.RequestID, q.RequestID)
		}
		fmt.Fprint(w, `</table>`)
	}

	fmt.Fprint(w, `<h2>Pending groups (legacy)</h2>`)
	if len(pending) == 0 {
		fmt.Fprint(w, `<div class="empty">No pending groups awaiting review.</div></body></html>`)
		return
	}

	fmt.Fprint(w, `<table><tr><th>Group</th><th>Submitted by</th><th>Email</th><th>Repos</th><th>Org requests</th><th>Action</th></tr>`)
	for _, p := range pending {
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>
<form method="POST" action="/admin/groups/%d/approve" style="display:inline"><button class="btn approve" type="submit">Approve</button></form>
<form method="POST" action="/admin/groups/%d/reject" style="display:inline"><button class="btn reject" type="submit">Reject</button></form>
</td></tr>`,
			template.HTMLEscapeString(p.Name),
			template.HTMLEscapeString(p.UserLogin),
			template.HTMLEscapeString(p.UserEmail),
			p.RepoCount, p.OrgRequests,
			p.GroupID, p.GroupID)
	}
	fmt.Fprint(w, `</table></body></html>`)
}

func (s *Server) handleApproveGroup(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	groupID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}

	// Look up the requesting user + group name BEFORE flipping
	// status, so we can email them after the flip succeeds.
	var requesterEmail, requesterLogin, groupName string
	_ = s.store.Pool().QueryRow(r.Context(), `
		SELECT u.email, u.login_name, g.name
		FROM aveloxis_ops.user_groups g
		JOIN aveloxis_ops.users u ON u.user_id = g.user_id
		WHERE g.group_id = $1`,
		groupID).Scan(&requesterEmail, &requesterLogin, &groupName)

	if err := s.store.ApproveGroup(r.Context(), groupID, sess.UserID); err != nil {
		s.logger.Warn("failed to approve group", "group_id", groupID, "error", err)
		http.Error(w, "Failed to approve group: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify the requester. Best-effort — don't block the redirect on
	// email delivery.
	if s.mailer != nil && requesterEmail != "" {
		go func() {
			defer safego.Recover(s.logger, "group-approved-email")
			if err := s.mailer.SendGroupApproved(requesterEmail, requesterLogin, groupName, groupID); err != nil {
				s.logger.Warn("failed to send group-approved email",
					"group_id", groupID, "to", requesterEmail, "error", err)
			}
		}()
	}

	http.Redirect(w, r, "/admin/groups/pending", http.StatusFound)
}

func (s *Server) handleRejectGroup(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	groupID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid group id", http.StatusBadRequest)
		return
	}
	if err := s.store.RejectGroup(r.Context(), groupID, sess.UserID); err != nil {
		s.logger.Warn("failed to reject group", "group_id", groupID, "error", err)
		http.Error(w, "Failed to reject group", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/admin/groups/pending", http.StatusFound)
}

func (s *Server) handleAdminUsers(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	users, err := s.store.ListUsers(r.Context())
	if err != nil {
		s.logger.Warn("failed to list users", "error", err)
		http.Error(w, "Failed to list users", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html><html><head><title>Users</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #f5f5f5; color: #333; }
  h1 { margin-bottom: 0.5rem; }
  .sub { color: #666; margin-bottom: 1.5rem; }
  table { border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  th, td { padding: 0.6rem 0.8rem; text-align: left; border-bottom: 1px solid #eee; font-size: 0.9rem; }
  th { background: #f8f8f8; font-weight: 600; }
  .btn { padding: 0.3rem 0.8rem; border: 1px solid #ddd; border-radius: 4px; cursor: pointer; font-size: 0.85rem; }
  .promote { background: #059669; color: white; border-color: #047857; }
  .demote { background: #dc2626; color: white; border-color: #b91c1c; }
  .badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.75rem; background: #fef3c7; color: #78350f; }
  .badge.admin { background: #dbeafe; color: #1e40af; }
  .self { color: #6b7280; font-size: 0.8rem; }
  .navlinks a { margin-right: 1rem; }
</style></head><body>
<h1>Users</h1>
<div class="sub navlinks">
  <a href="/dashboard">Back to dashboard</a>
  <a href="/admin/groups/pending">Pending groups</a>
</div>
<table><tr><th>ID</th><th>Login</th><th>Email</th><th>Provider</th><th>Role</th><th>Action</th></tr>`)

	for _, u := range users {
		role := `<span class="badge">user</span>`
		if u.IsAdmin {
			role = `<span class="badge admin">admin</span>`
		}
		action := ""
		if u.UserID == sess.UserID {
			action = `<span class="self">(you)</span>`
		} else if u.IsAdmin {
			action = fmt.Sprintf(`<form method="POST" action="/admin/users/%d/admin" style="display:inline"><input type="hidden" name="admin" value="false"><button class="btn demote" type="submit">Demote</button></form>`, u.UserID)
		} else {
			action = fmt.Sprintf(`<form method="POST" action="/admin/users/%d/admin" style="display:inline"><input type="hidden" name="admin" value="true"><button class="btn promote" type="submit">Promote to admin</button></form>`, u.UserID)
		}
		fmt.Fprintf(w, `<tr><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
			u.UserID,
			template.HTMLEscapeString(u.Login),
			template.HTMLEscapeString(u.Email),
			template.HTMLEscapeString(u.Provider),
			role, action)
	}
	fmt.Fprint(w, `</table></body></html>`)
}

func (s *Server) handleSetUserAdmin(w http.ResponseWriter, r *http.Request) {
	userID, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	isAdmin := strings.EqualFold(strings.TrimSpace(r.FormValue("admin")), "true")
	if err := s.store.SetUserAdmin(r.Context(), userID, isAdmin); err != nil {
		s.logger.Warn("failed to toggle admin role", "user_id", userID, "error", err)
		http.Error(w, "Failed to update role: "+err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/admin/users", http.StatusFound)
}
