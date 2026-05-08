// Package web — admin.go provides the v0.19.0 admin pages:
//   - /admin/groups/pending  list + approve/reject of pending submissions
//   - /admin/users           list + toggle admin role
//
// All routes here are gated by Server.requireAdmin (defined in server.go),
// which checks Session.IsAdmin. Non-admin authenticated users get a 403;
// unauthenticated users get redirected to /login.

package web

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
)

func (s *Server) handleAdminPendingGroups(w http.ResponseWriter, r *http.Request) {
	pending, err := s.store.ListPendingGroups(r.Context())
	if err != nil {
		s.logger.Warn("failed to list pending groups", "error", err)
		http.Error(w, "Failed to list pending groups", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, `<!DOCTYPE html><html><head><title>Pending Groups</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 2rem; background: #f5f5f5; color: #333; }
  h1 { margin-bottom: 0.5rem; }
  .sub { color: #666; margin-bottom: 1.5rem; }
  table { border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  th, td { padding: 0.6rem 0.8rem; text-align: left; border-bottom: 1px solid #eee; font-size: 0.9rem; }
  th { background: #f8f8f8; font-weight: 600; }
  .btn { padding: 0.3rem 0.8rem; border: 1px solid #ddd; border-radius: 4px; cursor: pointer; font-size: 0.85rem; margin-right: 0.4rem; }
  .approve { background: #059669; color: white; border-color: #047857; }
  .reject { background: #dc2626; color: white; border-color: #b91c1c; }
  .empty { color: #6b7280; font-style: italic; padding: 2rem; text-align: center; }
  .navlinks a { margin-right: 1rem; }
</style></head><body>
<h1>Pending Groups</h1>
<div class="sub navlinks">
  <a href="/dashboard">Back to dashboard</a>
  <a href="/admin/users">Users</a>
</div>
`)

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
