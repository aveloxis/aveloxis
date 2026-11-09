// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// The web GUI's adapter over the admin service (stage 1 of summary/59):
// a session becomes a Session actor, the request's Host and TLS state a
// LinkOrigin, and every refusal the service returns is answered from ONE
// table — a status and text on the admin pages (refusePage), a query flag
// the group page renders as its notice (addErrorFlag, orgErrorFlag).

// sessionActor is the service's view of a signed-in browser session.
func sessionActor(sess *Session) admin.Actor {
	// SignedInAt: the service's re-authentication rule for a sensitive
	// change to the actor's own account reads it (ASVS V7.5.2).
	return admin.Actor{UserID: sess.UserID, Kind: admin.Session, SignedInAt: sess.CreatedAt}
}

// linkOrigin is what this request contributes to a mailed confirmation
// link: its Host header and whether it arrived over TLS. The service uses
// the Host only under its loopback-and-dev-mode rule.
func linkOrigin(r *http.Request) admin.LinkOrigin {
	return admin.LinkOrigin{Host: r.Host, TLS: r.TLS != nil}
}

// newAdminService builds this process's Service: web.dev_mode and
// web.spa_url for the confirmation link, the GitHub base for org
// registrations, the auto-approve limit, and the mailer when one is
// attached. No hooks: the web process caches nothing the service changes
// — a session is a store row since 0.29.102, and its privilege is read
// from the users row on every request (LookupWebSession).
func (s *Server) newAdminService(m *mailer.Mailer) *admin.Service {
	opts := admin.Options{
		GitHubAPIBase:       s.ghAPIBase,
		GitLabAPIBase:       s.gitlabAPIBase,
		AutoApproveAddLimit: s.cfg.AutoApproveAddLimitValue(),
		DevMode:             s.cfg.DevMode,
		SPAURL:              s.cfg.SPAURL,
		SessionIdle:         s.cfg.SessionIdle(),
	}
	if m != nil { // a nil *Mailer must not become a non-nil interface
		opts.Mailer = m
	}
	logger := s.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return admin.New(s.store, opts, nil, logger)
}

// notMigratedMessage is the NotMigrated page: what ends it, with no schema
// object named (the code review of the fourteenth-review fix: the store's
// text, meant for the operator's console, named tables and columns).
var notMigratedMessage = "Not available until the database is migrated to this release: run " + db.DeployStepsAdvice + ", then retry. Nothing was changed."

// refusePage answers a service refusal on an admin page or form: true
// when err was one and the response is written; false when err is a
// failure the caller answers with serverError. A group that is not the
// caller's and one that does not exist answer alike (N11): 404.
func (s *Server) refusePage(w http.ResponseWriter, r *http.Request, err error) bool {
	ref, ok := admin.Classify(err)
	if !ok {
		return false
	}
	detail := func(fallback string) string {
		if ref.Detail != "" {
			return ref.Detail
		}
		return fallback
	}
	switch ref.Kind {
	case admin.Forbidden:
		http.Error(w, "Administrator access required.", http.StatusForbidden)
	case admin.NotFound, admin.NotOwned:
		http.NotFound(w, r)
	case admin.RequestNotFound:
		http.Error(w, "no such add request", http.StatusNotFound)
	case admin.UserNotFound:
		http.Error(w, detail("no user with that id"), http.StatusNotFound)
	case admin.GroupRejected:
		// v0.29.71 review round 2: a rejected group takes no approval.
		http.Error(w, "Cannot approve: the group has been rejected", http.StatusConflict)
	case admin.OrgUnregistrable:
		// A pending org nothing could ever enumerate (v0.29.57 round 3;
		// review 5267193512: the message names no URL).
		http.Error(w, "Cannot approve: "+ref.Detail, http.StatusConflict)
	case admin.LastAdmin, admin.Superseded, admin.Conflict:
		http.Error(w, detail("refused"), http.StatusConflict)
	case admin.SelfDemotion:
		http.Error(w, "Refusing self-demotion — use another admin account.", http.StatusBadRequest)
	case admin.ReauthenticationRequired:
		// A sensitive change to the administrator's own account by an older
		// sign-in (Copilot's seventh review of PR #246, 0.29.129).
		http.Error(w, "Sign in again first: "+detail("this change needs a recent sign-in")+".", http.StatusForbidden)
	case admin.InvalidInput, admin.TooMany, admin.OffHost:
		http.Error(w, detail("invalid request"), http.StatusBadRequest)
	case admin.Partial:
		http.Error(w, detail("some repositories could not be added")+"; try adding them again", http.StatusInternalServerError)
	case admin.Capacity:
		http.Error(w, ref.Exceeded.Message(), http.StatusTooManyRequests)
	case admin.NotMigrated:
		// The web process does not migrate (Copilot PR #252, fourteenth
		// review): a schema behind the binary is the operator's to fix.
		http.Error(w, notMigratedMessage, http.StatusServiceUnavailable)
	default:
		return false
	}
	return true
}

// addErrorFlag names the notice the group page shows for a failed
// repository add: "rejected" for a group an administrator rejected (trying
// again cannot work), "capacity"/"capacity_links" for the quota that
// refused (closing review r3 F1: the daily-additions refusal read as the
// allocation's), "invalid" for the caller's own input — a line that is
// not a repository URL, over-long, carrying credentials, or refused by the
// database as a value (follow-up 12: it said "try adding them again") —
// and "1", the retryable failure, for everything else.
func addErrorFlag(err error) string {
	ref, ok := admin.Classify(err)
	if !ok {
		return "1"
	}
	switch ref.Kind {
	case admin.GroupRejected:
		return "rejected"
	case admin.Capacity:
		if ref.Exceeded.Quota == capacity.QuotaRepoLinksPerDay {
			return "capacity_links"
		}
		return "capacity"
	case admin.InvalidInput, admin.TooMany:
		return "invalid"
	}
	return "1"
}

// orgErrorFlag names the group page's notice for a failed organization
// add: "host" (not this deployment's GitHub host), "gitlab_host" (not the
// configured GitLab instance; round-4 review of 0.29.100 — the page said
// "GitHub host" for it), "invalid" (the URL), "rejected" (the group), "1"
// otherwise.
func orgErrorFlag(err error) string {
	ref, ok := admin.Classify(err)
	if !ok {
		return "1"
	}
	switch ref.Kind {
	case admin.OffHost:
		if errors.Is(err, db.ErrOrgOffGitLabInstance) {
			return "gitlab_host"
		}
		return "host"
	case admin.InvalidInput:
		return "invalid"
	case admin.GroupRejected:
		return "rejected"
	}
	return "1"
}
