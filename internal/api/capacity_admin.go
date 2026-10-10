// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.89 (summary/53): the endpoints behind aveloxis-gui's Capacity page
// (administrators only, POST-everywhere like the other admin mutations),
// the caller's own capacity on /me, and removing a repository from one of
// the caller's groups (how an account makes room under its allocation).

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// signupDaysShown is how many UTC days of new accounts the page charts: a
// month, the span an administrator compares a day against.
const signupDaysShown = 30

// GET /api/v1/admin/capacity — every quota as applied (value, mode, who
// decides it), the contact address, new accounts per day, the allowlist.
func (s *Server) handleAdminCapacity(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	ctx := r.Context()
	quotas, err := s.store.EffectiveCapacityQuotas(ctx)
	if err != nil {
		s.serverError(w, r, "handleAdminCapacity", err)
		return
	}
	list := make([]capacity.EffectiveQuota, 0, len(quotas))
	for _, name := range capacity.QuotaNames() {
		list = append(list, quotas[name])
	}
	contact, err := s.store.GetCapacityContact(ctx)
	if err != nil {
		s.serverError(w, r, "handleAdminCapacity", err)
		return
	}
	signups, err := s.store.SignupsPerDay(ctx, signupDaysShown)
	if err != nil {
		s.serverError(w, r, "handleAdminCapacity", err)
		return
	}
	allow, err := s.store.ListSignupAllowlist(ctx)
	if err != nil {
		s.serverError(w, r, "handleAdminCapacity", err)
		return
	}
	overrides, err := s.store.CapacityAccountsWithOverrides(ctx)
	if err != nil {
		s.serverError(w, r, "handleAdminCapacity", err)
		return
	}
	busiest, err := s.store.BusiestAccountsToday(ctx)
	if err != nil {
		s.serverError(w, r, "handleAdminCapacity", err)
		return
	}
	jsonResponse(w, map[string]any{
		"quotas": list, "contact_email": contact, "signups_per_day": signups, "signup_allowlist": allow,
		"overrides": overrides, "busiest_today": busiest,
	})
}

// GET /api/v1/admin/capacity/largest — the accounts with the most
// repositories (a full read of the group links: loaded on request).
func (s *Server) handleAdminCapacityLargest(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	out, err := s.store.LargestAccounts(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminCapacityLargest", err)
		return
	}
	jsonResponse(w, map[string]any{"accounts": out})
}

// POST /api/v1/admin/capacity/quotas/{name} {allowed, mode} — only a quota
// whose aveloxis.json word is WEB (409 otherwise, naming the file).
func (s *Server) handleAdminCapacityQuota(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	var body struct {
		Allowed *int64 `json:"allowed"`
		Mode    string `json:"mode"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {allowed, mode}")
		return
	}
	if _, known := capacity.Shipped[name]; !known {
		writeAuthError(w, http.StatusNotFound, "no quota named "+strconv.Quote(name))
		return
	}
	mode, err := capacity.ParseMode(body.Mode)
	if err != nil || body.Allowed == nil || !validRateLimit(*body.Allowed) {
		writeAuthError(w, http.StatusBadRequest, "allowed must be a positive number, at most "+strconv.Itoa(maxAPITokenRateLimit)+", and mode one of enforce, shadow, off")
		return
	}
	if src := s.store.CapacitySource(name); src != capacity.SourceWeb {
		writeAuthError(w, http.StatusConflict, name+" is set in aveloxis.json ("+string(src)+"); change it there")
		return
	}
	if err := s.store.SetCapacityQuota(r.Context(), name, int(*body.Allowed), mode, info.UserID); err != nil {
		s.serverError(w, r, "handleAdminCapacityQuota", err)
		return
	}
	s.logger.Info("capacity quota changed", "quota", name, "allowed", *body.Allowed, "mode", string(mode), "by", info.UserID)
	jsonResponse(w, map[string]any{"name": name, "allowed": *body.Allowed, "mode": mode})
}

// POST /api/v1/admin/capacity/contact {contact_email}
func (s *Server) handleAdminCapacityContact(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var body struct {
		ContactEmail string `json:"contact_email"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {contact_email}")
		return
	}
	err := s.store.SetCapacityContact(r.Context(), body.ContactEmail, info.UserID)
	if errors.Is(err, db.ErrInvalidCapacitySettings) {
		writeAuthError(w, http.StatusBadRequest, "contact_email must be an email address of at most "+strconv.Itoa(db.MaxContactEmailBytes)+" bytes, or empty")
		return
	}
	if err != nil {
		s.serverError(w, r, "handleAdminCapacityContact", err)
		return
	}
	s.logger.Info("capacity contact address changed", "by", info.UserID)
	jsonResponse(w, map[string]any{"saved": true})
}

// GET /api/v1/admin/capacity/accounts?login=… or ?user_id=… — one
// account's capacity; 409 with the candidates when a login names several
// accounts apart from letter case.
func (s *Server) handleAdminCapacityAccount(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	// ?user_id= reaches an account whose login has a case twin (round 5
	// R5-1); otherwise ?login=.
	var a db.CapacityAccount
	var err error
	if raw := r.URL.Query().Get("user_id"); raw != "" {
		id, perr := strconv.Atoi(raw)
		if perr != nil || id <= 0 || id > math.MaxInt32 { // users.user_id is int4 (round 6 R6-d)
			writeAuthError(w, http.StatusBadRequest, "user_id must be a positive integer")
			return
		}
		a, err = s.store.CapacityAccountByID(r.Context(), id)
	} else {
		a, err = s.store.CapacityAccountByLogin(r.Context(), r.URL.Query().Get("login"))
	}
	if errors.Is(err, db.ErrNoSuchUser) {
		writeAuthError(w, http.StatusNotFound, "no such account")
		return
	}
	var amb *db.AmbiguousLoginError
	if errors.As(err, &amb) {
		// The accounts the login matches, so the page can open one by id.
		setNoStoreHeaders(w.Header())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		cands := make([]map[string]any, 0, len(amb.Candidates))
		for _, c := range amb.Candidates {
			cands = append(cands, map[string]any{"user_id": c.UserID, "login": c.Login, "provider": c.Provider, "gitlab_host": c.GitLabHost})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":      "more than one account has this login apart from letter case; choose one by user id",
			"candidates": cands,
		})
		return
	}
	if err != nil {
		s.serverError(w, r, "handleAdminCapacityAccount", err)
		return
	}
	jsonResponse(w, a)
}

// POST /api/v1/admin/capacity/accounts/{userID} {repos_allowed?,
// requests_per_hour?, requests_per_day?, links_per_day?, note} — an account's overrides
// (null: the quota's value; all null and no note removes them). The api
// processes apply them within a minute (the policy's refresh).
func (s *Server) handleAdminCapacityOverride(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	userID, err := strconv.Atoi(r.PathValue("userID"))
	if err != nil || userID <= 0 || userID > math.MaxInt32 { // users.user_id is int4 (round 6 R6-d)
		writeAuthError(w, http.StatusBadRequest, "user id must be a positive integer")
		return
	}
	var body struct {
		ReposAllowed    *int   `json:"repos_allowed"`
		RequestsPerHour *int   `json:"requests_per_hour"`
		RequestsPerDay  *int   `json:"requests_per_day"`
		LinksPerDay     *int   `json:"links_per_day"`
		Note            string `json:"note"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {repos_allowed?, requests_per_hour?, requests_per_day?, links_per_day?, note}")
		return
	}
	err = s.store.SetCapacityOverride(r.Context(), db.CapacityOverride{UserID: userID, ReposAllowed: body.ReposAllowed,
		RequestsPerHour: body.RequestsPerHour, RequestsPerDay: body.RequestsPerDay, LinksPerDay: body.LinksPerDay, Note: body.Note}, info.UserID)
	switch {
	case errors.Is(err, db.ErrInvalidCapacitySettings):
		writeAuthError(w, http.StatusBadRequest, "each value must be a positive number (or null for the quota's value); the note is at most "+strconv.Itoa(db.MaxCapacityNoteLength)+" characters")
		return
	case errors.Is(err, db.ErrNoSuchUser):
		writeAuthError(w, http.StatusNotFound, "no account with that user id")
		return
	case err != nil:
		s.serverError(w, r, "handleAdminCapacityOverride", err)
		return
	}
	s.logger.Info("capacity override changed", "user_id", userID, "by", info.UserID)
	jsonResponse(w, map[string]any{"user_id": userID, "saved": true})
}

// POST /api/v1/admin/capacity/signup-allowlist {cidr, note}
func (s *Server) handleAdminSignupAllowlistAdd(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var body struct {
		CIDR string `json:"cidr"`
		Note string `json:"note"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {cidr, note}")
		return
	}
	cidr, err := s.store.AddSignupAllowlist(r.Context(), body.CIDR, body.Note, info.UserID)
	if errors.Is(err, db.ErrInvalidCIDR) || errors.Is(err, db.ErrInvalidCapacitySettings) {
		writeAuthError(w, http.StatusBadRequest, "cidr must be a network in CIDR form (such as 192.0.2.0/24); the note is at most "+strconv.Itoa(db.MaxCapacityNoteLength)+" characters")
		return
	}
	if err != nil {
		s.serverError(w, r, "handleAdminSignupAllowlistAdd", err)
		return
	}
	s.logger.Info("sign-up allowlist entry added", "cidr", cidr, "by", info.UserID)
	jsonResponse(w, map[string]any{"cidr": cidr})
}

// POST /api/v1/admin/capacity/signup-allowlist/remove {cidr}
func (s *Server) handleAdminSignupAllowlistRemove(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var body struct {
		CIDR string `json:"cidr"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {cidr}")
		return
	}
	removed, err := s.store.RemoveSignupAllowlist(r.Context(), body.CIDR)
	if errors.Is(err, db.ErrInvalidCIDR) {
		writeAuthError(w, http.StatusBadRequest, "cidr must be a network in CIDR form")
		return
	}
	if err != nil {
		s.serverError(w, r, "handleAdminSignupAllowlistRemove", err)
		return
	}
	if !removed {
		writeAuthError(w, http.StatusNotFound, "that network is not on the allowlist")
		return
	}
	s.logger.Info("sign-up allowlist entry removed", "cidr", body.CIDR, "by", info.UserID)
	jsonResponse(w, map[string]any{"removed": true})
}

// POST /api/v1/groups/{groupID}/repos/{repoID}/remove — removes a
// repository from one of the caller's groups. The repository leaves the
// account's count only when no other group of theirs holds it.
func (s *Server) handleGroupRemoveRepo(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	groupID, gerr := strconv.ParseInt(r.PathValue("groupID"), 10, 64)
	repoID, rerr := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
	if gerr != nil || rerr != nil || groupID <= 0 || repoID <= 0 {
		writeAuthError(w, http.StatusBadRequest, "group and repository ids must be positive integers")
		return
	}
	err := s.store.RemoveRepoFromGroup(r.Context(), info.UserID, groupID, repoID)
	if errors.Is(err, db.ErrGroupNotOwned) {
		writeAuthError(w, http.StatusNotFound, "no group of yours with that id")
		return
	}
	if err != nil {
		s.serverError(w, r, "handleGroupRemoveRepo", err)
		return
	}
	// The caller's scope may have narrowed: drop only their cached scope.
	s.auth.invalidateUser(info.UserID)
	s.homeCache.invalidate(info.UserID)
	jsonResponse(w, map[string]any{"group_id": groupID, "repo_id": repoID, "removed": true})
}

// meCapacity is /me's capacity object: the account's repositories against
// its allocation and its session requests in the current windows (this
// process's counts; the day joins the other api processes' at each save).
// nil when the store cannot answer (logged): /me stays best-effort.
func (s *Server) meCapacity(r *http.Request, info authInfo) map[string]any {
	if s.store == nil || s.limiter == nil {
		return nil
	}
	repos, err := s.store.AccountRepoAllocation(r.Context(), info.UserID)
	if err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "could not read the repository allocation for /me", "user_id", info.UserID, "error", err)
		return nil
	}
	m := s.limiter.quotaMeter()
	var windows []map[string]any
	quotas := s.limiter.policy.sessionQuotas(info.UserID, info.IsAdmin)
	if info.APITokenID != 0 {
		quotas = s.limiter.policy.tokenQuotas(info)
	}
	for _, q := range quotas {
		if q.Mode == capacity.Off || q.Allowed <= 0 {
			continue
		}
		d := m.Peek(subjectOf(info), q)
		windows = append(windows, map[string]any{"quota": q.Name, "window": q.Window.String(), "used": d.Used,
			"allowed": q.Allowed, "mode": q.Mode, "reset_at": d.ResetAt.UTC().Format(time.RFC3339)})
	}
	return map[string]any{"repos": repos, "requests": windows, "contact": repos.Contact}
}
