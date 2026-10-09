// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.82 — the admin endpoints behind aveloxis-gui's API-token page
// (operator decisions 2026-10-08): grant a token to an existing account,
// list the granted tokens (never the token itself), revoke one, and read or
// change the defaults the grant form offers (5,000 calls per hour, 30 days).

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/db"
)

// maxAPITokenLifetimeDays is the store's maximum (db.MaxAPITokenLifetimeDays,
// 365: the operator's decision, the ASVS review's A3); checked here too so
// the refusal is a 400 with the reason.
const maxAPITokenLifetimeDays = int64(db.MaxAPITokenLifetimeDays)

// maxAPITokenRateLimit is the largest hourly allowance the column holds
// (rate_limit_per_hour INT).
const maxAPITokenRateLimit = math.MaxInt32

func validLifetimeDays(d int64) bool { return d > 0 && d <= maxAPITokenLifetimeDays }
func validRateLimit(n int64) bool    { return n > 0 && n <= maxAPITokenRateLimit }

// GET /api/v1/admin/api-tokens
func (s *Server) handleAdminAPITokens(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	tokens, err := s.store.ListAPITokens(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokens", err)
		return
	}
	settings, err := s.store.GetAPITokenSettings(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokens", err)
		return
	}
	if tokens == nil {
		tokens = []db.APIToken{}
	}
	jsonResponse(w, map[string]any{"tokens": tokens, "settings": settings})
}

// POST /api/v1/admin/api-tokens {user_id, label, lifetime_days?, rate_limit_per_hour?}
// Omitted values take the current defaults. The answer carries the token,
// the only time it is ever available.
func (s *Server) handleAdminAPITokenGrant(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var body struct {
		UserID           int    `json:"user_id"`
		Label            string `json:"label"`
		LifetimeDays     *int64 `json:"lifetime_days"`
		RateLimitPerHour *int64 `json:"rate_limit_per_hour"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {user_id, label, lifetime_days?, rate_limit_per_hour?}")
		return
	}
	if body.UserID <= 0 {
		writeAuthError(w, http.StatusBadRequest, "user_id names the account the token authenticates as")
		return
	}
	if strings.TrimSpace(body.Label) == "" {
		writeAuthError(w, http.StatusBadRequest, "label is required (who the token is for, or what it is used by)")
		return
	}
	if utf8.RuneCountInString(strings.TrimSpace(body.Label)) > db.MaxAPITokenLabelLength {
		writeAuthError(w, http.StatusBadRequest, "label is at most "+strconv.Itoa(db.MaxAPITokenLabelLength)+" characters")
		return
	}
	settings, err := s.store.GetAPITokenSettings(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokenGrant", err)
		return
	}
	days := int64(settings.DefaultLifetimeDays)
	if body.LifetimeDays != nil {
		days = *body.LifetimeDays
	}
	limit := int64(settings.DefaultRateLimitPerHour)
	if body.RateLimitPerHour != nil {
		limit = *body.RateLimitPerHour
	}
	if !validLifetimeDays(days) {
		writeAuthError(w, http.StatusBadRequest, "lifetime_days must be a positive number of days, at most "+strconv.FormatInt(maxAPITokenLifetimeDays, 10))
		return
	}
	if !validRateLimit(limit) {
		writeAuthError(w, http.StatusBadRequest, "rate_limit_per_hour must be a positive number, at most "+strconv.Itoa(maxAPITokenRateLimit))
		return
	}
	raw, tok, err := s.store.CreateAPIToken(r.Context(), db.APITokenGrant{
		UserID: body.UserID, Label: body.Label, CreatedBy: info.UserID,
		Lifetime: time.Duration(days) * 24 * time.Hour, RateLimitPerHour: int(limit),
	})
	if errors.Is(err, db.ErrAPITokenOwnerNotFound) {
		writeAuthError(w, http.StatusBadRequest, "no account with that user_id")
		return
	}
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokenGrant", err)
		return
	}
	s.logger.Info("API token granted", "token_id", tok.TokenID, "user_id", tok.UserID, "by", info.UserID,
		"rate_limit_per_hour", tok.RateLimitPerHour, "expires_at", tok.ExpiresAt)
	setNoStoreHeaders(w.Header()) // the one answer that carries the token
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token": raw, "token_id": tok.TokenID, "user_id": tok.UserID, "label": tok.Label,
		"created_at": tok.CreatedAt, "expires_at": tok.ExpiresAt, "rate_limit_per_hour": tok.RateLimitPerHour,
	})
}

// POST /api/v1/admin/api-tokens/{tokenID}/revoke
func (s *Server) handleAdminAPITokenRevoke(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("tokenID"), 10, 64)
	if err != nil || id <= 0 {
		writeAuthError(w, http.StatusBadRequest, "token id must be a positive integer")
		return
	}
	err = s.store.RevokeAPIToken(r.Context(), id, info.UserID)
	if errors.Is(err, db.ErrAPITokenNotFound) {
		writeAuthError(w, http.StatusNotFound, "no API token with that id")
		return
	}
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokenRevoke", err)
		return
	}
	// A revoked token must stop at once, not when its cached validation
	// ages out (60 s): drop every cached validation in this process.
	if s.auth != nil {
		s.auth.invalidateAll()
	}
	s.logger.Info("API token revoked", "token_id", id, "by", info.UserID)
	jsonResponse(w, map[string]any{"token_id": id, "revoked": true})
}

// GET /api/v1/admin/api-token-settings
func (s *Server) handleAdminAPITokenSettings(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.requireAdmin(w, r); !ok {
		return
	}
	settings, err := s.store.GetAPITokenSettings(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokenSettings", err)
		return
	}
	jsonResponse(w, settings)
}

// POST /api/v1/admin/api-token-settings {default_rate_limit_per_hour, default_lifetime_days}
func (s *Server) handleAdminAPITokenSettingsUpdate(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var body struct {
		DefaultRateLimitPerHour *int64 `json:"default_rate_limit_per_hour"`
		DefaultLifetimeDays     *int64 `json:"default_lifetime_days"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		writeAuthError(w, http.StatusBadRequest, "body must be JSON: {default_rate_limit_per_hour, default_lifetime_days}")
		return
	}
	if body.DefaultRateLimitPerHour == nil || !validRateLimit(*body.DefaultRateLimitPerHour) {
		writeAuthError(w, http.StatusBadRequest, "default_rate_limit_per_hour must be a positive number, at most "+strconv.Itoa(maxAPITokenRateLimit))
		return
	}
	if body.DefaultLifetimeDays == nil || !validLifetimeDays(*body.DefaultLifetimeDays) {
		writeAuthError(w, http.StatusBadRequest, "default_lifetime_days must be a positive number of days, at most "+strconv.FormatInt(maxAPITokenLifetimeDays, 10))
		return
	}
	st := db.APITokenSettings{DefaultRateLimitPerHour: int(*body.DefaultRateLimitPerHour), DefaultLifetimeDays: int(*body.DefaultLifetimeDays)}
	if err := s.store.SetAPITokenSettings(r.Context(), st, info.UserID); err != nil {
		s.serverError(w, r, "handleAdminAPITokenSettingsUpdate", err)
		return
	}
	s.logger.Info("API token defaults changed", "default_rate_limit_per_hour", st.DefaultRateLimitPerHour,
		"default_lifetime_days", st.DefaultLifetimeDays, "by", info.UserID)
	jsonResponse(w, map[string]any{"default_rate_limit_per_hour": st.DefaultRateLimitPerHour, "default_lifetime_days": st.DefaultLifetimeDays})
}

// POST /api/v1/auth/logout — ends the session token it is called with
// (OWASP ASVS V7.4.1; the 0.29.82 review: sign-out ended only the web
// process's cookie session, and a copied Bearer token stayed valid for its
// 30 days). Deleting a token needs only holding it; with api.require_auth
// on, an already-invalid token is refused (401) by the auth layer before
// this runs, otherwise it answers 204. An API token is revoked by an
// administrator.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	setNoStoreHeaders(w.Header())
	tok := bearerToken(r)
	if tok == "" {
		writeAuthError(w, http.StatusUnauthorized, "sign-out needs the session token (Bearer)")
		return
	}
	if strings.HasPrefix(tok, db.APITokenPrefix) {
		writeAuthError(w, http.StatusBadRequest, "an API token is revoked by an administrator, not signed out")
		return
	}
	if err := s.store.DeleteSessionToken(r.Context(), tok); err != nil {
		s.serverError(w, r, "handleLogout", err)
		return
	}
	if s.auth != nil {
		s.auth.forget(tok)
	}
	w.WriteHeader(http.StatusNoContent)
}
