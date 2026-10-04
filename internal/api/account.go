// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// The profile page's routes (2026-10-04). The front end has no page of the
// web process, so the account email — the forge's address taken at
// sign-in, or one the user confirms through a mailed link when the forge
// gave none — is read and changed here, through the SAME submission and
// confirmation the web process's /account/email pages use
// (web.SubmitAccountEmail, web.ConfirmAccountEmail): one place decides
// whether a link can be mailed and one classification of a token.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/web"
)

// accountStore is what the profile routes need from the store.
type accountStore interface {
	web.AccountEmailStore
	web.ConfirmEmailStore
	GetUserAccount(ctx context.Context, userID int) (provider, email string, err error)
	GetUserLivePendingEmail(ctx context.Context, userID int) (string, error)
	GetUserIdentity(ctx context.Context, userID int) (login, name, avatarURL string, err error)
}

// accountEmailBodyLimit bounds the JSON bodies of the two POSTs: an email
// address and a hex token are each far under a kilobyte.
const accountEmailBodyLimit = 4 << 10

// accountFields reads /me's account fields; a failed read is logged and
// yields empty strings, as the identity read does.
func (s *Server) accountFields(r *http.Request, userID int) (provider, email, pending string) {
	if s.accounts == nil {
		return "", "", ""
	}
	var err error
	if provider, email, err = s.accounts.GetUserAccount(r.Context(), userID); err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "/me: account fields unreadable", "user_id", userID, "error", err)
	}
	if pending, err = s.accounts.GetUserLivePendingEmail(r.Context(), userID); err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "/me: pending email unreadable", "user_id", userID, "error", err)
	}
	return provider, email, pending
}

// handleMeEmail — POST /api/v1/me/email {"email": "…"}: stores the address
// as pending and mails the confirmation link. 200 {"sent": true}; 422
// {"sent": false, "message": "…"} with the same user-facing reason the web
// form shows (not a deliverable address, mail not configured here, a
// failed save or send); 503 when this server has no store.
func (s *Server) handleMeEmail(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	if s.accounts == nil {
		http.Error(w, `{"error":"accounts unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, accountEmailBodyLimit)).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body: expected {\"email\": \"…\"}"}`, http.StatusBadRequest)
		return
	}
	login, _, _, err := s.accounts.GetUserIdentity(r.Context(), info.UserID)
	if err != nil {
		s.serverError(w, r, "handleMeEmail", err)
		return
	}
	policy := web.NewConfirmationPolicy(s.mailer, s.spaURL)
	if msg := web.SubmitAccountEmail(r.Context(), s.accounts, policy, s.logger, r, info.UserID, login, req.Email); msg != "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]any{"sent": false, "message": msg})
		return
	}
	jsonResponse(w, map[string]any{"sent": true})
}

// handleMeEmailConfirm — POST /api/v1/me/email/confirm {"token": "…"}:
// follows the mailed link for the signed-in account. 200 {"status":
// "confirmed"}; 200 {"status": "invalid"} for a token that is not live for
// THIS account (unknown, expired, used, another account's — nothing
// changed); 500 {"status": "error"} when the database failed (the link
// still works).
func (s *Server) handleMeEmailConfirm(w http.ResponseWriter, r *http.Request) {
	info, ok := s.requireUser(w, r)
	if !ok {
		return
	}
	if s.accounts == nil {
		http.Error(w, `{"error":"accounts unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, accountEmailBodyLimit)).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body: expected {\"token\": \"…\"}"}`, http.StatusBadRequest)
		return
	}
	switch web.ConfirmAccountEmail(r.Context(), s.accounts, s.logger, strings.TrimSpace(req.Token), info.UserID) {
	case web.ConfirmConfirmed:
		jsonResponse(w, map[string]any{"status": "confirmed"})
	case web.ConfirmError:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error"})
	default:
		jsonResponse(w, map[string]any{"status": "invalid"})
	}
}
