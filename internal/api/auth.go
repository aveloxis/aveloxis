// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.1 — Bearer session auth + per-user repo scope for the public
// analytics API (plan §2/§2b).
//
// Contract (operator, 2026-07-10): index.html is public; ALL
// analytics require login, and a logged-in user sees ONLY
// repositories in their own collection scope (user_repos of their
// approved groups). Admins are unscoped.
//
// Rollout switch: `api.require_auth` (default FALSE). The middleware
// infrastructure ships now but stays open until the GUI's token flow
// is deployed — flipping it earlier would break the existing
// server-rendered GUI's browser-side chart fetches. LAN-exempt
// clients (§3 exempt_cidrs) bypass auth even when enabled, so
// operator tooling keeps working.
//
// The "super token" tier (plan §3) plugs into resolveToken later:
// one lookup function is the seam.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// sessionStore is the role interface auth needs from the DB layer
// (*db.PostgresStore satisfies it; tests fake it).
type sessionStore interface {
	ValidateSessionToken(ctx context.Context, token string) (int, error)
	IsUserAdmin(ctx context.Context, userID int) (bool, error)
	GetUserRepoScope(ctx context.Context, userID int) ([]int64, error)
}

// authInfo is what a validated request carries in its context.
type authInfo struct {
	UserID  int
	IsAdmin bool
	Scope   map[int64]bool // nil when IsAdmin (unscoped)
}

type authCtxKey struct{}

// authCacheTTL bounds how long a validated token skips the DB.
// Short enough that revocation and scope changes propagate quickly.
const authCacheTTL = 60 * time.Second

type cachedAuth struct {
	info    authInfo
	expires time.Time
}

type authenticator struct {
	store   sessionStore
	require bool

	mu    sync.Mutex
	cache map[string]cachedAuth
}

func newAuthenticator(store sessionStore, require bool) *authenticator {
	return &authenticator{store: store, require: require, cache: map[string]cachedAuth{}}
}

// resolveToken validates a Bearer token, with a short cache. This is
// the single seam future super tokens extend.
func (a *authenticator) resolveToken(ctx context.Context, token string) (authInfo, bool) {
	now := time.Now()
	a.mu.Lock()
	if c, ok := a.cache[token]; ok && now.Before(c.expires) {
		a.mu.Unlock()
		return c.info, true
	}
	a.mu.Unlock()

	userID, err := a.store.ValidateSessionToken(ctx, token)
	if err != nil {
		return authInfo{}, false
	}
	info := authInfo{UserID: userID}
	if admin, err := a.store.IsUserAdmin(ctx, userID); err == nil && admin {
		info.IsAdmin = true
	}
	if !info.IsAdmin {
		ids, err := a.store.GetUserRepoScope(ctx, userID)
		if err != nil {
			return authInfo{}, false
		}
		info.Scope = make(map[int64]bool, len(ids))
		for _, id := range ids {
			info.Scope[id] = true
		}
	}
	a.mu.Lock()
	if len(a.cache) > 10000 {
		a.cache = map[string]cachedAuth{} // simple reset; tokens revalidate
	}
	a.cache[token] = cachedAuth{info: info, expires: now.Add(authCacheTTL)}
	a.mu.Unlock()
	return info, true
}

// invalidateAll drops every cached token validation so role and scope
// changes take effect immediately instead of after authCacheTTL.
// Called from the admin mutation handlers (promote/demote, group
// approve/reject) — without this, a freshly promoted user's own
// /api/v1/me kept answering is_admin=false for up to 60s, and a user
// whose group was just approved kept an empty scope. Admin mutations
// are rare, so re-validating every active token once is cheap.
func (a *authenticator) invalidateAll() {
	a.mu.Lock()
	a.cache = map[string]cachedAuth{}
	a.mu.Unlock()
}

// publicPaths bypass require_auth. This is the fail-closed boundary —
// keep the list tiny, explicit, and EXACT-MATCH only (no prefixes):
// health is the liveness probe; public/stats is the landing page's
// anonymous repo count (v0.27.59, still per-IP rate limited).
var publicPaths = map[string]bool{
	"/api/v1/health":       true,
	"/api/v1/public/stats": true,
}

// middleware enforces Bearer auth on every route except the
// publicPaths allowlist when require is set. Exempt-CIDR clients
// bypass (LAN tooling).
func (a *authenticator) middleware(rl *rateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.require || publicPaths[r.URL.Path] || rl.isExempt(rl.clientIP(r)) {
			// Best-effort: attach auth info when a token IS presented,
			// so scope checks apply even before require_auth flips on.
			if tok := bearerToken(r); tok != "" {
				if info, ok := a.resolveToken(r.Context(), tok); ok {
					r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, info))
				}
			}
			next.ServeHTTP(w, r)
			return
		}
		tok := bearerToken(r)
		if tok == "" {
			writeAuthError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		info, ok := a.resolveToken(r.Context(), tok)
		if !ok {
			writeAuthError(w, http.StatusUnauthorized, "invalid or expired session token")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authCtxKey{}, info)))
	})
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// Deliberate manual encode: WriteHeader already ran (non-200), so
	// jsonResponse's header set would be ineffective here.
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// sharedWithMeStore is the narrow seam authorizeRepo needs for the
// v0.27.82 shared-link auto-add (*db.PostgresStore satisfies it;
// tests fake it; a nil seam fails closed to the 403).
type sharedWithMeStore interface {
	EnsureRepoSharedWithUser(ctx context.Context, userID int, repoID int64) (bool, error)
}

// sharedWithMeHeader is the one-time notice a response carries when
// THIS request auto-added the repo to the caller's "Shared with Me"
// group — the GUI toasts it. Header (not body) because authorizeRepo
// guards 40+ handlers whose bodies it doesn't control.
const sharedWithMeHeader = "X-Aveloxis-Added-To-Group"

// authorizeRepo enforces §2b on a repo-scoped handler: unauthenticated
// contexts (auth off / exempt LAN) and admins pass; scoped users must
// have the repo in their user_repos. The 403 is STRUCTURED so the GUI
// renders an "ask for access" affordance instead of a dead end.
//
// v0.27.82 (operator decision 2026-08-04 — links are shareable): an
// out-of-scope repo that EXISTS is auto-added to the caller's
// implicit "Shared with Me" group and the request proceeds — the
// Starred/Comparisons pattern, triggered by viewing. The auto-add
// links only; it is structurally incapable of enqueueing collection
// (see db/shared_with_me.go's tripwire). Nonexistent repos, db
// errors, and nil-seam servers all FAIL CLOSED to the structured 403.
func (s *Server) authorizeRepo(w http.ResponseWriter, r *http.Request, repoID int64) bool {
	info, ok := r.Context().Value(authCtxKey{}).(authInfo)
	if !ok || info.IsAdmin {
		return true
	}
	if info.Scope[repoID] {
		return true
	}
	if s.sharedWithMe != nil && info.UserID > 0 {
		added, err := s.sharedWithMe.EnsureRepoSharedWithUser(r.Context(), info.UserID, repoID)
		switch {
		case err == nil:
			if added {
				// Scope changed: cached token validations must
				// re-resolve, and the user's home list now includes
				// the shared repo.
				if s.auth != nil {
					s.auth.invalidateAll()
				}
				s.homeCache.invalidate(info.UserID)
				w.Header().Set(sharedWithMeHeader, db.SharedWithMeGroupName)
				w.Header().Add("Access-Control-Expose-Headers", sharedWithMeHeader)
			}
			return true
		case errors.Is(err, db.ErrSharedRepoNotFound):
			// Nonexistent repo id — fall through to the 403.
		default:
			s.logger.Warn("shared-with-me auto-add failed — refusing access (fail closed)",
				"user_id", info.UserID, "repo_id", repoID, "error", err)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	// Deliberate manual encode: non-200 status already written.
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":   "repo_out_of_scope",
		"repo_id": repoID,
		"hint":    "add this repository to one of your groups to request access",
	})
	return false
}
