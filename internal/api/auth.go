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
// v0.29.82: operator-issued API tokens (the plan's "super tokens") resolve
// through the same seam, recognised by db.APITokenPrefix. The identify step
// resolves a request's token ONCE; the rate limiter (a valid session is not
// counted, an API token has its own hourly allowance) and this layer both
// read that answer (operator, 2026-10-08: the per-IP limit is only for
// callers without a valid token).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// sessionStore is the role interface auth needs from the DB layer
// (*db.PostgresStore satisfies it; tests fake it).
type sessionStore interface {
	ValidateSessionToken(ctx context.Context, token string) (int, error)
	ValidateAPIToken(ctx context.Context, token string) (db.APITokenIdentity, error)
	APITokenActive(ctx context.Context, token string) (bool, error)
	IsUserAdmin(ctx context.Context, userID int) (bool, error)
	GetUserRepoScope(ctx context.Context, userID int) ([]int64, error)
}

// authInfo is what a validated request carries in its context.
type authInfo struct {
	UserID  int
	IsAdmin bool
	Scope   map[int64]bool // nil when IsAdmin (unscoped)
	// APITokenID is set for an operator-issued API token (0 for a session),
	// with its hourly allowance.
	APITokenID       int64
	RateLimitPerHour int
}

// resolution is a request's one token lookup (identify), read by the rate
// limiter and the auth layer. presented=false: no Bearer token at all.
type resolution struct {
	presented bool
	info      authInfo
	err       error // nil, errInvalidToken, or the store's failure
}

type resolutionCtxKey struct{}

// resolutionOf returns the identify step's answer, if it ran.
func resolutionOf(r *http.Request) (resolution, bool) {
	res, ok := r.Context().Value(resolutionCtxKey{}).(resolution)
	return res, ok
}

// errLookupDeferred marks a token identify did not look up (rl.deferLookup):
// the limiter counts the request per IP, and the auth layer looks the token
// up if the limiter admitted it anyway.
var errLookupDeferred = errors.New("token lookup deferred: the address is over its limit")

// identify resolves the request's Bearer token once and records the answer
// for the layers after it. It refuses nothing itself. A session token in
// the cache costs nothing and a cached API token one recheck; an UNCACHED
// token from an address over its limit that recently sent an invalid token
// is not looked up (rl.deferLookup). A cached API token is never deferred
// (Copilot review 5475865946 on PR #228): it was valid within authCacheTTL,
// which a caller cannot fabricate, and deferring it handed a valid token
// the address's 429 instead of its own allowance.
func (a *authenticator) identify(rl *rateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := resolution{}
		if tok := bearerToken(r); tok != "" {
			res.presented = true
			info, cached := a.cached(tok)
			switch {
			case cached && info.APITokenID == 0:
				res.info = info
			case cached: // an API token: rechecked, never deferred
				res.info, res.err = a.resolveToken(r.Context(), tok)
			case rl != nil && rl.deferLookup(r):
				res.err = errLookupDeferred
			default:
				res.info, res.err = a.resolveToken(r.Context(), tok)
				if errors.Is(res.err, errInvalidToken) && rl != nil {
					rl.noteBadToken(r, tokenKind(tok))
				}
			}
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), resolutionCtxKey{}, res)))
	})
}

// tokenKind names a token's kind for logs, never the token.
func tokenKind(token string) string {
	if strings.HasPrefix(token, db.APITokenPrefix) {
		return "api"
	}
	return "session"
}

// cached returns a token's cached validation, if it is fresh.
func (a *authenticator) cached(token string) (authInfo, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if c, ok := a.cache[token]; ok && time.Now().Before(c.expires) {
		return c.info, true
	}
	return authInfo{}, false
}

// resolve answers for r's token: the identify step's answer when it ran,
// otherwise a lookup (a chain built without identify, as some tests do, or
// a lookup identify deferred for a request the limiter then admitted).
func (a *authenticator) resolve(r *http.Request, tok string) (authInfo, error) {
	if res, ok := resolutionOf(r); ok && res.presented && !errors.Is(res.err, errLookupDeferred) {
		return res.info, res.err
	}
	return a.resolveToken(r.Context(), tok)
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
	logger  *slog.Logger

	mu    sync.Mutex
	cache map[string]cachedAuth
	// gen counts invalidateAll calls: a resolve that read the store before a
	// bust must not cache what it read after it (worklist follow-up 7; the
	// shape homeCache's setIfGen already had).
	gen uint64
}

func newAuthenticator(store sessionStore, require bool, logger *slog.Logger) *authenticator {
	if logger == nil {
		logger = slog.Default()
	}
	return &authenticator{store: store, require: require, logger: logger, cache: map[string]cachedAuth{}}
}

// errInvalidToken is a token the store says is unknown or expired: the one
// answer a 401 stands for. Any other error from resolveToken is the STORE's
// failure (worklist follow-up 6, review round 1: the GUI drops its token on
// every 401, so a lost connection at any of the three lookups signed the user
// out); the middleware answers it 503 and logs it.
var errInvalidToken = errors.New("invalid or expired session token")

// resolveToken validates a Bearer token, with a short cache. This is
// the single seam future super tokens extend. It returns errInvalidToken
// for a token the store does not know, and the store's own error when a
// lookup failed (nothing is cached then).
func (a *authenticator) resolveToken(ctx context.Context, token string) (authInfo, error) {
	now := time.Now()
	a.mu.Lock()
	if c, ok := a.cache[token]; ok && now.Before(c.expires) {
		a.mu.Unlock()
		if c.info.APITokenID == 0 {
			return c.info, nil
		}
		// An API token is rechecked on every call (one indexed lookup;
		// ASVS V7.4.1, the 0.29.82 review A6): revoked or expired through
		// ANY api process, it stops at once. Its owner's role and scope stay
		// cached for authCacheTTL.
		active, err := a.store.APITokenActive(ctx, token)
		if err != nil {
			return authInfo{}, fmt.Errorf("recheck API token: %w", err)
		}
		if !active {
			a.forget(token)
			return authInfo{}, errInvalidToken
		}
		return c.info, nil
	}
	gen := a.gen
	a.mu.Unlock()

	var info authInfo
	if strings.HasPrefix(token, db.APITokenPrefix) {
		id, err := a.store.ValidateAPIToken(ctx, token)
		if errors.Is(err, db.ErrInvalidAPIToken) {
			return authInfo{}, errInvalidToken
		}
		if err != nil {
			return authInfo{}, fmt.Errorf("validate API token: %w", err)
		}
		info = authInfo{UserID: id.UserID, APITokenID: id.TokenID, RateLimitPerHour: id.RateLimitPerHour}
	} else {
		userID, err := a.store.ValidateSessionToken(ctx, token)
		if errors.Is(err, db.ErrInvalidSessionToken) {
			return authInfo{}, errInvalidToken
		}
		if err != nil {
			return authInfo{}, fmt.Errorf("validate token: %w", err)
		}
		info = authInfo{UserID: userID}
	}
	userID := info.UserID
	// An API token never carries the admin flag, for reads either (Copilot
	// review 5476707567 on PR #228: an admin's token was unscoped on every
	// data route): it is scoped to its owner's groups like any account's,
	// and outside them it is refused, never auto-added
	// (refuseTokenOutOfScope). Only a session asks. A lookup error is not
	// "not an admin" (SR-5;
	// worklist follow-up 6).
	if info.APITokenID == 0 {
		admin, err := a.store.IsUserAdmin(ctx, userID)
		if err != nil {
			return authInfo{}, fmt.Errorf("admin flag: %w", err)
		}
		info.IsAdmin = admin
	}
	if !info.IsAdmin {
		ids, err := a.store.GetUserRepoScope(ctx, userID)
		if err != nil {
			return authInfo{}, fmt.Errorf("repo scope: %w", err)
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
	if a.gen == gen { // no bust since the store was read
		a.cache[token] = cachedAuth{info: info, expires: now.Add(authCacheTTL)}
	}
	a.mu.Unlock()
	return info, nil
}

// refuseStoreError answers a resolveToken store failure: logged at ERROR
// (everything that errors is logged) and a 503 whose body says to retry,
// never the 401 body — the GUI treats a 401 as "the token is gone". A
// client that went away mid-lookup (r.Context() cancelled, the store's
// error wraps context.Canceled) is neither: nothing to serve, nothing to
// chase — no ERROR (batch-2 review round 2).
func (a *authenticator) refuseStoreError(w http.ResponseWriter, r *http.Request, err error) {
	if httpserver.RequestEnded(r.Context(), err) { // client gone, or http_timeout_seconds fired
		a.logger.Debug("session lookup abandoned — the request ended", "error", err)
		return
	}
	a.logger.Error("session token could not be resolved — store failure, request refused (503)", "error", err)
	writeAuthError(w, http.StatusServiceUnavailable, "session lookup failed; try again")
}

// invalidateAll drops every cached token validation so role and scope
// changes take effect immediately instead of after authCacheTTL.
// Called from the admin mutation handlers (promote/demote, group
// approve/reject) — without this, a freshly promoted user's own
// /api/v1/me kept answering is_admin=false for up to 60s, and a user
// whose group was just approved kept an empty scope — and, since
// v0.29.68, from a non-admin's add that linked or queued repositories
// (the caller's own scope changed). Both are human-paced, so
// re-validating every active token once is cheap.
func (a *authenticator) invalidateAll() {
	a.mu.Lock()
	a.cache = map[string]cachedAuth{}
	a.gen++
	a.mu.Unlock()
}

// invalidateUser drops one user's cached validations (their scope or role
// changed) and leaves everyone else's (the 0.29.82 review A2: an auto-add
// flushed every user's cache). The generation still moves, so a lookup in
// flight cannot re-cache what it read before the change.
func (a *authenticator) invalidateUser(userID int) {
	a.mu.Lock()
	for tok, c := range a.cache {
		if c.info.UserID == userID {
			delete(a.cache, tok)
		}
	}
	a.gen++
	a.mu.Unlock()
}

// forget drops one token's cached validation (sign-out, a failed recheck).
// The generation moves too, so a lookup in flight cannot re-cache the token
// after it was signed out.
func (a *authenticator) forget(token string) {
	a.mu.Lock()
	delete(a.cache, token)
	a.gen++
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
				info, err := a.resolve(r, tok)
				switch {
				case err == nil:
					r = r.WithContext(withIdentity(r.Context(), info))
				case errors.Is(err, errInvalidToken):
					// Best-effort: an unknown token is no token.
				default:
					// A presented token the store could not resolve: refuse,
					// rather than run the request unscoped.
					a.refuseStoreError(w, r, err)
					return
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
		info, err := a.resolve(r, tok)
		if errors.Is(err, errInvalidToken) {
			writeAuthError(w, http.StatusUnauthorized, errInvalidToken.Error())
			return
		}
		if err != nil {
			a.refuseStoreError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), info)))
	})
}

// withIdentity attaches a validated caller to ctx. A request carrying an
// API token also runs WithoutAdminPrivilege: the store's admin decisions
// see a non-admin (an API token never administers).
func withIdentity(ctx context.Context, info authInfo) context.Context {
	ctx = context.WithValue(ctx, authCtxKey{}, info)
	if info.APITokenID != 0 {
		ctx = db.WithoutAdminPrivilege(ctx)
	}
	return ctx
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func writeAuthError(w http.ResponseWriter, code int, msg string) {
	// A refusal is an answer about this caller: never stored (api.md's
	// "every 401/403"; L10 round 4 found three refusal writers without it).
	setNoStoreHeaders(w.Header())
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
	// An API token reads only its owner's groups: refused, never
	// auto-added, with the way to add it (operator decision 2026-10-09).
	if info.APITokenID != 0 {
		s.refuseTokenOutOfScope(w, r, info, repoID)
		return false
	}
	if s.sharedWithMe != nil && info.UserID > 0 {
		// A signed-in session is not rate limited (0.29.82), so each user's
		// auto-adds are capped before anything is written (ASVS V2.4.1, the
		// 0.29.82 review A2).
		var slot autoAddSlot // the window a refund goes back to
		if s.autoAdds != nil {
			var ok bool
			var retry int
			if slot, ok, retry = s.autoAdds.reserve(info.UserID); !ok {
				refuseAutoAdd(w, retry)
				return false
			}
		}
		added, err := s.sharedWithMe.EnsureRepoSharedWithUser(r.Context(), info.UserID, repoID)
		// Nothing added gives the slot back; the user's cached scope is
		// dropped when it changed or was stale (already linked) — only
		// theirs (the 0.29.82 review A2; one rule with the other implicit
		// link sites since 0.29.84).
		s.settleAutoAdd(info.UserID, slot, added, err)
		switch {
		case err == nil:
			if added {
				// Their home list now includes the shared repo.
				s.homeCache.invalidate(info.UserID)
				// The notice is for this caller alone: the answer that carries
				// it is never stored (the cached routes already re-mark it;
				// the plain metrics routes did not — L10 round 3).
				setNoStoreHeaders(w.Header())
				w.Header().Set(sharedWithMeHeader, db.SharedWithMeGroupName)
				w.Header().Add("Access-Control-Expose-Headers", sharedWithMeHeader)
			}
			return true
		case errors.Is(err, db.ErrSharedRepoNotFound):
			// Nonexistent repo id — fall through to the 403.
		default:
			httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "shared-with-me auto-add failed — refusing access (fail closed)",
				"user_id", info.UserID, "repo_id", repoID, "error", err)
		}
	}
	setNoStoreHeaders(w.Header()) // a refusal is about this caller (api.md: every 401/403)
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
