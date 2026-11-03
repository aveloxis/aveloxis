// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// csrfCookieName carries the session's form token (0.29.102, stage 2f of
// summary/59, B1/D-10): a double-submit pair — the cookie (HttpOnly, Secure
// unless dev_mode, SameSite Lax, the house attributes) and the same value
// in every server-rendered form's csrf_token field (or the X-CSRF-Token
// header of a fetch). A cross-site page can make the browser send the
// cookie but cannot read it to fill the field.
const csrfCookieName = "aveloxis_csrf"

// hstsValue is the Strict-Transport-Security every answer of a production
// web process carries (OWASP ASVS 5.0 V3.4.1): httpserver.HSTSValue, the
// one value the api process sends too.
const hstsValue = httpserver.HSTSValue

// csrfExempt lists the POST routes that take no form token: the two
// aveloxis-gui calls from its own origin with credentials (the origin
// check below is their guard — the SPA cannot read the HttpOnly cookie),
// and the API reverse proxy (JSON fetches from the page's own scripts; the
// API's routes decide the rest). Everything else that changes state is a
// server-rendered form.
var csrfExempt = map[string]bool{
	"/auth/token":  true,
	"/logout":      true,
	"/auth/logout": true,
}

// securityHeaders sets the one header set on every answer (D-11): a
// Content-Security-Policy with one script source — the pages carry no
// inline script and load no CDN since stage 2e — inline styles allowed
// (the pages' style attributes and the DOM builder's cssText; not a
// script vector), images from https (the forges' avatars), never framed,
// no base, no plugins; nosniff; the referrer trimmed off-site; the legacy
// frame refusal for old browsers. A handler may tighten it (the sign-up
// refusal page sets 'none').
func (s *Server) securityHeaders(next http.Handler) http.Handler {
	// Avatars come from the forges over https; under web.dev_mode (plain
	// HTTP development, the switch that also drops Secure) a self-hosted
	// GitLab on http:// may serve them (round 3 of the 0.29.102 review).
	img := "img-src 'self' https: data:"
	if s.cfg.DevMode {
		img = "img-src 'self' https: http: data:"
	}
	csp := "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " + img + "; connect-src 'self'; font-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		if !s.cfg.DevMode {
			// One year, subdomains included (ASVS V3.4.1; 0.29.113). A
			// front proxy that terminates TLS may replace it; under
			// web.dev_mode (plain HTTP) a browser would ignore it anyway.
			h.Set("Strict-Transport-Security", hstsValue)
		}
		next.ServeHTTP(w, r)
	})
}

// csrfGuard refuses cross-site state changes before any handler (B1).
// For every method but GET, HEAD and OPTIONS:
//
//   - the request's origin — the Origin header, else the Referer's — must
//     be this host or the configured SPA origin (web.spa_url); a browser
//     sends Origin on every cross-site POST, so a request with neither
//     header is not a cross-site browser request and passes to the next
//     check; a Sec-Fetch-Site of cross-site is refused whatever else says;
//   - a request that carries a session cookie and is not an exempt route
//     must carry the session's form token (csrfCookieName's value) in the
//     csrf_token field or the X-CSRF-Token header. An anonymous request
//     forges nothing: the route's gate answers it.
//
// Refusals are 403 and logged at WARN with the path and the origin.
//
// The form the guard reads the token from is bounded and typed first
// (0.29.106, W1 of the whole-branch review): the guard runs before the mux
// and every gate, so until then an anonymous request with any cookie value
// had its body parsed as a 32 MiB-in-memory multipart form with temp files
// for the rest. Every body on a form route is wrapped in ONE
// http.MaxBytesReader of formBodyMaxBytes — the handlers' own FormValue
// reads the same wrapped body — and only application/x-www-form-urlencoded
// is parsed (no page posts multipart): an overrun is a 413, another type a
// 415, a malformed form a 400, each logged — on the form path, where the
// guard parses; a request that carries the token in X-CSRF-Token (no
// first-party client does) is not parsed here and meets the handler's own
// refusal of the bounded body instead. The API reverse proxy is left out:
// the API bounds its own bodies, and a bound here would turn its 413 into
// a proxy failure.
func (s *Server) csrfGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
			return
		}
		if origin, ok := s.requestOrigin(r); !ok {
			s.logger.Warn("cross-site request refused", "method", r.Method, "path", r.URL.Path, "origin", logURL(origin))
			http.Error(w, "cross-site request refused", http.StatusForbidden)
			return
		}
		proxied := strings.HasPrefix(r.URL.Path, "/api/")
		if !proxied {
			r.Body = http.MaxBytesReader(w, r.Body, formBodyMaxBytes)
		}
		if _, err := s.readCookie(r, "aveloxis_session"); err == nil && !csrfExempt[r.URL.Path] && !proxied {
			cookie, err := s.readCookie(r, csrfCookieName)
			given := r.Header.Get("X-CSRF-Token")
			if given == "" {
				if !s.parseForm(w, r) {
					return
				}
				given = r.PostForm.Get("csrf_token")
			}
			if err != nil || cookie.Value == "" || given == "" || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(given)) != 1 {
				s.logger.Warn("form token missing or stale; the form was refused", "method", r.Method, "path", r.URL.Path)
				http.Error(w, "the form has expired — reload the page and try again", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// parseForm parses a server-rendered form's body under the guard's bound
// and type rule; false means it answered (413, 415 or 400, logged).
func (s *Server) parseForm(w http.ResponseWriter, r *http.Request) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		media, _, err := mime.ParseMediaType(ct)
		if err != nil || media != "application/x-www-form-urlencoded" {
			s.logger.Warn("form body refused: not a URL-encoded form", "path", r.URL.Path, "content_type", platform.TruncateForLog([]byte(ct), 120))
			http.Error(w, "forms are application/x-www-form-urlencoded", http.StatusUnsupportedMediaType)
			return false
		}
	}
	err := r.ParseForm()
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		return true
	case errors.As(err, &tooLarge):
		s.logger.Warn("form body refused: over the bound", "path", r.URL.Path, "limit_bytes", tooLarge.Limit)
		http.Error(w, "the form is too large", http.StatusRequestEntityTooLarge)
	default:
		// Through LogFailure: a read error can be the request's own end
		// (the client went away), which is not a failure.
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "form body refused: malformed", "path", r.URL.Path, "error", err)
		http.Error(w, "invalid form data", http.StatusBadRequest)
	}
	return false
}

// requestOrigin reports whether the request's origin is this deployment's
// (the Host, or web.spa_url's origin): httpserver.RequestOrigin, the one
// same-site rule, with the SPA origin admitted. The origin string comes
// back for the log.
func (s *Server) requestOrigin(r *http.Request) (string, bool) {
	return httpserver.RequestOrigin(r, s.isSPAOrigin)
}

// isSPAOrigin: the origin is web.spa_url's (scheme and host).
func (s *Server) isSPAOrigin(origin string) bool {
	if s.cfg.SPAURL == "" || origin == "" {
		return false
	}
	spa, err := url.Parse(s.cfg.SPAURL)
	if err != nil {
		return false
	}
	o, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return strings.EqualFold(o.Scheme, spa.Scheme) && strings.EqualFold(o.Host, spa.Host)
}

// csrfToken returns the request's form token, minting the cookie when the
// browser has none (a session from before this release, or the login
// page's own sign-out form): the pages render it into their forms.
func (s *Server) csrfToken(w http.ResponseWriter, r *http.Request) string {
	// The guard's own spelling (readCookie: __Host- outside dev_mode). A
	// bare r.Cookie here read a pre-0.29.113 aveloxis_csrf into every form
	// while the guard compared the prefixed one (review round 1, High).
	if c, err := s.readCookie(r, csrfCookieName); err == nil && c.Value != "" {
		return c.Value
	}
	token := rand.Text()
	http.SetCookie(w, s.cookie(csrfCookieName, token, int(db.WebSessionLifetime.Seconds())))
	return token
}

// formBodyMaxBytes bounds every form body the web process parses (0.29.106,
// W1): the largest legal form is the repository paste — admin.MaxPasteURLs
// lines of db.MaxAddURLBytes, each byte percent-encoded (three bytes) and
// each line ending in the CRLF a browser normalises a textarea to (%0D%0A,
// six) — plus the fixed fields: the three names, an int64 group id and the
// csrf token (review round 1 of 0.29.106: the first derivation counted LF
// and hid the difference in a 4 KiB allowance). TestFormBodyBoundIsDerived
// builds that maximal encoding and holds it within one line terminator of
// the bound. Whitespace the handler strips (blank lines, padding) counts
// against the bound too, so a maximal paste with padding is refused with
// a loud 413, never trimmed (review round 2 of 0.29.106: declined, by
// design). The API's JSON routes carry their own bounds (decodeJSONBody);
// the reverse proxy forwards those bodies unbounded here.
const (
	formBodyMaxBytes     int64 = int64(admin.MaxPasteURLs)*(3*int64(db.MaxAddURLBytes)+int64(len(formLineTerminator))) + int64(formFixedFieldsBytes)
	formLineTerminator         = "%0D%0A"
	formFixedFieldsBytes       = len("repo_urls=&group_id=&csrf_token=") + maxInt64Digits + csrfTokenLen
	maxInt64Digits             = 19 // len(strconv.FormatInt(math.MaxInt64, 10))
	csrfTokenLen               = 26 // rand.Text(): 26 base32 characters (about 128 bits of randomness), pinned by the test
)
