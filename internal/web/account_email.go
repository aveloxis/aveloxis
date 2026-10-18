// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/mailer"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// emailConfirmBase returns the base URL for the click-to-confirm link
// submitAccountEmail mails: the operator-configured site URL (which may carry
// a path; mailer.New has already trimmed it). The
// request Host header is attacker-controlled — a proxy that forwards Host
// (the usual nginx `proxy_set_header Host $host`) passes whatever the client
// sent — so deriving the link from it let an authenticated attacker submit a
// VICTIM's address with a crafted Host, have the victim receive a link to
// the attacker's server carrying the confirmation token, and replay that
// token to bind the victim's email to their own account (Copilot review on
// PR #207; CodeQL go/email-injection alert 197 traces the same request data
// into the message).
//
// So without a site URL the Host is used ONLY with web.dev_mode on AND when
// loopbackAuthority accepts it — the local-dev case that fallback existed
// for. dev_mode is required because a reverse proxy on the same host that
// does not forward Host (nginx's default for proxy_pass to 127.0.0.1) makes
// every visitor's request look loopback; the link would point at their own
// machine (round-4 review, operator decision, v0.29.30). Anywhere else this
// returns false and no link is mailed. It does not need the token, so the
// decision is made before anything is stored.
func emailConfirmBase(siteURL string, r *http.Request, devMode bool) (string, bool) {
	if siteURL != "" { // normalized once, by mailer.New
		return siteURL, true
	}
	if !devMode {
		return "", false
	}
	authority, ok := loopbackAuthority(r.Host)
	if !ok {
		return "", false
	}
	scheme := "https"
	if r.TLS == nil {
		scheme = "http"
	}
	return scheme + "://" + authority, true
}

// confirmationLink is the mailed link for a base from emailConfirmBase:
// the separate-repo front end's profile page when web.spa_url is set
// (it confirms through the API), this process's /account/email/confirm
// otherwise (2026-10-04). The token is hex, so it needs no escaping. On
// the front end it rides in the FRAGMENT: a fragment is never sent to the
// static server (so never in its access log), the front end's analytics
// tag excludes it, and the page moves it into the tab's session storage
// and drops it from the URL while its script loads, before any request
// is issued, so a signed-out click's login round trip carries a
// token-free ?next= (reviews 2026-10-04). spa_url is taken as written: config refuses a
// non-canonical value at load.
func confirmationLink(base, spaURL, token string) string {
	if spaURL != "" {
		return spaURL + "/profile.html#token=" + token
	}
	return base + "/account/email/confirm?token=" + token
}

// Why a confirmation link cannot be sent from this request.
var (
	errConfirmationMailDisabled = errors.New("mail is not configured, so no confirmation link can be sent")
	errConfirmationNoLinkBase   = errors.New("mail.site_url is not set — set it to this site's public URL to send confirmation links (web.dev_mode lets a loopback request build one for local development only; keep dev_mode off in production)")
)

// confirmationPolicy is everything that decides whether a confirmation
// link can be sent: the mailer and web.dev_mode. Both call sites — the
// account-email POST and the dashboard's email gate — take it from
// Server.confirmationPolicy, so they cannot disagree (round-5 review: the
// dashboard passed dev_mode separately, and a wrong value there brought
// back the v0.29.29 lockout with every test green).
type confirmationPolicy struct {
	mailer  confirmationMailer
	devMode bool
	spaURL  string // web.spa_url: the link lands on the front end's profile page
}

func (s *Server) confirmationPolicy() confirmationPolicy {
	return confirmationPolicy{mailer: s.mailer, devMode: s.cfg.DevMode, spaURL: s.cfg.SPAURL}
}

// ConfirmationPolicy is confirmationPolicy for the API process, which
// never builds a link from a request Host (devMode false): a configured
// mail.site_url, or no link at all.
type ConfirmationPolicy = confirmationPolicy

// NewConfirmationPolicy is the API's policy: its mailer and web.spa_url.
func NewConfirmationPolicy(m *mailer.Mailer, spaURL string) ConfirmationPolicy {
	return confirmationPolicy{mailer: m, spaURL: spaURL}
}

// AccountEmailStore is what SubmitAccountEmail needs from the store.
type AccountEmailStore = accountEmailStore

// confirmationBaseFor decides whether this request's user can be mailed a
// working confirmation link, and returns the link base when so.
// submitAccountEmail refuses on an error; the dashboard's email gate uses
// the same decision so it never sends a user to a form that would refuse
// them.
func confirmationBaseFor(p confirmationPolicy, r *http.Request) (string, error) {
	if !p.mailer.Enabled() {
		return "", errConfirmationMailDisabled
	}
	base, ok := emailConfirmBase(p.mailer.SiteURL(), r, p.devMode)
	if !ok {
		return "", errConfirmationNoLinkBase
	}
	return base, nil
}

// emailGateRedirect reports whether the dashboard must send the user to
// /account/email: they have neither a confirmed nor a pending address
// (v0.19.10; with a pending one the dashboard shows its "check your inbox"
// banner instead), AND a confirmation could actually be sent from here.
// Where it could not — mail off, or no trustworthy link base — the form
// refuses, so redirecting locked the user out of the dashboard; the
// operator chose to let them in without an address (v0.29.29).
func emailGateRedirect(confirmed, pending string, p confirmationPolicy, r *http.Request) bool {
	if strings.TrimSpace(confirmed) != "" || strings.TrimSpace(pending) != "" {
		return false
	}
	_, err := confirmationBaseFor(p, r)
	return err == nil
}

// accountEmailLookup is the store surface the dashboard's email gate reads.
type accountEmailLookup interface {
	GetUserEmail(ctx context.Context, userID int) (string, error)
	GetUserLivePendingEmail(ctx context.Context, userID int) (string, error)
}

// dashboardEmailGate reads the user's addresses, decides through
// emailGateRedirect whether the dashboard sends them to /account/email, and
// returns the pending address for the dashboard's banner. A lookup ERROR is
// not "no address" (SR-5): it is logged and the dashboard renders, rather
// than sending a user who has a confirmed address to the email form during
// a database blip.
func dashboardEmailGate(ctx context.Context, st accountEmailLookup, p confirmationPolicy, logger *slog.Logger, r *http.Request, userID int) (needsForm bool, pending string) {
	confirmed, err := st.GetUserEmail(ctx, userID)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "dashboard: could not read the user's email; rendering without the email-form redirect",
			"user_id", userID, "error", err)
		return false, ""
	}
	pending, err = st.GetUserLivePendingEmail(ctx, userID)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "dashboard: could not read the user's pending email; rendering without the email-form redirect",
			"user_id", userID, "error", err)
		return false, ""
	}
	return emailGateRedirect(confirmed, pending, p, r), pending
}

// accountEmailStore and confirmationMailer are the narrow surfaces
// submitAccountEmail needs, so it runs against fakes in tests.
type accountEmailStore interface {
	SetUserPendingEmail(ctx context.Context, userID int, email string) error
	ClearUserPendingEmailIf(ctx context.Context, userID int, email string) error
	CreateEmailConfirmation(ctx context.Context, userID int, email string) (string, error)
}

type confirmationMailer interface {
	SiteURL() string
	Enabled() bool
	SendEmailConfirmation(toEmail, login, confirmURL string, lifetime time.Duration) error
}

var (
	_ accountEmailStore  = (*db.PostgresStore)(nil)
	_ accountEmailLookup = (*db.PostgresStore)(nil)
	_ confirmationMailer = (*mailer.Mailer)(nil)
)

// submitAccountEmail processes a POSTed /account/email: the v0.20.4 flow that
// records the address as email_pending and mails a click-to-confirm link, so
// the address becomes users.email only when the link is followed. It
// returns "" when the address was recorded and the link handed to SMTP (the
// handler redirects), or the message the form should show.
//
// Nothing is stored until the submission is known to be mailable: the
// address passes the mailer's own recipient rule (so what is stored is the
// bare addr-spec Send will accept) and confirmationBaseFor allows it.
// Storing first left an email_pending behind on a refusal, and the
// dashboard then told the user a link had been sent. For the same reason a
// send that fails after storing clears that pending address again.
func submitAccountEmail(ctx context.Context, st accountEmailStore, p confirmationPolicy, logger *slog.Logger, r *http.Request, sess *Session) string {
	return submitAccountEmailAddress(ctx, st, p, logger, r, sess.UserID, sess.LoginName, r.FormValue("email"))
}

// SubmitAccountEmail is the API's entry to the same submission (its JSON
// body carries the address; the request supplies the Host only for a
// dev-mode web process, never here). The message is the user-facing
// reason it was refused, or "" when the confirmation was mailed.
func SubmitAccountEmail(ctx context.Context, st AccountEmailStore, p ConfirmationPolicy, logger *slog.Logger, r *http.Request, userID int, login, rawEmail string) string {
	return submitAccountEmailAddress(ctx, st, p, logger, r, userID, login, rawEmail)
}

// submitAccountEmailAddress is the one place an account email is stored
// and its confirmation mailed, for the web form and the API alike.
func submitAccountEmailAddress(ctx context.Context, st accountEmailStore, p confirmationPolicy, logger *slog.Logger, r *http.Request, userID int, login, rawEmail string) string {
	email, err := mailer.ParseRecipient(rawEmail)
	if err != nil {
		logger.Info("account email rejected: not a deliverable address",
			"user_id", userID, "error", truncateForLog([]byte(err.Error()), 200))
		return "Please enter a valid email address."
	}
	base, err := confirmationBaseFor(p, r)
	if err != nil {
		logger.Error("refusing an account email: "+err.Error(),
			"user_id", userID, "host", truncateForLog([]byte(r.Host), 200))
		return "Email confirmation is not configured on this site. Contact the operator."
	}
	if err := st.SetUserPendingEmail(ctx, userID, email); err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "failed to set pending email", "user_id", userID, "error", err)
		return "Could not save email. Try again."
	}
	token, err := st.CreateEmailConfirmation(ctx, userID, email)
	if err != nil {
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "failed to create email confirmation", "user_id", userID, "error", err)
		return "Could not generate confirmation. Try again."
	}
	if err := p.mailer.SendEmailConfirmation(email, login, confirmationLink(base, p.spaURL, token), db.EmailConfirmationLifetime); err != nil {
		if !mailer.IsSkip(err) { // a skip was already logged by the mailer
			// The send takes no context: its failure is never the
			// request's end (NET-6 review r6 F2), so no LogFailure.
			logger.Warn("failed to send confirmation email",
				"user_id", userID, "email", platform.RedactEmail(email), "error", err)
		}
		// Don't leave the dashboard announcing "we sent a confirmation
		// link" (operator decision, v0.29.29) — but the mail may still have
		// gone out: net/smtp reports QUIT's error after the server has
		// accepted the message. So clear the pending address only while it
		// is still this submission's (a second tab may have replaced it,
		// v0.29.30), and tell the user a link that does arrive still works:
		// its token is in the database until it expires.
		//
		// The clean-up is the second half of writes that already committed,
		// so it runs detached from the request (NET-6 review r7 F1: an SMTP
		// dial can outlast nginx and the bound, and on the cancelled request
		// context the clean-up failed at once, leaving the dashboard
		// announcing a link that was never sent) and any failure is a WARN.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), pendingEmailCleanupTimeout)
		defer cleanupCancel()
		if clearErr := st.ClearUserPendingEmailIf(cleanupCtx, userID, email); clearErr != nil {
			logger.Warn("failed to clear the pending email after a failed confirmation send",
				"user_id", userID, "error", clearErr)
		}
		return "We couldn't send the confirmation email. If one does arrive, its link still works; otherwise try again later."
	}
	return ""
}

// pendingEmailCleanupTimeout bounds submitAccountEmail's detached clean-up:
// one conditional UPDATE by the user's key, which takes milliseconds unless
// the pool or the database is stalled — the case the bound exists for. The
// house precedent for such a statement is 30 s (the deploy gate's
// deployGateDialTimeout, cmd/aveloxis).
const pendingEmailCleanupTimeout = 30 * time.Second

// loopbackAuthority parses an HTTP Host header ONCE and, when it names the
// loopback interface, returns it as a URL authority for an emailed link. A
// client-set Host may be trusted for that only when it is loopback: a
// loopback Host cannot reach anyone else's machine.
//
// The same parse feeds the decision and the link, and it is strict because
// its output is mailed:
//   - a port, when present, must be a canonical decimal in 1-65535 — an
//     earlier check ignored the port, so `localhost:<any text>` passed and
//     the text rode into the link;
//   - brackets must be balanced and may hold only an IPv6 literal;
//   - every IPv6 literal, IPv4-mapped included, comes back bracketed, as a
//     URL authority requires (RFC 3986 section 3.2.2). A bare "::1" is not
//     something a browser sends, but net/http accepts it.
func loopbackAuthority(hostHeader string) (string, bool) {
	host, port := hostHeader, ""
	bracketed := strings.HasPrefix(hostHeader, "[")
	if h, p, err := net.SplitHostPort(hostHeader); err == nil {
		if !isCanonicalPort(p) {
			return "", false
		}
		host, port = h, p
	} else if bracketed {
		// "[::1]" — SplitHostPort wants a port, so strip the brackets here.
		if !strings.HasSuffix(hostHeader, "]") {
			return "", false
		}
		host = hostHeader[1 : len(hostHeader)-1]
	}
	ip := net.ParseIP(host)
	ipv6Literal := ip != nil && strings.Contains(host, ":")
	switch {
	case bracketed && !ipv6Literal:
		return "", false
	case ip != nil && !ip.IsLoopback():
		return "", false
	case ip == nil && !strings.EqualFold(host, "localhost"):
		return "", false
	}
	if port != "" {
		return net.JoinHostPort(host, port), true
	}
	if ipv6Literal {
		return "[" + host + "]", true
	}
	return host, true
}

// isCanonicalPort reports whether p is a port in 1-65535 written as a
// canonical decimal: digits only, no sign, no leading zero.
func isCanonicalPort(p string) bool {
	n, err := strconv.ParseUint(p, 10, 16)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == p
}

// handleAccountEmail renders (GET) and processes (POST) the
// email-collection form. This is the v0.19.10 fallback when both /user
// and /user/emails came back empty during OAuth callback. A submitted
// address is stored as email_pending, a click-to-confirm link is mailed
// (v0.20.4), and the user redirects to /dashboard; users.email changes
// only when the link is followed.
func (s *Server) handleAccountEmail(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)

	if r.Method == http.MethodPost {
		if msg := submitAccountEmail(r.Context(), s.store, s.confirmationPolicy(), s.logger, r, sess); msg != "" {
			s.render(w, "account_email", map[string]any{
				"Session": sess,
				"Error":   msg,
			})
			return
		}
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}

	// GET: show the form. If the user already has a confirmed email,
	// they don't need to be here — bounce them back to dashboard.
	email, err := s.store.GetUserEmail(r.Context(), sess.UserID)
	if err != nil {
		// Showing the form is the safe fallback; the error is still logged.
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "account email form: could not read the user's email", "user_id", sess.UserID, "error", err)
	}
	if strings.TrimSpace(email) != "" {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
		return
	}
	// The confirm handler sends users here with a reason; say it.
	var notice string
	switch {
	case r.URL.Query().Get("expired") == "1":
		notice = "That confirmation link has expired, was already used, or belongs to another account. Enter your email to get a new one."
	case r.URL.Query().Get("error") == "1":
		notice = "We couldn't confirm your email just now. Try the link again, or enter your email to get a new one."
	}
	s.render(w, "account_email", map[string]any{
		"Session": sess,
		"Error":   notice,
	})
}

// handleEmailConfirm confirms the v0.20.4 click-to-confirm link for the
// logged-in user and redirects to the dashboard. A link that is not a live
// token for THIS account — unknown, expired, used, or another account's —
// redirects to /account/email?expired=1 and changes nothing (the owner's
// link stays usable; another account's still-live link is logged at WARN); a database
// failure redirects to ?error=1 with the transaction rolled back, so the
// link still works. The form explains both.
func (s *Server) handleEmailConfirm(w http.ResponseWriter, r *http.Request) {
	sess := s.getSession(r)
	switch ConfirmAccountEmail(r.Context(), s.store, s.logger, strings.TrimSpace(r.URL.Query().Get("token")), sess.UserID) {
	case ConfirmConfirmed:
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	case ConfirmError:
		http.Redirect(w, r, "/account/email?error=1", http.StatusFound)
	default:
		http.Redirect(w, r, "/account/email?expired=1", http.StatusFound)
	}
}

// ConfirmOutcome is what following a confirmation link came to.
type ConfirmOutcome int

const (
	// ConfirmConfirmed: the address is this account's confirmed email now.
	ConfirmConfirmed ConfirmOutcome = iota
	// ConfirmInvalid: not a live token for THIS account — unknown, expired,
	// used, empty, or another account's (logged at WARN); nothing changed,
	// and the owner's link stays usable.
	ConfirmInvalid
	// ConfirmError: the database failed; the transaction rolled back, so
	// the link still works.
	ConfirmError
)

// ConfirmEmailStore is what ConfirmAccountEmail needs from the store.
type ConfirmEmailStore interface {
	ConfirmEmailToken(ctx context.Context, token string, userID int) (string, error)
}

// ConfirmAccountEmail confirms the v0.20.4 click-to-confirm link for the
// signed-in user, for the web page and the API alike: one classification
// of the token's errors, so the two surfaces never disagree.
func ConfirmAccountEmail(ctx context.Context, st ConfirmEmailStore, logger *slog.Logger, token string, userID int) ConfirmOutcome {
	if token == "" {
		return ConfirmInvalid
	}
	if _, err := st.ConfirmEmailToken(ctx, token, userID); err != nil {
		var mismatch *db.TokenOwnerMismatchError
		if errors.As(err, &mismatch) {
			// The replay emailConfirmBase exists to prevent: say so.
			logger.Warn("email confirmation link belongs to another account",
				"token_user_id", mismatch.OwnerID, "session_user_id", userID)
			return ConfirmInvalid
		}
		if errors.Is(err, db.ErrConfirmationTokenInvalid) {
			logger.Info("email confirmation link rejected: unknown, expired, or already used",
				"session_user_id", userID)
			return ConfirmInvalid
		}
		httpserver.LogFailure(ctx, logger, slog.LevelWarn, err, "failed to confirm user email", "user_id", userID, "error", err)
		return ConfirmError
	}
	return ConfirmConfirmed
}
