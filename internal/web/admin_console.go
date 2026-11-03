// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

// Stage 3c of summary/59 (0.29.109): the API-tokens, Capacity and
// Collections pages of the administrator console. Each is a template plus
// thin handlers: a page handler reads what the page shows, a form handler
// parses the form and calls the admin service, which owns every rule
// (SR-18), answers a refusal from refusePage's table and redirects back
// with a notice flag. A destructive action has a confirm page (D-19) and
// no dialog. The one page that is not a redirect is the grant's: the raw
// token is shown once, never cached, never through a redirect.

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/admin"
	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
)

// adminNotices maps a page's ?ok= flag to the sentence it shows. Fixed
// text: a flag the table does not hold shows nothing, so a page never
// echoes a query value.
var adminNotices = map[string]string{
	"revoked":        "The token is revoked; every client that used it is refused from now on.",
	"defaults":       "The defaults are saved.",
	"quota":          "The quota is saved; the api processes apply it within a minute.",
	"contact":        "The contact address is saved.",
	"override":       "The account's own values are saved; the api processes apply them within a minute.",
	"cleared":        "The account's own values are removed; the quotas apply to it again.",
	"allowlisted":    "The network is on the sign-up allowlist.",
	"unlisted":       "The network is off the sign-up allowlist.",
	"created":        "The collection is created.",
	"saved":          "The collection is saved.",
	"deleted":        "The collection is deleted.",
	"group_added":    "The group is in the collection.",
	"group_removed":  "The group is out of the collection.",
	"sessions_ended": "The account's sessions are ended; its next request signs in again.",
}

func adminNotice(r *http.Request) string { return adminNotices[r.URL.Query().Get("ok")] }

// optionalInt64 reads an optional number from a console form: an empty
// field is nil (the default or the quota's value), a non-number is the
// field's own 400. Every
// number a console form carries is an int4 in the store (a quota, an
// allowance, a lifetime in days, a position), so it is parsed at that width:
// a value past int32 is "out of range" here, and the int the service and
// the store take can never narrow it (CodeQL go/incorrect-integer-conversion
// on the 0.29.111 tree, seven sites, all this reader's). The service's own
// rules stay the authority for the API path, which decodes JSON.
func optionalInt64(w http.ResponseWriter, r *http.Request, name string) (*int64, bool) {
	v := strings.TrimSpace(r.FormValue(name))
	if v == "" {
		return nil, true
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			http.Error(w, name+" is out of range", http.StatusBadRequest)
		} else {
			http.Error(w, name+" must be a number", http.StatusBadRequest)
		}
		return nil, false
	}
	return &n, true
}

// optionalInt is optionalInt64 for a field the service takes as an int.
func optionalInt(w http.ResponseWriter, r *http.Request, name string) (*int, bool) {
	n64, ok := optionalInt64(w, r, name)
	if !ok || n64 == nil {
		return nil, ok
	}
	n := int(*n64) // parsed at 32 bits above: never a narrowing
	return &n, true
}

// confirmPage renders the one confirm template for a destructive action:
// the question, the POST it submits to, and the hidden fields it carries.
func (s *Server) confirmPage(w http.ResponseWriter, r *http.Request, sess *Session, title, question, action, button, back string, fields map[string]string) {
	s.render(w, r, "admin_confirm", map[string]any{
		"Session": sess, "Title": title, "Question": question, "Action": action, "Button": button, "Back": back, "Fields": fields,
	})
}

// serviceErr answers a service error: a refusal from the page table, a
// failure as a logged 500.
func (s *Server) serviceErr(w http.ResponseWriter, r *http.Request, handler string, err error) {
	if !s.refusePage(w, r, err) {
		s.serverError(w, r, handler, err)
	}
}

// ─── API tokens ──────────────────────────────────────────────────

// tokenRow is one token as the page lists it, with its state in a word.
type tokenRow struct {
	db.APIToken
	Status string
}

func (s *Server) handleAdminAPITokens(w http.ResponseWriter, r *http.Request, sess *Session) {
	tokens, err := s.store.ListAPITokens(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokens", fmt.Errorf("list API tokens: %w", err))
		return
	}
	settings, err := s.store.GetAPITokenSettings(r.Context())
	if err != nil {
		s.serverError(w, r, "handleAdminAPITokens", fmt.Errorf("read the token defaults: %w", err))
		return
	}
	now := time.Now()
	rows := make([]tokenRow, 0, len(tokens))
	for _, t := range tokens {
		status := "active"
		switch {
		case t.RevokedAt != nil:
			status = "revoked"
		case !t.ExpiresAt.After(now):
			status = "expired"
		}
		rows = append(rows, tokenRow{APIToken: t, Status: status})
	}
	s.render(w, r, "admin_tokens", map[string]any{
		"Session": sess, "Notice": adminNotice(r), "Tokens": rows, "Settings": settings,
		"MaxLabel": db.MaxAPITokenLabelLength, "MaxDays": admin.MaxAPITokenLifetimeDays,
	})
}

// handleAdminAPITokenGrant answers the page that shows the token once:
// no redirect (a redirect would put it in a URL), Cache-Control no-store.
func (s *Server) handleAdminAPITokenGrant(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	userID, err := admin.ParseUserID(strings.TrimSpace(r.FormValue("user_id"))) // the one users.user_id rule (0.29.103)
	if err != nil {
		http.Error(w, "user_id must be a positive integer", http.StatusBadRequest)
		return
	}
	days, ok := optionalInt64(w, r, "lifetime_days")
	if !ok {
		return
	}
	hour, ok := optionalInt64(w, r, "rate_limit_per_hour")
	if !ok {
		return
	}
	day, ok := optionalInt64(w, r, "rate_limit_per_day")
	if !ok {
		return
	}
	raw, tok, err := s.admin.GrantAPIToken(r.Context(), sessionActor(sess), admin.TokenGrant{
		UserID: userID, Label: r.FormValue("label"), LifetimeDays: days, RateLimitPerHour: hour, RateLimitPerDay: day,
	})
	if err != nil {
		if errors.Is(err, db.ErrUserNotFound) {
			http.Error(w, "no account with that user_id", http.StatusBadRequest)
			return
		}
		s.serviceErr(w, r, "handleAdminAPITokenGrant", err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, r, "admin_token_granted", map[string]any{"Session": sess, "Token": raw, "Grant": tok})
}

func (s *Server) handleAdminAPITokenRevokeConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "tokenID")
	if !ok {
		http.Error(w, "invalid token id", http.StatusBadRequest)
		return
	}
	s.confirmPage(w, r, sess, "Revoke API token",
		fmt.Sprintf("Revoke token %d? Every client that uses it is refused from now on; a new token can be granted to the account.", id),
		fmt.Sprintf("/admin/api-tokens/%d/revoke", id), "Revoke", "/admin/api-tokens", nil)
}

func (s *Server) handleAdminAPITokenRevoke(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "tokenID")
	if !ok {
		http.Error(w, "invalid token id", http.StatusBadRequest)
		return
	}
	if err := s.admin.RevokeAPIToken(r.Context(), sessionActor(sess), id); err != nil {
		s.serviceErr(w, r, "handleAdminAPITokenRevoke", err)
		return
	}
	http.Redirect(w, r, "/admin/api-tokens?ok=revoked", http.StatusFound)
}

func (s *Server) handleAdminAPITokenDefaults(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	hour, ok := optionalInt64(w, r, "default_rate_limit_per_hour")
	if !ok {
		return
	}
	day, ok := optionalInt64(w, r, "default_rate_limit_per_day")
	if !ok {
		return
	}
	days, ok := optionalInt64(w, r, "default_lifetime_days")
	if !ok {
		return
	}
	if _, err := s.admin.SetAPITokenDefaults(r.Context(), sessionActor(sess), admin.TokenDefaults{RateLimitPerHour: hour, RateLimitPerDay: day, LifetimeDays: days}); err != nil {
		s.serviceErr(w, r, "handleAdminAPITokenDefaults", err)
		return
	}
	http.Redirect(w, r, "/admin/api-tokens?ok=defaults", http.StatusFound)
}

// ─── Capacity ────────────────────────────────────────────────────

// signupDaysShown is how many UTC days of new accounts the page lists: a
// month, the span an administrator compares a day against (the API's
// page shows the same).
const signupDaysShown = 30

// accountView is one account as the capacity page shows it: the store's
// record and its override values as text ("" = the quota's value), since
// a template prints a nil pointer as nothing and a set one as an address.
type accountView struct {
	db.CapacityAccount
	ReposAllowed, RequestsPerHour, RequestsPerDay, LinksPerDay string
}

func viewAccount(a db.CapacityAccount) accountView {
	str := func(p *int) string {
		if p == nil {
			return ""
		}
		return strconv.Itoa(*p)
	}
	return accountView{CapacityAccount: a, ReposAllowed: str(a.Override.ReposAllowed), RequestsPerHour: str(a.Override.RequestsPerHour),
		RequestsPerDay: str(a.Override.RequestsPerDay), LinksPerDay: str(a.Override.LinksPerDay)}
}

func viewAccounts(as []db.CapacityAccount) []accountView {
	out := make([]accountView, 0, len(as))
	for _, a := range as {
		out = append(out, viewAccount(a))
	}
	return out
}

func (s *Server) handleAdminCapacity(w http.ResponseWriter, r *http.Request, sess *Session) {
	ctx := r.Context()
	fail := func(what string, err error) {
		s.serverError(w, r, "handleAdminCapacity", fmt.Errorf("%s: %w", what, err))
	}
	quotas, err := s.store.EffectiveCapacityQuotas(ctx)
	if err != nil {
		fail("quotas", err)
		return
	}
	list := make([]capacity.EffectiveQuota, 0, len(quotas))
	for _, name := range capacity.QuotaNames() {
		list = append(list, quotas[name])
	}
	contact, err := s.store.GetCapacityContact(ctx)
	if err != nil {
		fail("contact", err)
		return
	}
	signups, err := s.store.SignupsPerDay(ctx, signupDaysShown)
	if err != nil {
		fail("sign-ups", err)
		return
	}
	allow, err := s.store.ListSignupAllowlist(ctx)
	if err != nil {
		fail("allowlist", err)
		return
	}
	overrides, err := s.store.CapacityAccountsWithOverrides(ctx)
	if err != nil {
		fail("overrides", err)
		return
	}
	busiest, err := s.store.BusiestAccountsToday(ctx)
	if err != nil {
		fail("busiest", err)
		return
	}
	data := map[string]any{
		"Session": sess, "Notice": adminNotice(r), "Quotas": list, "Contact": contact, "Signups": signups, "Allowlist": allow,
		"Overrides": viewAccounts(overrides), "Busiest": viewAccounts(busiest), "Modes": []capacity.Mode{capacity.Enforce, capacity.Shadow, capacity.Off},
		"MaxNote": db.MaxCapacityNoteLength,
	}
	// The largest accounts are a full read of the group links: on request.
	if r.URL.Query().Get("largest") == "1" {
		largest, err := s.store.LargestAccounts(ctx)
		if err != nil {
			fail("largest accounts", err)
			return
		}
		data["Largest"] = viewAccounts(largest)
		data["ShowLargest"] = true
	}
	// One account, by id (reaches a login's case twin) or by login.
	q := r.URL.Query()
	// The page echoes only a login that passed the one rule, into the login
	// field (round-2 review B-F1: an unbounded value was echoed before the
	// parse; round-3: an id in the login field looked up the login "7" on
	// the next press). A user id is shown by the account it found.
	if raw := q.Get("user_id"); raw != "" {
		id, err := admin.ParseUserID(raw)
		if err != nil {
			data["LookupError"] = "user_id must be a positive integer"
		} else {
			s.lookupAccount(data, func() (db.CapacityAccount, error) { return s.store.CapacityAccountByID(ctx, id) })
		}
	} else if raw := q.Get("login"); raw != "" {
		login, ok := admin.NormalizeLogin(raw)
		if !ok { // no forge issues one; refused before the query, never echoed
			data["LookupError"] = "no such account"
		} else {
			data["Lookup"] = login
			s.lookupAccount(data, func() (db.CapacityAccount, error) { return s.store.CapacityAccountByLogin(ctx, login) })
		}
	}
	s.render(w, r, "admin_capacity", data)
}

// lookupAccount fills the page's account card: the account, the
// candidates when a login names several apart from letter case, or the
// reason none was found. A read failure is a 500 like any other; it is
// reported as a lookup error so the rest of the page still renders.
func (s *Server) lookupAccount(data map[string]any, read func() (db.CapacityAccount, error)) {
	a, err := read()
	var amb *db.AmbiguousLoginError
	switch {
	case err == nil:
		data["Account"] = viewAccount(a)
	case errors.Is(err, db.ErrUserNotFound):
		data["LookupError"] = "no such account"
	case errors.As(err, &amb):
		data["Candidates"] = viewAccounts(amb.Candidates)
		data["LookupError"] = "more than one account has this login apart from letter case; open one by its id"
	default:
		s.logger.Warn("capacity page: account lookup failed", "error", err)
		data["LookupError"] = "the lookup failed; try again"
	}
}

func (s *Server) handleAdminCapacityQuota(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	allowed, ok := optionalInt64(w, r, "allowed")
	if !ok {
		return
	}
	name := pathName(r, "name")
	if _, err := s.admin.SetCapacityQuota(r.Context(), sessionActor(sess), name, admin.QuotaChange{Allowed: allowed, Mode: r.FormValue("mode")}); err != nil {
		s.serviceErr(w, r, "handleAdminCapacityQuota", err)
		return
	}
	http.Redirect(w, r, "/admin/capacity?ok=quota", http.StatusFound)
}

func (s *Server) handleAdminCapacityContact(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if err := s.admin.SetCapacityContact(r.Context(), sessionActor(sess), r.FormValue("contact_email")); err != nil {
		s.serviceErr(w, r, "handleAdminCapacityContact", err)
		return
	}
	http.Redirect(w, r, "/admin/capacity?ok=contact", http.StatusFound)
}

func (s *Server) handleAdminCapacityOverride(w http.ResponseWriter, r *http.Request, sess *Session) {
	userID, ok := pathUserID(r, "userID")
	if !ok {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	o := db.CapacityOverride{UserID: userID, Note: r.FormValue("note")}
	for name, dst := range map[string]**int{"repos_allowed": &o.ReposAllowed, "requests_per_hour": &o.RequestsPerHour, "requests_per_day": &o.RequestsPerDay, "links_per_day": &o.LinksPerDay} {
		v, ok := optionalInt(w, r, name)
		if !ok {
			return
		}
		*dst = v
	}
	if err := s.admin.SetCapacityOverride(r.Context(), sessionActor(sess), o); err != nil {
		s.serviceErr(w, r, "handleAdminCapacityOverride", err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/capacity?user_id=%d&ok=override", userID), http.StatusFound)
}

func (s *Server) handleAdminCapacityClearConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	userID, ok := pathUserID(r, "userID")
	if !ok {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	s.confirmPage(w, r, sess, "Remove an account's own values",
		fmt.Sprintf("Remove the own values of account %d? The quotas apply to it again.", userID),
		fmt.Sprintf("/admin/capacity/accounts/%d/clear", userID), "Remove", fmt.Sprintf("/admin/capacity?user_id=%d", userID), nil)
}

func (s *Server) handleAdminCapacityClear(w http.ResponseWriter, r *http.Request, sess *Session) {
	userID, ok := pathUserID(r, "userID")
	if !ok {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	// All null and no note: the store removes the row.
	if err := s.admin.SetCapacityOverride(r.Context(), sessionActor(sess), db.CapacityOverride{UserID: userID}); err != nil {
		s.serviceErr(w, r, "handleAdminCapacityClear", err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/capacity?user_id=%d&ok=cleared", userID), http.StatusFound)
}

func (s *Server) handleAdminAllowlistAdd(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	if _, err := s.admin.AddSignupAllowlist(r.Context(), sessionActor(sess), r.FormValue("cidr"), r.FormValue("note")); err != nil {
		s.serviceErr(w, r, "handleAdminAllowlistAdd", err)
		return
	}
	http.Redirect(w, r, "/admin/capacity?ok=allowlisted", http.StatusFound)
}

func (s *Server) handleAdminAllowlistRemoveConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	// The one CIDR rule the store applies, before the page carries the
	// value back (round-2 review of 0.29.109: the confirm page echoed any
	// string into its hidden field).
	cidr, err := db.NormalizeCIDR(r.URL.Query().Get("cidr"))
	if err != nil {
		http.Error(w, "cidr must be a network in CIDR form", http.StatusBadRequest)
		return
	}
	s.confirmPage(w, r, sess, "Remove a network from the sign-up allowlist",
		"Remove this network from the allowlist? Sign-ups from it count against the sign-up quota again.",
		"/admin/capacity/signup-allowlist/remove", "Remove", "/admin/capacity", map[string]string{"cidr": cidr})
}

func (s *Server) handleAdminAllowlistRemove(w http.ResponseWriter, r *http.Request, sess *Session) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	removed, err := s.admin.RemoveSignupAllowlist(r.Context(), sessionActor(sess), r.FormValue("cidr"))
	if err != nil {
		s.serviceErr(w, r, "handleAdminAllowlistRemove", err)
		return
	}
	if !removed {
		http.Error(w, "that network is not on the allowlist", http.StatusNotFound)
		return
	}
	http.Redirect(w, r, "/admin/capacity?ok=unlisted", http.StatusFound)
}

// ─── Collections ─────────────────────────────────────────────────

func collectionFormFields(w http.ResponseWriter, r *http.Request) (admin.CollectionFields, bool) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return admin.CollectionFields{}, false
	}
	// The adapter parses (at the column's width, optionalInt64); the int4
	// rule the service applies is the authority for the API path too
	// (whole-branch review of 0.29.109).
	position, ok := optionalInt(w, r, "position")
	if !ok {
		return admin.CollectionFields{}, false
	}
	f := admin.CollectionFields{Name: strings.TrimSpace(r.FormValue("name")), Description: strings.TrimSpace(r.FormValue("description"))}
	if position != nil {
		f.Position = *position
	}
	return f, true
}

func (s *Server) handleAdminCollections(w http.ResponseWriter, r *http.Request, sess *Session) {
	cols, err := s.store.ListCollections(r.Context(), sess.UserID)
	if err != nil {
		s.serverError(w, r, "handleAdminCollections", fmt.Errorf("list collections: %w", err))
		return
	}
	s.render(w, r, "admin_collections", map[string]any{"Session": sess, "Notice": adminNotice(r), "Collections": cols,
		"MaxName": admin.MaxGroupNameLength, "MaxDescription": admin.MaxCollectionDescriptionLength})
}

func (s *Server) handleAdminCollectionCreate(w http.ResponseWriter, r *http.Request, sess *Session) {
	f, ok := collectionFormFields(w, r)
	if !ok {
		return
	}
	id, err := s.admin.CreateCollection(r.Context(), sessionActor(sess), f)
	if err != nil {
		s.serviceErr(w, r, "handleAdminCollectionCreate", err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/collections/%d?ok=created", id), http.StatusFound)
}

// collectionByID finds one collection in the list (the store lists them
// in one read; there is no single-row reader). false: no such collection.
func (s *Server) collectionByID(r *http.Request, sess *Session, id int64) (db.CollectionSummary, bool, error) {
	cols, err := s.store.ListCollections(r.Context(), sess.UserID)
	if err != nil {
		return db.CollectionSummary{}, false, err
	}
	for _, c := range cols {
		if c.CollectionID == id {
			return c, true, nil
		}
	}
	return db.CollectionSummary{}, false, nil
}

func (s *Server) handleAdminCollection(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	if !ok {
		http.Error(w, "invalid collection id", http.StatusBadRequest)
		return
	}
	col, found, err := s.collectionByID(r, sess, id)
	if err != nil {
		s.serverError(w, r, "handleAdminCollection", fmt.Errorf("list collections: %w", err))
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	groups, err := s.store.GetCollectionGroups(r.Context(), id)
	if err != nil {
		s.serverError(w, r, "handleAdminCollection", fmt.Errorf("collection %d groups: %w", id, err))
		return
	}
	// The groups the administrator can add: their own that are not members
	// (the store refuses any group that is not admin-owned).
	own, err := s.store.GetUserGroups(r.Context(), sess.UserID)
	if err != nil {
		s.serverError(w, r, "handleAdminCollection", fmt.Errorf("own groups: %w", err))
		return
	}
	member := map[int64]bool{}
	for _, g := range groups {
		member[g.GroupID] = true
	}
	candidates := make([]db.UserGroup, 0, len(own))
	for _, g := range own {
		if !member[g.GroupID] {
			candidates = append(candidates, g)
		}
	}
	s.render(w, r, "admin_collection", map[string]any{
		"Session": sess, "Notice": adminNotice(r), "Collection": col, "Groups": groups, "Candidates": candidates,
		"MaxName": admin.MaxGroupNameLength, "MaxDescription": admin.MaxCollectionDescriptionLength,
	})
}

func (s *Server) handleAdminCollectionUpdate(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	if !ok {
		http.Error(w, "invalid collection id", http.StatusBadRequest)
		return
	}
	f, ok := collectionFormFields(w, r)
	if !ok {
		return
	}
	if err := s.admin.UpdateCollection(r.Context(), sessionActor(sess), id, f); err != nil {
		s.serviceErr(w, r, "handleAdminCollectionUpdate", err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/collections/%d?ok=saved", id), http.StatusFound)
}

func (s *Server) handleAdminCollectionDeleteConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	if !ok {
		http.Error(w, "invalid collection id", http.StatusBadRequest)
		return
	}
	col, found, err := s.collectionByID(r, sess, id)
	if err != nil {
		s.serverError(w, r, "handleAdminCollectionDeleteConfirm", err)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	s.confirmPage(w, r, sess, "Delete collection",
		fmt.Sprintf("Delete the collection %q? Its groups and their repositories stay; only the collection and its membership go.", col.Name),
		fmt.Sprintf("/admin/collections/%d/delete", id), "Delete", fmt.Sprintf("/admin/collections/%d", id), nil)
}

func (s *Server) handleAdminCollectionDelete(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	if !ok {
		http.Error(w, "invalid collection id", http.StatusBadRequest)
		return
	}
	if err := s.admin.DeleteCollection(r.Context(), sessionActor(sess), id); err != nil {
		s.serviceErr(w, r, "handleAdminCollectionDelete", err)
		return
	}
	http.Redirect(w, r, "/admin/collections?ok=deleted", http.StatusFound)
}

func (s *Server) handleAdminCollectionAddGroup(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	if !ok {
		http.Error(w, "invalid collection id", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form data", http.StatusBadRequest)
		return
	}
	groupID, err := strconv.ParseInt(strings.TrimSpace(r.FormValue("group_id")), 10, 64)
	if err != nil {
		http.Error(w, "group_id must be a number", http.StatusBadRequest)
		return
	}
	if err := s.admin.AddGroupToCollection(r.Context(), sessionActor(sess), id, groupID); err != nil {
		s.serviceErr(w, r, "handleAdminCollectionAddGroup", err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/collections/%d?ok=group_added", id), http.StatusFound)
}

func (s *Server) handleAdminCollectionRemoveGroupConfirm(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	groupID, ok2 := pathID(r, "groupID")
	if !ok || !ok2 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	s.confirmPage(w, r, sess, "Remove a group from the collection",
		fmt.Sprintf("Remove group %d from collection %d? The group and its repositories stay; they leave the collection.", groupID, id),
		fmt.Sprintf("/admin/collections/%d/groups/%d/remove", id, groupID), "Remove", fmt.Sprintf("/admin/collections/%d", id), nil)
}

func (s *Server) handleAdminCollectionRemoveGroup(w http.ResponseWriter, r *http.Request, sess *Session) {
	id, ok := pathID(r, "collectionID")
	groupID, ok2 := pathID(r, "groupID")
	if !ok || !ok2 {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	if err := s.admin.RemoveGroupFromCollection(r.Context(), sessionActor(sess), id, groupID); err != nil {
		s.serviceErr(w, r, "handleAdminCollectionRemoveGroup", err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/collections/%d?ok=group_removed", id), http.StatusFound)
}

// handleAdminCachePage renders the API cache's console page (0.29.123,
// summary/63 §9): a shell. Its script (static/admin-cache.js) reads the
// api's cache view through the /api/ proxy, because the caches live in the
// api process's memory, not in this one or the database.
func (s *Server) handleAdminCachePage(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.render(w, r, "admin_cache", map[string]any{"Session": sess})
}
