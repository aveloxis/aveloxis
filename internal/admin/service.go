// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package admin is the one owner of every account-facing administrative
// operation: groups, additions and their approval, roles, forge-ID
// adoption and the account e-mail flow (summary/59 §2; the target shape is
// summary/59-refactor-review/eval-2-service.md). The web GUI, the REST API
// and the CLI are adapters over this Service: they parse their transport,
// call one operation, and render its typed outcome. The Service decides who
// is an administrator (never the caller), applies the bounds, writes
// through the store, logs the audit line naming the actor, calls the
// process's cache hooks after the commit, and sends the mail — in that
// order, for every operation.
//
// Allowed imports: db, model, capacity, mailer, safego, platform (the URL
// parser), and httpserver for its request-context failure logger only.
// Not net/http, web, api, scheduler, collector or config
// (scripts/admin_service_pins_test.go).
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// ActorKind is the credential the caller presented. The Service decides
// administrator privilege from it (eval-2's SR-18 crux): an API token is
// never an administrator, whatever its owner's role, and the CLI always is.
type ActorKind int

const (
	// Session is a signed-in person: a browser session in the web process,
	// or a bearer session token at the API. An administrator only when the
	// users row says so, read fresh for every operation.
	Session ActorKind = iota + 1
	// APIToken is an operator-issued API token: scoped to its owner's
	// groups for reads and never an administrator (Copilot review
	// 5476707567 on PR #228). The API also marks the request context
	// db.WithoutAdminPrivilege, so the store agrees.
	APIToken
	// Operator is the command line: the person at the deployment's shell.
	// Passes every administrator gate (requireAdmin); UserID is the account
	// an operation acts FOR (a loader's dashboard user), or 0 — and the
	// store's own admin/non-admin split (a non-admin account's new
	// organisations and repositories pend for approval) reads THAT
	// account's row, so a loader run for a non-admin user pends its
	// additions rather than registering them.
	Operator
)

// Actor is who asks: the account and the credential it presented.
type Actor struct {
	UserID int
	Kind   ActorKind
	// SignedInAt is the sign-in behind a Session credential: the browser
	// session's creation, or that of the web session a bearer token was
	// minted from. Zero when the adapter does not know it (a parentless
	// token, a planted test session), which is never recent: a sensitive
	// change to the actor's own account needs it within Options.SessionIdle
	// (ASVS V7.5.2; db.SignInIsRecent).
	SignedInAt time.Time
}

func (a Actor) String() string {
	switch a.Kind {
	case Session:
		return fmt.Sprintf("user %d (session)", a.UserID)
	case APIToken:
		return fmt.Sprintf("user %d (api token)", a.UserID)
	case Operator:
		return fmt.Sprintf("operator (for user %d)", a.UserID)
	}
	return fmt.Sprintf("user %d (unknown credential)", a.UserID)
}

// Options is the configuration an adapter's process hands the Service
// once; the Service never reads aveloxis.json itself.
type Options struct {
	// GitHubAPIBase is github.base_url; New normalises it through
	// platform.GitHubAPIBaseOrPublic so it is never empty. It travels with
	// every organisation registration (the host gate).
	GitHubAPIBase string
	// GitLabAPIBase is gitlab.base_url (may be empty: no GitLab instance).
	// New derives the instance's web base from it; it travels with every
	// organisation registration and approval so a group on that instance
	// is classified GitLab whatever its host name says (round-2 review of
	// 0.29.100).
	GitLabAPIBase string
	// AutoApproveAddLimit is web.auto_approve_add_limit: a non-admin's paste
	// of at most this many new repositories is approved without review; 0
	// (the default) means every one waits for an administrator. Never orgs.
	AutoApproveAddLimit int
	// SessionIdle is web.session_idle_minutes as a duration: the
	// inactivity bound, and so the "recent sign-in" a sensitive change to
	// the actor's own account needs (ending every session of it). Zero
	// (unset; the CLI) means no bound: the rule passes.
	SessionIdle time.Duration
	// Mailer sends the add-request notifications and the account
	// confirmation link (*mailer.Mailer); nil when mail is not configured.
	// A nil *mailer.Mailer must be passed as a nil interface.
	Mailer Mailer
	// DevMode is web.dev_mode: the only state in which a confirmation link
	// may be built from a loopback request Host (account.go).
	DevMode bool
	// SPAURL is web.spa_url: when set, the confirmation link lands on the
	// front end's profile page instead of the web process's own route.
	SPAURL string
}

// Hooks are what the adapter's process does after a write commits: its
// caches are process-local, so each process registers its own. nil is a
// no-op (the CLI). A write made in one process reaches the OTHER process's
// caches only after their TTLs (the API's token cache, its home list) —
// a recorded limit, not a bus (eval-2 item 6).
type Hooks interface {
	// ScopeChanged: these users' repository scope changed (a link, a
	// removal, an approved request's pass).
	ScopeChanged(userIDs ...int)
	// RoleChanged: this user's administrator role changed.
	RoleChanged(userID int)
	// TokensChanged: an API token was revoked; every cached validation in
	// the process is stale (stage 3b).
	TokensChanged()
}

// Store is what the Service needs from the database: the narrow surfaces
// below, which *db.PostgresStore satisfies without change. The write
// methods among them are called from the Service only
// (scripts/admin_service_pins_test.go).
type Store interface {
	groupStore
	addRequestStore
	userStore
	accountStore
	forgeIDStore
	tokenStore
	capacityStore
	collectionStore
}

// tokenStore, capacityStore and collectionStore are the console's
// operations (stage 3b, 0.29.109): the writes are the service's alone.
type tokenStore interface {
	GetAPITokenSettings(ctx context.Context) (db.APITokenSettings, error)
	CreateAPIToken(ctx context.Context, g db.APITokenGrant) (string, db.APIToken, error)
	RevokeAPIToken(ctx context.Context, tokenID int64, revokedBy int) error
	SetAPITokenSettings(ctx context.Context, st db.APITokenSettings, updatedBy int) error
}

type capacityStore interface {
	CapacitySource(name string) capacity.Source
	SetCapacityQuota(ctx context.Context, name string, allowed int, mode capacity.Mode, by int) error
	SetCapacityContact(ctx context.Context, email string, by int) error
	SetCapacityOverride(ctx context.Context, o db.CapacityOverride, by int) error
	AddSignupAllowlist(ctx context.Context, cidr, note string, by int) (string, error)
	RemoveSignupAllowlist(ctx context.Context, cidr string) (bool, error)
}

type collectionStore interface {
	CreateCollection(ctx context.Context, name, description string, position int, createdBy int) (int64, error)
	UpdateCollection(ctx context.Context, collectionID int64, name, description string, position int) error
	DeleteCollection(ctx context.Context, collectionID int64) error
	AddGroupToCollection(ctx context.Context, collectionID, groupID int64) error
	RemoveGroupFromCollection(ctx context.Context, collectionID, groupID int64) error
}

type groupStore interface {
	CreateUserGroup(ctx context.Context, userID int, name string) (int64, error)
	GroupOwner(ctx context.Context, groupID int64) (ownerID int, name string, err error)
	RepoInGroup(ctx context.Context, groupID, repoID int64) (bool, error)
	AddReposToGroup(ctx context.Context, userID int, groupID int64, repoURLs []string, autoApproveLimit int, glWebBase string) (db.AddOutcome, error)
	AddOrgToGroup(ctx context.Context, userID int, groupID int64, orgURL, ghAPIBase, glWebBase string) (db.OrgAddOutcome, error)
	RemoveRepoFromGroup(ctx context.Context, userID int, groupID, repoID int64) (bool, error)
	// RejectGroup reports whether it changed the row: a group already
	// rejected (or gone between the owner read and the flip) is no change.
	RejectGroup(ctx context.Context, groupID int64, adminID int) (bool, error)
}

type addRequestStore interface {
	GetAddRequest(ctx context.Context, requestID int64) (db.AddRequest, error)
	DecideAddRequest(ctx context.Context, requestID int64, adminID int, approve bool, ghAPIBase, glWebBase string) (db.AddRequest, bool, error)
	ProcessApprovedAddRequest(ctx context.Context, requestID int64, glWebBase string) (processed, failed int, err error)
}

type userStore interface {
	IsUserAdmin(ctx context.Context, userID int) (bool, error)
	SetUserAdmin(ctx context.Context, userID int, isAdmin bool) error
	// 0.29.113 (ASVS V7.5.2, V7.4.5): an account's sessions, listed and ended.
	ListUserSessions(ctx context.Context, userID int) ([]db.UserSession, error)
	DeleteUserSessions(ctx context.Context, userID int) (int64, error)
	UserLabel(ctx context.Context, userID int) (string, error)
}

type accountStore interface {
	GetUserEmail(ctx context.Context, userID int) (string, error)
	GetUserLivePendingEmail(ctx context.Context, userID int) (string, error)
	SetUserPendingEmail(ctx context.Context, userID int, email string) error
	ClearUserPendingEmailIf(ctx context.Context, userID int, email string) error
	CreateEmailConfirmation(ctx context.Context, userID int, email string) (string, error)
	ConfirmEmailToken(ctx context.Context, token string, userID int) (string, error)
}

type forgeIDStore interface {
	AdoptPendingForgeIDChange(ctx context.Context, repoID int64, oldID, newID, adoptedBy, note string) error
}

var _ Store = (*db.PostgresStore)(nil)

// Service is the administrative operations over one store, for one process.
type Service struct {
	st     Store
	opts   Options
	hooks  Hooks
	logger *slog.Logger
	// glWebBase is the configured GitLab instance's web base ("" without
	// one): platform.GitLabWebBaseForAPI(opts.GitLabAPIBase), derived once.
	glWebBase string
}

// New builds the Service for one process. hooks may be nil (the CLI);
// logger may not: every failure, every audit line and every refusal of
// the store's is logged (an input refusal decided here, before any
// store call, is answered to the caller and not logged — except a paste
// with no usable line (its refused lines, INFO), a self-demotion (WARN)
// and an undeliverable account address (INFO), each logged for the
// operator on purpose).
func New(st Store, opts Options, hooks Hooks, logger *slog.Logger) *Service {
	if logger == nil {
		panic("admin.New: a logger is required")
	}
	opts.GitHubAPIBase = platform.GitHubAPIBaseOrPublic(opts.GitHubAPIBase)
	if hooks == nil {
		hooks = noHooks{}
	}
	glWebBase, _ := platform.GitLabWebBaseForAPI(opts.GitLabAPIBase)
	return &Service{st: st, opts: opts, hooks: hooks, logger: logger, glWebBase: glWebBase}
}

type noHooks struct{}

func (noHooks) ScopeChanged(...int) {}
func (noHooks) RoleChanged(int)     {}
func (noHooks) TokensChanged()      {}

// Options returns the effective options (the normalised GitHub base).
func (s *Service) Options() Options { return s.opts }

// gitlabHints is the configured instance's hint list for the URL
// classifiers (platform.GitLabWebHints; nil without an instance).
func (s *Service) gitlabHints() []string { return platform.GitLabWebHints(s.glWebBase) }

// ErrNotAdmin is the refusal when the actor is not an administrator: what
// every administrative operation returns before touching anything.
var ErrNotAdmin = errors.New("administrator access required")

// IsAdmin decides administrator privilege from the credential: an
// Operator always, an APIToken never, a Session when the users row says
// so — read now, not from a cached flag. A lookup error is returned, never
// read as "not an administrator" (SR-5).
func (s *Service) IsAdmin(ctx context.Context, a Actor) (bool, error) {
	switch a.Kind {
	case Operator:
		return true, nil
	case APIToken:
		return false, nil
	case Session:
		admin, err := s.st.IsUserAdmin(ctx, a.UserID)
		if err != nil {
			return false, fmt.Errorf("admin flag for user %d: %w", a.UserID, err)
		}
		return admin, nil
	}
	return false, nil
}

// requireAdmin is the first step of every administrative operation.
func (s *Service) requireAdmin(ctx context.Context, a Actor, op string) error {
	admin, err := s.IsAdmin(ctx, a)
	if err != nil {
		return err
	}
	if !admin {
		s.logger.Warn("admin operation refused: not an administrator", "op", op, "actor", a.String())
		return fmt.Errorf("%s by %s: %w", op, a, ErrNotAdmin)
	}
	return nil
}

// refused is the one line for a refusal the Service returns to its
// adapter: the error is a classified refusal (admin.Classify) on the
// caller's input or state — INFO, never a failure (WARN is for what the
// operator must act on; the API pins that a refused add is no WARN).
func (s *Service) refused(op string, a Actor, err error, attrs ...any) {
	s.logger.Info("admin: "+op+" refused", append(append([]any{"actor", a.String()}, attrs...), "error", err)...)
}

// audit is the INFO line every operation that changed something writes,
// naming the actor (summary/59 C5): the record an operator greps for.
func (s *Service) audit(op string, a Actor, attrs ...any) {
	s.logger.Info("admin: "+op, append([]any{"actor", a.String()}, attrs...)...)
}
