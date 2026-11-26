// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
)

// fakeStore is a Store whose every method is a function field; a nil
// field fails the test when called, so a test states exactly which store
// calls an operation may make.
type fakeStore struct {
	dismissRepositoryHost  func(context.Context, string, string) (int, error)
	listRepositoryHosts    func(context.Context, []string) ([]db.RepositoryHost, error)
	previewRepositoryHost  func(context.Context, string) (db.RepositoryHostImpact, error)
	distrustRepositoryHost func(context.Context, string, string, string) (db.RepositoryHostImpact, bool, error)
	restoreRepositoryHost  func(context.Context, string) (bool, error)
	t                      *testing.T

	createUserGroup           func(ctx context.Context, userID int, name string) (int64, error)
	groupOwner                func(ctx context.Context, groupID int64) (int, string, error)
	repoInGroup               func(ctx context.Context, groupID, repoID int64) (bool, error)
	addReposToGroup           func(ctx context.Context, userID int, groupID int64, urls []string, limit int, glWebBase string) (db.AddOutcome, error)
	addOrgToGroup             func(ctx context.Context, userID int, groupID int64, orgURL, ghAPIBase, glWebBase string) (db.OrgAddOutcome, error)
	removeRepoFromGroup       func(ctx context.Context, userID int, groupID, repoID int64) (bool, error)
	rejectGroup               func(ctx context.Context, groupID int64, adminID int) (bool, error)
	getAddRequest             func(ctx context.Context, requestID int64) (db.AddRequest, error)
	decideAddRequest          func(ctx context.Context, requestID int64, adminID int, approve bool, ghAPIBase, glWebBase string) (db.AddRequest, bool, error)
	processApprovedAddRequest func(ctx context.Context, requestID int64, glWebBase string) (int, int, error)
	isUserAdmin               func(ctx context.Context, userID int) (bool, error)
	setUserAdmin              func(ctx context.Context, userID int, isAdmin bool) error
	listUserSessions          func(ctx context.Context, userID int) ([]db.UserSession, error)
	deleteUserSessions        func(ctx context.Context, userID int) (int64, error)
	userLabel                 func(ctx context.Context, userID int) (string, error)
	getUserEmail              func(ctx context.Context, userID int) (string, error)
	getUserLivePendingEmail   func(ctx context.Context, userID int) (string, error)
	setUserPendingEmail       func(ctx context.Context, userID int, email string) error
	clearUserPendingEmailIf   func(ctx context.Context, userID int, email string) error
	createEmailConfirmation   func(ctx context.Context, userID int, email string) (string, error)
	confirmEmailToken         func(ctx context.Context, token string, userID int) (string, error)
	adoptPendingForgeIDChange func(ctx context.Context, repoID int64, oldID, newID, adoptedBy, note string) error
	// Stage 3b (0.29.109): tokens, capacity, collections.
	getAPITokenSettings       func(ctx context.Context) (db.APITokenSettings, error)
	createAPIToken            func(ctx context.Context, g db.APITokenGrant) (string, db.APIToken, error)
	revokeAPIToken            func(ctx context.Context, tokenID int64, revokedBy int) error
	setAPITokenSettings       func(ctx context.Context, st db.APITokenSettings, updatedBy int) error
	capacitySource            func(name string) capacity.Source
	setCapacityQuota          func(ctx context.Context, name string, allowed int, mode capacity.Mode, by int) error
	setCapacityContact        func(ctx context.Context, email string, by int) error
	setCapacityOverride       func(ctx context.Context, o db.CapacityOverride, by int) error
	addSignupAllowlist        func(ctx context.Context, cidr, note string, by int) (string, error)
	removeSignupAllowlist     func(ctx context.Context, cidr string) (bool, error)
	createCollection          func(ctx context.Context, name, description string, position int, createdBy int) (int64, error)
	updateCollection          func(ctx context.Context, collectionID int64, name, description string, position int) error
	deleteCollection          func(ctx context.Context, collectionID int64) error
	addGroupToCollection      func(ctx context.Context, collectionID, groupID int64) error
	removeGroupFromCollection func(ctx context.Context, collectionID, groupID int64) error
}

func (f *fakeStore) unexpected(name string) {
	f.t.Helper()
	f.t.Errorf("store.%s called; the operation under test must not reach it", name)
}

func (f *fakeStore) CreateUserGroup(ctx context.Context, userID int, name string) (int64, error) {
	if f.createUserGroup == nil {
		f.unexpected("CreateUserGroup")
		return 0, nil
	}
	return f.createUserGroup(ctx, userID, name)
}
func (f *fakeStore) GroupOwner(ctx context.Context, groupID int64) (int, string, error) {
	if f.groupOwner == nil {
		f.unexpected("GroupOwner")
		return 0, "", nil
	}
	return f.groupOwner(ctx, groupID)
}
func (f *fakeStore) RepoInGroup(ctx context.Context, groupID, repoID int64) (bool, error) {
	if f.repoInGroup == nil {
		f.unexpected("RepoInGroup")
		return false, nil
	}
	return f.repoInGroup(ctx, groupID, repoID)
}
func (f *fakeStore) AddReposToGroup(ctx context.Context, userID int, groupID int64, urls []string, limit int, glWebBase string) (db.AddOutcome, error) {
	if f.addReposToGroup == nil {
		f.unexpected("AddReposToGroup")
		return db.AddOutcome{}, nil
	}
	return f.addReposToGroup(ctx, userID, groupID, urls, limit, glWebBase)
}
func (f *fakeStore) AddOrgToGroup(ctx context.Context, userID int, groupID int64, orgURL, ghAPIBase, glWebBase string) (db.OrgAddOutcome, error) {
	if f.addOrgToGroup == nil {
		f.unexpected("AddOrgToGroup")
		return db.OrgAddOutcome{}, nil
	}
	return f.addOrgToGroup(ctx, userID, groupID, orgURL, ghAPIBase, glWebBase)
}
func (f *fakeStore) RemoveRepoFromGroup(ctx context.Context, userID int, groupID, repoID int64) (bool, error) {
	if f.removeRepoFromGroup == nil {
		f.unexpected("RemoveRepoFromGroup")
		return false, nil
	}
	return f.removeRepoFromGroup(ctx, userID, groupID, repoID)
}
func (f *fakeStore) RejectGroup(ctx context.Context, groupID int64, adminID int) (bool, error) {
	if f.rejectGroup == nil {
		f.unexpected("RejectGroup")
		return false, nil
	}
	return f.rejectGroup(ctx, groupID, adminID)
}
func (f *fakeStore) GetAddRequest(ctx context.Context, requestID int64) (db.AddRequest, error) {
	if f.getAddRequest == nil {
		f.unexpected("GetAddRequest")
		return db.AddRequest{}, nil
	}
	return f.getAddRequest(ctx, requestID)
}
func (f *fakeStore) DecideAddRequest(ctx context.Context, requestID int64, adminID int, approve bool, ghAPIBase, glWebBase string) (db.AddRequest, bool, error) {
	if f.decideAddRequest == nil {
		f.unexpected("DecideAddRequest")
		return db.AddRequest{}, false, nil
	}
	return f.decideAddRequest(ctx, requestID, adminID, approve, ghAPIBase, glWebBase)
}
func (f *fakeStore) ProcessApprovedAddRequest(ctx context.Context, requestID int64, glWebBase string) (int, int, error) {
	if f.processApprovedAddRequest == nil {
		f.unexpected("ProcessApprovedAddRequest")
		return 0, 0, nil
	}
	return f.processApprovedAddRequest(ctx, requestID, glWebBase)
}
func (f *fakeStore) IsUserAdmin(ctx context.Context, userID int) (bool, error) {
	if f.isUserAdmin == nil {
		f.unexpected("IsUserAdmin")
		return false, nil
	}
	return f.isUserAdmin(ctx, userID)
}
func (f *fakeStore) SetUserAdmin(ctx context.Context, userID int, isAdmin bool) error {
	if f.setUserAdmin == nil {
		f.unexpected("SetUserAdmin")
		return nil
	}
	return f.setUserAdmin(ctx, userID, isAdmin)
}
func (f *fakeStore) ListUserSessions(ctx context.Context, userID int) ([]db.UserSession, error) {
	if f.listUserSessions == nil {
		f.unexpected("ListUserSessions")
		return nil, nil
	}
	return f.listUserSessions(ctx, userID)
}
func (f *fakeStore) DeleteUserSessions(ctx context.Context, userID int) (int64, error) {
	if f.deleteUserSessions == nil {
		f.unexpected("DeleteUserSessions")
		return 0, nil
	}
	return f.deleteUserSessions(ctx, userID)
}
func (f *fakeStore) UserLabel(ctx context.Context, userID int) (string, error) {
	if f.userLabel == nil {
		f.unexpected("UserLabel")
		return "", nil
	}
	return f.userLabel(ctx, userID)
}
func (f *fakeStore) GetUserEmail(ctx context.Context, userID int) (string, error) {
	if f.getUserEmail == nil {
		f.unexpected("GetUserEmail")
		return "", nil
	}
	return f.getUserEmail(ctx, userID)
}
func (f *fakeStore) GetUserLivePendingEmail(ctx context.Context, userID int) (string, error) {
	if f.getUserLivePendingEmail == nil {
		f.unexpected("GetUserLivePendingEmail")
		return "", nil
	}
	return f.getUserLivePendingEmail(ctx, userID)
}
func (f *fakeStore) SetUserPendingEmail(ctx context.Context, userID int, email string) error {
	if f.setUserPendingEmail == nil {
		f.unexpected("SetUserPendingEmail")
		return nil
	}
	return f.setUserPendingEmail(ctx, userID, email)
}
func (f *fakeStore) ClearUserPendingEmailIf(ctx context.Context, userID int, email string) error {
	if f.clearUserPendingEmailIf == nil {
		f.unexpected("ClearUserPendingEmailIf")
		return nil
	}
	return f.clearUserPendingEmailIf(ctx, userID, email)
}
func (f *fakeStore) CreateEmailConfirmation(ctx context.Context, userID int, email string) (string, error) {
	if f.createEmailConfirmation == nil {
		f.unexpected("CreateEmailConfirmation")
		return "", nil
	}
	return f.createEmailConfirmation(ctx, userID, email)
}
func (f *fakeStore) ConfirmEmailToken(ctx context.Context, token string, userID int) (string, error) {
	if f.confirmEmailToken == nil {
		f.unexpected("ConfirmEmailToken")
		return "", nil
	}
	return f.confirmEmailToken(ctx, token, userID)
}
func (f *fakeStore) AdoptPendingForgeIDChange(ctx context.Context, repoID int64, oldID, newID, adoptedBy, note string) error {
	if f.adoptPendingForgeIDChange == nil {
		f.unexpected("AdoptPendingForgeIDChange")
		return nil
	}
	return f.adoptPendingForgeIDChange(ctx, repoID, oldID, newID, adoptedBy, note)
}

var _ Store = (*fakeStore)(nil)

// adminIs is an IsUserAdmin that answers the same for every user.
func adminIs(v bool) func(context.Context, int) (bool, error) {
	return func(context.Context, int) (bool, error) { return v, nil }
}

func (f *fakeStore) GetAPITokenSettings(ctx context.Context) (db.APITokenSettings, error) {
	if f.getAPITokenSettings == nil {
		f.unexpected("GetAPITokenSettings")
		return db.APITokenSettings{}, nil
	}
	return f.getAPITokenSettings(ctx)
}
func (f *fakeStore) CreateAPIToken(ctx context.Context, g db.APITokenGrant) (string, db.APIToken, error) {
	if f.createAPIToken == nil {
		f.unexpected("CreateAPIToken")
		return "", db.APIToken{}, nil
	}
	return f.createAPIToken(ctx, g)
}
func (f *fakeStore) RevokeAPIToken(ctx context.Context, tokenID int64, revokedBy int) error {
	if f.revokeAPIToken == nil {
		f.unexpected("RevokeAPIToken")
		return nil
	}
	return f.revokeAPIToken(ctx, tokenID, revokedBy)
}
func (f *fakeStore) SetAPITokenSettings(ctx context.Context, st db.APITokenSettings, updatedBy int) error {
	if f.setAPITokenSettings == nil {
		f.unexpected("SetAPITokenSettings")
		return nil
	}
	return f.setAPITokenSettings(ctx, st, updatedBy)
}
func (f *fakeStore) CapacitySource(name string) capacity.Source {
	if f.capacitySource == nil {
		return capacity.SourceWeb
	}
	return f.capacitySource(name)
}
func (f *fakeStore) SetCapacityQuota(ctx context.Context, name string, allowed int, mode capacity.Mode, by int) error {
	if f.setCapacityQuota == nil {
		f.unexpected("SetCapacityQuota")
		return nil
	}
	return f.setCapacityQuota(ctx, name, allowed, mode, by)
}
func (f *fakeStore) SetCapacityContact(ctx context.Context, email string, by int) error {
	if f.setCapacityContact == nil {
		f.unexpected("SetCapacityContact")
		return nil
	}
	return f.setCapacityContact(ctx, email, by)
}
func (f *fakeStore) SetCapacityOverride(ctx context.Context, o db.CapacityOverride, by int) error {
	if f.setCapacityOverride == nil {
		f.unexpected("SetCapacityOverride")
		return nil
	}
	return f.setCapacityOverride(ctx, o, by)
}
func (f *fakeStore) AddSignupAllowlist(ctx context.Context, cidr, note string, by int) (string, error) {
	if f.addSignupAllowlist == nil {
		f.unexpected("AddSignupAllowlist")
		return "", nil
	}
	return f.addSignupAllowlist(ctx, cidr, note, by)
}
func (f *fakeStore) RemoveSignupAllowlist(ctx context.Context, cidr string) (bool, error) {
	if f.removeSignupAllowlist == nil {
		f.unexpected("RemoveSignupAllowlist")
		return false, nil
	}
	return f.removeSignupAllowlist(ctx, cidr)
}
func (f *fakeStore) CreateCollection(ctx context.Context, name, description string, position int, createdBy int) (int64, error) {
	if f.createCollection == nil {
		f.unexpected("CreateCollection")
		return 0, nil
	}
	return f.createCollection(ctx, name, description, position, createdBy)
}
func (f *fakeStore) UpdateCollection(ctx context.Context, collectionID int64, name, description string, position int) error {
	if f.updateCollection == nil {
		f.unexpected("UpdateCollection")
		return nil
	}
	return f.updateCollection(ctx, collectionID, name, description, position)
}
func (f *fakeStore) DeleteCollection(ctx context.Context, collectionID int64) error {
	if f.deleteCollection == nil {
		f.unexpected("DeleteCollection")
		return nil
	}
	return f.deleteCollection(ctx, collectionID)
}
func (f *fakeStore) AddGroupToCollection(ctx context.Context, collectionID, groupID int64) error {
	if f.addGroupToCollection == nil {
		f.unexpected("AddGroupToCollection")
		return nil
	}
	return f.addGroupToCollection(ctx, collectionID, groupID)
}
func (f *fakeStore) RemoveGroupFromCollection(ctx context.Context, collectionID, groupID int64) error {
	if f.removeGroupFromCollection == nil {
		f.unexpected("RemoveGroupFromCollection")
		return nil
	}
	return f.removeGroupFromCollection(ctx, collectionID, groupID)
}

// sentMail records one mailer call.
type sentMail struct {
	kind     string // "confirmation" | "submitted" | "decided"
	to       string
	login    string
	link     string
	lifetime time.Duration
	group    string
	reqKind  string
	count    int
	sample   []string
	reqID    int64
	approved bool
}

// fakeMailer records sends; done is signalled after each (the Service
// mails in the background).
type fakeMailer struct {
	mu       sync.Mutex
	site     string
	enabled  bool
	operator string
	sendErr  error
	sent     []sentMail
	done     chan struct{}
}

func newFakeMailer() *fakeMailer { return &fakeMailer{done: make(chan struct{}, 16)} }

func (f *fakeMailer) record(m sentMail) error {
	f.mu.Lock()
	f.sent = append(f.sent, m)
	f.mu.Unlock()
	if f.done != nil {
		f.done <- struct{}{}
	}
	return f.sendErr
}
func (f *fakeMailer) Enabled() bool         { return f.enabled }
func (f *fakeMailer) SiteURL() string       { return f.site }
func (f *fakeMailer) OperatorEmail() string { return f.operator }
func (f *fakeMailer) SendEmailConfirmation(to, login, link string, lifetime time.Duration) error {
	return f.record(sentMail{kind: "confirmation", to: to, login: login, link: link, lifetime: lifetime})
}
func (f *fakeMailer) SendAddRequestSubmitted(to, login, group, kind string, count int, sample []string, reqID int64) error {
	return f.record(sentMail{kind: "submitted", to: to, login: login, group: group, reqKind: kind, count: count, sample: sample, reqID: reqID})
}
func (f *fakeMailer) SendAddRequestDecided(to, login, group, kind string, approved bool, count int) error {
	return f.record(sentMail{kind: "decided", to: to, login: login, group: group, reqKind: kind, approved: approved, count: count})
}

// mails returns a copy of what was sent.
func (f *fakeMailer) mails() []sentMail {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentMail(nil), f.sent...)
}

// waitMail waits for the next background send.
func (f *fakeMailer) waitMail(t *testing.T) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(10 * time.Second):
		t.Fatal("no mail was sent within 10s")
	}
}

var _ Mailer = (*fakeMailer)(nil)

// recHooks records the hooks the Service called, in order.
type recHooks struct {
	mu    sync.Mutex
	calls []string
	done  chan struct{}
}

func newRecHooks() *recHooks { return &recHooks{done: make(chan struct{}, 16)} }

func (h *recHooks) add(s string) {
	h.mu.Lock()
	h.calls = append(h.calls, s)
	h.mu.Unlock()
	if h.done != nil {
		h.done <- struct{}{}
	}
}
func (h *recHooks) ScopeChanged(ids ...int) {
	var b strings.Builder
	b.WriteString("scope")
	for _, id := range ids {
		b.WriteString(" ")
		b.WriteString(itoa(id))
	}
	h.add(b.String())
}
func (h *recHooks) RoleChanged(id int) { h.add("role " + itoa(id)) }
func (h *recHooks) TokensChanged()     { h.add("tokens") }
func (h *recHooks) got() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...)
}

// waitHook waits for the next hook call (the pass runs in the background).
func (h *recHooks) waitHook(t *testing.T) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		t.Fatal("no hook ran within 10s")
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// logSink is a log buffer safe to read while a background goroutine logs.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logSink) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *logSink) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitLog polls the sink until want appears.
func (b *logSink) waitLog(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(b.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q in the log within 10s; log:\n%s", want, b.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// testService builds a Service over the fake store with recording hooks
// and a log sink.
func testService(t *testing.T, st *fakeStore, opts Options) (*Service, *recHooks, *logSink) {
	t.Helper()
	st.t = t
	hooks := newRecHooks()
	logs := &logSink{}
	return New(st, opts, hooks, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))), hooks, logs
}

func (f *fakeStore) ListRepositoryHosts(ctx context.Context, also []string) ([]db.RepositoryHost, error) {
	if f.listRepositoryHosts == nil {
		f.unexpected("ListRepositoryHosts")
		return nil, nil
	}
	return f.listRepositoryHosts(ctx, also)
}
func (f *fakeStore) PreviewRepositoryHostDistrust(ctx context.Context, host string) (db.RepositoryHostImpact, error) {
	if f.previewRepositoryHost == nil {
		f.unexpected("PreviewRepositoryHostDistrust")
		return db.RepositoryHostImpact{}, nil
	}
	return f.previewRepositoryHost(ctx, host)
}
func (f *fakeStore) DistrustAndPurgeRepositoryHost(ctx context.Context, host, by, note string) (db.RepositoryHostImpact, bool, error) {
	if f.distrustRepositoryHost == nil {
		f.unexpected("DistrustAndPurgeRepositoryHost")
		return db.RepositoryHostImpact{}, false, nil
	}
	return f.distrustRepositoryHost(ctx, host, by, note)
}
func (f *fakeStore) RestoreRepositoryHost(ctx context.Context, host string) (bool, error) {
	if f.restoreRepositoryHost == nil {
		f.unexpected("RestoreRepositoryHost")
		return false, nil
	}
	return f.restoreRepositoryHost(ctx, host)
}
func (f *fakeStore) DismissRepositoryHostSignals(ctx context.Context, host, by string) (int, error) {
	if f.dismissRepositoryHost == nil {
		f.unexpected("DismissRepositoryHostSignals")
		return 0, nil
	}
	return f.dismissRepositoryHost(ctx, host, by)
}
