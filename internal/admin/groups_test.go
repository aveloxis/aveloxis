// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestCreateGroupBounds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		want    string // the name the store receives; "" = refused
		wantErr error
	}{
		{"trimmed", "  My Group \n", "My Group", nil},
		{"empty", "   ", "", ErrInvalidInput},
		{"at the limit", strings.Repeat("g", MaxGroupNameLength), strings.Repeat("g", MaxGroupNameLength), nil},
		{"over the limit", strings.Repeat("g", MaxGroupNameLength+1), "", ErrInvalidInput},
		{"runes, not bytes", strings.Repeat("é", MaxGroupNameLength), strings.Repeat("é", MaxGroupNameLength), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			st := &fakeStore{}
			if tc.want != "" {
				st.createUserGroup = func(_ context.Context, uid int, name string) (int64, error) {
					got = name
					if uid != 7 {
						t.Errorf("owner = %d, want the actor's 7", uid)
					}
					return 42, nil
				}
			}
			svc, _, logs := testService(t, st, Options{})
			id, err := svc.CreateGroup(context.Background(), Actor{UserID: 7, Kind: Session}, tc.in)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateGroup(%q) = %d, %v; want err %v", tc.in, id, err, tc.wantErr)
			}
			if tc.want != "" && (id != 42 || got != tc.want || !strings.Contains(logs.String(), "admin: create group")) {
				t.Errorf("CreateGroup(%q) = %d, stored %q; want 42, %q, and an audit line:\n%s", tc.in, id, got, tc.want, logs.String())
			}
		})
	}
}

// TestGroupAccessRule (eval-2 item 5): the owner reads and writes; an
// administrator's Session or the Operator reads another account's group
// (logged); an administrator's API token does not; nobody but the owner
// writes; a missing group is NotFound for everyone, including
// administrators.
func TestGroupAccessRule(t *testing.T) {
	const owner, admin, stranger = 7, 1, 9
	for _, tc := range []struct {
		name   string
		a      Actor
		want   Access
		row    bool // the users row says admin
		exists bool
		ok     bool
		err    error
		logged bool
	}{
		{name: "owner reads", a: Actor{UserID: owner, Kind: Session}, want: Read, exists: true, ok: true},
		{name: "owner writes", a: Actor{UserID: owner, Kind: Session}, want: Write, exists: true, ok: true},
		{name: "owner's api token reads", a: Actor{UserID: owner, Kind: APIToken}, want: Read, exists: true, ok: true},
		{name: "owner's api token writes", a: Actor{UserID: owner, Kind: APIToken}, want: Write, exists: true, ok: true},
		{name: "operator for the owner writes", a: Actor{UserID: owner, Kind: Operator}, want: Write, exists: true, ok: true},
		{name: "admin session reads", a: Actor{UserID: admin, Kind: Session}, want: Read, row: true, exists: true, ok: true, logged: true},
		{name: "admin session writes", a: Actor{UserID: admin, Kind: Session}, want: Write, row: true, exists: true, err: db.ErrGroupNotOwned},
		{name: "admin api token reads", a: Actor{UserID: admin, Kind: APIToken}, want: Read, row: true, exists: true, err: db.ErrGroupNotOwned},
		{name: "operator reads", a: Actor{UserID: 0, Kind: Operator}, want: Read, exists: true, ok: true, logged: true},
		{name: "operator writes another account's group", a: Actor{UserID: 0, Kind: Operator}, want: Write, exists: true, err: db.ErrGroupNotOwned},
		{name: "stranger reads", a: Actor{UserID: stranger, Kind: Session}, want: Read, exists: true, err: db.ErrGroupNotOwned},
		{name: "stranger writes", a: Actor{UserID: stranger, Kind: Session}, want: Write, exists: true, err: db.ErrGroupNotOwned},
		{name: "missing group, owner-like id", a: Actor{UserID: owner, Kind: Session}, want: Read, err: db.ErrGroupNotFound},
		{name: "missing group, admin", a: Actor{UserID: admin, Kind: Session}, want: Read, row: true, err: db.ErrGroupNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{
				groupOwner: func(_ context.Context, gid int64) (int, string, error) {
					if !tc.exists {
						return 0, "", db.ErrGroupNotFound
					}
					return owner, "g", nil
				},
				isUserAdmin: adminIs(tc.row),
			}
			svc, _, logs := testService(t, st, Options{})
			ref, err := svc.GroupAccess(context.Background(), tc.a, 5, tc.want)
			if tc.ok {
				if err != nil || ref != (GroupRef{ID: 5, Name: "g", OwnerID: owner}) {
					t.Fatalf("GroupAccess = %+v, %v; want granted", ref, err)
				}
			} else if !errors.Is(err, tc.err) || ref != (GroupRef{}) {
				t.Fatalf("GroupAccess = %+v, %v; want %v", ref, err, tc.err)
			}
			if logged := strings.Contains(logs.String(), "read another account's group"); logged != tc.logged {
				t.Errorf("admin-read INFO logged = %v, want %v:\n%s", logged, tc.logged, logs.String())
			}
		})
	}
	// A failed owner lookup is the failure it is, not "not yours" (SR-5).
	st := &fakeStore{groupOwner: func(context.Context, int64) (int, string, error) { return 0, "", errors.New("closed pool") }}
	svc, _, _ := testService(t, st, Options{})
	_, err := svc.GroupAccess(context.Background(), Actor{UserID: owner, Kind: Session}, 5, Read)
	if _, refused := Classify(err); err == nil || refused {
		t.Errorf("GroupAccess on a failed lookup = %v; want the failure, unclassified", err)
	}
}

func TestAuthorizeGroupRepoRequiresMembership(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   bool
		err  error
		want error
	}{
		{"member", true, nil, nil},
		{"not a member", false, nil, ErrRepoNotInGroup},
		{"lookup failed", false, errors.New("closed pool"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{
				groupOwner:  func(context.Context, int64) (int, string, error) { return 7, "g", nil },
				repoInGroup: func(context.Context, int64, int64) (bool, error) { return tc.in, tc.err },
			}
			svc, _, _ := testService(t, st, Options{})
			ref, err := svc.AuthorizeGroupRepo(context.Background(), Actor{UserID: 7, Kind: Session}, 5, 99)
			switch {
			case tc.err != nil:
				if _, refused := Classify(err); err == nil || refused {
					t.Fatalf("a failed membership read = %v; want the failure", err)
				}
			case tc.want != nil:
				if !errors.Is(err, tc.want) {
					t.Fatalf("err = %v, want %v", err, tc.want)
				}
				if ref, ok := Classify(err); !ok || ref.Kind != NotFound {
					t.Errorf("Classify = %+v", ref)
				}
			default:
				if err != nil || ref.ID != 5 {
					t.Fatalf("= %+v, %v", ref, err)
				}
			}
		})
	}
	// A stranger never reaches the membership read.
	st := &fakeStore{groupOwner: func(context.Context, int64) (int, string, error) { return 7, "g", nil }, isUserAdmin: adminIs(false)}
	svc, _, _ := testService(t, st, Options{})
	if _, err := svc.AuthorizeGroupRepo(context.Background(), Actor{UserID: 9, Kind: Session}, 5, 99); !errors.Is(err, db.ErrGroupNotOwned) {
		t.Errorf("stranger = %v", err)
	}
}

// TestAddToGroupParsesEveryLine: a missing scheme is supplied, a GitHub,
// GitLab or generic git repository URL goes through as its canonical
// spelling, and every other line is reported, redacted, and skipped.
func TestAddToGroupParsesEveryLine(t *testing.T) {
	var got []string
	st := &fakeStore{addReposToGroup: func(_ context.Context, uid int, gid int64, urls []string, limit int, _ string) (db.AddOutcome, error) {
		got = urls
		if uid != 7 || gid != 5 || limit != 3 {
			t.Errorf("store received user %d group %d limit %d; want 7, 5, 3 (the Options' auto-approve limit)", uid, gid, limit)
		}
		return db.AddOutcome{Linked: 1, Enqueued: 2}, nil
	}}
	svc, hooks, logs := testService(t, st, Options{AutoApproveAddLimit: 3})
	out, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, []string{
		"github.com/chaoss/augur",
		"  https://gitlab.com/fdroid/fdroidclient.git  ",
		"https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
		"",
		"not a url",
		"https://github.com",
		"https://user:secret@github.com/a/b",
		"https://github.com/owner/repo/tree/main",
		"git@github.com:chaoss/augur",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"https://github.com/chaoss/augur", "https://gitlab.com/fdroid/fdroidclient", "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux"}
	if !slices.Equal(got, want) {
		t.Errorf("store received %q, want %q", got, want)
	}
	if len(out.Invalid) != 5 {
		t.Errorf("Invalid = %q, want five refused lines", out.Invalid)
	}
	if !slices.ContainsFunc(out.Invalid, func(s string) bool { return strings.Contains(s, "git SSH address") }) {
		t.Errorf("the SCP-form line must be named for what it is, not as credentials: %q", out.Invalid)
	}
	for _, line := range out.Invalid {
		if strings.Contains(line, "secret") {
			t.Errorf("a refused line carries the credential: %q", line)
		}
	}
	if !slices.ContainsFunc(out.Invalid, func(s string) bool { return strings.Contains(s, "credentials") }) {
		t.Errorf("the credentialed line must say why: %q", out.Invalid)
	}
	if out.Linked != 1 || out.Enqueued != 2 {
		t.Errorf("outcome = %+v", out.AddOutcome)
	}
	if h := hooks.got(); !slices.Equal(h, []string{"scope 7"}) {
		t.Errorf("hooks = %v, want the actor's scope", h)
	}
	if !strings.Contains(logs.String(), "admin: add repositories") || !strings.Contains(logs.String(), "invalid=5") {
		t.Errorf("audit line missing or without the refused count:\n%s", logs.String())
	}
}

func TestAddToGroupRefusals(t *testing.T) {
	t.Run("too many lines", func(t *testing.T) {
		svc, _, _ := testService(t, &fakeStore{}, Options{})
		// Non-blank lines: blank ones are not counted (round-2 review).
		paste := make([]string, MaxPasteURLs+1)
		for i := range paste {
			paste[i] = "github.com/o/r"
		}
		_, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, paste)
		if ref, ok := Classify(err); !errors.Is(err, ErrTooMany) || !ok || ref.Kind != TooMany || !strings.Contains(ref.Detail, "1000") {
			t.Errorf("= %v (%+v)", err, ref)
		}
	})
	t.Run("no usable line", func(t *testing.T) {
		svc, _, _ := testService(t, &fakeStore{}, Options{})
		for _, paste := range [][]string{nil, {"", "  "}, {"junk"}} {
			if _, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, paste); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("AddToGroup(%q) = %v; want ErrInvalidInput", paste, err)
			}
		}
	})
	t.Run("the store's refusals pass through, scope hook only when something linked", func(t *testing.T) {
		for _, tc := range []struct {
			err    error
			linked int
			hook   bool
		}{
			{db.ErrGroupNotOwned, 0, false},
			{db.ErrGroupRejected, 0, false},
			{db.ErrURLTooLong, 0, false},
			{&capacity.Exceeded{Quota: "repos"}, 0, false},
			{db.ErrAddItemsFailed, 2, true}, // PR #218 review C1: the others linked
		} {
			st := &fakeStore{addReposToGroup: func(context.Context, int, int64, []string, int, string) (db.AddOutcome, error) {
				return db.AddOutcome{Linked: tc.linked}, tc.err
			}}
			svc, hooks, logs := testService(t, st, Options{})
			out, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, []string{"github.com/a/b"})
			if !errors.Is(err, tc.err) || out.Linked != tc.linked {
				t.Errorf("err %v: = %+v, %v", tc.err, out, err)
			}
			if _, ok := Classify(err); !ok {
				t.Errorf("%v not classified", err)
			}
			if h := hooks.got(); (len(h) == 1) != tc.hook {
				t.Errorf("err %v: hooks = %v, want hook=%v", tc.err, h, tc.hook)
			}
			// A refusal is one INFO line; items the pass could not add are a
			// WARN (the operator should know).
			wantLine, wantLevel := "refused", "level=INFO"
			if errors.Is(tc.err, db.ErrAddItemsFailed) {
				wantLine, wantLevel = "some could not be added", "level=WARN"
			}
			if !strings.Contains(logs.String(), wantLine) || !strings.Contains(logs.String(), wantLevel) {
				t.Errorf("err %v: want %s %q in the log:\n%s", tc.err, wantLevel, wantLine, logs.String())
			}
		}
	})
	t.Run("a store failure is wrapped, not classified", func(t *testing.T) {
		st := &fakeStore{addReposToGroup: func(context.Context, int, int64, []string, int, string) (db.AddOutcome, error) {
			return db.AddOutcome{}, errors.New("closed pool")
		}}
		svc, _, _ := testService(t, st, Options{})
		_, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, []string{"github.com/a/b"})
		if _, ok := Classify(err); err == nil || ok || !strings.Contains(err.Error(), "user 7") {
			t.Errorf("= %v", err)
		}
	})
}

// TestAddToGroupNotifiesAPendingRequest: a paste parked on a request mails
// the operator through the one notifier, from the single-row read.
func TestAddToGroupNotifiesAPendingRequest(t *testing.T) {
	st := &fakeStore{
		addReposToGroup: func(context.Context, int, int64, []string, int, string) (db.AddOutcome, error) {
			return db.AddOutcome{Pending: 2, RequestID: 77}, nil
		},
		getAddRequest: func(_ context.Context, id int64) (db.AddRequest, error) {
			if id != 77 {
				t.Errorf("GetAddRequest(%d), want 77", id)
			}
			return db.AddRequest{RequestID: 77, UserLogin: "alice", GroupName: "g", Kind: "repos", ItemCount: 2, SampleURLs: []string{"https://github.com/a/b"}}, nil
		},
	}
	m := newFakeMailer()
	m.operator = "ops@example.com"
	svc, hooks, _ := testService(t, st, Options{Mailer: m})
	if _, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, []string{"github.com/a/b", "github.com/c/d"}); err != nil {
		t.Fatal(err)
	}
	m.waitMail(t)
	sent := m.mails()
	if len(sent) != 1 || sent[0].kind != "submitted" || sent[0].to != "ops@example.com" || sent[0].reqID != 77 || sent[0].count != 2 || !slices.Equal(sent[0].sample, []string{"https://github.com/a/b"}) {
		t.Errorf("sent = %+v", sent)
	}
	if h := hooks.got(); len(h) != 0 {
		t.Errorf("a pending paste linked nothing; hooks = %v", h)
	}
}

func TestAddOrgToGroup(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		svc, _, _ := testService(t, &fakeStore{}, Options{})
		if _, err := svc.AddOrgToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, "  "); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("= %v", err)
		}
	})
	t.Run("registered: no hook, no mail", func(t *testing.T) {
		st := &fakeStore{addOrgToGroup: func(_ context.Context, uid int, gid int64, u, base, _ string) (db.OrgAddOutcome, error) {
			if u != "https://github.com/chaoss" || base != "https://api.github.com" {
				t.Errorf("store received %q on %q", u, base)
			}
			return db.OrgAddOutcome{Registered: true}, nil
		}}
		m := newFakeMailer()
		m.operator = "ops@example.com"
		svc, hooks, logs := testService(t, st, Options{Mailer: m})
		out, err := svc.AddOrgToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, " https://github.com/chaoss ")
		if err != nil || !out.Registered {
			t.Fatalf("= %+v, %v", out, err)
		}
		if h := hooks.got(); len(h) != 0 {
			t.Errorf("hooks = %v; registering links nothing (PR #218 review C3)", h)
		}
		if len(m.mails()) != 0 || !strings.Contains(logs.String(), "admin: add organization") {
			t.Errorf("mails %v, log:\n%s", m.mails(), logs.String())
		}
	})
	t.Run("pending: the operator is notified", func(t *testing.T) {
		st := &fakeStore{
			addOrgToGroup: func(context.Context, int, int64, string, string, string) (db.OrgAddOutcome, error) {
				return db.OrgAddOutcome{RequestID: 9}, nil
			},
			getAddRequest: func(context.Context, int64) (db.AddRequest, error) {
				return db.AddRequest{RequestID: 9, Kind: "org", OrgURL: "https://github.com/chaoss", UserLogin: "alice", GroupName: "g", ItemCount: 1}, nil
			},
		}
		m := newFakeMailer()
		m.operator = "ops@example.com"
		svc, _, _ := testService(t, st, Options{Mailer: m})
		if _, err := svc.AddOrgToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, "https://github.com/chaoss"); err != nil {
			t.Fatal(err)
		}
		m.waitMail(t)
		if sent := m.mails(); len(sent) != 1 || sent[0].reqKind != "org" || !slices.Equal(sent[0].sample, []string{"https://github.com/chaoss"}) {
			t.Errorf("sent = %+v; an org's sample is its URL", sent)
		}
	})
	t.Run("off host names this deployment's host", func(t *testing.T) {
		st := &fakeStore{addOrgToGroup: func(context.Context, int, int64, string, string, string) (db.OrgAddOutcome, error) {
			return db.OrgAddOutcome{}, db.ErrOrgOffGitHubHost
		}}
		svc, _, _ := testService(t, st, Options{GitHubAPIBase: "https://github.example/api/v3"})
		_, err := svc.AddOrgToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, "https://github.com/chaoss")
		ref, ok := Classify(err)
		if !errors.Is(err, db.ErrOrgOffGitHubHost) || !ok || ref.Kind != OffHost || !strings.Contains(ref.Detail, "(github.example)") {
			t.Errorf("= %v, %+v", err, ref)
		}
	})
}

func TestRemoveFromGroup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		removed bool
		err     error
		hooks   []string
	}{
		{"removed", true, nil, []string{"scope 7"}},
		{"was not in the group", false, nil, nil},
		{"not the owner", false, db.ErrGroupNotOwned, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{removeRepoFromGroup: func(_ context.Context, uid int, gid, rid int64) (bool, error) {
				if uid != 7 || gid != 5 || rid != 99 {
					t.Errorf("store received %d %d %d", uid, gid, rid)
				}
				return tc.removed, tc.err
			}}
			svc, hooks, logs := testService(t, st, Options{})
			removed, err := svc.RemoveFromGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, 99)
			if removed != tc.removed || !errors.Is(err, tc.err) {
				t.Fatalf("= %v, %v", removed, err)
			}
			if h := hooks.got(); !slices.Equal(h, tc.hooks) {
				t.Errorf("hooks = %v, want %v", h, tc.hooks)
			}
			if audited := strings.Contains(logs.String(), `msg="admin: remove repository"`); audited != tc.removed {
				t.Errorf("audited = %v:\n%s", audited, logs.String())
			}
		})
	}
}

// TestAddToGroupCountsOnlyNonBlankLines (round-2 review of 0.29.100): the
// paste bound counts URLs, not lines — a textarea paste of exactly
// MaxPasteURLs URLs ends in a newline, and blank lines between URLs are
// not URLs. One URL more than the bound is still TooMany.
func TestAddToGroupCountsOnlyNonBlankLines(t *testing.T) {
	var got int
	st := &fakeStore{addReposToGroup: func(_ context.Context, _ int, _ int64, urls []string, _ int, _ string) (db.AddOutcome, error) {
		got = len(urls)
		return db.AddOutcome{Enqueued: len(urls)}, nil
	}}
	svc, _, _ := testService(t, st, Options{})
	paste := []string{""}
	for i := 0; i < MaxPasteURLs; i++ {
		paste = append(paste, fmt.Sprintf("https://github.com/o/r%d", i))
		if i == 10 {
			paste = append(paste, "   ")
		}
	}
	paste = append(paste, "")
	if _, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, paste); err != nil {
		t.Fatalf("%d URLs among %d lines = %v; want accepted", MaxPasteURLs, len(paste), err)
	}
	if got != MaxPasteURLs {
		t.Errorf("the store received %d URLs; want %d", got, MaxPasteURLs)
	}
	paste = append(paste, "https://github.com/o/one-more")
	if _, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, paste); !errors.Is(err, ErrTooMany) {
		t.Errorf("%d URLs = %v; want ErrTooMany", MaxPasteURLs+1, err)
	}
}

// TestInstanceWebBaseReachesTheStore (round-2 review of 0.29.100): the
// Options' gitlab.base_url reaches the store's organisation classification
// as the instance's web base, on registration and on approval; without
// one the store receives "".
func TestInstanceWebBaseReachesTheStore(t *testing.T) {
	for _, tc := range []struct{ apiBase, want string }{
		{"https://code.example.org/api/v4/", "https://code.example.org"},
		{"http://127.0.0.1:8080/api/v4", "http://127.0.0.1:8080"},
		{"", ""},
	} {
		var gotAdd, gotDecide, gotRepos, gotPass string
		passed := make(chan string, 1) // the approval pass runs in the background
		st := &fakeStore{
			isUserAdmin: adminIs(true),
			addOrgToGroup: func(_ context.Context, _ int, _ int64, _, _, gl string) (db.OrgAddOutcome, error) {
				gotAdd = gl
				return db.OrgAddOutcome{Registered: true}, nil
			},
			decideAddRequest: func(_ context.Context, _ int64, _ int, _ bool, _, gl string) (db.AddRequest, bool, error) {
				gotDecide = gl
				return db.AddRequest{}, false, nil
			},
			addReposToGroup: func(_ context.Context, _ int, _ int64, _ []string, _ int, gl string) (db.AddOutcome, error) {
				gotRepos = gl
				return db.AddOutcome{Linked: 1}, nil
			},
			processApprovedAddRequest: func(_ context.Context, _ int64, gl string) (int, int, error) {
				passed <- gl
				return 0, 0, nil
			},
		}
		svc, _, _ := testService(t, st, Options{GitLabAPIBase: tc.apiBase})
		if _, err := svc.AddOrgToGroup(context.Background(), adminSession, 5, "https://code.example.org/g"); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.DecideAddRequest(context.Background(), adminSession, 9, true); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.AddToGroup(context.Background(), adminSession, 5, []string{"https://code.example.org/g/p"}); err != nil {
			t.Fatal(err)
		}
		svc.runApprovedPass(db.AddRequest{RequestID: 9, UserID: 7})
		select {
		case gotPass = <-passed:
		case <-time.After(5 * time.Second):
			t.Fatal("the approval pass never reached the store")
		}
		if gotAdd != tc.want || gotDecide != tc.want || gotRepos != tc.want || gotPass != tc.want {
			t.Errorf("gitlab.base_url %q: the store received %q (org add), %q (decide), %q (repos add), %q (approval pass); want %q", tc.apiBase, gotAdd, gotDecide, gotRepos, gotPass, tc.want)
		}
	}
}

// TestAddOrgToGroupOffTheGitLabInstanceIsOffHost (round-3 review of
// 0.29.100): the store's GitLab twin of the host refusal is classified
// OffHost like the GitHub one, naming the configured instance (or that
// none is configured), never the GitHub host.
func TestAddOrgToGroupOffTheGitLabInstanceIsOffHost(t *testing.T) {
	for _, tc := range []struct{ apiBase, want string }{
		{"https://code.example.org/api/v4", "GitLab instance (https://code.example.org)"},
		{"", "no GitLab instance is configured"},
	} {
		st := &fakeStore{
			isUserAdmin: adminIs(true),
			addOrgToGroup: func(context.Context, int, int64, string, string, string) (db.OrgAddOutcome, error) {
				return db.OrgAddOutcome{}, db.ErrOrgOffGitLabInstance
			},
		}
		svc, _, logs := testService(t, st, Options{GitLabAPIBase: tc.apiBase})
		_, err := svc.AddOrgToGroup(context.Background(), adminSession, 5, "https://gitlab.com/elsewhere")
		ref, ok := Classify(err)
		if !errors.Is(err, db.ErrOrgOffGitLabInstance) || !ok || ref.Kind != OffHost || !strings.Contains(ref.Detail, tc.want) || strings.Contains(ref.Detail, "github") {
			t.Errorf("gitlab.base_url %q: = %v (%+v); want OffHost naming %q", tc.apiBase, err, ref, tc.want)
		}
		if !strings.Contains(logs.String(), "refused") {
			t.Errorf("no refusal line:\n%s", logs.String())
		}
	}
}

// TestAddToGroupNamesAnSCPAddressByThePlatformsRule (0.29.106 whole-branch
// review A1): the "a git SSH address" reason is given exactly where the
// platform's one predicate (platform.SCPCloneAddress) says so; a line the
// platform refuses as credentials (`git@host:1234/repo`: an all-digit owner
// reads as a port — the decided collateral) is named as credentials here
// too, never as a git address. The service had a second spelling.
func TestAddToGroupNamesAnSCPAddressByThePlatformsRule(t *testing.T) {
	for _, tc := range []struct {
		line, reason string
	}{
		{"git@github.com:org/repo.git", "git SSH address"},
		{"deploy@gitlab.example.org:group/sub/project", "git SSH address"},
		{"git@host:1234/repo", "credentials"},
		{"token@host:8443/o/n", "credentials"},
		{"user@host:22/x", "credentials"},
	} {
		st := &fakeStore{addReposToGroup: func(_ context.Context, _ int, _ int64, urls []string, _ int, _ string) (db.AddOutcome, error) {
			if len(urls) != 1 || urls[0] != "https://github.com/chaoss/augur" {
				t.Errorf("%q reached the store as %q", tc.line, urls)
			}
			return db.AddOutcome{Linked: 1}, nil
		}}
		svc, _, _ := testService(t, st, Options{})
		out, err := svc.AddToGroup(context.Background(), Actor{UserID: 7, Kind: Session}, 5, []string{tc.line, "https://github.com/chaoss/augur"})
		if err != nil {
			t.Fatalf("%q: %v", tc.line, err)
		}
		if len(out.Invalid) != 1 || !strings.Contains(out.Invalid[0], tc.reason) {
			t.Errorf("%q: Invalid = %q; want one line naming %q", tc.line, out.Invalid, tc.reason)
		}
	}
}

// TestServiceDecidesNoAddressShapeItself pins the owner: the service asks
// the platform whether a line is a clone address or carries credentials
// and spells no "@" rule of its own (comment-stripped; a second spelling is
// the defect A1 found). Scope, declared: the ban is on the "@" and '@'
// literals; a spelling that reaches the byte another way (a raw-string
// regexp, strings.IndexByte(s, 64), a byte compare) is outside it — the
// wiring half (AddToGroup calls platform.SCPCloneAddress) is the pin that
// holds against those.
func TestServiceDecidesNoAddressShapeItself(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageSource(t, "internal/admin", 5))
	if strings.Contains(src, `"@"`) || strings.Contains(src, "'@'") {
		t.Error("internal/admin decides on an \"@\" itself; the one rule is platform.SCPCloneAddress / platform.RefuseURLUserinfo")
	}
	if !strings.Contains(srctest.PackageFunc(t, "internal/admin", "func (s *Service) AddToGroup("), "platform.SCPCloneAddress(") {
		t.Error("AddToGroup does not ask platform.SCPCloneAddress for the git-address reason")
	}
}
