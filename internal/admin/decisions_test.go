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

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

var adminSession = Actor{UserID: 1, Kind: Session}

func pendingReq(kind string) db.AddRequest {
	return db.AddRequest{RequestID: 77, UserID: 7, UserLogin: "alice", UserEmail: "alice@example.com", GroupID: 5, GroupName: "g", Kind: kind, Status: "pending", ItemCount: 3}
}

// TestDecideAddRequestApproval: a first approval of a repos request runs
// the pass in the background (the admin's response does not wait), drops
// the requester's scope now and again when the pass ends, and mails the
// requester once.
func TestDecideAddRequestApproval(t *testing.T) {
	passed := make(chan int64, 1)
	st := &fakeStore{
		isUserAdmin: adminIs(true),
		decideAddRequest: func(_ context.Context, id int64, adminID int, approve bool, base, _ string) (db.AddRequest, bool, error) {
			if id != 77 || adminID != 1 || !approve || base != "https://api.github.com" {
				t.Errorf("store received %d %d %v %q", id, adminID, approve, base)
			}
			r := pendingReq("repos")
			r.Status = "approved"
			return r, true, nil
		},
		processApprovedAddRequest: func(ctx context.Context, id int64, _ string) (int, int, error) {
			if ctx.Err() != nil {
				t.Error("the pass must run detached from the admin's request")
			}
			passed <- id
			return 3, 0, nil
		},
	}
	m := newFakeMailer()
	svc, hooks, logs := testService(t, st, Options{Mailer: m})
	changed, err := svc.DecideAddRequest(context.Background(), adminSession, 77, true)
	if err != nil || !changed {
		t.Fatalf("= %v, %v", changed, err)
	}
	if id := <-passed; id != 77 {
		t.Errorf("pass ran for %d", id)
	}
	m.waitMail(t)
	hooks.waitHook(t)
	hooks.waitHook(t)
	if h := hooks.got(); !slices.Equal(h, []string{"scope 7", "scope 7"}) {
		t.Errorf("hooks = %v; want the requester's scope at the decision and at the pass's end", h)
	}
	if sent := m.mails(); len(sent) != 1 || sent[0].kind != "decided" || sent[0].to != "alice@example.com" || !sent[0].approved || sent[0].count != 3 {
		t.Errorf("sent = %+v", sent)
	}
	logs.waitLog(t, "approved add-request processed")
	if !strings.Contains(logs.String(), "admin: decide add request") || !strings.Contains(logs.String(), "actor=\"user 1 (session)\"") {
		t.Errorf("audit line must name the actor:\n%s", logs.String())
	}
}

// TestDecideAddRequestOrgApprovalListsNothing: an org approval registers
// in the store and runs no pass here (summary/59 C2); the requester is
// mailed and their scope dropped (serve links later; the TTL covers it).
func TestDecideAddRequestOrgApprovalListsNothing(t *testing.T) {
	st := &fakeStore{
		isUserAdmin: adminIs(true),
		decideAddRequest: func(context.Context, int64, int, bool, string, string) (db.AddRequest, bool, error) {
			r := pendingReq("org")
			r.Status = "approved"
			return r, true, nil
		},
	}
	m := newFakeMailer()
	svc, hooks, _ := testService(t, st, Options{Mailer: m})
	if changed, err := svc.DecideAddRequest(context.Background(), adminSession, 77, true); err != nil || !changed {
		t.Fatalf("= %v, %v", changed, err)
	}
	m.waitMail(t)
	if h := hooks.got(); !slices.Equal(h, []string{"scope 7"}) {
		t.Errorf("hooks = %v", h)
	}
}

func TestDecideAddRequestRejection(t *testing.T) {
	st := &fakeStore{
		isUserAdmin: adminIs(true),
		decideAddRequest: func(_ context.Context, _ int64, _ int, approve bool, _, _ string) (db.AddRequest, bool, error) {
			if approve {
				t.Error("approve = true on a rejection")
			}
			r := pendingReq("repos")
			r.Status = "rejected"
			return r, true, nil
		},
	}
	m := newFakeMailer()
	svc, hooks, _ := testService(t, st, Options{Mailer: m})
	if changed, err := svc.DecideAddRequest(context.Background(), adminSession, 77, false); err != nil || !changed {
		t.Fatalf("= %v, %v", changed, err)
	}
	m.waitMail(t)
	if sent := m.mails(); len(sent) != 1 || sent[0].approved {
		t.Errorf("sent = %+v", sent)
	}
	if h := hooks.got(); !slices.Equal(h, []string{"scope 7"}) {
		t.Errorf("hooks = %v", h)
	}
}

// TestDecideAddRequestSecondClick: an already-decided request changes
// nothing and mails nobody; re-approving an APPROVED repos request resumes
// its pass (no mail); re-approving an approved org request does nothing.
func TestDecideAddRequestSecondClick(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kind     string
		status   string
		approve  bool
		wantPass bool
	}{
		{"re-approve approved repos resumes the pass", "repos", "approved", true, true},
		{"re-approve approved org", "org", "approved", true, false},
		{"re-reject rejected", "repos", "rejected", false, false},
		{"approve a rejected request", "repos", "rejected", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			passed := make(chan struct{}, 1)
			st := &fakeStore{
				isUserAdmin: adminIs(true),
				decideAddRequest: func(context.Context, int64, int, bool, string, string) (db.AddRequest, bool, error) {
					r := pendingReq(tc.kind)
					r.Status = tc.status
					return r, false, nil
				},
				processApprovedAddRequest: func(context.Context, int64, string) (int, int, error) {
					passed <- struct{}{}
					return 0, 0, nil
				},
			}
			m := newFakeMailer()
			svc, hooks, logs := testService(t, st, Options{Mailer: m})
			changed, err := svc.DecideAddRequest(context.Background(), adminSession, 77, tc.approve)
			if err != nil || changed {
				t.Fatalf("= %v, %v; want unchanged", changed, err)
			}
			if tc.wantPass {
				<-passed
				hooks.waitHook(t)
				if h := hooks.got(); !slices.Equal(h, []string{"scope 7"}) {
					t.Errorf("hooks = %v; the resumed pass drops the requester's scope when it ends", h)
				}
			} else {
				if !strings.Contains(logs.String(), "already decided") {
					t.Errorf("a second click must be logged:\n%s", logs.String())
				}
				if h := hooks.got(); len(h) != 0 {
					t.Errorf("hooks = %v", h)
				}
			}
			if len(m.mails()) != 0 {
				t.Errorf("a second click mailed %+v", m.mails())
			}
		})
	}
}

// TestDecideAddRequestRefusals: the store's refusals pass through typed;
// an organisation nothing could enumerate becomes OrgUnregistrable with
// the one advice sentence; a failure is wrapped and unclassified.
func TestDecideAddRequestRefusals(t *testing.T) {
	for _, tc := range []struct {
		err  error
		kind Kind
	}{
		{db.ErrAddRequestNotFound, RequestNotFound},
		{db.ErrGroupRejected, GroupRejected},
		{db.ErrOrgOffGitHubHost, OrgUnregistrable},
		{db.ErrOrgOffGitLabInstance, OrgUnregistrable},
		{platform.ErrURLUserinfo, OrgUnregistrable},
		{db.ErrURLTooLong, OrgUnregistrable},
	} {
		st := &fakeStore{
			isUserAdmin: adminIs(true),
			decideAddRequest: func(context.Context, int64, int, bool, string, string) (db.AddRequest, bool, error) {
				return db.AddRequest{}, false, tc.err
			},
		}
		svc, hooks, logs := testService(t, st, Options{GitHubAPIBase: "https://github.example/api/v3"})
		_, err := svc.DecideAddRequest(context.Background(), adminSession, 77, true)
		ref, ok := Classify(err)
		if !errors.Is(err, tc.err) || !ok || ref.Kind != tc.kind {
			t.Errorf("%v: = %v (%+v)", tc.err, err, ref)
		}
		if tc.kind == OrgUnregistrable && (ref.Detail != db.OrgApprovalRefusalAdvice(tc.err, "https://github.example/api/v3", "") || !strings.Contains(ref.Detail, "reject the request")) {
			t.Errorf("%v: Detail = %q, want the one advice sentence", tc.err, ref.Detail)
		}
		if len(hooks.got()) != 0 || !strings.Contains(logs.String(), "refused") {
			t.Errorf("%v: hooks %v, log:\n%s", tc.err, hooks.got(), logs.String())
		}
	}
	st := &fakeStore{isUserAdmin: adminIs(true), decideAddRequest: func(context.Context, int64, int, bool, string, string) (db.AddRequest, bool, error) {
		return db.AddRequest{}, false, errors.New("closed pool")
	}}
	svc, _, _ := testService(t, st, Options{})
	_, err := svc.DecideAddRequest(context.Background(), adminSession, 77, true)
	if _, ok := Classify(err); err == nil || ok || !strings.Contains(err.Error(), "77") {
		t.Errorf("failure = %v", err)
	}
}

// TestApprovedPassOutcomesAreLogged: each outcome of the background pass
// is one log line at its level, and the requester's scope hook runs
// whatever happened.
func TestApprovedPassOutcomesAreLogged(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n, f     int
		err      error
		wantLine string
		level    string
	}{
		{"processed", 3, 0, nil, "approved add-request processed", "INFO"},
		{"some failed", 2, 1, nil, "items that could not be added", "WARN"},
		{"in progress", 0, 0, db.ErrAddRequestInProgress, "already being processed", "INFO"},
		{"group rejected", 0, 0, db.ErrGroupRejected, "its group was rejected", "INFO"},
		{"failed", 1, 0, errors.New("closed pool"), "re-approving resumes it", "WARN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{processApprovedAddRequest: func(context.Context, int64, string) (int, int, error) { return tc.n, tc.f, tc.err }}
			svc, hooks, logs := testService(t, st, Options{})
			svc.runApprovedPass(pendingReq("repos"))
			hooks.waitHook(t)
			logs.waitLog(t, tc.wantLine)
			if !strings.Contains(logs.String(), "level="+tc.level) {
				t.Errorf("want level %s:\n%s", tc.level, logs.String())
			}
			if h := hooks.got(); !slices.Equal(h, []string{"scope 7"}) {
				t.Errorf("hooks = %v", h)
			}
		})
	}
}

func TestNotifyAddRequestSubmitted(t *testing.T) {
	t.Run("nothing without a mailer, an operator address or a request", func(t *testing.T) {
		m := newFakeMailer()
		for _, tc := range []struct {
			name string
			opts Options
			id   int64
		}{
			{"no mailer", Options{}, 1},
			{"no operator address", Options{Mailer: m}, 1},
			{"request 0", Options{Mailer: m}, 0},
		} {
			svc, _, _ := testService(t, &fakeStore{}, tc.opts) // a store read would fail the test
			svc.NotifyAddRequestSubmitted(tc.id)
		}
		if len(m.mails()) != 0 {
			t.Errorf("sent %+v", m.mails())
		}
	})
	t.Run("a failed read is logged, nothing sent", func(t *testing.T) {
		st := &fakeStore{getAddRequest: func(context.Context, int64) (db.AddRequest, error) { return db.AddRequest{}, errors.New("closed pool") }}
		m := newFakeMailer()
		m.operator = "ops@example.com"
		svc, _, logs := testService(t, st, Options{Mailer: m})
		svc.NotifyAddRequestSubmitted(5)
		logs.waitLog(t, "add-request email: request could not be read")
		if !strings.Contains(logs.String(), "level=WARN") || len(m.mails()) != 0 {
			t.Errorf("log:\n%s\nmails %+v", logs.String(), m.mails())
		}
	})
	t.Run("a send failure is logged once; a skip is not", func(t *testing.T) {
		for _, tc := range []struct {
			err     error
			wantLog bool
		}{{errors.New("535"), true}, {skipErr(), false}} {
			st := &fakeStore{getAddRequest: func(context.Context, int64) (db.AddRequest, error) { return pendingReq("repos"), nil }}
			m := newFakeMailer()
			m.operator = "ops@example.com"
			m.sendErr = tc.err
			svc, _, logs := testService(t, st, Options{Mailer: m})
			svc.NotifyAddRequestSubmitted(77)
			m.waitMail(t)
			if tc.wantLog {
				logs.waitLog(t, "failed to send add-request email")
			} else if strings.Contains(logs.String(), "failed to send") {
				t.Errorf("a skip was logged again:\n%s", logs.String())
			}
		}
	})
}

func TestRejectGroup(t *testing.T) {
	var got int64
	st := &fakeStore{isUserAdmin: adminIs(true), groupOwner: func(context.Context, int64) (int, string, error) { return 7, "g", nil }, rejectGroup: func(_ context.Context, gid int64, adminID int) (bool, error) {
		got = gid
		if adminID != 1 {
			t.Errorf("adminID = %d", adminID)
		}
		return true, nil
	}}
	svc, hooks, logs := testService(t, st, Options{})
	if err := svc.RejectGroup(context.Background(), adminSession, 5); err != nil || got != 5 {
		t.Fatalf("= %v, got %d", err, got)
	}
	if h := hooks.got(); len(h) != 0 {
		t.Errorf("hooks = %v; a rejection changes nobody's scope (visibility is never gated)", h)
	}
	if !strings.Contains(logs.String(), "admin: reject group") {
		t.Errorf("no audit line:\n%s", logs.String())
	}
	// A second rejection is idempotent (the store flips no row) and audits
	// nothing: an audit line records a change (whole-branch review A4).
	st.rejectGroup = func(context.Context, int64, int) (bool, error) { return false, nil }
	audits := strings.Count(logs.String(), "admin: reject group")
	if err := svc.RejectGroup(context.Background(), adminSession, 5); err != nil {
		t.Fatalf("second rejection = %v", err)
	}
	if n := strings.Count(logs.String(), "admin: reject group"); n != audits {
		t.Errorf("a rejection that changed nothing was audited (%d audit lines, was %d):\n%s", n, audits, logs.String())
	}
	st.rejectGroup = func(context.Context, int64, int) (bool, error) { return false, errors.New("closed pool") }
	if err := svc.RejectGroup(context.Background(), adminSession, 5); err == nil || !strings.Contains(err.Error(), "group 5") {
		t.Errorf("failure = %v", err)
	}
	// A group that does not exist is NotFound, never audited (round 1 F7).
	st.groupOwner = func(context.Context, int64) (int, string, error) { return 0, "", db.ErrGroupNotFound }
	st.rejectGroup = func(context.Context, int64, int) (bool, error) {
		t.Error("the store must not be asked to reject a missing group")
		return false, nil
	}
	if err := svc.RejectGroup(context.Background(), adminSession, 6); !errors.Is(err, db.ErrGroupNotFound) {
		t.Errorf("missing group = %v", err)
	}
}

func TestAdoptForgeID(t *testing.T) {
	t.Run("inputs", func(t *testing.T) {
		svc, _, _ := testService(t, &fakeStore{isUserAdmin: adminIs(true)}, Options{})
		// Besides the required fields and the note bound: an id that is not
		// a forge ID (a positive integer as the forges write it) — text, a
		// leading zero, a sign, spaces, a 6,000-byte string — is refused
		// before the store or the log sees it (round-2 review).
		for _, tc := range []struct{ old, new, note string }{
			{"", "2", ""}, {"1", "", ""}, {"1", "2", strings.Repeat("n", AdoptNoteMaxLength+1)},
			{"x", "2", ""}, {"1", "02", ""}, {"-1", "2", ""}, {"1", "+2", ""}, {" 1", "2", ""}, {"1", "0", ""},
			{strings.Repeat("9", 6000), "2", ""}, {"1", "2\n", ""},
		} {
			if err := svc.AdoptForgeID(context.Background(), adminSession, 9, tc.old, tc.new, tc.note); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("AdoptForgeID(%q, %q, note %d bytes) = %v", tc.old, tc.new, len(tc.note), err)
			}
		}
	})
	t.Run("adopted with the actor's label", func(t *testing.T) {
		var by string
		st := &fakeStore{
			isUserAdmin: adminIs(true),
			userLabel:   func(context.Context, int) (string, error) { return "alice (github)", nil },
			adoptPendingForgeIDChange: func(_ context.Context, rid int64, o, n, adoptedBy, note string) error {
				by = adoptedBy
				if rid != 9 || o != "1" || n != "2" || note != "why" {
					t.Errorf("store received %d %q %q %q", rid, o, n, note)
				}
				return nil
			},
		}
		svc, _, logs := testService(t, st, Options{})
		if err := svc.AdoptForgeID(context.Background(), adminSession, 9, "1", "2", "why"); err != nil || by != "alice (github)" {
			t.Fatalf("= %v, by %q", err, by)
		}
		if !strings.Contains(logs.String(), "admin: adopt forge-ID change") {
			t.Errorf("no audit line:\n%s", logs.String())
		}
	})
	t.Run("operator label; a failed label lookup records the id", func(t *testing.T) {
		var by []string
		st := &fakeStore{
			userLabel: func(context.Context, int) (string, error) { return "", errors.New("closed pool") },
			adoptPendingForgeIDChange: func(_ context.Context, _ int64, _, _, adoptedBy, _ string) error {
				by = append(by, adoptedBy)
				return nil
			},
			isUserAdmin: adminIs(true),
		}
		svc, _, logs := testService(t, st, Options{})
		_ = svc.AdoptForgeID(context.Background(), Actor{Kind: Operator}, 9, "1", "2", "")
		_ = svc.AdoptForgeID(context.Background(), adminSession, 9, "1", "2", "")
		if !slices.Equal(by, []string{"operator", "user 1"}) || !strings.Contains(logs.String(), "label lookup failed") {
			t.Errorf("adopted_by = %q; log:\n%s", by, logs.String())
		}
	})
	t.Run("refusals", func(t *testing.T) {
		for _, tc := range []struct {
			err  error
			kind Kind
			want string
		}{
			{db.ErrNoPendingForgeIDChange, NotFound, "no pending forge-ID change"},
			{db.ErrForgeIDNotAsExpected, Superseded, "no longer stores 1"},
			// Copilot PR #252, fourteenth review: the api and web processes do
			// not migrate; on a schema behind the binary the adoption is a
			// refusal naming the migrate, not a server error.
			{fmt.Errorf("%w: run %s", db.ErrSchemaNotMigrated, db.DeployStepsAdvice), NotMigrated, "deploy-checklist"},
		} {
			st := &fakeStore{isUserAdmin: adminIs(true), userLabel: func(context.Context, int) (string, error) { return "a", nil },
				adoptPendingForgeIDChange: func(context.Context, int64, string, string, string, string) error { return tc.err }}
			svc, _, logs := testService(t, st, Options{})
			err := svc.AdoptForgeID(context.Background(), adminSession, 9, "1", "2", "")
			ref, ok := Classify(err)
			if !errors.Is(err, tc.err) || !ok || ref.Kind != tc.kind || !strings.Contains(ref.Detail, tc.want) {
				t.Errorf("%v: = %v (%+v)", tc.err, err, ref)
			}
			if !strings.Contains(logs.String(), "refused") {
				t.Errorf("%v not logged:\n%s", tc.err, logs.String())
			}
			if tc.kind == NotMigrated && !strings.Contains(logs.String(), "level=ERROR") {
				t.Errorf("a schema behind the binary is a deployment fault, logged at ERROR:\n%s", logs.String())
			}
		}
	})
}
