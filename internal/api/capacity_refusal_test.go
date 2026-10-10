// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.89: viewing a repository outside the caller's groups adds it to
// "Shared with Me"; with the account's repository allocation full, the
// answer is the capacity body (403, kind allocation, the way to make room
// and whom to write), not the out-of-scope 403 and not a 500.
func TestAuthorizeRepoAtTheAllocationAnswersTheCapacityBody(t *testing.T) {
	full := &capacity.Exceeded{Kind: capacity.KindAllocation, Quota: capacity.QuotaReposPerAccount, Used: 1000, Allowed: 1000, Wanted: 1, Contact: "help@example.org"}
	fake := &fakeSharedWithMe{err: fmt.Errorf("link: %w", full)}
	s := autoAddServer(fake)
	w := httptest.NewRecorder()
	if s.authorizeRepo(w, scopedReq(7, map[int64]bool{}), 99) {
		t.Fatal("a refused auto-add must not authorize the request")
	}
	var body capacityRefusalBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v %s", err, w.Body.String())
	}
	if w.Code != http.StatusForbidden || body.Error != "capacity_limit" || body.Kind != "allocation" || body.Wanted != 1 ||
		!strings.Contains(body.Message, "remove repositories") || !strings.Contains(body.Message, "help@example.org") ||
		w.Header().Get("Retry-After") != "" {
		t.Fatalf("= %d %+v (Retry-After %q); want 403 capacity_limit allocation, no Retry-After", w.Code, body, w.Header().Get("Retry-After"))
	}
}

// Every API function that calls a writer of a group link answers the
// allocation's refusal through refuseCapacity (the denominator: every call
// site is examined, so a new one cannot skip the rule).
func TestEveryGroupLinkSiteRendersTheCapacityRefusal(t *testing.T) {
	writers := []string{".AddRepoToGroupByID(", ".AddReposToGroup(", ".CopyCollectionToGroup(", ".EnsureRepoSharedWithUser("}
	files := srctest.PackageFiles(t, "internal/api", 20)
	examined := 0
	for name, src := range files {
		code := srctest.StripGoComments(src)
		for _, fn := range strings.Split(code, "\nfunc ")[1:] {
			calls := false
			for _, wr := range writers {
				if strings.Contains(fn, wr) && !strings.Contains(fn[:strings.Index(fn, "{")], wr) {
					calls = true
				}
			}
			if !calls {
				continue
			}
			examined++
			if !strings.Contains(fn, "s.refuseCapacity(w, info, ") {
				t.Errorf("%s: func %s calls a group-link writer without refuseCapacity", name, strings.SplitN(fn, "{", 2)[0])
			}
		}
	}
	srctest.MinCount(t, "API functions calling a group-link writer", examined, 5)
}

// Review round 1 F3 (L18): the auto-add cap is a capacity quota, and its
// refusal is the one capacity body (429, Retry-After, the kind message),
// not a hand-written error.
func TestAutoAddRefusalIsTheCapacityBody(t *testing.T) {
	s := autoAddServer(&fakeSharedWithMe{added: true})
	s.autoAdds = newAutoAddLimiter()
	// The contact comes from the capacity policy (L10 r2: unasserted).
	s.limiter = &rateLimiter{policy: &capacityPolicy{store: &fakeCapacityStore{contact: "help@example.org"}, now: time.Now, spawn: func(f func()) { f() }}}
	for i := 0; i < sharedWithMeAddsPerHour; i++ {
		if _, ok, _ := s.autoAdds.reserve(7); !ok {
			t.Fatalf("reservation %d refused", i+1)
		}
	}
	w := httptest.NewRecorder()
	if s.authorizeRepo(w, scopedReq(7, map[int64]bool{}), 99) {
		t.Fatal("past the cap the view must be refused")
	}
	var body capacityRefusalBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v %s", err, w.Body.String())
	}
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" || body.Error != "capacity_limit" ||
		body.Quota != capacity.QuotaSharedWithMeAddsPerHour || body.Allowed != sharedWithMeAddsPerHour ||
		!strings.Contains(body.Message, "by viewing them") || body.Contact != "help@example.org" {
		t.Fatalf("= %d %+v", w.Code, body)
	}
}
