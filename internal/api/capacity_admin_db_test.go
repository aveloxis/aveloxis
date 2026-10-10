// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// v0.29.89 end to end on the real store: the Capacity page's routes, /me's
// numbers, and removing a repository freeing a place.
func TestCapacityAdminRoutesEndToEnd(t *testing.T) {
	s, store, admin, owner := apiTokenAdminServer(t)
	ctx := context.Background()
	before, err := store.GetCapacityQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	contact, err := store.GetCapacityContact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		if q, ok := before[capacity.QuotaReposPerAccount]; ok {
			_ = store.SetCapacityQuota(c, capacity.QuotaReposPerAccount, q.Allowed, q.Mode, 0)
		}
		_ = store.SetCapacityContact(c, contact, 0)
		store.SetCapacitySources(nil)
		for _, q := range []string{
			`UPDATE aveloxis_ops.capacity_quotas SET updated_by = NULL WHERE updated_by = $1`,
			`UPDATE aveloxis_ops.capacity_settings SET updated_by = NULL WHERE updated_by = $1`,
			`DELETE FROM aveloxis_ops.signup_allowlist WHERE added_by = $1`,
		} {
			_, _ = store.Pool().Exec(c, q, admin)
		}
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_capacity WHERE user_id = $1`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, owner)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avcapadmin'`)
	})
	call := func(method, path string, body any, as int, isAdmin bool) *httptest.ResponseRecorder {
		return adminCall(t, s, method, path, body, as, isAdmin)
	}
	// Non-admins are refused.
	if w := call("GET", "/api/v1/admin/capacity", nil, owner, false); w.Code != http.StatusForbidden {
		t.Fatalf("non-admin = %d", w.Code)
	}
	// Quotas: save, read back, refuse a config-owned one.
	if w := call("POST", "/api/v1/admin/capacity/quotas/repos_per_account", map[string]any{"allowed": 2, "mode": "enforce"}, admin, true); w.Code != http.StatusOK {
		t.Fatalf("save quota = %d %s", w.Code, w.Body.String())
	}
	w := call("GET", "/api/v1/admin/capacity", nil, admin, true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `{"name":"repos_per_account","allowed":2,"mode":"enforce","source":"WEB","editable":true}`) {
		t.Fatalf("capacity = %d %s", w.Code, w.Body.String())
	}
	for _, bad := range []struct {
		path string
		body map[string]any
		code int
	}{
		{"/api/v1/admin/capacity/quotas/no_such_quota", map[string]any{"allowed": 1, "mode": "enforce"}, http.StatusNotFound},
		{"/api/v1/admin/capacity/quotas/repos_per_account", map[string]any{"allowed": 0, "mode": "enforce"}, http.StatusBadRequest},
		{"/api/v1/admin/capacity/quotas/repos_per_account", map[string]any{"allowed": 5, "mode": "loud"}, http.StatusBadRequest},
		{"/api/v1/admin/capacity/contact", map[string]any{"contact_email": "not an address"}, http.StatusBadRequest},
		{"/api/v1/admin/capacity/signup-allowlist", map[string]any{"cidr": "nonsense"}, http.StatusBadRequest},
		{"/api/v1/admin/capacity/accounts/999999999", map[string]any{"repos_allowed": 5}, http.StatusNotFound},
		{"/api/v1/admin/capacity/accounts/" + strconv.Itoa(owner), map[string]any{"repos_allowed": -1}, http.StatusBadRequest},
		{"/api/v1/admin/capacity/accounts/" + strconv.Itoa(owner), map[string]any{"requests_per_day": int64(3_000_000_000)}, http.StatusBadRequest}, // past INT (ASVS V1.4.2)
		{"/api/v1/admin/capacity/contact", map[string]any{"contact_email": "a@b.org?bcc=x@y.org"}, http.StatusBadRequest},                           // no mailto parameters (V1.2.2)
		{"/api/v1/admin/capacity/contact", map[string]any{"contact_email": "a%40b@c.org"}, http.StatusBadRequest},
	} {
		if w := call("POST", bad.path, bad.body, admin, true); w.Code != bad.code {
			t.Errorf("POST %s %v = %d %s; want %d", bad.path, bad.body, w.Code, w.Body.String(), bad.code)
		}
	}
	store.SetCapacitySources(map[string]capacity.Source{capacity.QuotaReposPerAccount: capacity.SourceShadow})
	if w := call("POST", "/api/v1/admin/capacity/quotas/repos_per_account", map[string]any{"allowed": 9, "mode": "enforce"}, admin, true); w.Code != http.StatusConflict {
		t.Errorf("a config-owned quota = %d %s; want 409", w.Code, w.Body.String())
	}
	store.SetCapacitySources(nil)
	// The allowlist stores networks normalised.
	w = call("POST", "/api/v1/admin/capacity/signup-allowlist", map[string]any{"cidr": "198.51.100.77/24", "note": "lab"}, admin, true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"cidr":"198.51.100.0/24"`) {
		t.Fatalf("allowlist add = %d %s", w.Code, w.Body.String())
	}
	if w := call("POST", "/api/v1/admin/capacity/signup-allowlist/remove", map[string]any{"cidr": "198.51.100.0/24"}, admin, true); w.Code != http.StatusOK {
		t.Errorf("allowlist remove = %d", w.Code)
	}
	if w := call("POST", "/api/v1/admin/capacity/signup-allowlist/remove", map[string]any{"cidr": "198.51.100.0/24"}, admin, true); w.Code != http.StatusNotFound {
		t.Errorf("removing it twice = %d; want 404", w.Code)
	}
	// An override raises one account; the account lookup shows it.
	if w := call("POST", "/api/v1/admin/capacity/accounts/"+strconv.Itoa(owner), map[string]any{"repos_allowed": 3, "note": "course"}, admin, true); w.Code != http.StatusOK {
		t.Fatalf("override = %d %s", w.Code, w.Body.String())
	}
	w = call("GET", "/api/v1/admin/capacity/accounts?login=avx-it-apitok-owner-api", nil, admin, true)
	var acct struct {
		Override struct {
			ReposAllowed *int   `json:"repos_allowed"`
			Note         string `json:"note"`
		} `json:"override"`
		Repos struct {
			Used, Allowed int
		} `json:"repos"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &acct); err != nil || w.Code != http.StatusOK || acct.Override.ReposAllowed == nil ||
		*acct.Override.ReposAllowed != 3 || acct.Override.Note != "course" || acct.Repos.Allowed != 3 {
		t.Fatalf("account = %d %s", w.Code, w.Body.String())
	}
	// Round 5 R5-1: a login with a case twin is answered 409 with both
	// candidates, and either opens by ?user_id=.
	var twin int
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, gl_oauth_host) VALUES ('AVX-IT-APITOK-OWNER-API', 'gitlab', 'https://gitlab.example.org') RETURNING user_id`).Scan(&twin); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, twin)
	})
	w = call("GET", "/api/v1/admin/capacity/accounts?login=avx-it-apitok-owner-api", nil, admin, true)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"candidates"`) || !strings.Contains(w.Body.String(), `"user_id":`+strconv.Itoa(twin)) ||
		!strings.Contains(w.Body.String(), `"user_id":`+strconv.Itoa(owner)) || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatalf("an ambiguous login = %d %s; want 409 with both candidates", w.Code, w.Body.String())
	}
	w = call("GET", "/api/v1/admin/capacity/accounts?user_id="+strconv.Itoa(owner), nil, admin, true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"user_id":`+strconv.Itoa(owner)) || !strings.Contains(w.Body.String(), `"provider":"github"`) {
		t.Fatalf("lookup by user_id = %d %s", w.Code, w.Body.String())
	}
	for _, bad := range []string{"abc", "0", "3000000000"} { // 3e9: past int4 (round 6 R6-d)
		if w := call("GET", "/api/v1/admin/capacity/accounts?user_id="+bad, nil, admin, true); w.Code != http.StatusBadRequest {
			t.Errorf("user_id=%s = %d; want 400", bad, w.Code)
		}
	}
	if w := call("POST", "/api/v1/admin/capacity/accounts/3000000000", map[string]any{"note": "x"}, admin, true); w.Code != http.StatusBadRequest {
		t.Errorf("an override for a user id past int4 = %d; want 400", w.Code)
	}
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, twin)

	// /me carries the numbers; a removal frees a place.
	gid, err := store.CreateUserGroup(ctx, owner, "capadmin")
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for i := 0; i < 4; i++ {
		var id int64
		if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id) VALUES ($1, '_avcapadmin', $2, 1) RETURNING repo_id`,
			"https://github.com/_avcapadmin/r"+strconv.Itoa(i)+strconv.FormatInt(time.Now().UnixNano(), 10), "r"+strconv.Itoa(i)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids[:3] {
		if _, err := store.AddRepoToGroupByID(ctx, gid, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.AddRepoToGroupByID(ctx, gid, ids[3]); err == nil {
		t.Fatal("a fourth repository past the override of 3 must be refused")
	}
	r := httptest.NewRequest("GET", "/api/v1/me", nil)
	mw := httptest.NewRecorder()
	s.mux.ServeHTTP(mw, asUser(r, owner, false))
	if !strings.Contains(mw.Body.String(), `"repos":{"used":3,"allowed":3,"mode":"enforce","exempt":false`) {
		t.Fatalf("/me capacity = %s", mw.Body.String())
	}
	rm := call("POST", "/api/v1/groups/"+strconv.FormatInt(gid, 10)+"/repos/"+strconv.FormatInt(ids[0], 10)+"/remove", nil, owner, false)
	if rm.Code != http.StatusOK {
		t.Fatalf("remove = %d %s", rm.Code, rm.Body.String())
	}
	if _, err := store.AddRepoToGroupByID(ctx, gid, ids[3]); err != nil {
		t.Errorf("after a removal the fourth repository must fit: %v", err)
	}
	if w := call("POST", "/api/v1/groups/999999999/repos/"+strconv.FormatInt(ids[0], 10)+"/remove", nil, owner, false); w.Code != http.StatusNotFound {
		t.Errorf("removing from a group that is not yours = %d; want 404", w.Code)
	}
}
