// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.30.0 Phase C — the API-keys admin endpoints behind the aveloxis-gui
// "API keys" page: one GET for the whole fleet (GitHub plus every GitLab
// instance), add, remove. The api process accepts a token on the way in,
// never reads one back, and never sends one anywhere.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/forgekeys"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type fakeAPIKeyStore struct {
	keys      []db.AdminAPIKey
	tokens    map[int64]string // what was stored — the store side only; never returned by the fake's reads
	reports   []db.ForgeKeyReportRow
	registry  map[string]model.Platform
	repos     map[model.Platform]int
	misrouted []db.MisroutedRepo
	nextID    int64
	failList  bool
}

func (f *fakeAPIKeyStore) ListAdminAPIKeys(context.Context) ([]db.AdminAPIKey, error) {
	if f.failList {
		return nil, errors.New("pq: relation secret_internal_detail does not exist")
	}
	return append([]db.AdminAPIKey(nil), f.keys...), nil
}

func (f *fakeAPIKeyStore) InsertAPIKey(_ context.Context, name, token, platformName, instanceURL string) (int64, error) {
	for id, tok := range f.tokens {
		if tok == token {
			for _, k := range f.keys {
				if k.OAuthID == id && k.Platform == platformName {
					return 0, &db.APIKeyExistsError{OAuthID: id, InstanceURL: k.InstanceURL}
				}
			}
		}
	}
	f.nextID++
	if f.tokens == nil {
		f.tokens = map[int64]string{}
	}
	f.tokens[f.nextID] = token
	f.keys = append(f.keys, db.AdminAPIKey{OAuthID: f.nextID, Name: name, Platform: platformName, InstanceURL: instanceURL,
		KeyID: platform.KeyID(token), KeyMask: platform.MaskToken(token), CreatedAt: time.Now()})
	return f.nextID, nil
}

func (f *fakeAPIKeyStore) DeleteAPIKey(_ context.Context, oauthID int64) (db.AdminAPIKey, error) {
	for i, k := range f.keys {
		if k.OAuthID == oauthID {
			f.keys = append(f.keys[:i], f.keys[i+1:]...)
			delete(f.tokens, oauthID)
			return k, nil
		}
	}
	return db.AdminAPIKey{}, db.ErrAPIKeyNotFound
}

func (f *fakeAPIKeyStore) LoadForgeKeyReports(context.Context) ([]db.ForgeKeyReportRow, error) {
	return f.reports, nil
}

func (f *fakeAPIKeyStore) LoadGitLabInstanceRegistry(context.Context) (map[string]model.Platform, error) {
	return f.registry, nil
}

func (f *fakeAPIKeyStore) CountReposByPlatform(context.Context) (map[model.Platform]int, error) {
	return f.repos, nil
}

func (f *fakeAPIKeyStore) MisroutedGitLabRepos(context.Context) ([]db.MisroutedRepo, error) {
	return f.misrouted, nil
}

func reportRow(t *testing.T, age float64, r forgekeys.Report) db.ForgeKeyReportRow {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return db.ForgeKeyReportRow{Reporter: r.Reporter, ReportedAt: time.Now().Add(-time.Duration(age) * time.Second), AgeSeconds: age, Report: b}
}

// tenSites is the operator model: GitHub plus ten distinct GitLab
// installations on one page.
func tenSites() (map[string]model.Platform, []forgekeys.ReportInstance) {
	registry := map[string]model.Platform{"https://gitlab.com": model.PlatformGitLab}
	instances := []forgekeys.ReportInstance{
		{Platform: "github", PlatformID: 1, WebURL: "https://github.com", APIURL: "https://api.github.com", Main: true, ActiveKeys: 1},
		{Platform: "gitlab", PlatformID: 2, WebURL: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Main: true, ActiveKeys: 1},
	}
	for i := 1; i <= 9; i++ {
		base := "https://gitlab" + string(rune('0'+i)) + ".example.invalid"
		id := model.Platform(100 + i)
		registry[base] = id
		instances = append(instances, forgekeys.ReportInstance{Platform: "gitlab", PlatformID: int(id), WebURL: base, APIURL: base + "/api/v4"})
	}
	return registry, instances
}

func adminKeysServer(store *fakeAPIKeyStore, logs *bytes.Buffer) *Server {
	s := &Server{apiKeys: store, logger: slog.New(slog.NewTextHandler(logs, nil))}
	return s
}

func adminReq(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	return req.WithContext(context.WithValue(req.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true}))
}

func TestAdminAPIKeysListOnePageForEveryForge(t *testing.T) {
	registry, instances := tenSites()
	const ghTok, glTok, fdTok = "ghp_config_000000000000", "glpat-stored-com-0000", "glpat-stored-gitlab3-00"
	store := &fakeAPIKeyStore{registry: registry, repos: map[model.Platform]int{2: 96, 103: 4}}
	if _, err := store.InsertAPIKey(context.Background(), "com", glTok, "gitlab", "https://gitlab.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertAPIKey(context.Background(), "site3", fdTok, "gitlab", "https://gitlab3.example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertAPIKey(context.Background(), "gone", "glpat-orphan-00000000", "gitlab", "https://gone.example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.InsertAPIKey(context.Background(), "gh", "ghp_stored_not_yet_000", "github", ""); err != nil {
		t.Fatal(err)
	}
	store.reports = []db.ForgeKeyReportRow{
		reportRow(t, 12, forgekeys.Report{Reporter: "serve@kate#a1", IntervalSeconds: 60, Instances: instances,
			Unregistered: []forgekeys.ReportInstance{{Platform: "gitlab", WebURL: "https://new.example.invalid", APIURL: "https://new.example.invalid/api/v4"}},
			Keys: []forgekeys.ReportKey{
				{KeyID: platform.KeyID(ghTok), Masked: platform.MaskToken(ghTok), Source: forgekeys.SourceConfig, Platform: "github", PlatformID: 1, InstanceURL: "https://github.com", State: platform.KeyActive, Health: platform.HealthOK},
				{KeyID: platform.KeyID(glTok), Masked: platform.MaskToken(glTok), OAuthID: 1, Source: forgekeys.SourceDatabase, Platform: "gitlab", PlatformID: 2, InstanceURL: "https://gitlab.com", State: platform.KeyActive, Health: platform.HealthResting},
			},
			NotLoaded: []forgekeys.NotLoaded{{OAuthID: 3, KeyID: platform.KeyID("glpat-orphan-00000000"), Reason: forgekeys.ReasonOrphan, Tag: "https://gone.example.invalid"}},
		}),
		reportRow(t, 7200, forgekeys.Report{Reporter: "serve@old#zz", IntervalSeconds: 60}),
	}
	logs := &bytes.Buffer{}
	s := adminKeysServer(store, logs)
	rec := httptest.NewRecorder()
	s.handleAdminAPIKeys(rec, adminReq("GET", "/api/v1/admin/api-keys", ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body)
	}
	for _, tok := range []string{ghTok, glTok, fdTok, "glpat-orphan-00000000", "ghp_stored_not_yet_000"} {
		if strings.Contains(rec.Body.String(), tok) {
			t.Fatalf("the list response carries a token (%s): %s", tok, rec.Body)
		}
	}
	var got apiKeysResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Forges) != 11 || got.Forges[0].Platform != "github" {
		t.Fatalf("forges = %d (first %q), want GitHub + 10 GitLab sites on one page", len(got.Forges), got.Forges[0].Platform)
	}
	by := map[string]forgeJSON{}
	for _, f := range got.Forges {
		by[f.WebURL] = f
	}
	com := by["https://gitlab.com"]
	if com.PlatformID != 2 || !com.Main || com.APIURL != "https://gitlab.com/api/v4" || com.RepoCount != 96 || len(com.Keys) != 1 ||
		com.Keys[0].Status != keyStatusLoaded || com.Keys[0].Health != "resting" || !com.Keys[0].Removable || com.Keys[0].OAuthID != 1 {
		t.Errorf("gitlab.com = %+v", com)
	}
	site3 := by["https://gitlab3.example.invalid"]
	if len(site3.Keys) != 1 || site3.Keys[0].Status != keyStatusNotLoaded || site3.Keys[0].Reason != reasonPending || site3.RepoCount != 4 {
		t.Errorf("site 3 = %+v, want its stored key pending pickup", site3)
	}
	if s9 := by["https://gitlab9.example.invalid"]; s9.ActiveKeys != 0 || len(s9.Keys) != 0 || !s9.Registered || !s9.Reported {
		t.Errorf("keyless site 9 = %+v", s9)
	}
	gh := by["https://github.com"]
	if len(gh.Keys) != 2 {
		t.Fatalf("github keys = %+v, want the config key and the pending stored key", gh.Keys)
	}
	for _, k := range gh.Keys {
		if k.Source == "config" && (k.Removable || k.Status != keyStatusLoaded) {
			t.Errorf("config key = %+v, want loaded and not removable", k)
		}
	}
	if len(got.Orphaned) != 1 || got.Orphaned[0].OAuthID != 3 || got.Orphaned[0].Reason != forgekeys.ReasonOrphan || !got.Orphaned[0].Removable {
		t.Errorf("orphaned = %+v, want stored key 3", got.Orphaned)
	}
	if len(got.Unregistered) != 1 || got.Unregistered[0].WebURL != "https://new.example.invalid" {
		t.Errorf("unregistered = %+v", got.Unregistered)
	}
	if len(got.Reporters) != 2 || got.Reporters[0].Stale || !got.Reporters[1].Stale {
		t.Errorf("reporters = %+v, want the fresh one first and the two-hour-old one stale", got.Reporters)
	}
}

// With no fresh report the page still lists every stored key, grouped by
// the registry (gitlab.com is the main instance), each "no serve reporting".
func TestAdminAPIKeysListWithoutReports(t *testing.T) {
	store := &fakeAPIKeyStore{registry: map[string]model.Platform{"https://gitlab.com": 2, "https://salsa.example.invalid": 101}}
	if _, err := store.InsertAPIKey(context.Background(), "", "glpat-untagged-main-0", "gitlab", ""); err != nil {
		t.Fatal(err)
	}
	s := adminKeysServer(store, &bytes.Buffer{})
	rec := httptest.NewRecorder()
	s.handleAdminAPIKeys(rec, adminReq("GET", "/api/v1/admin/api-keys", ""))
	var got apiKeysResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	var com *forgeJSON
	for i := range got.Forges {
		if got.Forges[i].WebURL == "https://gitlab.com" {
			com = &got.Forges[i]
		}
	}
	if com == nil || len(com.Keys) != 1 || com.Keys[0].Reason != reasonNoReporter || com.Reported {
		t.Fatalf("gitlab.com without reports = %+v", com)
	}
}

func TestAdminAPIKeysListHidesStoreErrors(t *testing.T) {
	logs := &bytes.Buffer{}
	s := adminKeysServer(&fakeAPIKeyStore{failList: true}, logs)
	rec := httptest.NewRecorder()
	s.handleAdminAPIKeys(rec, adminReq("GET", "/api/v1/admin/api-keys", ""))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "secret_internal_detail") {
		t.Fatalf("store error = %d %q, want a 500 without the database text", rec.Code, rec.Body)
	}
	if !strings.Contains(logs.String(), "secret_internal_detail") {
		t.Error("the database error must be logged")
	}
}

func TestAdminAPIKeysAddValidation(t *testing.T) {
	registry, instances := tenSites()
	const cfgTok = "glpat-config-of-site3-0"
	store := &fakeAPIKeyStore{registry: registry, reports: []db.ForgeKeyReportRow{
		reportRow(t, 5, forgekeys.Report{Reporter: "serve@kate#a1", IntervalSeconds: 60, Instances: instances, Keys: []forgekeys.ReportKey{
			{KeyID: platform.KeyID(cfgTok), Source: forgekeys.SourceConfig, Platform: "gitlab", PlatformID: 103, InstanceURL: "https://gitlab3.example.invalid", State: platform.KeyActive},
		}}),
	}}
	if _, err := store.InsertAPIKey(context.Background(), "", "glpat-already-stored-0", "gitlab", "https://gitlab.com"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, body, query string
		want              int
		wantIn            string
	}{
		{"github key", `{"platform":"github","token":"ghp_new_key_0000000000","name":"ops"}`, "", http.StatusCreated, ""},
		{"gitlab key for site 5, http spelling", `{"platform":"gitlab","instance_url":"http://GitLab5.example.invalid/","token":"glpat-new-site5-000000"}`, "", http.StatusCreated, "https://gitlab5.example.invalid"},
		{"unknown platform", `{"platform":"bitbucket","token":"abcdefghijklmnop"}`, "", http.StatusBadRequest, "platform"},
		{"github with an instance", `{"platform":"github","instance_url":"https://gitlab.com","token":"ghp_x_0000000000000"}`, "", http.StatusBadRequest, "instance"},
		{"gitlab without an instance", `{"platform":"gitlab","token":"glpat-x-000000000000"}`, "", http.StatusBadRequest, "instance"},
		{"unregistered instance", `{"platform":"gitlab","instance_url":"https://nowhere.example.invalid","token":"glpat-x-000000000000"}`, "", http.StatusBadRequest, "not a registered GitLab instance"},
		{"empty token", `{"platform":"github","token":"   "}`, "", http.StatusBadRequest, "token"},
		{"token with a space", `{"platform":"github","token":"ghp_abc def_00000000"}`, "", http.StatusBadRequest, "printable"},
		{"non-ASCII token", `{"platform":"github","token":"ghp_ünïcode_00000000"}`, "", http.StatusBadRequest, "printable"},
		{"over-long token", `{"platform":"github","token":"` + strings.Repeat("a", 513) + `"}`, "", http.StatusBadRequest, "512"},
		{"GitHub token under GitLab", `{"platform":"gitlab","instance_url":"https://gitlab.com","token":"github_pat_11AAAAAA0000"}`, "", http.StatusBadRequest, "GitHub token"},
		{"GitLab token under GitHub", `{"platform":"github","token":"glpat-looks-like-gitlab"}`, "", http.StatusBadRequest, "GitLab token"},
		{"token in the query string", `{"platform":"github","token":"ghp_body_000000000000"}`, "token=ghp_query_0000000000", http.StatusBadRequest, "JSON body"},
		{"malformed JSON", `{"platform":`, "", http.StatusBadRequest, "JSON"},
		{"already stored here", `{"platform":"gitlab","instance_url":"https://gitlab.com","token":"glpat-already-stored-0"}`, "", http.StatusConflict, "already stored"},
		{"already stored elsewhere", `{"platform":"gitlab","instance_url":"https://gitlab4.example.invalid","token":"glpat-already-stored-0"}`, "", http.StatusConflict, "remove it first"},
		{"a config key of another instance", `{"platform":"gitlab","instance_url":"https://gitlab.com","token":"` + cfgTok + `"}`, "", http.StatusConflict, "config file"},
		{"a config key of the same instance", `{"platform":"gitlab","instance_url":"https://gitlab3.example.invalid","token":"` + cfgTok + `"}`, "", http.StatusConflict, "config file"},
		{"body over 8 KB", `{"platform":"github","name":"` + strings.Repeat("n", 9000) + `","token":"ghp_big_00000000000"}`, "", http.StatusRequestEntityTooLarge, ""},
	}
	for _, tc := range cases {
		logs := &bytes.Buffer{}
		s := adminKeysServer(store, logs)
		path := "/api/v1/admin/api-keys"
		if tc.query != "" {
			path += "?" + tc.query
		}
		rec := httptest.NewRecorder()
		s.handleAdminAPIKeyAdd(rec, adminReq("POST", path, tc.body))
		if rec.Code != tc.want || (tc.wantIn != "" && !strings.Contains(rec.Body.String(), tc.wantIn)) {
			t.Errorf("%s: %d %s, want %d containing %q", tc.name, rec.Code, rec.Body, tc.want, tc.wantIn)
		}
		for _, tok := range []string{"ghp_new_key_0000000000", "glpat-new-site5-000000", "glpat-already-stored-0", cfgTok, "ghp_query_0000000000", "ghp_body_000000000000", "github_pat_11AAAAAA0000"} {
			if strings.Contains(rec.Body.String(), tok) || strings.Contains(logs.String(), tok) {
				t.Errorf("%s: token %s appears in the response or the log", tc.name, tok)
			}
		}
		if tc.want == http.StatusCreated && !strings.Contains(logs.String(), "API key added") {
			t.Errorf("%s: an add must write an audit line; logs: %s", tc.name, logs)
		}
	}
	// The site-5 key was stored under the registry's spelling.
	var stored string
	for _, k := range store.keys {
		if k.KeyID == platform.KeyID("glpat-new-site5-000000") {
			stored = k.InstanceURL
		}
	}
	if stored != "https://gitlab5.example.invalid" {
		t.Errorf("site-5 key stored under %q, want the registered web URL", stored)
	}
}

func TestAdminAPIKeysDelete(t *testing.T) {
	const tok = "glpat-config-and-db-00"
	store := &fakeAPIKeyStore{registry: map[string]model.Platform{"https://gitlab.com": 2}, reports: []db.ForgeKeyReportRow{
		reportRow(t, 5, forgekeys.Report{Reporter: "serve@kate#a1", IntervalSeconds: 60, Keys: []forgekeys.ReportKey{
			{KeyID: platform.KeyID(tok), Source: forgekeys.SourceConfig, Platform: "gitlab", PlatformID: 2, InstanceURL: "https://gitlab.com", State: platform.KeyActive},
		}}),
	}}
	id, err := store.InsertAPIKey(context.Background(), "dup", tok, "gitlab", "https://gitlab.com")
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	s := adminKeysServer(store, logs)

	rec := httptest.NewRecorder()
	req := adminReq("POST", "/api/v1/admin/api-keys/1/delete", "")
	req.SetPathValue("oauthID", "1")
	s.handleAdminAPIKeyDelete(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", rec.Code, rec.Body)
	}
	var got apiKeyDeleteResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.OAuthID != id || got.KeyID != platform.KeyID(tok) || !got.StillLoadedFromConfig || strings.Contains(rec.Body.String(), tok) {
		t.Errorf("delete response = %s", rec.Body)
	}
	if !strings.Contains(logs.String(), "API key removed") || strings.Contains(logs.String(), tok) {
		t.Errorf("audit log = %s", logs)
	}

	for path, want := range map[string]int{"1": http.StatusNotFound, "abc": http.StatusBadRequest, "-3": http.StatusBadRequest} {
		rec := httptest.NewRecorder()
		req := adminReq("POST", "/api/v1/admin/api-keys/"+path+"/delete", "")
		req.SetPathValue("oauthID", path)
		s.handleAdminAPIKeyDelete(rec, req)
		if rec.Code != want {
			t.Errorf("delete %s = %d, want %d", path, rec.Code, want)
		}
	}
}

func TestAdminAPIKeyRoutesRequireAdmin(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"tok": true}} // not admin
	s := portalServer(t, store)
	for _, h := range []http.HandlerFunc{s.handleAdminAPIKeys, s.handleAdminAPIKeyAdd, s.handleAdminAPIKeyDelete} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/v1/admin/api-keys", strings.NewReader(`{"platform":"github","token":"ghp_nonadmin_000000000"}`))
		req.Header.Set("Authorization", "Bearer tok")
		h(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("non-admin = %d, want 403", rec.Code)
		}
	}
}

// The api process never reads a token back: no non-test source in the
// package names the token column, and no response type has a field that
// could carry one.
func TestAPIPackageNeverReadsTokens(t *testing.T) {
	for rel, src := range srctest.PackageFiles(t, "internal/api", 10) {
		if strings.Contains(srctest.StripGoComments(src), "access_token") {
			t.Errorf("%s names access_token — the admin API lists keys through db.ListAdminAPIKeys (key_id and key_mask only)", rel)
		}
	}
	// Every JSON field name reachable from the response types — walked by
	// reflection through nested structs, slices, pointers and embedded
	// fields, so a token field added to forgekeys.ReportKey (served inside
	// loaded_by) fails here (review of Phase C, finding 8a).
	seen := map[reflect.Type]bool{}
	fields := 0
	var walk func(reflect.Type, string)
	walk = func(rt reflect.Type, path string) {
		for rt.Kind() == reflect.Pointer || rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array || rt.Kind() == reflect.Map {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct || seen[rt] || rt.PkgPath() == "time" {
			return
		}
		seen[rt] = true
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			name := strings.ToLower(strings.Split(f.Tag.Get("json"), ",")[0])
			if name == "" {
				name = strings.ToLower(f.Name)
			}
			fields++
			if strings.Contains(name, "token") || strings.Contains(name, "secret") {
				t.Errorf("%s.%s (json %q) could carry a token", path, f.Name, name)
			}
			walk(f.Type, path+"."+f.Name)
		}
	}
	for _, typ := range []any{apiKeysResponse{}, apiKeyAddResponse{}, apiKeyDeleteResponse{}} {
		walk(reflect.TypeOf(typ), reflect.TypeOf(typ).Name())
	}
	srctest.MinCount(t, "JSON fields walked in the admin key responses", fields, 60)
	if !seen[reflect.TypeOf(forgekeys.ReportKey{})] || !seen[reflect.TypeOf(forgekeys.ReportInstance{})] {
		t.Error("the walk must reach forgekeys.ReportKey and ReportInstance")
	}
}

// A stored key refused as a conflict (its token is another instance's
// config key) is listed NOT loaded under its own instance, with the
// reason — never as "loaded" because the config copy under the other
// instance shares its key_id (review of Phase C, finding 2). The config
// copy is listed under the instance that holds it.
func TestAdminAPIKeysListConflictIsNotLoaded(t *testing.T) {
	const tok = "glpat-shared-token-000"
	registry := map[string]model.Platform{"https://gitlab.com": 2, "https://b.example.invalid": 101}
	store := &fakeAPIKeyStore{registry: registry}
	if _, err := store.InsertAPIKey(context.Background(), "dup", tok, "gitlab", "https://gitlab.com"); err != nil {
		t.Fatal(err)
	}
	store.reports = []db.ForgeKeyReportRow{reportRow(t, 5, forgekeys.Report{Reporter: "serve@kate#a1", IntervalSeconds: 60,
		Instances: []forgekeys.ReportInstance{
			{Platform: "gitlab", PlatformID: 2, WebURL: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", Main: true},
			{Platform: "gitlab", PlatformID: 101, WebURL: "https://b.example.invalid", APIURL: "https://b.example.invalid/api/v4", ActiveKeys: 1},
		},
		Keys: []forgekeys.ReportKey{{KeyID: platform.KeyID(tok), Masked: platform.MaskToken(tok), Source: forgekeys.SourceConfig,
			Platform: "gitlab", PlatformID: 101, InstanceURL: "https://b.example.invalid", State: platform.KeyActive, Health: platform.HealthOK}},
		NotLoaded: []forgekeys.NotLoaded{{OAuthID: 1, KeyID: platform.KeyID(tok), Platform: "gitlab", Tag: "https://gitlab.com",
			Source: forgekeys.SourceDatabase, Reason: forgekeys.ReasonConflict, Detail: "the same token is a key of https://b.example.invalid in the config file"}},
	})}
	s := adminKeysServer(store, &bytes.Buffer{})
	rec := httptest.NewRecorder()
	s.handleAdminAPIKeys(rec, adminReq("GET", "/api/v1/admin/api-keys", ""))
	var got apiKeysResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	by := map[string]forgeJSON{}
	for _, f := range got.Forges {
		by[f.WebURL] = f
	}
	com, b := by["https://gitlab.com"], by["https://b.example.invalid"]
	if len(com.Keys) != 1 || com.Keys[0].Status != keyStatusNotLoaded || com.Keys[0].Reason != forgekeys.ReasonConflict || com.Health[keyStatusNotLoaded] != 1 {
		t.Errorf("gitlab.com's stored copy = %+v (health %v), want not_loaded: conflict", com.Keys, com.Health)
	}
	if len(b.Keys) != 1 || b.Keys[0].Source != "config" || b.Keys[0].Status != keyStatusLoaded {
		t.Errorf("instance B = %+v, want its config copy loaded", b.Keys)
	}
}

// A key add-key stored WITHOUT --instance (tag "") belongs to the main
// instance; re-adding it on the page for the main instance is "already
// stored", not a move (review of Phase C, finding 5). The tag resolves the
// way the running processes load it (forgekeys.InstanceForTag).
func TestAdminAPIKeysAddUntaggedMainIsSameInstance(t *testing.T) {
	registry := map[string]model.Platform{"https://gitlab.com": 2, "https://b.example.invalid": 101}
	store := &fakeAPIKeyStore{registry: registry}
	if _, err := store.InsertAPIKey(context.Background(), "cli", "glpat-cli-untagged-00", "gitlab", ""); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		instance, wantIn string
	}{
		{"https://gitlab.com", "already stored (oauth_id 1)"},
		{"https://b.example.invalid", "remove it first"},
	} {
		s := adminKeysServer(store, &bytes.Buffer{})
		rec := httptest.NewRecorder()
		s.handleAdminAPIKeyAdd(rec, adminReq("POST", "/api/v1/admin/api-keys", `{"platform":"gitlab","instance_url":"`+tc.instance+`","token":"glpat-cli-untagged-00"}`))
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), tc.wantIn) {
			t.Errorf("re-add for %s = %d %q, want 409 containing %q", tc.instance, rec.Code, rec.Body, tc.wantIn)
		}
	}
}
