// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package forgekeys

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

type fakeLoader struct {
	mu     sync.Mutex
	stored map[string]Stored
	err    map[string]error
}

func (f *fakeLoader) Load(_ context.Context, p string) (Stored, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.err[p]; err != nil {
		return Stored{}, err
	}
	return f.stored[p], nil
}

type fakeReports struct {
	mu      sync.Mutex
	saved   map[string][]byte
	saveErr error
}

func (f *fakeReports) SaveForgeKeyReport(_ context.Context, reporter string, report []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	if f.saved == nil {
		f.saved = map[string][]byte{}
	}
	f.saved[reporter] = append([]byte(nil), report...)
	return nil
}

type maintainerFixture struct {
	m       *Maintainer
	loader  *fakeLoader
	reports *fakeReports
	gh      *platform.KeyPool
	gl      *gitlab.Instances
	logs    *bytes.Buffer
}

// newMaintainerFixture: GitHub with one config key; GitLab main instance A
// with a config key and a keyless instance B — both with a pool, as
// buildForgeClients builds them.
func newMaintainerFixture(t *testing.T) *maintainerFixture {
	t.Helper()
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	instances := []config.GitLabInstance{
		{WebBase: "https://a.example.invalid", APIURL: "https://a.example.invalid/api/v4", APIKeys: []string{"glpat-a-config-000"}, Primary: true},
		{WebBase: "https://b.example.invalid", APIURL: "https://api.b.example.invalid/api/v4"},
	}
	pools, _, err := PartitionGitLabTokens(instances, Stored{})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]model.Platform{"https://a.example.invalid": model.PlatformGitLab, "https://b.example.invalid": 101}
	var specs []*gitlab.InstanceSpec
	for _, in := range instances {
		pool := platform.NewKeyPool(pools[in.WebBase], logger)
		c, err := gitlab.New(ids[in.WebBase], in.WebBase, in.APIURL, pool, logger)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, &gitlab.InstanceSpec{ID: ids[in.WebBase], WebBase: in.WebBase, APIURL: in.APIURL, Primary: in.Primary, Client: c})
	}
	router, err := gitlab.NewInstances(specs)
	if err != nil {
		t.Fatal(err)
	}
	gh := platform.NewKeyPool([]string{"ghp_config_0000000000"}, logger)
	f := &maintainerFixture{
		loader:  &fakeLoader{stored: map[string]Stored{}, err: map[string]error{}},
		reports: &fakeReports{},
		gh:      gh, gl: router, logs: logs,
	}
	f.m = NewMaintainer(MaintainerConfig{
		GitHubConfigTokens: []string{"ghp_config_0000000000"},
		GitHubAPIURL:       "https://api.github.com",
		GitHub:             gh,
		Instances:          instances,
		GitLab:             router,
		Loader:             f.loader,
		Reports:            f.reports,
		Reporter:           "serve@test#boot",
		Logger:             logger,
	})
	return f
}

func activeKeyIDs(snaps []platform.KeySnapshot) map[string]bool {
	out := map[string]bool{}
	for _, s := range snaps {
		if s.State == platform.KeyActive {
			out[s.KeyID] = true
		}
	}
	return out
}

// The live reload: a stored key added for keyless instance B makes B
// collectable and reaches only B's pool; removing it takes B back out.
func TestMaintainerReloadAppliesPerInstance(t *testing.T) {
	f := newMaintainerFixture(t)
	ctx := context.Background()
	if _, err := f.gl.ForRepo(101, "https://b.example.invalid/g/r"); !errors.Is(err, gitlab.ErrInstanceNotConfigured) {
		t.Fatalf("B before reload: %v", err)
	}

	f.loader.stored["gitlab"] = Stored{Database: []db.StoredAPIKey{{OAuthID: 7, Token: "glpat-b-stored-000", InstanceURL: "https://b.example.invalid"}}}
	f.loader.stored["github"] = Stored{Database: []db.StoredAPIKey{{OAuthID: 8, Token: "ghp_stored_00000000000"}}}
	if err := f.m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.gl.ForRepo(101, "https://b.example.invalid/g/r"); err != nil {
		t.Fatalf("B after a key was stored for it: %v", err)
	}
	for _, ik := range f.gl.KeySnapshots() {
		ids := activeKeyIDs(ik.Keys)
		if ik.WebBase == "https://a.example.invalid" && (ids[platform.KeyID("glpat-b-stored-000")] || !ids[platform.KeyID("glpat-a-config-000")]) {
			t.Fatalf("A's keys = %v: B's stored key leaked into A, or A lost its config key", ids)
		}
		if ik.WebBase == "https://b.example.invalid" && (len(ids) != 1 || !ids[platform.KeyID("glpat-b-stored-000")]) {
			t.Fatalf("B's keys = %v, want exactly its stored key", ids)
		}
	}
	gh, _ := f.gh.Snapshot()
	if ids := activeKeyIDs(gh); len(ids) != 2 {
		t.Fatalf("GitHub keys after reload = %v, want config + stored", ids)
	}

	f.loader.stored["gitlab"] = Stored{}
	if err := f.m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.gl.ForRepo(101, "https://b.example.invalid/g/r"); !errors.Is(err, gitlab.ErrInstanceNotConfigured) {
		t.Fatalf("B after its key was removed: %v, want not configured", err)
	}
	if !strings.Contains(f.logs.String(), "API keys reloaded") {
		t.Errorf("a reload that changed pools must log it; logs:\n%s", f.logs.String())
	}
}

// SR-5: a failed read changes NOTHING — not even the pool whose read
// succeeded — and never reads as "no stored keys".
func TestReloadReadErrorChangesNothing(t *testing.T) {
	f := newMaintainerFixture(t)
	ctx := context.Background()
	f.loader.stored["github"] = Stored{Database: []db.StoredAPIKey{{OAuthID: 8, Token: "ghp_stored_00000000000"}}}
	f.loader.stored["gitlab"] = Stored{Database: []db.StoredAPIKey{{OAuthID: 7, Token: "glpat-b-stored-000", InstanceURL: "https://b.example.invalid"}}}
	if err := f.m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := f.gh.Snapshot()

	for _, failing := range []string{"github", "gitlab"} {
		f.loader.err = map[string]error{failing: errors.New("connection reset")}
		f.loader.stored["github"] = Stored{} // would remove the stored GitHub key if applied
		f.loader.stored["gitlab"] = Stored{}
		if err := f.m.Reload(ctx); err == nil {
			t.Fatalf("%s read error: Reload returned nil", failing)
		}
		after, _ := f.gh.Snapshot()
		if len(activeKeyIDs(after)) != len(activeKeyIDs(before)) {
			t.Fatalf("%s read error changed the GitHub pool: %v -> %v", failing, activeKeyIDs(before), activeKeyIDs(after))
		}
		if _, err := f.gl.ForRepo(101, "https://b.example.invalid/g/r"); err != nil {
			t.Fatalf("%s read error removed B's key: %v", failing, err)
		}
	}
}

// A context cancelled during the read is shutdown, not a failure: Reload
// returns it and logs nothing at ERROR.
func TestReloadCancellationIsQuiet(t *testing.T) {
	f := newMaintainerFixture(t)
	f.loader.err = map[string]error{"github": context.Canceled}
	err := f.m.Reload(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Reload = %v, want context.Canceled", err)
	}
	if strings.Contains(f.logs.String(), "level=ERROR") {
		t.Errorf("cancellation logged at ERROR:\n%s", f.logs.String())
	}
}

// Orphans WARN and conflicts ERROR once per change, not every tick.
func TestReloadWarnsOncePerChange(t *testing.T) {
	f := newMaintainerFixture(t)
	ctx := context.Background()
	f.loader.stored["gitlab"] = Stored{Database: []db.StoredAPIKey{
		{OAuthID: 1, Token: "glpat-orphan-0000", InstanceURL: "https://gone.example.invalid"},
		{OAuthID: 2, Token: "glpat-a-config-000", InstanceURL: "https://b.example.invalid"},
	}}
	for i := 0; i < 3; i++ {
		if err := f.m.Reload(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(f.logs.String(), "stored GitLab keys name an instance that is not configured"); n != 1 {
		t.Errorf("orphan WARN logged %d times over 3 identical reloads, want 1", n)
	}
	if n := strings.Count(f.logs.String(), "stored GitLab key conflicts with a config key"); n != 1 {
		t.Errorf("conflict ERROR logged %d times over 3 identical reloads, want 1", n)
	}
	if strings.Contains(f.logs.String(), "glpat-orphan-0000") || strings.Contains(f.logs.String(), "glpat-a-config-000") {
		t.Errorf("a log line carries a raw token:\n%s", f.logs.String())
	}
}

// The report carries every pool's keys by key_id with source, health and
// the instance's API URL, the refused keys, and never a token.
func TestMaintainerReport(t *testing.T) {
	f := newMaintainerFixture(t)
	ctx := context.Background()
	f.loader.stored["gitlab"] = Stored{Database: []db.StoredAPIKey{
		{OAuthID: 7, Token: "glpat-b-stored-000", InstanceURL: "https://b.example.invalid"},
		{OAuthID: 9, Token: "glpat-orphan-0000", InstanceURL: "https://gone.example.invalid"},
	}}
	if err := f.m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.m.Report(ctx); err != nil {
		t.Fatal(err)
	}
	raw := f.reports.saved["serve@test#boot"]
	for _, tok := range []string{"glpat-b-stored-000", "glpat-a-config-000", "ghp_config_0000000000", "glpat-orphan-0000"} {
		if bytes.Contains(raw, []byte(tok)) {
			t.Fatalf("the report carries token %s: %s", tok, raw)
		}
	}
	var r Report
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if r.Reporter != "serve@test#boot" || r.IntervalSeconds != int(Interval.Seconds()) {
		t.Errorf("report header = %+v", r)
	}
	inst := map[string]ReportInstance{}
	for _, in := range r.Instances {
		inst[in.WebURL] = in
	}
	if gh := inst["https://github.com"]; gh.Platform != "github" || gh.APIURL != "https://api.github.com" || gh.ActiveKeys != 1 {
		t.Errorf("github instance = %+v", gh)
	}
	if b := inst["https://b.example.invalid"]; b.PlatformID != 101 || b.APIURL != "https://api.b.example.invalid/api/v4" || b.ActiveKeys != 1 || b.Main {
		t.Errorf("instance B = %+v", b)
	}
	keys := map[string]ReportKey{}
	for _, k := range r.Keys {
		keys[k.KeyID] = k
	}
	bk := keys[platform.KeyID("glpat-b-stored-000")]
	if bk.OAuthID != 7 || bk.Source != SourceDatabase || bk.InstanceURL != "https://b.example.invalid" || bk.PlatformID != 101 ||
		bk.Health != platform.HealthOK || bk.State != platform.KeyActive || bk.Masked != platform.MaskToken("glpat-b-stored-000") {
		t.Errorf("B's stored key in the report = %+v", bk)
	}
	if ak := keys[platform.KeyID("glpat-a-config-000")]; ak.Source != SourceConfig || ak.GraphQLRemaining != nil {
		t.Errorf("A's config key = %+v, want source config and no GraphQL budget (GitLab)", ak)
	}
	if gk := keys[platform.KeyID("ghp_config_0000000000")]; gk.GraphQLRemaining == nil || gk.Platform != "github" {
		t.Errorf("GitHub key = %+v, want a GraphQL budget", gk)
	}
	if len(r.NotLoaded) != 1 || r.NotLoaded[0].OAuthID != 9 || r.NotLoaded[0].Reason != ReasonOrphan {
		t.Errorf("not_loaded = %+v, want the orphan row 9", r.NotLoaded)
	}
	if len(r.Unregistered) != 0 {
		t.Errorf("unregistered = %+v, want none (both configured instances are routed)", r.Unregistered)
	}

	// A configured instance the registry has no id for yet is reported as
	// unregistered, with its API URL.
	f.m.cfg.Instances = append(f.m.cfg.Instances, config.GitLabInstance{WebBase: "https://c.example.invalid", APIURL: "https://c.example.invalid/api/v4"})
	if err := f.m.Report(ctx); err != nil {
		t.Fatal(err)
	}
	var r2 Report
	if err := json.Unmarshal(f.reports.saved["serve@test#boot"], &r2); err != nil {
		t.Fatal(err)
	}
	if len(r2.Unregistered) != 1 || r2.Unregistered[0].WebURL != "https://c.example.invalid" || r2.Unregistered[0].APIURL != "https://c.example.invalid/api/v4" {
		t.Errorf("unregistered = %+v, want instance C", r2.Unregistered)
	}
}

// A failed save is returned to the caller, which classifies and logs it.
func TestReportSaveErrorReturned(t *testing.T) {
	f := newMaintainerFixture(t)
	f.reports.saveErr = errors.New("db down")
	if err := f.m.Report(context.Background()); err == nil {
		t.Fatal("a failed save must be returned")
	}
}

// The web process's org-scan reload is a GitHub-only Maintainer (no GitLab
// router): it reads and reconciles GitHub keys only, and serializes like
// serve's — two org scans can run at once, and an older read applied over a
// newer one would bring a removed key back (review of Phase C, finding 7).
func TestGitHubOnlyMaintainer(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	pool := platform.NewKeyPool([]string{"ghp_config_0000000000"}, logger)
	loader := &fakeLoader{stored: map[string]Stored{
		"github": {Database: []db.StoredAPIKey{{OAuthID: 1, Token: "ghp_stored_00000000000"}}},
	}, err: map[string]error{"gitlab": errors.New("must not be read")}}
	m := NewMaintainer(MaintainerConfig{GitHubConfigTokens: []string{"ghp_config_0000000000"}, GitHub: pool, Loader: loader, Logger: logger})
	if err := m.Reload(context.Background()); err != nil {
		t.Fatalf("GitHub-only reload = %v (it must not read GitLab keys)", err)
	}
	if pool.Len() != 2 {
		t.Fatalf("pool has %d keys, want config + stored", pool.Len())
	}
	if err := m.Report(context.Background()); err != nil {
		t.Fatalf("a maintainer without a report store must not fail Report: %v", err)
	}
}
