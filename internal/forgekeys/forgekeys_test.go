// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package forgekeys

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

func dbKeys(tag string, ids []int64, toks ...string) []db.StoredAPIKey {
	out := make([]db.StoredAPIKey, 0, len(toks))
	for i, tok := range toks {
		out = append(out, db.StoredAPIKey{OAuthID: ids[i], Token: tok, InstanceURL: tag})
	}
	return out
}

func concat(parts ...[]db.StoredAPIKey) []db.StoredAPIKey {
	var out []db.StoredAPIKey
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// v0.30.0 (multi-instance GitLab), moved from cmd/aveloxis in Phase C so
// startup, the live reload, add-key and the admin API share ONE resolver
// (SR-17): every GitLab token belongs to exactly one instance. A config
// token goes to its instance; a stored token to the instance its tag names
// (compared normalized, scheme-less); an untagged stored token and every
// Augur token to the MAIN instance only; a tag no configured instance has is
// never loaded; an instance with no tokens gets none.
func TestPartitionGitLabTokensNoCrossInstanceLeak(t *testing.T) {
	main := config.GitLabInstance{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", APIKeys: []string{"main-cfg"}, Primary: true}
	fd := config.GitLabInstance{WebBase: "https://gitlab.freedesktop.org", APIURL: "https://gitlab.freedesktop.org/api/v4", APIKeys: []string{"fd-cfg"}}
	salsa := config.GitLabInstance{WebBase: "https://salsa.debian.org", APIURL: "https://salsa.debian.org/api/v4"}
	instances := []config.GitLabInstance{main, fd, salsa}

	stored := Stored{
		Database: concat(
			dbKeys("", []int64{1, 2}, "main-db", "main-cfg"), // a duplicate of a config key for the same instance is kept once
			dbKeys("https://gitlab.freedesktop.org", []int64{3}, "fd-db"),
			dbKeys("https://GitLab.FreeDesktop.org/", []int64{4}, "fd-db2"), // tags compare normalized
			dbKeys("https://gone.example.org", []int64{5, 6}, "orphan-1", "orphan-2"),
			dbKeys("http://gitlab.freedesktop.org", []int64{7}, "fd-http"), // a scheme twin of the configured instance
		),
		Augur: []string{"augur-1", "main-db"}, // Augur keys are main-instance keys; a duplicate is kept once
	}
	pools, part, err := PartitionGitLabTokens(instances, stored)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"https://gitlab.com":             {"main-cfg", "main-db", "augur-1"},
		"https://gitlab.freedesktop.org": {"fd-cfg", "fd-db", "fd-db2", "fd-http"},
		"https://salsa.debian.org":       nil,
	}
	for base, toks := range want {
		got := append([]string(nil), pools[base]...)
		sort.Strings(got)
		sort.Strings(toks)
		if !reflect.DeepEqual(got, toks) && !(len(got) == 0 && len(toks) == 0) {
			t.Errorf("pool %s = %v, want %v", base, pools[base], toks)
		}
	}
	if len(pools) != len(want) {
		t.Errorf("pools has %d instances, want %d: %v", len(pools), len(want), pools)
	}
	if part.Orphans["https://gone.example.org"] != 2 || len(part.Orphans) != 1 {
		t.Errorf("orphans = %v, want 2 tokens under the unconfigured instance only", part.Orphans)
	}
	// Every loaded key carries its source (config first) and oauth_id.
	src := map[string]Key{}
	for _, keys := range part.Keys {
		for _, k := range keys {
			src[k.Token] = k
		}
	}
	if src["main-cfg"].Source != SourceConfig || src["main-db"].Source != SourceDatabase || src["main-db"].OAuthID != 1 || src["augur-1"].Source != SourceAugur {
		t.Errorf("sources = %+v, want config > database > augur with oauth ids", src)
	}
	orphaned := 0
	for _, nl := range part.NotLoaded {
		if nl.Reason == ReasonOrphan && nl.Tag == "https://gone.example.org" && nl.OAuthID >= 5 && nl.Masked != "" && nl.KeyID != "" {
			orphaned++
		}
		if strings.Contains(nl.Masked, "orphan-") {
			t.Errorf("NotLoaded carries a token-bearing mask for a short token: %+v", nl)
		}
	}
	if orphaned != 2 {
		t.Errorf("NotLoaded orphans = %+v, want both orphan rows with ids, key ids and masks", part.NotLoaded)
	}

	// An unparseable stored tag is an orphan, never a match.
	_, part, err = PartitionGitLabTokens(instances, Stored{Database: dbKeys("not a url", []int64{9}, "x")})
	if err != nil || part.Orphans["not a url"] != 1 {
		t.Errorf("unparseable tag: orphans=%v err=%v", part.Orphans, err)
	}
}

// A token in two instances' CONFIG entries is fatal (the operator owns that
// file, and startup refuses it). A STORED token whose instance differs from
// the config entry holding the same token is not loaded — config wins — and
// is reported as a conflict, identically at startup and on every reload: a
// database row can never take a working config key out of service, and a
// key added through the admin page can never stop the next restart.
func TestPartitionGitLabTokensConflicts(t *testing.T) {
	main := config.GitLabInstance{WebBase: "https://gitlab.com", APIURL: "https://gitlab.com/api/v4", APIKeys: []string{"main-cfg"}, Primary: true}
	fd := config.GitLabInstance{WebBase: "https://gitlab.freedesktop.org", APIURL: "https://gitlab.freedesktop.org/api/v4", APIKeys: []string{"fd-cfg"}}
	salsa := config.GitLabInstance{WebBase: "https://salsa.debian.org", APIURL: "https://salsa.debian.org/api/v4"}

	_, _, err := PartitionGitLabTokens([]config.GitLabInstance{main, {WebBase: fd.WebBase, APIURL: fd.APIURL, APIKeys: []string{"main-cfg"}}}, Stored{})
	if err == nil || !strings.Contains(err.Error(), "https://gitlab.com") || !strings.Contains(err.Error(), "https://gitlab.freedesktop.org") {
		t.Fatalf("a token in two instances' config entries = %v, want an error naming both", err)
	}

	pools, part, err := PartitionGitLabTokens([]config.GitLabInstance{main, fd, salsa}, Stored{
		Database: dbKeys("https://salsa.debian.org", []int64{42}, "fd-cfg"),
		Augur:    []string{"fd-cfg"},
	})
	if err != nil {
		t.Fatalf("a stored token conflicting with a config key must not be fatal: %v", err)
	}
	if !reflect.DeepEqual(pools["https://gitlab.freedesktop.org"], []string{"fd-cfg"}) || len(pools["https://salsa.debian.org"]) != 0 || len(pools["https://gitlab.com"]) != 1 {
		t.Fatalf("pools = %v, want fd-cfg only on freedesktop (config wins)", pools)
	}
	conflicts := map[Source]NotLoaded{}
	for _, nl := range part.NotLoaded {
		if nl.Reason == ReasonConflict {
			conflicts[nl.Source] = nl
		}
	}
	db42, aug := conflicts[SourceDatabase], conflicts[SourceAugur]
	if db42.OAuthID != 42 || db42.Tag != "https://salsa.debian.org" || !strings.Contains(db42.Detail, "https://gitlab.freedesktop.org") {
		t.Errorf("database conflict = %+v, want oauth_id 42 under salsa naming freedesktop", db42)
	}
	if aug.Reason != ReasonConflict || aug.OAuthID != 0 {
		t.Errorf("augur conflict = %+v, want a conflict with no oauth_id", aug)
	}
}

// GitHub has one instance: config ∪ stored ∪ Augur, each token once, in
// precedence order config > database > augur (the pre-Phase C loader
// appended config and database tokens without dedup, so one token could be
// two pool entries).
func TestGitHubKeysDedupPrecedence(t *testing.T) {
	keys := GitHubKeys([]string{"a", "b", "a"}, Stored{
		Database: dbKeys("", []int64{10, 11}, "b", "c"),
		Augur:    []string{"c", "d"},
	})
	got := make([]string, 0, len(keys))
	for _, k := range keys {
		got = append(got, string(k.Source)+":"+k.Token)
	}
	want := []string{"config:a", "config:b", "database:c", "augur:d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GitHubKeys = %v, want %v", got, want)
	}
	if keys[2].OAuthID != 11 || !reflect.DeepEqual(Tokens(keys), []string{"a", "b", "c", "d"}) {
		t.Fatalf("oauth id / Tokens wrong: %+v", keys)
	}
}

func TestInstanceForTag(t *testing.T) {
	instances := []config.GitLabInstance{
		{WebBase: "https://gitlab.com", Primary: true},
		{WebBase: "https://code.example.edu/gitlab"},
	}
	for tag, want := range map[string]string{
		"":                                 "https://gitlab.com",
		"https://gitlab.com":               "https://gitlab.com",
		"http://Code.Example.edu/gitlab/":  "https://code.example.edu/gitlab",
		"https://code.example.edu":         "",
		"https://code.example.edu/gitlab2": "",
		"not a url":                        "",
	} {
		got, ok := InstanceForTag(instances, tag)
		if got != want || ok != (want != "") {
			t.Errorf("InstanceForTag(%q) = (%q, %v), want %q", tag, got, ok, want)
		}
	}
}

// The Augur fallback is decided ONCE — on the first SUCCESSFUL read of a
// platform's stored keys — and held for the process. A read error before
// that decides nothing (SR-5; review of Phase C, finding 6): a transient
// error at startup must not leave an Augur-keys-only fleet keyless until
// restart. After the decision, adding the first stored key does not swap
// Augur's keys out, and removing the last does not swap them back.
func TestLoaderAugurDecision(t *testing.T) {
	ctx := context.Background()
	var fail bool
	stored := []db.StoredAPIKey{}
	l := newLoaderWith(true, discard(),
		func(context.Context, string) ([]db.StoredAPIKey, error) {
			if fail {
				return nil, errors.New("connection reset")
			}
			return stored, nil
		},
		func(context.Context, string) ([]string, error) { return []string{"augur-1"}, nil })

	fail = true
	if _, err := l.Load(ctx, "github"); err == nil {
		t.Fatal("a failed read must be returned")
	}
	if l.AugurFallback("github") {
		t.Fatal("a failed read must not decide the fallback")
	}

	fail = false
	got, err := l.Load(ctx, "github")
	if err != nil || len(got.Augur) != 1 || !l.AugurFallback("github") {
		t.Fatalf("first successful read with no stored keys = (%+v, %v), want Augur keys included", got, err)
	}

	stored = []db.StoredAPIKey{{OAuthID: 1, Token: "gui-key"}}
	got, err = l.Load(ctx, "github")
	if err != nil || len(got.Augur) != 1 || len(got.Database) != 1 {
		t.Fatalf("after a stored key appears = (%+v, %v), want the decision held (Augur keys still included)", got, err)
	}

	// A platform first read WITH stored keys decides "no Augur".
	l2 := newLoaderWith(true, discard(),
		func(context.Context, string) ([]db.StoredAPIKey, error) {
			return []db.StoredAPIKey{{OAuthID: 2, Token: "k"}}, nil
		},
		func(context.Context, string) ([]string, error) { return []string{"augur-1"}, nil })
	if got, _ := l2.Load(ctx, "gitlab"); len(got.Augur) != 0 || l2.AugurFallback("gitlab") {
		t.Fatalf("stored keys at the first read = %+v, want no Augur keys", got)
	}
	// use_augur_keys off: never.
	l3 := newLoaderWith(false, discard(),
		func(context.Context, string) ([]db.StoredAPIKey, error) { return nil, nil },
		func(context.Context, string) ([]string, error) { return []string{"augur-1"}, nil })
	if got, _ := l3.Load(ctx, "github"); len(got.Augur) != 0 {
		t.Fatalf("use_augur_keys off = %+v, want no Augur keys", got)
	}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
