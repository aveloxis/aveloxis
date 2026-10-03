// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// api.response_cache_mb and api.cache_rewarm_seconds (v0.29.73): absent →
// the default, an explicit 0 → off, a positive value as written, and a value
// no accessor can honour refused at load naming the key (SR-10: one default
// layer, never a silent clamp).
func TestAPIResponseCacheKnobs(t *testing.T) {
	write := func(body string) string {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := []struct {
		json      string
		wantBytes int64
		wantEvery time.Duration
	}{
		{`{}`, 2048 << 20, time.Minute},
		{`{"api": {"response_cache_mb": 1, "cache_rewarm_seconds": 1}}`, 1 << 20, time.Second},
		{`{"api": {"response_cache_mb": 0, "cache_rewarm_seconds": 0}}`, 0, 0},
	}
	for _, c := range cases {
		cfg, err := Load(write(c.json))
		if err != nil {
			t.Fatalf("%s: %v", c.json, err)
		}
		if got := cfg.API.ResponseCacheBytes(); got != c.wantBytes {
			t.Errorf("%s: ResponseCacheBytes = %d, want %d", c.json, got, c.wantBytes)
		}
		if got := cfg.API.CacheRewarmInterval(); got != c.wantEvery {
			t.Errorf("%s: CacheRewarmInterval = %v, want %v", c.json, got, c.wantEvery)
		}
	}
	for _, bad := range []struct{ json, key string }{
		{`{"api": {"response_cache_mb": -1}}`, "response_cache_mb"},
		{`{"api": {"response_cache_mb": 9223372036854775807}}`, "response_cache_mb"},
		{`{"api": {"cache_rewarm_seconds": -1}}`, "cache_rewarm_seconds"},
		{`{"api": {"cache_rewarm_seconds": 9223372036854775807}}`, "cache_rewarm_seconds"},
	} {
		if _, err := Load(write(bad.json)); err == nil || !strings.Contains(err.Error(), bad.key) {
			t.Errorf("%s must be refused at load naming %s, got %v", bad.json, bad.key, err)
		}
	}
	// The largest accepted values stay inside int64 / time.Duration (on a
	// 64-bit build; on 32 bits an int cannot reach them).
	if strconv.IntSize == 64 {
		mb, sec := MaxResponseCacheMB, MaxCacheRewarmSeconds // variables: no constant int conversion
		if b := (APIConfig{ResponseCacheMB: intPtr(int(mb))}).ResponseCacheBytes(); b <= 0 {
			t.Errorf("MaxResponseCacheMB overflows: %d", b)
		}
		if d := (APIConfig{CacheRewarmSeconds: intPtr(int(sec))}).CacheRewarmInterval(); d <= 0 {
			t.Errorf("MaxCacheRewarmSeconds overflows: %v", d)
		}
	}
}

func intPtr(n int) *int { return &n }

// api.front_end_secret (v0.29.73, review round 2): empty is off; a value an
// attacker could guess is refused at load, naming the key and never echoing
// the value.
func TestAPIFrontEndSecret(t *testing.T) {
	write := func(body string) string {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg, err := Load(write(`{}`))
	if err != nil || cfg.API.FrontEndSecret != "" {
		t.Fatalf("absent: %q, %v; want empty (off)", cfg.API.FrontEndSecret, err)
	}
	good := strings.Repeat("a1", MinFrontEndSecretLen/2)
	if cfg, err := Load(write(`{"api": {"front_end_secret": "` + good + `"}}`)); err != nil || cfg.API.FrontEndSecret != good {
		t.Errorf("a %d-character secret must load: %v", len(good), err)
	}
	// The shipped nginx placeholder (aveloxis-gui deploy/nginx-cache) is
	// short on purpose, so a copied placeholder is refused, never a secret
	// everyone knows (review round 3).
	if _, err := Load(write(`{"api": {"front_end_secret": "REPLACE_ME"}}`)); err == nil {
		t.Error("the shipped placeholder must be refused at load")
	}
	short := good[:MinFrontEndSecretLen-1]
	_, err = Load(write(`{"api": {"front_end_secret": "` + short + `"}}`))
	if err == nil || !strings.Contains(err.Error(), "front_end_secret") {
		t.Errorf("a %d-character secret must be refused naming the key, got %v", len(short), err)
	} else if strings.Contains(err.Error(), short) {
		t.Error("the refusal must not echo the secret")
	}
}
