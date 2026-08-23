// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.1 (A6) — mark-gone-repos command contract.

func markGoneSrc(t *testing.T) string {
	t.Helper()
	return srctest.Read(t, "cmd/aveloxis/mark_gone_repos.go")
}

func TestMarkGoneReposCommandRegistered(t *testing.T) {
	main, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(main), "markGoneReposCmd(&cfgPath)") {
		t.Error("mark-gone-repos must be registered in main.go")
	}
	src := markGoneSrc(t)
	for _, needle := range []string{`Use:   "mark-gone-repos"`, `"dry-run"`, `"limit"`} {
		if !strings.Contains(src, needle) {
			t.Errorf("mark_gone_repos.go must contain %q", needle)
		}
	}
}

// v0.21.5 policy: only serve + migrate run migrations. Strip
// comments first so prose mentioning the contract can't false-match.
func TestMarkGoneReposDoesNotMigrate(t *testing.T) {
	src := markGoneSrc(t)
	var code []string
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("mark-gone-repos must NOT run migrations (v0.21.5 contract)")
	}
}

// SR-16: only DEFINITIVE probe answers decide. A transport error and
// every indeterminate status must SKIP (rerun retries) — never stamp,
// never clear. And the probe must be the SHARED resolver (SR-17), not
// a private HTTP client.
func TestMarkGoneReposIsDefinitiveOnly(t *testing.T) {
	src := markGoneSrc(t)
	if !strings.Contains(src, "collector.ResolveRedirectTarget(") {
		t.Error("the probe must reuse collector.ResolveRedirectTarget — one probe for prelim, reconcile-repos, and this command")
	}
	if !strings.Contains(src, "http.StatusNotFound") || !strings.Contains(src, "http.StatusGone") {
		t.Error("gone requires a definitive 404/410")
	}
	// The error arm must skip, not decide.
	i := strings.Index(src, "perr != nil")
	if i < 0 {
		t.Fatal("probe error arm missing")
	}
	errArm := src[i:]
	if end := strings.Index(errArm, "}"); end > 0 {
		errArm = errArm[:end]
	}
	if !strings.Contains(errArm, "skipped++") || !strings.Contains(errArm, "continue") {
		t.Error("a probe ERROR must skip the repo (SR-16), never stamp or clear")
	}
	// Resurrection is bidirectional: 200 on a gone-stamped repo
	// clears + re-enqueues.
	if !strings.Contains(src, "ClearRepoGone(") || !strings.Contains(src, "EnqueueRepo(") {
		t.Error("a definitive 200 on a gone-stamped repo must clear the stamp and re-enqueue")
	}
}
