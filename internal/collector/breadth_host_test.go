// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.57 (Copilot review 5260539069 on PR #210) — the scheduler learned
// cfg.GitHub.BaseURL for its org-scan and analysis clients, but the breadth
// worker built its own client on a hardcoded "https://api.github.com" with
// the SAME key pool. On a GitHub Enterprise deployment the Enterprise token
// therefore still left its configured host, which is the defect the
// scheduler-side fix was supposed to close.
//
// It escaped that fix because the guard enumerating scheduler clients scans
// the SCHEDULER package, and this client is constructed one package away.

import (
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestBreadthWorkerSendsKeysOnlyToTheGivenHost(t *testing.T) {
	const enterprise = "https://ghe.example.invalid/api/v3"
	keys := platform.NewKeyPool([]string{"ghp_enterprise_token_never_to_public_github"}, slog.New(slog.DiscardHandler))

	w := NewBreadthWorker(nil, keys, enterprise, slog.New(slog.DiscardHandler))

	// Checked BEFORE any request: if the base were still public GitHub,
	// driving the worker would hand the Enterprise token to a third party
	// for real. The assertion fails without making that call.
	if got := w.http.BaseURL(); got != enterprise {
		t.Errorf("breadth worker base = %q, want the configured host %q — its keys are the scheduler's", got, enterprise)
	}
}

// An empty base keeps public GitHub, so a deployment that configures nothing
// is unaffected.
func TestBreadthWorkerDefaultsToPublicGitHub(t *testing.T) {
	keys := platform.NewKeyPool([]string{"ghp_x"}, slog.New(slog.DiscardHandler))
	w := NewBreadthWorker(nil, keys, "", slog.New(slog.DiscardHandler))
	if got := w.http.BaseURL(); got != "https://api.github.com" {
		t.Errorf("breadth worker base = %q, want https://api.github.com", got)
	}
}
