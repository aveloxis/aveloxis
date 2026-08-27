// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// v0.28.18 — the count probes carry an error arm (SR-16). Pre-.18
// countGitLabResource returned 0 on ANY error and on a missing X-Total
// (GitLab omits it above 10,000 records), so the snapshot stored
// pr_count = 0 and the accurate prior snapshot rotated to history. The
// L10 re-run added: disabled features are a DEFINITIVE zero (no probes,
// no logs), and the >10,000 case resolves through GitLab GraphQL — the
// count REST cannot give — with the residual logged at INFO, not WARN.
type countsRouterState struct {
	mode       string
	probeHits  int32 // issues_statistics + merge_requests REST probes
	graphqlHit int32
}

func (st *countsRouterState) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/api/graphql"):
			atomic.AddInt32(&st.graphqlHit, 1)
			body, _ := io.ReadAll(r.Body)
			var req struct {
				Variables map[string]any `json:"variables"`
			}
			_ = json.Unmarshal(body, &req)
			if st.mode == "no-x-total-graphql" && req.Variables["fullPath"] == "owner/repo" {
				// The live 2026-08-27 petsc/petsc shape.
				_, _ = w.Write([]byte(`{"data":{"project":{"opened":{"count":232},"closed":{"count":834},"merged":{"count":8400}}}}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"project":null}}`))
		case strings.HasSuffix(path, "/issues_statistics"):
			atomic.AddInt32(&st.probeHits, 1)
			if st.mode == "issues-forbidden" || st.mode == "members-only" {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"403 Forbidden"}`))
				return
			}
			_, _ = w.Write([]byte(`{"statistics":{"counts":{"all":9,"closed":4,"opened":5}}}`))
		case strings.Contains(path, "/merge_requests"):
			atomic.AddInt32(&st.probeHits, 1)
			state := r.URL.Query().Get("state")
			switch {
			case st.mode == "members-only", st.mode == "merged-forbidden" && state == "merged":
				// 403 is the cheapest definitive-error shape the client does
				// not retry; a 5xx follows the same arm after the retry budget.
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"message":"403 Forbidden"}`))
				return
			case strings.HasPrefix(st.mode, "no-x-total") && state == "opened":
				// GitLab above 10,000 records: 200, no X-Total.
				_, _ = w.Write([]byte(`[{"iid":1}]`))
				return
			}
			totals := map[string]string{"opened": "3", "closed": "2", "merged": "1"}
			w.Header().Set("X-Total", totals[state])
			_, _ = w.Write([]byte(`[{"iid":1}]`))
		case strings.HasSuffix(path, "/repository/tree"):
			_, _ = w.Write([]byte(`[]`))
		case strings.HasSuffix(path, "/languages"):
			_, _ = w.Write([]byte(`{}`))
		default:
			// GitLab's project payload: *_enabled is feature_available?(user)
			// (false for BOTH disabled and members-only-not-a-member);
			// *_access_level is the feature's real setting.
			enabled, level := `true`, `enabled`
			switch st.mode {
			case "disabled":
				enabled, level = `false`, `disabled`
			case "members-only":
				enabled, level = `false`, `private`
			}
			_, _ = w.Write([]byte(`{"id":101,"default_branch":"main","web_url":"https://gitlab.com/owner/repo","path_with_namespace":"owner/repo","star_count":1,"forks_count":0,"open_issues_count":5,"issues_enabled":` + enabled + `,"merge_requests_enabled":` + enabled + `,"issues_access_level":"` + level + `","merge_requests_access_level":"` + level + `","statistics":{"commit_count":12}}`))
		}
	})
}

func TestFetchRepoInfoCountsHealthy(t *testing.T) {
	st := &countsRouterState{mode: "healthy"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.PRCountUnknown || info.IssuesCountUnknown {
		t.Errorf("healthy probes must not mark counts unknown: pr=%v issues=%v", info.PRCountUnknown, info.IssuesCountUnknown)
	}
	if info.PRCount != 6 || info.PRsOpen != 3 || info.PRsClosed != 2 || info.PRsMerged != 1 {
		t.Errorf("PR counts = %d/%d/%d/%d, want 6/3/2/1", info.PRCount, info.PRsOpen, info.PRsClosed, info.PRsMerged)
	}
	if info.IssuesCount != 9 || info.IssuesClosed != 4 {
		t.Errorf("issue counts = %d/%d, want 9/4", info.IssuesCount, info.IssuesClosed)
	}
	if capture.has(slog.LevelWarn, "count unavailable") || capture.has(slog.LevelInfo, "counts unavailable") {
		t.Error("no unavailable-count log expected on the healthy path")
	}
	if atomic.LoadInt32(&st.graphqlHit) != 0 {
		t.Error("GraphQL must not be consulted when REST X-Total is present")
	}
}

func TestFetchRepoInfoDisabledFeaturesAreDefinitiveZero(t *testing.T) {
	st := &countsRouterState{mode: "disabled"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.PRCountUnknown || info.IssuesCountUnknown {
		t.Errorf("issues/MRs DISABLED is a definitive zero, never unknown: pr=%v issues=%v", info.PRCountUnknown, info.IssuesCountUnknown)
	}
	if info.PRCount != 0 || info.IssuesCount != 0 || info.IssuesEnabled || info.PRsEnabled {
		t.Errorf("disabled features: counts %d/%d enabled %v/%v, want 0/0 false/false", info.PRCount, info.IssuesCount, info.IssuesEnabled, info.PRsEnabled)
	}
	if n := atomic.LoadInt32(&st.probeHits); n != 0 {
		t.Errorf("disabled features must not be probed (they 404 and log every cycle) — %d probe requests", n)
	}
	if capture.has(slog.LevelWarn, "unavailable") || capture.has(slog.LevelInfo, "unavailable") {
		t.Error("no unavailable-count log expected for disabled features")
	}
}

// A members-only feature the token cannot see reads *_enabled=false
// exactly like a disabled one — but its counts are UNKNOWN, not zero
// (the L10 re-run's L8 finding: a narrowed token must never store 0 and
// rotate the accurate snapshot away).
func TestFetchRepoInfoMembersOnlyFeaturesAreUnknownNotZero(t *testing.T) {
	st := &countsRouterState{mode: "members-only"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if !info.PRCountUnknown || !info.IssuesCountUnknown {
		t.Errorf("members-only (access_level=private, *_enabled=false, probes 403) must mark counts UNKNOWN: pr=%v issues=%v", info.PRCountUnknown, info.IssuesCountUnknown)
	}
	if n := atomic.LoadInt32(&st.probeHits); n == 0 {
		t.Error("members-only features must still be probed — only access_level=disabled skips the probes")
	}
	if !capture.has(slog.LevelWarn, "unavailable") {
		t.Error("expected the unavailable-count WARN")
	}
}

func TestFetchRepoInfoMarksPRCountsUnknownOnProbeError(t *testing.T) {
	st := &countsRouterState{mode: "merged-forbidden"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo must not fail the whole snapshot on one count probe: %v", err)
	}
	if !info.PRCountUnknown {
		t.Fatal("a failed merge-request count probe must mark PRCountUnknown (pre-v0.28.18: silently stored 0)")
	}
	if info.PRCount != 0 || info.PRsOpen != 0 {
		t.Errorf("unknown PR counts must not carry partial sums (open probe succeeded, merged failed): %d/%d", info.PRCount, info.PRsOpen)
	}
	if info.IssuesCountUnknown || info.IssuesCount != 9 {
		t.Errorf("issue counts are independent of the MR probes: unknown=%v count=%d", info.IssuesCountUnknown, info.IssuesCount)
	}
	if !capture.has(slog.LevelWarn, "merge request count unavailable") {
		t.Error("a transport/HTTP failure is a WARN naming the unavailable merge request count")
	}
	if atomic.LoadInt32(&st.graphqlHit) != 0 {
		t.Error("GraphQL is the fallback for the X-Total LIMIT only, not for transport errors")
	}
}

func TestFetchRepoInfoResolvesCountsAboveXTotalLimitViaGraphQL(t *testing.T) {
	st := &countsRouterState{mode: "no-x-total-graphql"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.PRCountUnknown {
		t.Fatal("GraphQL supplied the counts — they must not be marked unknown")
	}
	if info.PRsOpen != 232 || info.PRsClosed != 834 || info.PRsMerged != 8400 || info.PRCount != 9466 {
		t.Errorf("GraphQL counts = %d/%d/%d total %d, want 232/834/8400 total 9466", info.PRsOpen, info.PRsClosed, info.PRsMerged, info.PRCount)
	}
	if atomic.LoadInt32(&st.graphqlHit) != 1 {
		t.Errorf("expected exactly one GraphQL count query, got %d", st.graphqlHit)
	}
	if capture.has(slog.LevelWarn, "unavailable") {
		t.Error("the documented >10,000 case resolved via GraphQL must not WARN")
	}
}

func TestFetchRepoInfoMarksPRCountsUnknownWhenXTotalOmittedAndGraphQLFails(t *testing.T) {
	st := &countsRouterState{mode: "no-x-total"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if !info.PRCountUnknown {
		t.Fatal("a 200 without X-Total means MORE than GitLab will count (>10,000 records), never zero — with GraphQL null it must mark PRCountUnknown")
	}
	if !capture.has(slog.LevelInfo, "merge request counts unavailable") {
		t.Error("the documented limit is an INFO, not a WARN that fires every cycle forever")
	}
	if capture.has(slog.LevelWarn, "merge request count unavailable") {
		t.Error("no WARN for the documented >10,000 case")
	}
}

func TestFetchRepoInfoMarksIssueCountsUnknownOnStatsError(t *testing.T) {
	st := &countsRouterState{mode: "issues-forbidden"}
	client, capture := newTestClientWithCapture(t, st.handler())
	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if !info.IssuesCountUnknown {
		t.Fatal("a failed issues_statistics read must mark IssuesCountUnknown (pre-v0.28.18: 'counts will be zero')")
	}
	if info.PRCountUnknown || info.PRCount != 6 {
		t.Errorf("PR counts are independent of the issue stats: unknown=%v count=%d", info.PRCountUnknown, info.PRCount)
	}
	if !capture.has(slog.LevelWarn, "issue statistics unavailable") {
		t.Error("expected a WARN naming the unavailable issue statistics")
	}
}
