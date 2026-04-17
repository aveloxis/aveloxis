package gitlab

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// captureHandler is a slog.Handler that appends every log record's message
// (plus its attrs) to an in-memory buffer so tests can assert on what was
// logged. Safe for concurrent use by one goroutine at a time.
type captureHandler struct {
	mu   sync.Mutex
	logs []capturedLog
}

type capturedLog struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := capturedLog{level: r.Level, message: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		entry.attrs[a.Key] = a.Value.Any()
		return true
	})
	h.logs = append(h.logs, entry)
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *captureHandler) has(level slog.Level, substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.logs {
		if e.level == level && strings.Contains(strings.ToLower(e.message), strings.ToLower(substr)) {
			return true
		}
	}
	return false
}

// newTestClientWithCapture returns a GitLab Client wired to an httptest.Server
// and a captureHandler so tests can assert on log output.
func newTestClientWithCapture(t *testing.T, handler http.Handler) (*Client, *captureHandler) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	capture := &captureHandler{}
	logger := slog.New(capture)
	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	httpClient := platform.NewHTTPClient(server.URL, keys, logger, platform.AuthGitLab)
	return &Client{http: httpClient, logger: logger, host: "gitlab.com"}, capture
}

// repoInfoRouter builds a handler that simulates the GitLab endpoints that
// FetchRepoInfo calls. projectJSON controls what /projects/:pp?statistics=true
// returns; everything else gets minimal valid responses so the call completes.
func repoInfoRouter(projectJSON string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/issues_statistics"):
			w.Write([]byte(`{"statistics":{"counts":{"all":0,"closed":0,"opened":0}}}`))
		case strings.Contains(path, "/merge_requests"):
			w.Header().Set("X-Total", "0")
			w.Write([]byte(`[]`))
		case strings.HasSuffix(path, "/repository/tree"):
			w.Write([]byte(`[]`))
		default:
			// Primary /projects/:pp response (statistics=true).
			w.Write([]byte(projectJSON))
		}
	})
}

// TestFetchRepoInfoStatisticsNilLogsWarning — when GitLab returns a project
// payload without a `statistics` object, FetchRepoInfo currently silently
// reports CommitCount=0. We want a WARN log so ops can see this case, and we
// want CommitCount=0 preserved (since the real count comes later from the
// facade phase and is backfilled at the store level).
func TestFetchRepoInfoStatisticsNilLogsWarning(t *testing.T) {
	// Note: no "statistics" key at all — GitLab does this when the token
	// lacks Reporter+ access on private projects or on some self-hosted setups.
	projectJSON := `{
		"id": 101,
		"default_branch": "main",
		"web_url": "https://gitlab.com/owner/repo",
		"star_count": 1,
		"forks_count": 0,
		"open_issues_count": 0,
		"last_activity_at": "2026-01-01T00:00:00Z",
		"archived": false,
		"issues_enabled": true,
		"merge_requests_enabled": true,
		"wiki_enabled": true,
		"pages_access_level": "enabled"
	}`

	client, capture := newTestClientWithCapture(t, repoInfoRouter(projectJSON))

	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.CommitCount != 0 {
		t.Errorf("CommitCount = %d, want 0 when statistics is nil (facade+backfill supplies real value later)", info.CommitCount)
	}
	if !capture.has(slog.LevelWarn, "statistics") {
		t.Error("expected a WARN log mentioning 'statistics' when GitLab returns no statistics object — without visibility, ops cannot tell 'repo has zero commits' apart from 'token lacks Reporter access'")
	}
}

// TestFetchRepoInfoCommitCountZeroLogsInfo — when GitLab returns statistics
// with commit_count=0 (common for freshly-mirrored/imported projects while
// the async stats worker has not yet run), log a hint so we can diagnose
// and so the downstream facade backfill has a signal to work with.
func TestFetchRepoInfoCommitCountZeroLogsInfo(t *testing.T) {
	projectJSON := `{
		"id": 102,
		"default_branch": "main",
		"web_url": "https://gitlab.com/owner/repo",
		"statistics": { "commit_count": 0 }
	}`

	client, capture := newTestClientWithCapture(t, repoInfoRouter(projectJSON))

	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.CommitCount != 0 {
		t.Errorf("CommitCount = %d, want 0", info.CommitCount)
	}
	if !capture.has(slog.LevelInfo, "commit_count") && !capture.has(slog.LevelWarn, "commit_count") {
		t.Error("expected an INFO/WARN log mentioning 'commit_count' when GitLab reports zero commits — this is how ops see the stale-stats case so they understand why repo_info.commit_count is 0 pre-backfill")
	}
}

// TestFetchRepoInfoPopulatedStatsNoWarning — the happy path must be silent.
// Logging on every successful fetch would spam logs unacceptably.
func TestFetchRepoInfoPopulatedStatsNoWarning(t *testing.T) {
	projectJSON := `{
		"id": 103,
		"default_branch": "main",
		"web_url": "https://gitlab.com/owner/repo",
		"statistics": { "commit_count": 12345 }
	}`

	client, capture := newTestClientWithCapture(t, repoInfoRouter(projectJSON))

	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}
	if info.CommitCount != 12345 {
		t.Errorf("CommitCount = %d, want 12345", info.CommitCount)
	}
	if capture.has(slog.LevelWarn, "statistics") || capture.has(slog.LevelWarn, "commit_count") {
		t.Error("happy path must not emit warnings about statistics/commit_count — would spam logs on every successful fetch")
	}
}

// Keep the encoding/json import used above even when tests with inline
// literals dominate — future tests may encode structs directly.
var _ = json.Marshal
