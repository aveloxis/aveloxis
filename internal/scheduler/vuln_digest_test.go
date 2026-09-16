// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailer"
)

// TestDigestWindow pins the pure window semantics: first run opens one
// interval back (never the epoch — enabling on an established fleet
// must not dump all history), later runs open at the previous stamp.
func TestDigestWindow(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	since, due := digestWindow(now, time.Time{}, day)
	if !due {
		t.Error("first run must be due immediately")
	}
	if !since.Equal(now.Add(-day)) {
		t.Errorf("first-run window must open one interval back, got %v", since)
	}

	last := now.Add(-2 * time.Hour)
	if _, due := digestWindow(now, last, day); due {
		t.Error("2h after the last digest with a 24h interval must NOT be due")
	}

	last = now.Add(-25 * time.Hour)
	since, due = digestWindow(now, last, day)
	if !due {
		t.Error("25h after the last digest with a 24h interval must be due")
	}
	if !since.Equal(last) {
		t.Errorf("window must open at the previous stamp (no gaps), got %v want %v", since, last)
	}
}

func TestDigestStampRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "vuln-digest-last")
	if got := readDigestStamp(path); !got.IsZero() {
		t.Errorf("absent stamp must read as zero time, got %v", got)
	}
	want := time.Date(2026, 7, 15, 10, 30, 0, 0, time.UTC)
	if err := writeDigestStamp(path, want); err != nil {
		t.Fatalf("write stamp: %v", err)
	}
	if got := readDigestStamp(path); !got.Equal(want) {
		t.Errorf("stamp round-trip: got %v want %v", got, want)
	}
	// Garbled stamp degrades to first-run semantics, never panics.
	if err := os.WriteFile(path, []byte("not-a-number"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := readDigestStamp(path); !got.IsZero() {
		t.Errorf("garbled stamp must read as zero time, got %v", got)
	}
}

// TestVulnDigestTickerGating pins the run-loop wiring: the ticker is
// created only when vulnDigestReady says so (its decision is tested
// behaviorally in TestVulnDigestReady), and the select loop routes the
// tick through safego to runVulnDigest.
func TestVulnDigestTickerGating(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "vulnDigestC, stopVulnDigest := s.startVulnDigest()") || !strings.Contains(s, "defer stopVulnDigest()") {
		t.Error("Run must take the digest tick channel from startVulnDigest (tested in TestStartVulnDigest) and stop it on return")
	}
	if !strings.Contains(s, "case <-vulnDigestC:") {
		t.Error("run loop must consume the digest ticker channel")
	}
	if !strings.Contains(s, `safego.Go(s.logger, "vuln-digest"`) {
		t.Error("digest pass must run under safego like the other background passes")
	}
}

type recordingDigestMailer struct {
	calls      int
	since      time.Time
	items      []db.VulnDigestItem
	err        error
	deliverErr error
}

func (r *recordingDigestMailer) Deliverable(string) error { return r.deliverErr }

func (r *recordingDigestMailer) SendVulnerabilityDigest(to string, since time.Time, items []db.VulnDigestItem) error {
	r.calls++
	r.since = since
	r.items = items
	return r.err
}

// TestRunVulnDigestEndToEnd drives the real runVulnDigest against the
// scratch DB with a recording mailer: a freshly detected unresolved
// CRITICAL finding is emailed; the stamp advances; a second run inside
// the interval does nothing; a FAILED send leaves the stamp untouched
// so the window retries.
func TestRunVulnDigestEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Seed: repo + one unresolved CRITICAL finding detected "now".
	repoID := int64(987654301)
	pool := store.Pool()
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_groups (repo_group_id, rg_name)
		VALUES (1, 'digest-test') ON CONFLICT (repo_group_id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/digest/tester', 'digest', 'tester')
		ON CONFLICT (repo_id) DO NOTHING`, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_deps_vulnerabilities
		    (repo_id, vuln_id, package_name, package_purl, severity, summary, first_detected_at, last_seen_at)
		VALUES ($1, 'GHSA-digest-test', 'digestpkg', 'pkg:pypi/digestpkg@1.0.0', 'CRITICAL', 'digest test finding', NOW(), NOW())
		ON CONFLICT (repo_id, vuln_id, package_purl) DO UPDATE
		    SET resolved_at = NULL, first_detected_at = NOW()`, repoID); err != nil {
		t.Fatal(err)
	}

	stamp := filepath.Join(t.TempDir(), "vuln-digest-last")
	rec := &recordingDigestMailer{}
	s := &Scheduler{
		store:  store,
		logger: logger,
		cfg: Config{Mail: &config.MailConfig{
			OperatorEmail:         "ops@example.com",
			VulnDigestMinSeverity: "HIGH",
		}},
		digestMailer:    rec,
		digestStampPath: stamp,
	}

	s.runVulnDigest(ctx)
	if rec.calls != 1 {
		t.Fatalf("expected 1 digest send, got %d", rec.calls)
	}
	found := false
	for _, it := range rec.items {
		if it.VulnID == "GHSA-digest-test" && it.RepoOwner == "digest" {
			found = true
			if !strings.EqualFold(it.Severity, "CRITICAL") {
				t.Errorf("severity: got %q", it.Severity)
			}
		}
	}
	if !found {
		t.Fatalf("seeded finding missing from digest items (%d items)", len(rec.items))
	}
	if readDigestStamp(stamp).IsZero() {
		t.Error("stamp must advance after a successful send")
	}

	// Second run inside the interval: nothing happens.
	s.runVulnDigest(ctx)
	if rec.calls != 1 {
		t.Errorf("digest inside the interval must not re-send, got %d calls", rec.calls)
	}

	// Failed send leaves the stamp untouched (window retries).
	os.Remove(stamp)
	rec2 := &recordingDigestMailer{err: errors.New("smtp down")}
	s.digestMailer = rec2
	s.runVulnDigest(ctx)
	if rec2.calls != 1 {
		t.Fatalf("expected the failed send to have been attempted once, got %d", rec2.calls)
	}
	// v0.29.29: with no stamp (first run) the window opens one interval
	// back from NOW, so leaving the stamp absent slid the window forward
	// on every failed tick. The failed send pins it at `since` instead.
	if got := readDigestStamp(stamp); got.Unix() != rec2.since.Unix() {
		t.Errorf("a failed first-run send must pin the window at since=%v, stamp is %v", rec2.since, got)
	}
	pinned := readDigestStamp(stamp)
	s.runVulnDigest(ctx)
	if got := readDigestStamp(stamp); !got.Equal(pinned) {
		t.Errorf("a second failed send must not move the window: %v -> %v", pinned, got)
	}

	// An unwritable stamp path: the window cannot be pinned, and that is
	// logged rather than lost.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var holdLogs bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&holdLogs, nil))
	s.digestStampPath = filepath.Join(blocker, "vuln-digest-last")
	s.digestMailer = &recordingDigestMailer{err: errors.New("smtp down")}
	s.runVulnDigest(ctx)
	if !strings.Contains(holdLogs.String(), "could not pin the retry window") {
		t.Errorf("a failed pin must be logged; log:\n%s", holdLogs.String())
	}
	s.logger = logger
	s.digestStampPath = stamp

	// A mailer skip is a failure too, never a delivery.
	os.Remove(stamp)
	rec3 := &recordingDigestMailer{err: fmt.Errorf("%w: test", mailer.ErrRecipientSkipped)}
	s.digestMailer = rec3
	s.runVulnDigest(ctx)
	if rec3.calls != 1 {
		t.Fatalf("expected the skipped send to have been attempted once, got %d", rec3.calls)
	}
	if got := readDigestStamp(stamp); got.Unix() != rec3.since.Unix() {
		t.Errorf("a skipped send is a failed send: the window must stay at since=%v, stamp is %v", rec3.since, got)
	}
}

// TestHoldDigestWindow pins the failed-send rule without a database: an
// existing stamp is left as is, and a missing one is written as `since`,
// so repeated failures keep the same window.
func TestHoldDigestWindow(t *testing.T) {
	stamp := filepath.Join(t.TempDir(), "vuln-digest-last")
	since := time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC)
	if err := holdDigestWindow(stamp, time.Time{}, since); err != nil {
		t.Fatal(err)
	}
	if got := readDigestStamp(stamp); !got.Equal(since) {
		t.Errorf("first-run failure: stamp = %v, want since %v", got, since)
	}
	later := since.Add(3 * time.Hour)
	if err := holdDigestWindow(stamp, since, later); err != nil {
		t.Fatal(err)
	}
	if got := readDigestStamp(stamp); !got.Equal(since) {
		t.Errorf("with a stamp, a failure must leave it: got %v, want %v", got, since)
	}
}

// TestVulnDigestReady: the ticker starts only when a digest could ever be
// delivered. A disabled mailer or an undeliverable operator_email used to
// log "operator vulnerability digest enabled" and then query every hour
// without sending (round-3 review of v0.29.28).
func TestVulnDigestReady(t *testing.T) {
	mail := &config.MailConfig{OperatorEmail: "ops@example.com"}
	for _, tc := range []struct {
		name    string
		mailer  digestMailer
		mail    *config.MailConfig
		wantOK  bool
		wantErr bool // a deliverability error the caller logs at ERROR
	}{
		{name: "no mailer injected", mail: mail},
		{name: "no mail block", mailer: &recordingDigestMailer{}},
		{name: "no operator_email", mailer: &recordingDigestMailer{}, mail: &config.MailConfig{}},
		{name: "deliverable", mailer: &recordingDigestMailer{}, mail: mail, wantOK: true},
		{name: "mailer disabled", mailer: &recordingDigestMailer{deliverErr: mailer.ErrNotConfigured}, mail: mail, wantErr: true},
		{name: "undeliverable operator_email", mailer: &recordingDigestMailer{deliverErr: mailer.ErrRecipientSkipped}, mail: mail, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Scheduler{cfg: Config{Mail: tc.mail}, digestMailer: tc.mailer}
			ok, err := s.vulnDigestReady()
			if ok != tc.wantOK || (err != nil) != tc.wantErr {
				t.Errorf("vulnDigestReady() = %v, %v; want ok=%v err=%v", ok, err, tc.wantOK, tc.wantErr)
			}
		})
	}
}

// TestStartVulnDigest: the startup decision and what it logs — the ERROR
// configuration.md promises when a configured digest could never be
// delivered, INFO when it starts, and nothing when it is not configured.
func TestStartVulnDigest(t *testing.T) {
	mail := &config.MailConfig{OperatorEmail: "ops@example.com"}
	for _, tc := range []struct {
		name     string
		mailer   digestMailer
		mail     *config.MailConfig
		wantTick bool
		wantLog  string
		noLog    string
	}{
		{name: "deliverable", mailer: &recordingDigestMailer{}, mail: mail, wantTick: true, wantLog: "level=INFO msg=\"operator vulnerability digest enabled\""},
		{name: "undeliverable", mailer: &recordingDigestMailer{deliverErr: mailer.ErrNotConfigured}, mail: mail,
			wantLog: "level=ERROR msg=\"operator vulnerability digest NOT started", noLog: "digest enabled"},
		{name: "not configured", mailer: &recordingDigestMailer{}, mail: &config.MailConfig{}, noLog: "digest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			s := &Scheduler{logger: slog.New(slog.NewTextHandler(&logs, nil)), cfg: Config{Mail: tc.mail}, digestMailer: tc.mailer}
			c, stop := s.startVulnDigest()
			defer stop()
			if (c != nil) != tc.wantTick {
				t.Errorf("tick channel present = %v, want %v", c != nil, tc.wantTick)
			}
			if tc.wantLog != "" && !strings.Contains(logs.String(), tc.wantLog) {
				t.Errorf("log lacks %q:\n%s", tc.wantLog, logs.String())
			}
			if tc.noLog != "" && strings.Contains(logs.String(), tc.noLog) {
				t.Errorf("log has %q:\n%s", tc.noLog, logs.String())
			}
		})
	}
}
