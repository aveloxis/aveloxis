// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// The coverage object's lockfiles_unread (0.29.214) on the three answers
// that carry it: null until a reading of the lockfiles does not finish, then
// the reason and the date of the last reading that did; and an answer cached
// before the reading must not outlive it (the writer moves the repository's
// cache state).
func TestTheCoverageObjectCarriesTheLockfileCaution(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	tag := fmt.Sprintf("_avlock%d", time.Now().UnixNano())
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'locks', $2, 1) RETURNING repo_id`, "https://github.com/"+tag+"/locks", tag).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repo_dependency_coverage WHERE repo_id = $1`, repoID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	s, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs, ResponseCacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	sid := strconv.FormatInt(repoID, 10)
	// Each answer's coverage object, raw (to see the key and its null) and
	// decoded.
	answers := func() map[string]map[string]json.RawMessage {
		t.Helper()
		out := map[string]map[string]json.RawMessage{}
		for _, path := range []string{"/api/v1/repos/" + sid + "/vulnerabilities", "/api/v1/repos/" + sid + "/stats", "/api/v1/repos/stats?ids=" + sid} {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.RemoteAddr = "127.0.0.1:50000"
			s.Handler().ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s = %d %s", path, w.Code, w.Body.String())
			}
			var body map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if inner, batch := body[sid]; batch {
				body = nil
				if err := json.Unmarshal(inner, &body); err != nil {
					t.Fatal(err)
				}
			}
			var cov map[string]json.RawMessage
			if err := json.Unmarshal(body["coverage"], &cov); err != nil {
				t.Fatalf("GET %s: no coverage object: %s", path, w.Body.String())
			}
			out[path] = cov
		}
		return out
	}
	record := func(r model.LockfileReading) {
		t.Helper()
		if err := store.RecordLockfileReading(ctx, repoID, r); err != nil {
			t.Fatal(err)
		}
	}

	// Read, with the snapshot a completed reading leaves: the key is there
	// and null. (0.29.215: "behind" is decided from the lockfile rows that
	// are STORED, so the test stores some, as a reading does.)
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID,
		[]*db.RepoLockfileInfo{{Ecosystem: "npm", LockfilePath: "package-lock.json", LockfileKind: "package-lock.json", EntryCount: 1, DirectCount: 1}},
		nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	record(model.LockfileReading{Completed: true, Lockfiles: true})
	// A completed scan that found nothing: the repository reads clean until
	// its lockfiles go unread (0.29.216), on all three answers.
	if err := store.RecordScanCoverage(ctx, repoID, model.ScanCoverage{Outcome: model.ScanSent, FirstScan: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetVulnScanLastRun(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	clean := func(when string, want bool) {
		t.Helper()
		for path, cov := range answers() {
			if got := string(cov["clean_result"]); got != strconv.FormatBool(want) {
				t.Errorf("%s: %s: clean_result = %s; want %v", when, path, got, want)
			}
			if string(cov["state"]) != `"scanned"` || string(cov["count_blank"]) != "false" {
				t.Errorf("%s: %s: state %s, count_blank %s; the caution changes neither", when, path, cov["state"], cov["count_blank"])
			}
		}
	}
	clean("read", true)
	for path, cov := range answers() {
		if raw, has := cov["lockfiles_unread"]; !has || string(raw) != "null" {
			t.Errorf("%s: lockfiles_unread = %s (present %v); want null", path, raw, has)
		}
	}
	// Behind: the reason, and the date of the reading that finished. These
	// are the answers just cached.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true})
	for path, cov := range answers() {
		var u *vulnclass.LockfilesUnread
		if err := json.Unmarshal(cov["lockfiles_unread"], &u); err != nil || u == nil {
			t.Fatalf("%s: lockfiles_unread = %s", path, cov["lockfiles_unread"])
		}
		if u.Reason != model.InventoryAttemptReadMemoryLimit || u.LastReadAt == nil || time.Since(*u.LastReadAt) > time.Minute {
			t.Errorf("%s: %+v; want the memory limit and the reading just made", path, u)
		}
	}
	clean("behind", false)
	// Some lockfiles too large, in a reading that replaced the snapshot
	// (0.29.216): its own reason, and no clean claim either.
	record(model.LockfileReading{NotCompleted: model.LockfileUnreadTooLarge, Lockfiles: true})
	for path, cov := range answers() {
		var u *vulnclass.LockfilesUnread
		if err := json.Unmarshal(cov["lockfiles_unread"], &u); err != nil || u == nil || u.Reason != model.LockfileUnreadTooLarge {
			t.Errorf("%s: too large: %s", path, cov["lockfiles_unread"])
		}
	}
	clean("some too large", false)
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadMemoryLimit, Lockfiles: true})
	// The date is sent as a UTC instant.
	for path, cov := range answers() {
		var raw struct {
			LastReadAt string `json:"last_read_at"`
		}
		if err := json.Unmarshal(cov["lockfiles_unread"], &raw); err != nil || !strings.HasSuffix(raw.LastReadAt, "Z") {
			t.Errorf("%s: last_read_at = %q (%v); want a UTC instant", path, raw.LastReadAt, err)
		}
	}
	// The lockfiles are deleted: the snapshot is replaced by an empty one
	// and there is nothing to caution about again.
	if err := store.ReplaceRepoLockfileSnapshot(ctx, repoID, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	record(model.LockfileReading{Completed: true})
	for path, cov := range answers() {
		if string(cov["lockfiles_unread"]) != "null" {
			t.Errorf("%s: after the lockfiles were deleted: %s", path, cov["lockfiles_unread"])
		}
	}
	clean("read again", true)
	// Never read.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadFailed, Lockfiles: true})
	for path, cov := range answers() {
		var u *vulnclass.LockfilesUnread
		if err := json.Unmarshal(cov["lockfiles_unread"], &u); err != nil || u == nil || u.Reason != model.InventoryAttemptReadFailed || u.LastReadAt != nil {
			t.Errorf("%s: never read: %s", path, cov["lockfiles_unread"])
		}
		// The key is sent, and null: a client tells the two cautions apart
		// by it.
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(cov["lockfiles_unread"], &keys); err != nil || string(keys["last_read_at"]) != "null" {
			t.Errorf("%s: never read: last_read_at = %s (in %s); want the key, null", path, keys["last_read_at"], cov["lockfiles_unread"])
		}
	}
}
