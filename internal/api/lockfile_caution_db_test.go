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

	// Read: the key is there and null.
	record(model.LockfileReading{Completed: true, Lockfiles: true})
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
	// The lockfiles are deleted: nothing to caution about again.
	record(model.LockfileReading{Completed: true})
	for path, cov := range answers() {
		if string(cov["lockfiles_unread"]) != "null" {
			t.Errorf("%s: after the lockfiles were deleted: %s", path, cov["lockfiles_unread"])
		}
	}
	// Never read.
	record(model.LockfileReading{NotCompleted: model.InventoryAttemptReadFailed, Lockfiles: true})
	for path, cov := range answers() {
		var u *vulnclass.LockfilesUnread
		if err := json.Unmarshal(cov["lockfiles_unread"], &u); err != nil || u == nil || u.Reason != model.InventoryAttemptReadFailed || u.LastReadAt != nil {
			t.Errorf("%s: never read: %s", path, cov["lockfiles_unread"])
		}
	}
}
