// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestPageLookupErrorsAreNotNotFound pins the web half of worklist follow-up
// 12: the group page, the repository page and the SBOM download turned
// EVERY lookup error into a 404 or a 403 (SR-5), and the SBOM download wrote
// the error's text into its body. "Not yours" and "no such repository" keep
// their 404/403; a store failure is a logged 500 with a generic body.
func TestPageLookupErrorsAreNotNotFound(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const login = "_avweb_lookup_probe"
	const other = "_avweb_lookup_other"
	const repoURL = "https://github.com/_avweb-lookup-owner/_avweb-lookup-repo"
	pool := store.Pool()
	clean := func() {
		for _, l := range []string{login, other} {
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, l)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, l)
			_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, l)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, repoURL)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, repoURL)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "lookup probe")
	if err != nil {
		t.Fatal(err)
	}
	otherUID, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: other, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	otherGID, err := store.CreateUserGroup(ctx, otherUID, "someone else's group")
	if err != nil {
		t.Fatal(err)
	}
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Owner: "_avweb-lookup-owner", Name: "_avweb-lookup-repo", GitURL: repoURL, Platform: model.PlatformGitHub})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddRepoToGroupByID(ctx, gid, repoID); err != nil {
		t.Fatal(err)
	}

	var logs strings.Builder
	s := New(store, config.WebConfig{}, nil, "", slog.New(slog.NewTextHandler(&logs, nil)))
	s.sessions["probe-token"] = &Session{UserID: uid, LoginName: login, ExpiresAt: time.Now().Add(time.Hour)}
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: "probe-token"})
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	groupPage := fmt.Sprintf("/groups/%d", gid)
	repoPage := fmt.Sprintf("/groups/%d/repos/%d", gid, repoID)
	sbom := fmt.Sprintf("/groups/%d/repos/%d/sbom?format=cyclonedx", gid, repoID)

	// The answers that ARE the client's: not yours, no such repository.
	if w := get(groupPage); w.Code != http.StatusOK {
		t.Fatalf("GET %s = %d %s", groupPage, w.Code, w.Body.String())
	}
	if w := get(repoPage); w.Code != http.StatusOK {
		t.Fatalf("GET %s = %d %s", repoPage, w.Code, w.Body.String())
	}
	if w := get(fmt.Sprintf("/groups/%d", otherGID)); w.Code != http.StatusNotFound {
		t.Errorf("someone else's group page = %d; want 404", w.Code)
	}
	if w := get(fmt.Sprintf("/groups/%d/repos/%d", otherGID, repoID)); w.Code != http.StatusForbidden {
		t.Errorf("a repository page in someone else's group = %d; want 403", w.Code)
	}
	if w := get(fmt.Sprintf("/groups/%d/repos/%d/sbom", otherGID, repoID)); w.Code != http.StatusForbidden {
		t.Errorf("an SBOM in someone else's group = %d; want 403", w.Code)
	}
	if w := get(fmt.Sprintf("/groups/%d/repos/%d", gid, repoID+1000000)); w.Code != http.StatusNotFound {
		t.Errorf("a repository id nobody has = %d; want 404", w.Code)
	}
	// The SBOM of a repository nobody has reaches the generator, whose
	// error carried the store's text into a 500 body ("SBOM generation
	// failed: repo N not found: no rows in result set"); it is a 404.
	if w := get(fmt.Sprintf("/groups/%d/repos/%d/sbom", gid, repoID+1000000)); w.Code != http.StatusNotFound || strings.Contains(w.Body.String(), "no rows") {
		t.Errorf("an SBOM of a repository id nobody has = %d %q; want a plain 404", w.Code, w.Body.String())
	}

	// The store fails: every page is a logged 500 with a generic body, not
	// "not found" and not "forbidden", and never the store's text.
	store.Close()
	for _, path := range []string{groupPage, repoPage, sbom} {
		logs.Reset()
		w := get(path)
		body := w.Body.String()
		if w.Code != http.StatusInternalServerError {
			t.Errorf("GET %s over a closed pool = %d %q; want 500", path, w.Code, body)
		}
		if strings.Contains(body, "closed pool") || strings.Contains(body, "pgx") || strings.Contains(body, "aveloxis_") {
			t.Errorf("GET %s: the body carries the store's text: %q", path, body)
		}
		if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "closed pool") {
			t.Errorf("GET %s: the cause is not in the log:\n%s", path, logs.String())
		}
	}
}

// TestRepoPageClassifiesTheRepoLookup is the structural half for the one
// arm the DB-tier test cannot reach: handleRepoDetail's repository lookup
// runs after a successful ownership lookup on the same concrete store, and
// no fault can be injected between two statements of one pool (a closed
// pool fails the ownership arm first). GetRepoByID's contract — the typed
// ErrRepoNotFound for "no such repository", every other error as itself —
// is pinned at runtime by the store's own test; this pins that the handler
// asks the typed question and sends everything else to serverError.
func TestRepoPageClassifiesTheRepoLookup(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/web/server.go"), "func (s *Server) handleRepoDetail("))
	if !strings.Contains(body, "errors.Is(err, db.ErrRepoNotFound)") || strings.Count(body, `s.serverError(w, "handleRepoDetail", err)`) != 2 {
		t.Error("handleRepoDetail must answer 404 only for db.ErrRepoNotFound and send the ownership lookup's and the repository lookup's other errors to serverError (a store failure is not \"not found\")")
	}
}

// TestSBOMDownloadClassifiesTheGenerator is the structural half for the
// SBOM handler's generator arm, which the DB-tier test reaches only for
// "no such repository" (the closed pool fails the ownership arm first): the
// typed not-found is a 404, everything else goes to serverError, and the
// body never carries the error's text (it did through v0.29.67).
func TestSBOMDownloadClassifiesTheGenerator(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/web/server.go"), "func (s *Server) handleSBOMDownload("))
	if !strings.Contains(body, "errors.Is(err, db.ErrRepoNotFound)") || strings.Count(body, `s.serverError(w, "handleSBOMDownload", err)`) != 2 || strings.Contains(body, "err.Error()") {
		t.Error("handleSBOMDownload must answer 404 for db.ErrRepoNotFound, send the ownership lookup's and the generator's other errors to serverError, and never write err.Error() into the body")
	}
}

// TestLogURLRedactsBeforeTruncating pins the one URL log spelling (batch 5b
// review round 1): a userinfo longer than the truncation window survived
// the old redact-after-truncate order into the WARN.
func TestLogURLRedactsBeforeTruncating(t *testing.T) {
	long := "https://alice:" + strings.Repeat("s", 250) + "@github.com/orgname"
	got := logURL(long)
	if strings.Contains(got, "sss") || !strings.Contains(got, "***@github.com/orgname") {
		t.Errorf("logURL(<250-byte userinfo>) = %q; want the userinfo redacted before any truncation", got)
	}
	// That every log attribute in this package goes through logURL is the
	// AST tripwire's rule (scripts/url_log_redaction_test.go, for every
	// package that defines a logURL — review round 3: a line-based check
	// here missed a hand-wrapped call). Only the inline order is banned here.
	for _, f := range srctest.PackageFiles(t, "internal/web", 1) {
		if strings.Contains(srctest.StripGoComments(f), "RedactURLUserinfo(truncateForLog(") {
			t.Error("the inline redact-after-truncate spelling is banned")
		}
	}
}

// TestAddErrorFlagNamesTheNotice pins the flag the group page shows after a
// failed add (follow-up 12): a data exception the database raised for the
// caller's own value (a NUL byte, SQLSTATE class 22) is the caller's
// mistake — "invalid", not "try adding them again".
func TestAddErrorFlagNamesTheNotice(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"rejected group", db.ErrGroupRejected, "rejected"},
		{"over-long URL", fmt.Errorf("add: %w", db.ErrURLTooLong), "invalid"},
		{"credentialed URL", platform.ErrURLUserinfo, "invalid"},
		{"data exception (NUL byte)", fmt.Errorf("add: %w", &pgconn.PgError{Code: "22021", Message: "invalid byte sequence"}), "invalid"},
		{"a store failure", errors.New("closed pool"), "1"},
		{"a program limit that is not the caller's value (54000)", &pgconn.PgError{Code: "54000"}, "1"},
	} {
		if got := addErrorFlag(tc.err); got != tc.want {
			t.Errorf("%s: addErrorFlag = %q; want %q", tc.name, got, tc.want)
		}
	}
}
