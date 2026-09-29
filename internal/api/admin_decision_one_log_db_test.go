// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestAdminDecisionFailureLogsOneLine (AVELOXIS_TEST_DB) — PR #218 review
// C15: the two admin decision handlers logged a WARN and then called
// serverError, which logs the same failure at ERROR — one failure, two
// lines. A store failure is one ERROR line naming the handler, the target
// and the decision, and a generic 500 body.
func TestAdminDecisionFailureLogsOneLine(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	closed, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	for _, tc := range []struct {
		name, path, handler, target string
		serve                       func(*Server, http.ResponseWriter, *http.Request)
		values                      map[string]string
	}{
		{"group decision", "/api/v1/admin/groups/42/approve", "handleAdminGroupDecision", "group 42",
			(*Server).handleAdminGroupDecision, map[string]string{"groupID": "42", "decision": "approve"}},
		{"add-request decision", "/api/v1/admin/add-requests/43/reject", "handleAdminAddRequestDecision", "request 43",
			(*Server).handleAdminAddRequestDecision, map[string]string{"requestID": "43", "decision": "reject"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			s := &Server{store: closed, logger: slog.New(slog.NewTextHandler(logs, nil)), auth: newAuthenticator(closed, false, nil)}
			r := httptest.NewRequest(http.MethodPost, tc.path, nil)
			for k, v := range tc.values {
				r.SetPathValue(k, v)
			}
			r = r.WithContext(context.WithValue(r.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true}))
			w := httptest.NewRecorder()
			tc.serve(s, w, r)
			if w.Code != http.StatusInternalServerError || strings.Contains(w.Body.String(), "closed pool") {
				t.Fatalf("= %d %q; want a generic 500", w.Code, w.Body.String())
			}
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("one failure logged %d lines; want 1:\n%s", len(lines), logs.String())
			}
			l := lines[0]
			if !strings.Contains(l, "level=ERROR") || !strings.Contains(l, "handler="+tc.handler) || !strings.Contains(l, tc.target) || !strings.Contains(l, "decision") || !strings.Contains(l, "closed pool") {
				t.Errorf("the one line must be an ERROR naming the handler, %q, the decision and the cause: %s", tc.target, l)
			}
		})
	}
}
