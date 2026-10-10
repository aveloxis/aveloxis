// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRequestPathDeadlinesAreReviewed — NET-6 review r2 F2:
// httpserver.RequestEnded logs a context.DeadlineExceeded reaching a
// handler's error helper at Debug, because the request's own bound
// (http_timeout_seconds) already logged a WARN. That holds only while no
// handler hands the store a deadline of its own — whose expiry would then
// be a real failure hidden at Debug. Every derived deadline in the api,
// web and monitor packages is listed here with why its errors do NOT reach
// those helpers; a new one fails until it is reviewed and added.
func TestRequestPathDeadlinesAreReviewed(t *testing.T) {
	reviewed := map[string]int{
		// OAuth callbacks: the forge exchange and user fetch are bounded by
		// oauthCallbackTimeout; their failures go to logOAuthFailure (WARN
		// with provider and phase), never serverError.
		"internal/web/server.go": 3,
		// (the third: submitAccountEmail's detached pending-email clean-up,
		// pendingEmailCleanupTimeout — its failure is a plain WARN, never
		// through RequestEnded; NET-6 review r7 F1.)
		// The home-repos background refresh runs on context.Background(),
		// outside any request; it logs its own WARN.
		"internal/api/portal.go": 1,
		// The re-warm's replay bound (http_timeout_seconds, v0.29.73): the
		// replayed handler's error helper drops its expiry to Debug and
		// writes nothing, so replay itself reports the expiry ("request
		// deadline exceeded") in the re-warm's WARN.
		"internal/api/repo_page_rewarm.go": 1,
		// The capacity policy's read (v0.29.89; the 0.29.85 session
		// threshold read before it) runs on context.Background() off the
		// request path, bounded by its refresh period; it logs its own WARN.
		"internal/api/capacity_limits.go": 1,
	}
	derived := regexp.MustCompile(`context\.With(Timeout|Deadline)(Cause)?\(`)
	root := srctest.Root(t)
	found := map[string]int{}
	examined := 0
	for _, dir := range []string{"internal/api", "internal/web", "internal/monitor"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			rel, _ := filepath.Rel(root, path)
			if n := len(derived.FindAllString(srctest.StripGoComments(string(b)), -1)); n > 0 {
				found[filepath.ToSlash(rel)] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for f, n := range found {
		if reviewed[f] != n {
			t.Errorf("%s derives %d deadline(s), %d reviewed: a handler's own deadline expiring would be logged at Debug by httpserver.RequestEnded — route its errors elsewhere, then add it here with the reason", f, n, reviewed[f])
		}
	}
	for f, n := range reviewed {
		if found[f] != n {
			t.Errorf("%s: %d reviewed deadline(s) but %d found — update the list", f, n, found[f])
		}
	}
	srctest.MinCount(t, "api/web/monitor Go files examined", examined, 20)
}

// TestRequestEndIsClassifiedInOnePlace — NET-6 review r3 F1: r2 routed three
// helpers through httpserver.RequestEnded and left six sites (the monitor's
// serverError, the OAuth sign-in and the repo and monitor page stats)
// checking context.Canceled alone, so a request cut by http_timeout_seconds
// logged an ERROR or a WARN beside the bound's. In the api, web and monitor
// packages a request's end is classified by httpserver.RequestEnded (or the
// request context itself); a bare context.Canceled / DeadlineExceeded test
// — or a WithoutCancel, which would stop the bound cancelling the query —
// needs a reviewed entry here.
func TestRequestEndIsClassifiedInOnePlace(t *testing.T) {
	reviewed := map[string]int{
		// logOAuthFailure: DeadlineExceeded from the 30 s callback bound on a
		// live request is that bound's own expiry (ERROR, naming it); and
		// submitAccountEmail's clean-up runs on WithoutCancel on purpose —
		// the second half of committed writes (NET-6 review r7 F1).
		"internal/web/server.go": 2,
	}
	bare := regexp.MustCompile(`context\.(Canceled|DeadlineExceeded|WithoutCancel)\b`)
	root := srctest.Root(t)
	found := map[string]int{}
	examined := 0
	for _, dir := range []string{"internal/api", "internal/web", "internal/monitor"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			rel, _ := filepath.Rel(root, path)
			if n := len(bare.FindAllString(srctest.StripGoComments(string(b)), -1)); n > 0 {
				found[filepath.ToSlash(rel)] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for f, n := range found {
		if reviewed[f] != n {
			t.Errorf("%s tests the request's end %d time(s) outside httpserver.RequestEnded (%d reviewed): route it through RequestEnded, or review and list it", f, n, reviewed[f])
		}
	}
	for f, n := range reviewed {
		if found[f] != n {
			t.Errorf("%s: %d reviewed but %d found — update the list", f, n, found[f])
		}
	}
	srctest.MinCount(t, "api/web/monitor Go files examined", examined, 20)
}
