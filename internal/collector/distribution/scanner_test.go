// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/depsdev"
	"github.com/aveloxis/aveloxis/internal/platform/ecosystems"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// v0.25.0 — scanner contract tests.
//
// The contract loosening surfaced by the 2026-05-22/23 production
// diagnostic on the chaoss.tv aveloxis DB: every distribution-scan
// failure during the ecosyste.ms outage was a Julia or R repo
// where ecosyste.ms 500'd and every other source legitimately
// returned "no data." The pre-v0.25.0 contract recorded these as
// failures (any error + zero data = failure), driving the 10-strike
// sideline path and locking the affected cohort out for 6 months.
// The v0.25.0 contract distinguishes "this repo legitimately
// publishes nothing we can see" (success, no retry) from "sources
// we needed had transient failures" (failure, retry sooner).

// ---- Source-contract tests (pin design invariants) -------------------

func TestCompositeScannerHasCrossCheckSourcesField(t *testing.T) {
	body := readFile(t, "scanner.go")
	if !strings.Contains(body, "CrossCheckSources bool") {
		t.Fatal("CompositeScanner must declare CrossCheckSources bool field for the v0.25.0 cross-check guarantee — operator wants both external sources queried per repo")
	}
}

func TestNewCompositeScannerDefaultsCrossCheckSourcesTrue(t *testing.T) {
	cs := NewCompositeScanner(nil, nil, nil, nil)
	if !cs.CrossCheckSources {
		t.Error("NewCompositeScanner must default CrossCheckSources=true per v0.25.0 — operator explicitly asked for both deps.dev AND ecosyste.ms queried per repo, with separate rows per source")
	}
}

func TestScannerTracksEnabledAndErroredSources(t *testing.T) {
	body := readFile(t, "scanner.go")
	if !strings.Contains(body, "enabledSources") {
		t.Error("Scan must track enabledSources counter to compare against erroredSources for the v0.25.0 loosened contract")
	}
	if !strings.Contains(body, "erroredSources") {
		t.Error("Scan must track erroredSources counter for the v0.25.0 loosened contract")
	}
	// Pin the actual gating condition.
	if !strings.Contains(body, "erroredSources == enabledSources") {
		t.Error("Scan must keep the every-enabled-source-errored gate (erroredSources == enabledSources) — the only failure besides a GitHub non-answer (v0.29.55). The pre-v0.25.0 'any error' gate caused legitimate-no-data repos to be sidelined for 180 days.")
	}
}

func TestScannerEmitsErrorLogWhenBothExternalSourcesFail(t *testing.T) {
	body := readFile(t, "scanner.go")
	// Per operator direction: when BOTH external registries fail in
	// the same scan, emit a distinct ERROR-level log line with each
	// source's error labeled as its own slog key — never joined or
	// swallowed.
	if !strings.Contains(body, "BOTH external registries failed") {
		t.Error("Scan must emit a distinct log message when both external registries fail — operator wants per-source visibility while building confidence in the error classification")
	}
	if !strings.Contains(body, "Logger.Error") && !strings.Contains(body, ".Error(") {
		t.Error("The both-external-failed log entry must be at ERROR level (not WARN) per operator direction")
	}
	for _, requiredKey := range []string{
		"deps_dev_class",
		"deps_dev_error",
		"ecosystems_class",
		"ecosystems_error",
	} {
		if !strings.Contains(body, requiredKey) {
			t.Errorf("both-external-failed log must include slog key %q so per-source errors are visible labeled (not aggregated via errors.Join)", requiredKey)
		}
	}
}

func TestScannerGitHubErrorsStayWarnLevel(t *testing.T) {
	body := readFile(t, "scanner.go")
	// GitHub source error logs should remain WARN — 403/404/304 from
	// GitHub on these endpoints are common and benign.
	for _, expected := range []string{
		`"distribution: github release assets failed"`,
		`"distribution: github packages failed"`,
		`"distribution: github manifests failed"`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("GitHub source error log %q must remain in source as a WARN-level entry — pre-v0.25.0 log shape preserved", expected)
		}
	}
}

// ---- Behavioral tests ------------------------------------------------

// TestScannerSucceedsWhenLegitimateNoDataAcrossWorkingSources pins
// the headline v0.25.0 fix: a repo where ecosyste.ms returns 500
// but every other source legitimately returns empty must record a
// successful scan (no error), NOT trip the 10-strike sideline.
//
// This is the Julia/R diagnostic case from 2026-05-22/23: deps.dev
// doesn't index Julia/CRAN, ecosyste.ms was 500-storming, and the
// pre-v0.25.0 contract locked these repos out of recollection for
// 180 days.
func TestScannerSucceedsWhenLegitimateNoDataAcrossWorkingSources(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// deps.dev returns empty for this repo (Julia/R aren't indexed).
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[]}`)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ecosyste.ms 500: classic outage.
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	t.Cleanup(ecoServer.Close)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// All GitHub sources return empty arrays. Legitimate "this
		// repo doesn't publish via release assets / GitHub Packages,
		// and has no recognized manifests at the root."
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ghServer.Close)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true /*crossCheck*/)

	dists, manifests, complete, err := scanner.Scan(context.Background(), 999, "julialang", "Project.jl", "https://github.com/julialang/Project.jl")
	_ = complete // v0.25.0 signature; this test focuses on the err+data check, see TestScannerMarksScanIncompleteOnTransientExternalError for the complete-flag-specific test.

	// Empty-but-clean from working sources = success in the v0.25.0 contract.
	if err != nil {
		t.Fatalf("Julia-style repo (deps.dev empty + ecosyste.ms 500 + github empty) must NOT fail the scan under the v0.25.0 contract — got err=%v", err)
	}
	if len(dists) != 0 || len(manifests) != 0 {
		t.Errorf("expected zero evidence (all sources empty), got %d dists, %d manifests", len(dists), len(manifests))
	}
}

// TestScannerFailsOnlyWhenEveryEnabledSourceErrored pins the strict
// half of the contract: if NOTHING completed cleanly, the scan
// genuinely failed (caller routes to RecordDistributionFailure).
//
// The GitHub side must fail with ANSWERS (422, ErrRequestRejected), which
// are returned at once and are not v0.29.55 non-answers — so the error
// comes from the v0.25.0 gate. Any error that is not an answer
// (platform.IsDefinitiveAnswer false — see githubErrorIsNonAnswer) would
// instead fail the scan through the non-answer check and leave the gate
// unpinned (review rounds 5–7: the old 401 fixture ended in a deadline).
func TestScannerFailsOnlyWhenEveryEnabledSourceErrored(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "deps.dev down", http.StatusInternalServerError)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "ecosyste.ms down", http.StatusInternalServerError)
	}))
	t.Cleanup(ecoServer.Close)

	// Every GitHub source answers with a 422 (ErrRequestRejected): an error
	// that IS an answer, returned at once, so it reaches the v0.25.0
	// every-source-errored gate rather than the v0.29.55 non-answer
	// failure. (Through v0.29.54 this used a 401; its retries end in a
	// deadline, which v0.29.55 counts as a non-answer, so the fixture no
	// longer reached the gate it pins — review round 5.) Combined with
	// deps.dev/ecosyste.ms 500s, every enabled source errors → the scan
	// must fail.
	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"Validation Failed"}`, http.StatusUnprocessableEntity)
	}))
	t.Cleanup(ghServer.Close)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true)

	// Short ctx so a fixture that drifts into retries fails fast instead
	// of hanging the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, _, err := scanner.Scan(ctx, 1, "x", "y", "https://github.com/x/y")
	if err == nil {
		t.Fatal("when EVERY source errors, scan must return non-nil error so RecordDistributionFailure runs and backoff applies")
	}
	// On correct code only the gate's joined error carries the GitHub
	// answers (the non-answer failure joins non-answers). The answer/
	// non-answer split itself is pinned by the "rejected (422)" case of
	// TestCompositeScannerGitHubNonAnswerFailsTheScan.
	if !errors.Is(err, platform.ErrRequestRejected) {
		t.Fatalf("err = %v — the scan did not fail through the every-source-errored gate", err)
	}
}

// TestScannerEmitsErrorLogOnDoubleExternalFailure pins the
// per-source visibility behavior. When both deps.dev and ecosyste.ms
// fail in the same scan, an ERROR-level log line must surface each
// source's error labeled as its own key — for operator confidence
// in the classification while we build out the fleet.
func TestScannerEmitsErrorLogOnDoubleExternalFailure(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "deps.dev outage", http.StatusInternalServerError)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "ecosyste.ms outage", http.StatusInternalServerError)
	}))
	t.Cleanup(ecoServer.Close)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`) // GitHub sources clean.
	}))
	t.Cleanup(ghServer.Close)

	// Capture slog output into a buffer.
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true)
	scanner.Logger = logger

	_, _, _, _ = scanner.Scan(context.Background(), 42, "owner", "repo", "https://github.com/owner/repo")

	logs := buf.String()
	if !strings.Contains(logs, "BOTH external registries failed") {
		t.Errorf("expected ERROR log entry 'BOTH external registries failed' when both deps.dev and ecosyste.ms fail; got logs: %s", logs)
	}
	// Verify the per-source key labeling is present.
	for _, key := range []string{
		`"deps_dev_class"`,
		`"deps_dev_error"`,
		`"ecosystems_class"`,
		`"ecosystems_error"`,
	} {
		if !strings.Contains(logs, key) {
			t.Errorf("expected per-source key %s in the both-failed log entry; got logs: %s", key, logs)
		}
	}
	// Verify the level is ERROR (or higher), not WARN.
	if !strings.Contains(logs, `"level":"ERROR"`) {
		t.Errorf("both-external-failed log entry must be at ERROR level; got: %s", logs)
	}
}

// TestScannerCrossCheckTrueQueriesBothExternalSources pins that
// with CrossCheckSources=true (default) both deps.dev AND
// ecosyste.ms are queried for every repo even when one returns data.
func TestScannerCrossCheckTrueQueriesBothExternalSources(t *testing.T) {
	var depsHits, ecoHits atomic.Int64

	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		depsHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// Return one package so the first-source-succeeded short
		// circuit would fire under CrossCheckSources=false.
		_, _ = io.WriteString(w, `{"versions":[{"versionKey":{"system":"PYPI","name":"x","version":"1"}}]}`)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ecoHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ecoServer.Close)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ghServer.Close)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true /*crossCheck*/)
	_, _, _, _ = scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")

	if depsHits.Load() == 0 {
		t.Error("deps.dev must be queried when CrossCheckSources=true")
	}
	if ecoHits.Load() == 0 {
		t.Error("ecosyste.ms MUST be queried even when deps.dev returned data — CrossCheckSources=true is the operator's lock-in for two-source cross-checking")
	}
}

// TestScannerCrossCheckFalseSkipsEcosystemsAfterDepsSuccess pins the
// opt-out behavior: with CrossCheckSources=false, ecosyste.ms is
// skipped when deps.dev already returned data. This is the API-
// budget-conscious mode.
func TestScannerCrossCheckFalseSkipsEcosystemsAfterDepsSuccess(t *testing.T) {
	var ecoHits atomic.Int64

	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[{"versionKey":{"system":"PYPI","name":"x","version":"1"}}]}`)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ecoHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ecoServer.Close)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ghServer.Close)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, false /*crossCheck off*/)
	_, _, _, _ = scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")

	if ecoHits.Load() != 0 {
		t.Errorf("with CrossCheckSources=false, ecosyste.ms must NOT be queried after deps.dev returned data — got %d hits", ecoHits.Load())
	}
}

// ---- helpers ---------------------------------------------------------

func readFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

// buildTestScanner wires up real depsdev/ecosystems/github clients
// pointed at the supplied test-server URLs. The github client gets
// its baseURL via the existing httpclient pattern (KeyPool + token).
func buildTestScanner(t *testing.T, depsURL, ecoURL, ghURL string, crossCheck bool) *CompositeScanner {
	t.Helper()
	logger := slog.Default()

	depsClient := depsdev.New(depsdev.Options{BaseURL: depsURL})
	ecoClient := ecosystems.New(ecosystems.Options{BaseURL: ecoURL})

	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	ghClient := github.New(ghURL, keys, logger)

	return &CompositeScanner{
		DepsDev:           depsClient,
		Ecosystems:        ecoClient,
		GitHub:            ghClient,
		Logger:            logger,
		CrossCheckSources: crossCheck,
	}
}

// silence "imported and not used" for db / json which appear in
// sibling files we may end up cross-importing. Defensive against
// linter complaints in CI.
var _ = db.ToolVersion
var _ json.Decoder

// TestCompositeScannerGitHubNonAnswerFailsTheScan (v0.29.55 review round 3;
// operator decision (a), 2026-09-17): GitHub source errors never failed or
// marked a scan, on the grounds that 403/404/304 from these endpoints are
// routinely benign. That covers ANSWERS; it also swallowed failures that say
// nothing (retries exhausted — four "github manifests failed … exhausted 10
// retries" lines in the 2026-09-17 incident log — a cut-off body, an empty
// key pool), so the scan was stored complete: the manifest snapshot replaced
// by what little was seen and the repo held for the 180-day cadence. A GitHub
// non-answer now FAILS the scan (the worker records a strike; nothing is
// replaced; quadratic backoff; the 10-strike sideline). Marking it incomplete
// instead was rejected: an incomplete row re-claims immediately at the head
// of the queue with no backoff. A GitHub answer (404) still does neither.
func TestCompositeScannerGitHubNonAnswerFailsTheScan(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[]}`)
	}))
	t.Cleanup(depsServer.Close)
	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ecoServer.Close)

	for name, tc := range map[string]struct {
		handle  func(w http.ResponseWriter, r *http.Request) bool // true = handled
		wantErr bool
	}{
		"root listing cut off": {func(w http.ResponseWriter, r *http.Request) bool {
			if r.URL.Path == "/repos/x/y/contents" {
				_, _ = io.WriteString(w, `[{"type":"fi`)
				return true
			}
			return false
		}, true},
		"manifest content cut off": {func(w http.ResponseWriter, r *http.Request) bool {
			switch r.URL.Path {
			case "/repos/x/y/contents":
				_, _ = io.WriteString(w, `[{"type":"file","name":"package.json","path":"package.json"}]`)
				return true
			case "/repos/x/y/contents/package.json":
				_, _ = io.WriteString(w, `{"encoding":"base64","content":"eyJuYW1l`)
				return true
			}
			return false
		}, true},
		"release listing cut off": {func(w http.ResponseWriter, r *http.Request) bool {
			if r.URL.Path == "/repos/x/y/releases" {
				_, _ = io.WriteString(w, `[{"assets":[{"na`)
				return true
			}
			return false
		}, true},
		"packages listing cut off": {func(w http.ResponseWriter, r *http.Request) bool {
			if r.URL.Path == "/users/x/packages" {
				_, _ = io.WriteString(w, `[{"na`)
				return true
			}
			return false
		}, true},
		// Review round 6: an error that IS an answer (a rejected request)
		// must not fail the scan when other sources are clean — pins that
		// githubErrorIsNonAnswer excludes answers, not only that the
		// non-answer arms exist.
		"release listing rejected (422)": {func(w http.ResponseWriter, r *http.Request) bool {
			if r.URL.Path == "/repos/x/y/releases" {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = io.WriteString(w, `{"message":"Validation Failed"}`)
				return true
			}
			return false
		}, false},
		"root listing 404": {func(w http.ResponseWriter, r *http.Request) bool {
			if r.URL.Path == "/repos/x/y/contents" {
				w.WriteHeader(http.StatusNotFound)
				return true
			}
			return false
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tc.handle(w, r) {
					return
				}
				_, _ = io.WriteString(w, `[]`)
			}))
			t.Cleanup(ghServer.Close)
			scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true)
			_, manifests, complete, err := scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("scan succeeded (complete=%v, manifests=%v) — a GitHub non-answer must fail the scan so nothing is replaced", complete, manifests)
				}
				return
			}
			if err != nil || !complete {
				t.Fatalf("err=%v complete=%v — a GitHub answer (404, 422) does not fail the scan: it succeeds and is complete", err, complete)
			}
		})
	}
}
