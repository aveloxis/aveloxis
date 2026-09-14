// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round 7 on PR #191 (v0.28.18): the pass-24 pin listed seven
// failure-log messages by hand, so a ticker task it did not name — or a
// site added later — could go on reporting shutdown as a failure
// (runSearchResolve's "failed to link contributor" and refreshUserOrgs'
// six per-repo WARNs did exactly that). This pin derives the task set
// STRUCTURALLY: every `s.<task>(ctx…)` call under a `case <-…:` arm of
// Run's select loop is a ticker task, and inside each task EVERY failure
// log (`s.logger.Warn/Error(…, "error", X)`) whose producing call is
// ctx-bound (the statement assigning X mentions ctx or reads a response
// body) must classify `errors.Is(X, context.Canceled)` and return before
// it logs. A loop-top ctx.Err() guard does not satisfy it — the in-flight
// call that observed the cancellation still reaches the log (L14).
//
// Scope (pass 34 widened it, pass 35 again): every `s.<fn>(ctx…)`
// reachable from Run — the select arms AND the prelude — followed to a
// fixpoint; the first cut stopped at the task body and missed
// refreshOrgs' two forge helpers, checkForRenames, largeRepoExclusions,
// the history worker, the leftover-staging drain, the metadata
// backfill, the mailing-list loops and the DB-health probe. Delegates
// in OTHER packages (collector: breadth, enrich, facade, staged,
// gap fill, refresh, scorecard, mailing-list worker/processor) are
// outside this pin; passes 35–36 swept them by hand (L11), which is a
// point-in-time sweep, not enforcement — the cross-package analyzer is
// the P0 item in summary/26.
//
// Precision (pass 34): the classification must sit between the
// error's producing statement and the log — a fixed lookback let a
// NEIGHBOUR's classification satisfy a later arm (the pass-24
// regression shape) — and the producer regex must not match `==`.
// Exempted sites are printed so the exemption set stays reviewable.
func TestEveryTickerTaskClassifiesCancellation(t *testing.T) {
	files := schedulerSources(t)
	run := srctest.StripGoComments(srctest.FuncBody(t, files["scheduler.go"], "func (s *Scheduler) Run("))

	armRe := regexp.MustCompile(`case <-[\w.]+:`)
	callRe := regexp.MustCompile(`\bs\.(\w+)\(ctx\b`)
	arms := armRe.FindAllStringIndex(run, -1)
	if len(arms) < 8 {
		t.Fatalf("found only %d `case <-…:` arms in Run — the select loop moved; re-anchor this pin", len(arms))
	}
	// Every `s.<fn>(ctx…)` in Run is a task: the select arms AND the
	// prelude (processLeftoverStagingBackground, runRepoMetadataBackfill,
	// recoverStale — pass 35 found the first cut skipped the prelude).
	tasks := map[string]bool{}
	for _, m := range callRe.FindAllStringSubmatch(run, -1) {
		tasks[m[1]] = true
	}
	if len(tasks) < 10 {
		t.Fatalf("derived only %d ticker tasks from Run: %v — expected at least 10", len(tasks), sortedKeys(tasks))
	}

	// Fixpoint: every `s.<fn>(ctx…)` a task (or a reached helper) calls
	// joins the set. Run itself is excluded — it is the dispatcher.
	bodyOf := func(name string) string {
		sig := "func (s *Scheduler) " + name + "("
		for _, src := range files {
			if strings.Contains(src, sig) {
				return srctest.StripGoComments(srctest.FuncBody(t, src, sig))
			}
		}
		return ""
	}
	bodies := map[string]string{}
	queue := sortedKeys(tasks)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if _, seen := bodies[name]; seen || name == "Run" {
			continue
		}
		body := bodyOf(name)
		if body == "" {
			t.Errorf("ticker task %s: definition not found in internal/scheduler", name)
			bodies[name] = ""
			continue
		}
		bodies[name] = body
		for _, m := range callRe.FindAllStringSubmatch(body, -1) {
			if _, seen := bodies[m[1]]; !seen {
				queue = append(queue, m[1])
			}
		}
	}
	if len(bodies) < 20 {
		t.Fatalf("fixpoint reached only %d task+helper bodies: %v — expected at least 20", len(bodies), sortedKeys(boolKeys(bodies)))
	}

	// Run is the dispatcher, not a task, but its PRELUDE has its own
	// ctx-bound failure logs (pass 37: the five startup steps were
	// classified and nothing enforced it) — check its body too.
	bodies["Run"] = run
	for _, name := range sortedKeys(boolKeys(bodies)) {
		body := bodies[name]
		if body == "" {
			continue
		}
		violations, exempt := cancelViolations(body)
		for _, e := range exempt {
			t.Logf("%s: exempt (producer is not ctx-bound): %s", name, e)
		}
		for _, v := range violations {
			t.Errorf("%s: %s", name, v)
		}
	}
}

// Info counts too: a failure recorded at Info (`… failed`, then
// `failed++`) is still shutdown-as-failure (pass 36 — the metadata
// backfill's FetchRepoInfo arm hid there).
var failureLogRe = regexp.MustCompile(`s\.logger\.(Warn|Error|Info)\(`)

// cancelViolations applies the rule to ONE comment-stripped function
// body: every failure log whose error is ctx-bound must be preceded —
// between its producing statement and the log — by
// `errors.Is(X, context.Canceled)` followed by a return. Returns the
// violations and the exempted (non-ctx-bound) sites.
func cancelViolations(body string) (violations, exempt []string) {
	for _, m := range failureLogRe.FindAllStringIndex(body, -1) {
		args := callArgs(body, m[1]-1)
		errIdent := errorAttr(args)
		if errIdent == "" {
			continue // not a failure log (no "error", X attribute)
		}
		logAt := m[0]
		armRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(errIdent) + ` != nil \{`)
		arms := armRe.FindAllStringIndex(body[:logAt], -1)
		// A failure log with no `if X != nil {` arm (a state-transition
		// log carrying the last error, e.g. the DB-health "unavailable"
		// WARN) is judged by its producer like any other: exempt when the
		// error is not ctx-bound, a violation otherwise.
		anchor := logAt
		if len(arms) > 0 {
			anchor = arms[len(arms)-1][0]
		}
		prodAt, producer, chain := producerChain(body, anchor, errIdent)
		if prodAt < 0 {
			violations = append(violations, "failure log "+strconv.Quote(snippet(body, logAt))+" — no assignment of "+errIdent+" found before its arm")
			continue
		}
		if !strings.Contains(strings.ToLower(producer), "ctx") && !strings.Contains(producer, "resp.Body") {
			exempt = append(exempt, strconv.Quote(snippet(body, logAt))+" ← "+strconv.Quote(strings.TrimSpace(producer)))
			continue
		}
		// The classification must lie between THIS error's producing
		// statement and its log, and must return — a neighbour's
		// earlier classification does not count. Any identifier on the
		// alias chain (`lastErr = err` ← `err := Ping(ctx)`) may carry it.
		span := body[prodAt:logAt]
		cls := -1
		for _, id := range chain {
			if i := strings.Index(span, "errors.Is("+id+", context.Canceled)"); i >= 0 && (cls < 0 || i < cls) {
				cls = i
			}
		}
		if cls < 0 || !returnsInsideBlock(span, cls) {
			violations = append(violations, "failure log "+strconv.Quote(snippet(body, logAt))+" must classify errors.Is("+errIdent+
				", context.Canceled) AND return before it logs — shutdown is not a failure (producer: "+strconv.Quote(strings.TrimSpace(producer))+")")
		}
	}
	return violations, exempt
}

// The shapes the pass-34 reviewer probed against the first cut, kept
// as fixtures so the analyzer's precision cannot regress silently:
// a neighbour's classification must not satisfy a later arm (M6, the
// pass-24 regression shape), `err == nil` must not read as a producer
// (M5), a fall-through classification is decorative, and the two
// legitimate shapes pass.
func TestTickerCancelAnalyzerCatchesTheProbedShapes(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		violations int
		exempt     int
	}{
		{"classified before the arm", `
	err := s.store.A(ctx)
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		s.logger.Warn("a failed", "error", err)
	}
`, 0, 0},
		{"classified as the arm's first statement", `
	x, err := s.store.A(ctx, 1)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		s.logger.Error("a failed", "n", x, "error", err)
	}
`, 0, 0},
		{"M6: a neighbour's classification does not cover a later arm", `
	err := s.store.A(ctx)
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		s.logger.Warn("a failed", "error", err)
	}
	err = s.store.B(ctx)
	if err != nil {
		s.logger.Warn("b failed", "error", err)
	}
`, 1, 0},
		{"M5: err == nil is not a producer", `
	n, err := s.store.Count(ctx)
	if err == nil {
		use(n)
	}
	if err != nil {
		s.logger.Warn("count failed", "error", err)
	}
`, 1, 0},
		{"decorative: classification that falls through", `
	err := s.store.A(ctx)
	if errors.Is(err, context.Canceled) {
		s.logger.Debug("canceled")
	}
	if err != nil {
		s.logger.Warn("a failed", "error", err)
	}
`, 1, 0},
		{"alias chain: the source error's classification counts for the alias", `
	err := s.store.Ping(ctx)
	if errors.Is(err, context.Canceled) {
		return
	}
	if err != nil {
		lastErr = err
	} else {
		lastErr = nil
	}
	if down {
		s.logger.Warn("database unavailable", "error", lastErr)
	}
`, 0, 0},
		{"alias chain: an unclassified source is a violation, never exempt", `
	err := s.store.Ping(ctx)
	if err != nil {
		lastErr = err
	} else {
		lastErr = nil
	}
	if down {
		s.logger.Warn("database unavailable", "error", lastErr)
	}
`, 1, 0},
		{"alias chain through := is followed", `
	err := s.store.Ping(ctx)
	aerr := err
	if aerr != nil {
		s.logger.Warn("ping failed", "error", aerr)
	}
`, 1, 0},
		{"alias chain through a %w wrap is followed", `
	err := s.store.Ping(ctx)
	werr := fmt.Errorf("ping: %w", err)
	if werr != nil {
		s.logger.Warn("ping failed", "error", werr)
	}
`, 1, 0},
		{"alias chain through a %w wrap with a trailing non-error arg", `
	err := s.store.Ping(ctx)
	werr := fmt.Errorf("ping: %w: %s", err, detail.String())
	if werr != nil {
		s.logger.Warn("ping failed", "error", werr)
	}
`, 1, 0},
		{"decorative: classification without a return, unrelated return later", `
	err := s.store.A(ctx)
	if errors.Is(err, context.Canceled) {
		s.logger.Debug("canceled")
	}
	if other {
		return
	}
	if err != nil {
		s.logger.Warn("a failed", "error", err)
	}
`, 1, 0},
		{"alias chain through an indexed %[1]w wrap is followed", `
	err := s.store.Ping(ctx)
	werr := fmt.Errorf("ping: %[1]w (%[1]v)", err)
	if werr != nil {
		s.logger.Warn("ping failed", "error", werr)
	}
`, 1, 0},
		{"decorative: the classification's return is nested in an inner if", `
	err := s.store.A(ctx)
	if errors.Is(err, context.Canceled) {
		if other {
			return
		}
	}
	if err != nil {
		s.logger.Warn("a failed", "error", err)
	}
`, 1, 0},
		{"an Info-level failure log counts", `
	info, err := client.FetchRepoInfo(ctx, o, r)
	if err != nil {
		s.logger.Info("backfill: FetchRepoInfo failed (will retry)", "error", err)
		failed++
	}
`, 1, 0},
		{"a derived context is still ctx-bound (heartbeatCtx)", `
	err := s.store.HeartbeatJob(heartbeatCtx, id)
	if err != nil {
		s.logger.Warn("heartbeat failed", "error", err)
	}
`, 1, 0},
		{"exempt: producer is not ctx-bound", `
	home, err := os.UserHomeDir()
	if err != nil {
		s.logger.Warn("no home", "error", err)
	}
`, 0, 1},
		{"body-read producer counts as ctx-bound", `
	decErr := json.NewDecoder(resp.Body).Decode(&items)
	if decErr != nil {
		s.logger.Warn("decode failed", "error", decErr)
	}
`, 1, 0},
		{"a message field is not an error value", `
	if !outcome.success {
		s.logger.Warn("job failed", "error", outcome.errMsg)
	}
`, 0, 0},
	}
	for _, tc := range cases {
		v, e := cancelViolations(tc.body)
		if len(v) != tc.violations || len(e) != tc.exempt {
			t.Errorf("%s: got %d violation(s) %v and %d exempt %v, want %d / %d", tc.name, len(v), v, len(e), e, tc.violations, tc.exempt)
		}
	}
}

// schedulerSources returns every non-test Go file of the package keyed
// by basename.
func schedulerSources(t *testing.T) map[string]string {
	t.Helper()
	dir := filepath.Join(srctest.Root(t), "internal", "scheduler")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		out[n] = srctest.Read(t, filepath.Join("internal", "scheduler", n))
	}
	if len(out) < 5 {
		t.Fatalf("read only %d scheduler sources", len(out))
	}
	return out
}

// callArgs returns the text between the call's opening paren at open
// and its matching close, skipping parens inside string literals.
func callArgs(src string, open int) string {
	depth, inStr := 0, byte(0)
	for i := open; i < len(src); i++ {
		c := src[i]
		switch {
		case inStr != 0:
			if c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '`':
			inStr = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return src[open+1 : i]
			}
		}
	}
	return src[open:]
}

// A bare error identifier (optionally `.Error()`); a field such as
// `outcome.errMsg` is a message string, not an error value.
var errorAttrRe = regexp.MustCompile(`"error",\s*(\w+)(?:\.Error\(\))?\s*[,)]`)

func errorAttr(args string) string {
	// args excludes the call's closing paren; restore it so a trailing
	// `"error", err` (the common shape) still terminates the identifier.
	if m := errorAttrRe.FindStringSubmatch(args + ")"); m != nil {
		return m[1]
	}
	return ""
}

// producerChain returns the offset and text of the statement that
// PRODUCED ident's value before the anchor, following aliases: the
// last assignment of ident (`x :=`, `x =`, `a, x :=` — never `x ==`),
// skipping `= nil` resets, and when the right-hand side is a bare
// identifier (`lastErr = err`) the same lookup for THAT identifier,
// up to four hops. Returns every identifier on the chain so a
// classification of the source error counts for the alias (pass 36:
// the DB-health "unavailable" WARN carries lastErr whose last textual
// assignment was `lastErr = nil` — the first cut read it as not
// ctx-bound and exempted the site, hiding the Ping classification
// from the pin).
func producerChain(body string, anchor int, ident string) (int, string, []string) {
	chain := []string{ident}
	cur, at := ident, anchor
	for hop := 0; hop < 4; hop++ {
		re := regexp.MustCompile(`\b` + regexp.QuoteMeta(cur) + `\s*(:=|=[^=])`)
		locs := re.FindAllStringIndex(body[:at], -1)
		// The right-hand side starts after the operator: a `:=` match ends
		// on `=`, a `= x` match ends one byte INTO the value (pass 37 —
		// the first cut read `aerr := err` as `= err`-less and exempted
		// the alias).
		rhsOf := func(loc []int) string {
			start := loc[1]
			if !strings.HasSuffix(body[loc[0]:loc[1]], ":=") {
				start = loc[1] - 1
			}
			rhs := body[start:]
			if j := strings.IndexAny(rhs, "\n;"); j >= 0 {
				rhs = rhs[:j]
			}
			return strings.TrimSpace(rhs)
		}
		found := -1
		for i := len(locs) - 1; i >= 0; i-- {
			if rhsOf(locs[i]) == "nil" {
				continue
			}
			found = i
			break
		}
		if found < 0 {
			return -1, "", chain
		}
		start := locs[found][0]
		stmt := body[start:at]
		rhs := rhsOf(locs[found])
		// A bare alias (`lastErr = err`) or a %w wrap
		// (`werr := fmt.Errorf("x: %w", err)`) hops to the wrapped error.
		if m := regexp.MustCompile(`^(\w+)$`).FindStringSubmatch(rhs); m != nil && m[1] != cur && m[1] != "nil" {
			chain = append(chain, m[1])
			cur, at = m[1], start
			continue
		}
		if w := wrappedErrIdent(rhs); w != "" && w != cur {
			chain = append(chain, w)
			cur, at = w, start
			continue
		}
		return start, stmt, chain
	}
	return -1, "", chain
}

// wrappedErrIdent returns the identifier a `fmt.Errorf("…%w…", …)` wraps
// — the argument at %w's position among the verbs — when it is a bare
// identifier; "" otherwise. Pass 38: matching the LAST argument missed
// `fmt.Errorf("%w: %s", err, stderr.String())` (facade's wrap shape).
func wrappedErrIdent(rhs string) string {
	m := regexp.MustCompile(`^fmt\.Errorf\("((?:[^"\\]|\\.)*)"\s*,\s*(.*)\)$`).FindStringSubmatch(rhs)
	if m == nil {
		return ""
	}
	// Verb positions follow fmt's rules: an explicit index `%[n]w` names
	// argument n and later verbs continue from it (pass 39).
	verbs := regexp.MustCompile(`%[+\-# 0-9.]*(\[(\d+)\])?[a-zA-Z%]`).FindAllStringSubmatch(m[1], -1)
	pos := -1
	next := 0
	for _, v := range verbs {
		if v[0] == "%%" {
			continue
		}
		idx := next
		if v[2] != "" {
			n, _ := strconv.Atoi(v[2])
			idx = n - 1
		}
		if strings.HasSuffix(v[0], "w") {
			pos = idx
			break
		}
		next = idx + 1
	}
	if pos < 0 {
		return ""
	}
	args := splitTopLevel(m[2])
	if pos >= len(args) {
		return ""
	}
	if a := regexp.MustCompile(`^\s*(\w+)\s*$`).FindStringSubmatch(args[pos]); a != nil {
		return a[1]
	}
	return ""
}

// splitTopLevel splits an argument list on commas outside parens/strings.
func splitTopLevel(s string) []string {
	var out []string
	depth, start, inStr := 0, 0, byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr != 0:
			if c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '"' || c == '`':
			inStr = c
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// returnsInsideBlock reports whether the brace block that follows the
// classification at cls (an `if errors.Is(…) {`) contains a return at
// ITS OWN level — a fall-through classification followed by an
// unrelated later return is decorative (pass 38), and so is a return
// nested in an inner `if` that may not fire (pass 39).
func returnsInsideBlock(span string, cls int) bool {
	open := strings.IndexByte(span[cls:], '{')
	if open < 0 {
		return false
	}
	depth := 0
	for i := cls + open; i < len(span); i++ {
		switch span[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return false
			}
		default:
			if depth == 1 && strings.HasPrefix(span[i:], "return") && (i == 0 || !isIdentByte(span[i-1])) {
				return true
			}
		}
	}
	return false
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func boolKeys(m map[string]string) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

func snippet(body string, at int) string {
	end := at + 60
	if end > len(body) {
		end = len(body)
	}
	return strings.TrimSpace(body[at:end])
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
