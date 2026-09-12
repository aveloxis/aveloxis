<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Testing

Aveloxis follows TDD as a discipline: failing test first, then implementation, then verify. This chapter covers what kinds of tests exist, when to use which, and the patterns that have proved durable.

## The three tiers

### Tier 1 — Unit tests

Run against pure code, no database, no network. Live in `*_test.go` next to the code.

```bash
go test ./...
```

This is what CI runs and what every contributor runs constantly. Should always be green. Should always be fast (the full unit suite finishes in a few minutes, except for `internal/platform` which has ~40 s of network mock tests).

### Tier 2 — Integration tests

Run against a real PostgreSQL via the `AVELOXIS_TEST_DB` env var. Live in `*_integration_test.go` files.

```bash
AVELOXIS_TEST_DB="postgres://aveloxis:pw@localhost:5432/aveloxis_scratch?sslmode=prefer" \
    go test ./internal/db/ -v
```

Every integration test starts with:

```go
func TestSomething(t *testing.T) {
    dsn := os.Getenv("AVELOXIS_TEST_DB")
    if dsn == "" {
        t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
    }
    // ... rest of test
}
```

This means `go test ./...` (no env var) skips them cleanly. CI runs both modes — see `.github/workflows/integration.yml` for the Postgres-service-container recipe.

**Safety:** integration tests assume a scratch database. Some helpers (`store.RealignDueDates`, the `dm_` aggregate refresh, the `cntrb_id` migration) update every matching row. Pointing them at a production database is destructive.

### Tier 3 — `data-test` harness (cross-version regression detection)

The `aveloxis data-test` subcommand collects the same repo into two scratch databases (one with a released-tag binary, one with the local working-tree binary) and row-count-diffs them. Catches schema-regression data loss before it ships.

```bash
aveloxis data-test \
  --released-tag 0.23.3 \
  --repo https://github.com/augurlabs/augur
```

This is documented in [`docs/guide/data-test.md`](../guide/data-test.md). You don't run it during normal feature work — it's the operator-side gate before a release.

## Static analysis — the three blocking linters (v0.27.36)

Every change must pass all three before it ships; CI (`lint.yml`) blocks on
each:

1. **`go vet ./...`** — the official analyzer; always compatible with the
   toolchain.
2. **`staticcheck ./...`** — blocking since v0.25.36; SA-class findings are
   bug-shaped.
3. **`golangci-lint run`** — blocking since v0.27.36. Configured in
   `.golangci.yml` (errcheck, govet, ineffassign, staticcheck, unused) with a
   documented exclusion philosophy: catch real bugs, don't drown signal in
   noise. The CI version is PINNED (bump deliberately, run locally first).

The exclusion rules in `.golangci.yml` are contracts, not conveniences —
each carries a rationale comment (e.g. `syscall.Kill` in the straggler-kill
defers expects ESRCH; `Tx.Rollback` in error paths is the pgx idiom; JSON
encoding to a ResponseWriter has no recovery once headers are sent). Adding
an exclusion requires the same justification in a comment. Discarding an
error WITHOUT an exclusion match requires an explicit `_ =` plus a comment
saying why best-effort is correct there — the audit history (summary/18)
shows silent error-dropping is this codebase's most repeated incident class.

Related tripwire: `TestNoInlineScanNilConditionals` (internal/db) bans the
`rows.Scan(...) == nil` inline-condition form, which silently drops rows on
scan failure and is invisible to errcheck because the value IS used.

## The standing gate list (definition of done)

Every release candidate passes, in order (v0.27.43, summary/18 Phase
5d — each tier's CI home named):

1. **Unit** — `go test ./...` (test.yml, with `-race`).
2. **go vet** — lint.yml.
3. **staticcheck** — lint.yml, blocking since v0.25.36.
4. **golangci-lint** — lint.yml, blocking since v0.27.36, version-pinned.
5. **Cascade integration** — `AVELOXIS_TEST_DB=<scratch> go test ./...`
   (integration.yml: Postgres service, `-race`, TZ=America/Chicago).
6. **Fresh-empty-DB gate** — required whenever schema.sql or
   migrate.go change: create an empty database, run the db+collector
   suites against it (proves from-scratch migration ordering; the
   v0.27.9 lesson).
7. **`aveloxis data-verify`** — runs automatically at the end of the
   integration workflow against the CI database (smoke-tests the
   battery AND catches fixture residue from the test suite); run it
   manually against a scratch or production database for release
   candidates (`--ground-truth N` adds live-forge comparison, needs
   API keys — never in CI). FAIL exits 1.
8. **Network canaries** — network-canary.yml, weekly: live-API
   contract checks (OSV, registries, GraphQL parity fields,
   tool-binary versions).
9. **The local knob canary** (`TestLocalDevBuildDepsCanary`,
   internal/collector/local_canary_test.go) — before flipping any
   collection knob that changes dependency or findings volume, run
   the 22-repo profile harness against a dedicated scratch DB. It
   drives the real clone→analysis→scan machinery, needs NO GitHub
   API keys, and reports the baseline-vs-knobs-on delta per
   ecosystem (optional `AVELOXIS_CANARY_TRANSITIVE=1` third phase):

   ```bash
   createdb aveloxis_canary19
   AVELOXIS_CANARY_DB="postgres://…/aveloxis_canary19?sslmode=prefer"      go test ./internal/collector/ -run TestLocalDevBuildDepsCanary -v -timeout 180m
   ```

   Its first run (2026-07-21) caught three real integration bugs
   the fixture suites could not see — treat "the parsers are
   fixture-tested" as necessary but NOT sufficient for a
   volume-knob flip.

10. **Docs build** — docs.yml: `sphinx-build -W --keep-going` over
    `docs/`; every Sphinx/MyST warning (unknown fence language, a block
    that does not lex in its declared language, a dead cross-reference)
    is an error, matching Read the Docs' `fail_on_warning`.

## TDD discipline

The contract:

1. **Write a failing test.** Run it. See it fail. The failure message should make it obvious what behavior is missing.
2. **Implement the minimum needed to pass.** Don't add features beyond what the test requires.
3. **Verify everything passes.** Run `go test ./...` AND the integration tier if your change touches the DB.
4. **Then refactor if useful.** With tests passing, you can clean up confidently.

This isn't optional. It's the rule that prevents bugs like the v0.21.0 backfill that referenced a column name that didn't exist — the source-contract test PASSED because the SQL string contained the wrong column name and the test scanned for that same wrong name. v0.21.1's lesson: source-contract tests verify the code SAYS what you wrote, not that what you wrote is CORRECT against the actual schema. Hence the integration tier.

## The source-contract pattern

A test that reads the implementation file and asserts certain text/structure exists. Looks like this:

```go
func TestRunMigrationsAddsOnUpdateCascadeToAllCntrbIDFKs(t *testing.T) {
    src, err := os.ReadFile("migrate.go")
    if err != nil {
        t.Fatal(err)
    }
    code := string(src)

    if !strings.Contains(code, "ON UPDATE CASCADE") {
        t.Error("RunMigrations must add ON UPDATE CASCADE to every " +
            "cntrb_id child FK. Without this, the v0.22.2 cntrb_id " +
            "data migration can't propagate UPDATEs to child rows.")
    }
}
```

### When to use a source-contract test

- Pinning a load-bearing design choice that a "helpful" refactor might silently revert. Example: the v0.20.18 negative tripwire that fails CI if `batch_size` ever reappears as a JSON tag (the field was removed; tests prevent it sneaking back).
- Verifying the FROM of a query, the WHERE clause, the column list, when behavioral testing would require a complex DB fixture.
- Pinning that a function is wired into the right caller (`TestRunMigrationsInvokesEnsureCntrbIDFKIndexes` proves the helper is actually called from RunMigrations).

### When NOT to use a source-contract test

- When you can write a behavioral test that actually exercises the code. Behavioral > source-contract every time.
- When the "contract" is just gofmt formatting (the v0.22.0 phase-5 test had to be rewritten to be whitespace-tolerant after gofmt re-aligned a struct).

### The v0.21.1 lesson

Source-contract tests can give false confidence:

> v0.21.0 backfill SQL referenced `aveloxis_scan.scancode_scans.created_at` but the table uses `data_collection_date`. The source-contract test pinned `MAX(created_at)` as a needle in `migrate.go` and passed because both sides of the contract agreed on the wrong answer. Production migrate failed with `ERROR: column "created_at" does not exist (SQLSTATE 42703)`.

If you write a source-contract test for SQL that references a column from a different table, **also** write an integration test that runs the migration against a fresh database. The combination catches both refactor-rename drift (source-contract) and column-name typos (integration).

### Shared helpers: `internal/srctest` (v0.27.118)

All NEW source-contract tests use `internal/srctest` — the single, fixture-tested
home for the operations that used to be duplicated ad hoc (~40 read helpers, five
incompatible function-body extractors with *different scan windows*, comment
strippers, repo-root finders):

- `srctest.Root(t)` / `srctest.Read(t, "docs/...")` — repo-root discovery and
  root-relative reads (no more `../../` fragility)
- `srctest.FuncBody(t, src, "func (s *T) Name(")` — THE brace-counting extractor
  (string/comment-literal aware; excludes trailing comments — the legacy
  cut-at-next-`func ` windows were a false-match hazard)
- `srctest.StripGoComments` / `srctest.StripSQLComments` — literal-aware (a `//`
  inside a string or a `);` inside a SQL comment can no longer corrupt a scan)
- `srctest.BacktickLiterals`, `srctest.NormalizeWS`, `srctest.ContainsNormalized`
- `srctest.MinCount(t, what, got, min)` — the mandatory "my own corpus walk
  broke" guard for any test that scans directories

Adoption is **strangler-only**: legacy tests migrate when touched, tracked by the
shrink-only ratchet (`scripts/srctest_ratchet_test.go` +
`scripts/srctest_migration_baseline.txt` — defining a NEW duplicate helper fails
the build; a migrated site's baseline line must be deleted in the same diff).
**There is deliberately NO bulk migration of the remaining legacy files** —
refactoring working pins is itself a regression risk. When you do migrate a pin,
follow the **re-red protocol**: green → temporarily inject the violation the pin
exists to catch → confirm RED → revert → green. Interdependent pins (ones that
read other test files or share allowlists) migrate as a closed cluster in one
change.

### SQL write scanning: `internal/srctest/sqlscan` (v0.27.124)

Tests that reason about SQL WRITE statements use the Phase 2 engine
instead of ad-hoc regexes:

- `sqlscan.Statements(srctest.PackageFiles(t, "internal/db", 30))` — every
  backtick SQL literal in a package, comment-stripped (literal-aware), SPLIT at
  quote-/`$$`-aware top-level semicolons, and attributed to its source file.
  Splitting matters: in the old concatenated-literal scan, a `SET col =` in an
  unrelated trailing statement of a multi-statement literal counted as a writer
  for the leading statement's table.
- `sqlscan.FindWrites(stmts, "aveloxis_data.repos")` — the INSERT/UPDATE
  statements naming a schema-qualified table (aveloxis_ops works too).
- `Stmt.WritesColumn(table, col)` — writer-position detection (INSERT column
  list or UPDATE SET left-hand side; WHERE/RETURNING/RHS appearances do NOT
  count — the v0.27.116 contract, fixtures moved wholesale).
- `Stmt.SetExprs(col)` — the depth-counting, quote-aware RHS extractor the
  Phase 3 write-policy matchers classify.
- `Stmt.WhereGuardsEmpty(col)` — recognizes the guarded fill-empty shape
  (`SET col = $N ... WHERE COALESCE(col, '') = ''` / `col IS NULL`).

Known blind spots are documented in the package doc (concatenated SQL is
invisible; tagged dollar-quotes untracked). The flagship consumer is
`internal/db/column_writer_tripwire_test.go`; port verified behavior-identical
(same zero-violation pass on the real corpus; fake-column and stale-allowlist
mutations flag identically).

### Column write-policy registry (v0.27.126)

`internal/db/column_write_policy_test.go` declares each protected column's ONE
write policy (`FillEmptyOnly` / `PreferNonemptyIncoming` /
`PreferNonNullIncoming` / `GreatestNonNull` / `AlwaysRefresh` / `InsertOnly`);
the sqlscan policy engine verifies every UPDATE SET assignment in the store
corpus conforms. Matchers are ORDER-SENSITIVE — the verbatim shipped round-11
bug (`COALESCE(NULLIF(EXCLUDED.platform_repo_id, ''), repos.platform_repo_id)`,
incoming-first where fill-empty-only was intended) is a permanent canary
fixture in the engine's suite, and re-red-verified against the live registry.
Triage protocol for a red run (in the failure message; never weaken a
matcher): fix the SQL | correct the registered policy | add an `Exception`
WITH a reason. Exceptions that suppress nothing are reported STALE and fail
the run. New schema columns with a meaningful write discipline register at
introduction (schema-migrations.md checklist step 7).

### Standing-rules registry (v0.27.127)

`scripts/standing_rules.go` is the machine-readable table of the project's
cross-cutting rules — {ID, one-line Statement, EnforcedBy test names,
ProcessOnly, Retired}. `TestStandingRulesRegistry` keeps it true: every
active rule's enforcing tests must exist; every `SR-<n>` citation in any
test must name a registered rule; IDs are never reused (Retired rules keep
theirs forever). New tripwires that enforce a standing rule cite their
SR-ID in a comment (`// Enforces SR-11 ...`). ProcessOnly rules are
review-time discipline and the analyzer candidates below.

### Convergence-contracts registry (v0.27.146 — SR-19 mechanized)

Anything documented as "rerun until done" / "self-draining" / "the
marker is the resume state" is a **convergence contract**: re-running
must skip completed work and reach a stable done state. The motivating
incident: `heal-collection-gaps` promised "rerun until 0 candidates"
while healed repos never left the candidate set — the contract was
prose, not a property.

`scripts/convergence_contracts_test.go` keeps every such contract
paired with a test that DRIVES its loop to done:

- The scanner sweeps every non-test source in `cmd/` + `internal/` +
  `scripts/` for the marker phrases; every hit must be registered in
  `convergenceContracts` with at least one **driving test**, or carry a
  reviewed exemption with a reason.
- Driving tests are verified as real test FuncDecls via `go/parser`
  (never a source regex — the round-22 lesson).
- Staleness both ways: a registered/exempt file that stops carrying a
  marker fails until the row is removed.

**Registering a site is not the work — the driving test is.** A driving
test performs the unit of work, applies the SAME completion stamp the
production path applies, and asserts the claim/candidate/batch query
stops returning the row. Exemplars: `TestGapHealConvergesToZeroCandidates`
(the flagship e2e: candidate → threshold-0 fill against an httptest
forge → `RefreshQueueGatheredCounts` → candidate set empty),
`TestWhitespaceRewalkClaimDrainsStampedRepos`,
`TestMessageHealWorklistDrainsOnStamp`, and the dedup e2e's
candidate-drain assertion. A new resumable command writes its
convergence test first, then adds the registry row pointing at it.

### Analyzer graduation criteria (deliberately deferred)

All cross-cutting checks stay in `go test` (runs identically everywhere, zero
toolchain maintenance). Build a custom `go/analysis` analyzer ONLY when a rule
(a) needs type information or call-graph facts a text scan cannot approximate —
the live candidates are "a lookup ERROR is not 'no'" (`errors.Is`/ErrNoRows
discipline) and safego adoption — AND (b) has caused ≥2 review-missed incidents,
AND (c) the text-scan form has produced wrong results twice despite fixture
fixes.

### SQL parsing: tested micro-parsers now; pg_query_go pilot flagged

The SQL scanning helpers are regex/micro-parser based ON PURPOSE — every recorded
fragility incident was in a *scattered, untested* copy, which centralization plus
hostile fixtures addresses. **FLAGGED PILOT (operator, 2026-08-20)**: when the
write-policy engine lands, a shadow implementation on pg_query_go (real Postgres
AST) runs in an isolated nested module (`tools/sqlcheck-pilot/`, own go.mod so
the CGo dep never touches the main module) and is diffed against the regex
engine over the same corpus. If it actually works (no new false positives,
catches shapes regex can't, acceptable build friction, shorter check code), the
operator decides a going-forward migration plan then. Nothing ships on it until
that decision.

## Where expected values come from

The 2026-07-21 audit (`summary/17-wrong-answer-tests-audit.md`) found a recurring failure shape across the suite: **both-sides-agree-on-the-wrong-answer tests** — tests whose expected values were derived from the implementation under test, so implementation and expectation could be wrong *together* and the test would pass forever. Three shipped bugs came from this shape: the v0.21.0 backfill column name, the purl non-canonical encodings (pinned as correct by their own tests), and the PEP 639 license drift (every mock fixture predated the ecosystem change).

The rule: **every expected value in a test must trace to an authority that is not the code under test.** In descending order of strength:

1. **The specification itself** — committed as a fixture the test reads at run time, so refreshing the fixture refreshes the constraints. Examples: `internal/collector/testdata/purl_spec_cases.json` (purl-spec canonical cases), `testdata/sbom_schemas/` (official CycloneDX 1.5 + SPDX 2.3 JSON schemas — the SBOM test reads the schemas' own `required` lists and enums), `spdx_license_ids.txt` (the official SPDX id list).
2. **The reference implementation's source** — when compatibility with another system is the claim, fetch that system's actual code and hand-derive vectors from it. Example: `augur_uuid_groundtruth_test.go` derives its UUID strings from chaoss/augur's `AugurUUID.py` byte rules, with one vector corroborated by a real production row.
3. **Hand computation from published formulas** — work the arithmetic in the test's comment so a reviewer can check it. Example: the COCOMO test's `ln(100)·1.0997 → e^x → ×2.94 ≈ 465.3` derivation.
4. **Real Postgres** — for SQL semantics (`date_trunc` bucketing, `ON CONFLICT` arbiters, FK behavior), the database is the ground truth; seed known rows and assert exact values. Never assert what a query "should" return by reading its SQL.
5. **Captured real responses** — fixtures fetched from the live API and committed verbatim (trimmed, never reshaped). Example: `testdata/registries/*.json`. Pair them with a network-gated live canary so drift between the frozen fixture and the living API surfaces weekly instead of never.

Self-authored mock responses are the *weakest* form of expected value — acceptable only for shapes you control end-to-end (our own JSON envelopes), never for third-party API responses, spec formats, or SQL semantics.

**Negative controls keep ground-truth tests honest.** A test that validates against an external authority should, where practical, also prove it can still *see* the bug class it guards — e.g. the bucket-alignment test asserts the pre-fix bare `date_trunc` form actually diverges under a non-UTC session; if it stops diverging, the test complains that its own detection power died.

### REGRESSION-PIN labeling

Some tests deliberately pin **current behavior** rather than independently-derived correctness — freezing output before a refactor, pinning a quirk we've decided to keep. These are legitimate, but they must not masquerade as correctness tests. Label them:

```go
// REGRESSION-PIN: expected values captured from the v0.27.x output,
// NOT independently derived. This test detects CHANGE, not
// correctness — if it fails after an intentional behavior change,
// re-capture; if it fails unexpectedly, investigate.
```

An unlabeled test asserting exact values is implicitly claiming those values are *right*, not merely *current*. Reviewers hold it to the ground-truth rule above.

### The review question

For every new test, ask: **"Could this test pass if the code were wrong in the way that matters?"** If the expected value came from running the code, the answer is yes — and the test is a change-detector at best. Trace the expectation to one of the five authorities above, or label it REGRESSION-PIN.

## The behavioral test pattern

Tests that exercise actual code through its public API:

```go
func TestSalvageHelperBehavioral(t *testing.T) {
    tempDir := t.TempDir()
    outputPath := filepath.Join(tempDir, "scan.json")
    jsonContent := map[string]any{
        "headers": []map[string]any{{
            "tool_version": "32.5.0",
            "errors":       []string{"Path: foo/bar.pdf"},
            "extra_data":   map[string]any{"files_count": 38},
        }},
    }
    b, _ := json.Marshal(jsonContent)
    os.WriteFile(outputPath, b, 0o644)

    filesCount, headerErrors, ok := salvageScancodeOutput(outputPath)
    if !ok {
        t.Fatal("expected salvage to succeed for valid JSON")
    }
    if filesCount != 38 {
        t.Errorf("expected filesCount=38, got %d", filesCount)
    }
    if len(headerErrors) != 1 {
        t.Errorf("expected 1 header error, got %d", len(headerErrors))
    }
}
```

Behavioral tests are preferred when they're cheap. Use source-contract tests only when behavioral testing is genuinely hard.

## The integration test pattern

```go
//go:build integration  // optional, if you want a build tag

func TestUpsertCommitProtectedFromInvalidUTF8(t *testing.T) {
    dsn := os.Getenv("AVELOXIS_TEST_DB")
    if dsn == "" {
        t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
    }

    ctx := context.Background()
    logger := slog.New(slog.NewTextHandler(io.Discard, nil))
    store, err := NewPostgresStore(ctx, dsn, logger)
    if err != nil {
        t.Fatalf("connect: %v", err)
    }
    defer store.Close()

    store.SetMatviewSkip(true)
    if err := RunMigrations(ctx, store, logger); err != nil {
        t.Fatalf("migrate: %v", err)
    }

    // Seed any prerequisite rows (e.g. a parent repo for FK satisfaction).
    repoID := int64(-191919)
    if _, err := store.pool.Exec(ctx, `
        INSERT INTO aveloxis_data.repos (repo_id, platform_id, repo_git, repo_owner, repo_name, repo_archived)
        VALUES ($1, 1, 'https://example.invalid/x', 'x', 'x', FALSE)
        ON CONFLICT (repo_id) DO NOTHING`, repoID); err != nil {
        t.Fatalf("seed: %v", err)
    }
    t.Cleanup(func() {
        _, _ = store.pool.Exec(context.Background(),
            `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
        _, _ = store.pool.Exec(context.Background(),
            `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
    })

    // The actual test...
}
```

Patterns to follow:

- **Negative repo IDs** for fixture rows so they can't collide with operator-imported data.
- **`t.Cleanup`** to delete fixtures even if the test fails mid-run.
- **Pre-cleanup** at the top of tests that share fixture row IDs across runs (the test DB persists between invocations; without pre-cleanup a previous failure leaves rows that break the next run).
- **Use `store.SetMatviewSkip(true)`** unless you specifically test matview behavior. Building 20 matviews on every test run is several seconds wasted.

## What to test

### Always

- The success case (the happy path).
- At least one edge case — empty input, zero values, NULL, the boundary condition.
- Any branch in the code. If `if err != nil { ... } else { ... }`, both branches need coverage.

### Often valuable

- Idempotency: re-running the operation shouldn't change state. Especially for migrations + backfills.
- Concurrency edge cases: what happens if two collectors hit the same row?
- Error-class behavior: does the right error type bubble?
- Source-contract pins for invariants that "helpful" refactors might violate. The v0.20.18 dead-config tripwire and v0.22.13's R2-cntrb-login-preservation pin are good models.

### Skip

- Trivial getters/setters.
- Pure wiring (e.g. struct initialization in main.go) — unless the wiring is load-bearing (e.g. `TestRunMigrationsInvokesUTF8Tracer`).
- Standard library behavior (don't test `time.Now()`).

## Naming

Test names should describe what's pinned, not just what's tested:

```
TestRunOneAttemptsSalvageOnSubprocessFailure  // good — describes the contract
TestRunOne                                    // bad — too vague
TestSalvageWorks                              // bad — what does "works" mean
```

The maintainers use long test names. They show up in `go test -v` output and serve as documentation.

## Run patterns

```bash
# Everything (unit only, since AVELOXIS_TEST_DB not set)
go test ./...

# One package, verbose
go test ./internal/db/ -v

# Single test
go test ./internal/collector/ -run TestSalvageHelperBehavioral -v

# Pattern match
go test ./internal/db/ -run "TestUTF8Tracer.*" -v

# Race detector (run periodically; slows tests ~2x)
go test ./... -race

# Force re-run (skip caching)
go test ./... -count=1

# Integration tier
AVELOXIS_TEST_DB="postgres://..." go test ./internal/db/ -v -timeout 120s
```

## What good test failures look like

When a test fails, the failure message should:

1. Say what was expected vs what was observed.
2. Explain WHY this matters (the load-bearing invariant being violated).
3. Point at the relevant release note or production incident if applicable.

```go
if !strings.Contains(code, "ON UPDATE CASCADE") {
    t.Error("RunMigrations must add ON UPDATE CASCADE to every " +
        "cntrb_id child FK. Without this, the v0.22.2 cntrb_id " +
        "data migration can't propagate UPDATEs to child rows. " +
        "See the v0.22.1 release notes.")
}
```

A future contributor seeing this fail will understand both what to fix and why. Compare to a bare `t.Error("missing CASCADE")` which leaves them spelunking.

## Writing tests for code that depends on time

Use `time.Now()` directly in production code; in tests, either:

- Use real durations and accept ~10 ms tolerance: `if elapsed > 100*time.Millisecond { ... }`.
- Inject a clock function: `clock func() time.Time` field on the struct.

Aveloxis tends to use the first pattern — most time-sensitive code is at second / minute granularity, so millisecond drift in tests doesn't matter.

## Writing tests for goroutines

If your test spawns goroutines, use `sync.WaitGroup` or buffered channels to coordinate. Never use `time.Sleep` to wait for a goroutine — it's flaky.

For watchdog / ticker-style code, accept an injectable ticker duration:

```go
type LongJobsWatchdog struct {
    interval time.Duration
    // ...
}

// In tests: use 10ms; in production: 30s.
```

## Writing tests for HTTP clients

`httptest.NewServer` for full mock servers. For finer control, implement `http.RoundTripper` and inject it.

```go
type mockTransport struct {
    handler http.HandlerFunc
}
func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
    rec := httptest.NewRecorder()
    m.handler(rec, req)
    return rec.Result(), nil
}
```

See `internal/platform/httpclient_test.go` for patterns. Tests like `TestHTTPClientTreats500AsTransient` use httptest to verify retry behavior without hitting GitHub.

## CI

GitHub Actions runs:

- `test.yml`: `go test ./...` (with `-race`) on every push.
- `integration.yml`: PostgreSQL service container + `AVELOXIS_TEST_DB` integration tests.
- `lint.yml`: `go vet` → `staticcheck` → `golangci-lint` (pinned version), all blocking.
- `codeql.yml`: security scanning.
- `cifuzz.yml`: ClusterFuzzLite over the 7 native Go fuzz targets.
- `network-canary.yml`: weekly live-API contract checks (`AVELOXIS_TEST_NETWORK=1`).
- `docs.yml`: `sphinx-build -W --keep-going` over `docs/` — every Sphinx/MyST warning (unknown fence language, block that does not lex, dead cross-reference) is an error.

The first four plus `docs.yml` must pass for a PR to merge — a Sphinx warning fails the docs job the same way a lint finding fails `lint.yml`. The maintainers can override but rarely do.
