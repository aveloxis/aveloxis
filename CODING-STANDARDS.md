# Go Coding Standards

This document is the reference for code review in this repository. Reviewers (human or Claude Code) cite rules by ID (for example `ERR-3`, `SR-5`, `L11`) when flagging an issue, and flag only what the rules or a concrete defect justify. Style that `gofmt` and the linters already enforce is not worth a review comment.

It works together with, and does not replace:

- the **standing rules** SR-1 … SR-20 (`CLAUDE.md` "Standing rules"; machine-readable in `scripts/standing_rules.go`, each with its enforcing test);
- the **review lenses** L1 … L17 (`docs/contributing/review-lenses.md`) and the **review protocol** (`docs/contributing/review-pass-brief.md`);
- the contributor handbook in `docs/contributing/` (code conventions, testing, schema migrations, adding a platform, phase, endpoint or visualization).

When this document and one of those disagree, the more specific project rule wins, and the disagreement is itself a finding against this document.

Rules marked **(enforced: …)** are checked mechanically; a reviewer only needs to flag them when the check was bypassed or cannot see the case. Rules marked **(house)** record a decision this project made on purpose, often against a generic Go convention; they cite why.

## Severity and disposition

Every finding gets exactly one severity. Severity orders the report; it does not decide whether a finding is fixed.

| Level | Meaning |
|---|---|
| **BLOCKER** | Correctness bug, data loss or corruption, security hole, race, resource leak, panic on a reachable path, a migration unsafe on an existing fleet, a standing rule (SR-n) broken |
| **MAJOR** | A rule below broken in a way that costs real maintenance or operations (a swallowed error, a lost cancellation, a test that passes with the fix reverted) |
| **MINOR** | A clear improvement, low risk |
| **NIT** | Taste. Keep these rare; batch them in one comment |

**Disposition (house; `review-pass-brief.md`).** A finding is reported only after it is **verified against the code** (read the file, run the test, or reproduce it). Every verified finding, whatever its severity, is then either:

- **taken**: fixed red-first (a test that fails for the right reason, then the fix), with a class sweep of the sibling sites (L11), and mutation-proven where the fix is behavioural; or
- **declined**: with the reason written at the site (a comment) and in the release ledger.

"Deferred" means a worklist entry with the reason, not an unrecorded promise. A fix is new code and gets the same review (L10): the loop repeats until a fresh-context round reports **zero verified findings**. A change reported without its findings table (taken / declined, with reasons) is not finished.

---

## 1. Tooling baseline (TOOL)

- **TOOL-1** Code passes `gofmt`, and imports are grouped standard library first. (enforced: `gofmt -l` in the gates; `TestImportGroupsKeepStdlibFirst`; SR-15's ASCII-quote tripwires `TestNoCurlyQuotesInGoSources` / `TestTypographySurvivesGofmt`.) `goimports` is recommended on save; CI does not yet check it.
- **TOOL-2** `go vet ./...`, `staticcheck ./...` and `golangci-lint` at the CI-pinned version are clean. `.golangci.yml` is authoritative, including its documented exclusions. A `//nolint` names its linter and carries a trailing reason: `//nolint:gosec // path is validated by resolveRepoPath`. Run the CI-version binary; bare staticcheck differs from golangci-lint's (ST1000).
- **TOOL-3** Tests run with `-race` in CI (`test.yml`, and `integration.yml` with `-shuffle=on`). A PR that introduces a data race is a BLOCKER. Cost tests (`*CostIsLinear`, `*LookupsAreLogarithmic`) skip under the race detector and run in the non-race cost step; `scripts/cost_tests_scheduled_test.go` enforces the scheduling.
- **TOOL-4** A newly reachable known vulnerability is a BLOCKER. Run `govulncheck ./...` before a release. (Not yet a CI job; adding one is recommended.)
- **TOOL-5** The `go` directive in `go.mod` defines the language version. Use features available at that version (`iter`, range-over-func, `strings.SplitSeq`, `context.WithoutCancel`); do not write shims for older versions.

## 2. Package layout and structure (PKG)

- **PKG-1** (house) `cmd/aveloxis/main.go` wires the root command. `cmd/aveloxis/*_cmd.go` hold one cobra command each (`runX` handler, `newXCommand` constructor, a registration pin), operator-facing output, and the process-wiring builders (`mailerConfigFrom`, `newWebServer`, `apiOptions`, `wireDigestMailer`, pinned by `TestProcessMailWiring`). Logic moves to `internal/` when a second caller needs it or when it is not about the command line. New business logic in `cmd/` is a MAJOR; existing files are not findings.
- **PKG-2** Everything is under `internal/`. Only put code in a public package when an external consumer actually exists.
- **PKG-3** Package names are short, lowercase, no underscores, named for what they provide. No `util`, `common`, `helpers` or `misc` packages.
- **PKG-4** No import cycles worked around with interface packages whose only purpose is breaking the cycle. Restructure instead.
- **PKG-5** (house) No package-level mutable state, except: flags registered in `main`; `sync.Once`-guarded lazy init; compiled regexps and constant lookup tables, including tables filled once from embedded data; and **test seams**: a package-level function or value variable that tests replace (`sendSignal`, `restTransportRetrySleep`, `graphqlRetrySleep`, `registrySleep`, `toolFetchClient`), provided a test pins its production default. Global DB handles, loggers or HTTP clients used as dependencies are a MAJOR; pass them in.
- **PKG-6** No `init()` with side effects (network, filesystem, environment reads). Parsing embedded data is acceptable (`internal/spdx/data.go`).

## 3. Naming (NAME)

- **NAME-1** (house) Go conventions: `MixedCaps`; initialisms fully capitalized in new code (`repoID`, `HTTPClient`, `URL`, `SQL`); no `Get` prefix on field accessors (`Owner()`, not `GetOwner()`). **Exempt:** store and query methods that fetch from the database or a forge (`GetRepoByID` and its many siblings); that is the established store vocabulary, and ST1003 stays off in `.golangci.yml`. Existing identifiers are never renamed for style alone: source pins read many of them as strings.
- **NAME-2** Don't stutter in new code: `repo.Info`, not `repo.RepoInfo`. The same-name form (`config.Config`, `scheduler.Scheduler`) is the Go convention, not stutter. Existing names are not findings.
- **NAME-3** Short names for short scopes (`i`, `r`, `ctx`); descriptive names for package-level identifiers and long-lived variables.
- **NAME-4** Receiver names are one or two letters, consistent across a type's methods, never `this` or `self`. (enforced: staticcheck ST1006/ST1016.)
- **NAME-5** Booleans read as predicates: `isFork`, `hasLicense`, `skipArchived`.

## 4. Errors (ERR)

- **ERR-1** Everything that errors is logged or returned; nothing is discarded silently. In tests, fixture setup goes through the retrying helpers (`mustExecRetry`, `cleanupExecRetry`) or checks the error; an ignored fixture error turns a setup failure into a misleading assertion failure. `_ = f()` needs a comment saying why the error cannot matter. An unchecked error on a write, commit, close-of-writer or network call is a BLOCKER. (partly enforced: errcheck with the documented exclusions in `.golangci.yml`; `TestRunMigrationsNoBareExecWithoutCheck` and other site pins; `TestNoDiscardedWriteErrors` for `_ =` on store writes, `Exec` and `os.WriteFile`; `TestWritableFileClosesAreChecked` for the Close of a file opened for writing.)
- **ERR-2** Wrap with context at a meaningful boundary: `fmt.Errorf("fetch contributors for %s: %w", repo, err)`. Error strings are lowercase, without trailing punctuation or a "failed to" prefix. (Log **messages** may say "failed to …"; see LOG-3.)
- **ERR-3** Use `%w` when callers may inspect the cause and `%v` when hiding a detail on purpose. Compare with `errors.Is` / `errors.As`; never `==` on a wrapped error and never string-match `err.Error()` (`TestNoDecisionsOnErrorText` fails on `strings.Contains`/`HasPrefix`/`HasSuffix`/`EqualFold` over an `.Error()` value). A new error shape wraps a sentinel (`ErrTransient`, `ErrNotFound`, `ErrGone`, `ErrResourceLimits`, …) or classifies through `platform.ClassifyError`, or every `Class*` gate downstream is decorative.
- **ERR-4** (house) Log at the boundary that decides what to do with the error. A layer that logs **and** returns must add something the caller lacks (the repository, the phase, the counts), not repeat the same line; one failure, one line. Classify shutdown first: a `context.Canceled` (or `ctx.Err() != nil`) is returned without an error-level log. Logging and then continuing as if the operation succeeded is a BLOCKER.
- **ERR-5** Sentinels are package-level `var ErrX = errors.New(...)`; export only those callers branch on. A typed "not found" is the only thing that means "absent" (SR-5); see §19.
- **ERR-6** Aggregate independent failures with `errors.Join` (the fail-closed migrate collector is the model), not string concatenation.
- **ERR-7** `panic` only for programmer errors that make continuing meaningless (an impossible state, a violated invariant, a misused test seam). Never on bad input, network or DB failure. Library code does not call `log.Fatal` or `os.Exit`.
- **ERR-8** A goroutine that can panic on external data recovers at its boundary through `internal/safego` (`safego.Go`, `safego.Recover`). (enforced at the audited sites: `TestSafegoAdoptionAtAuditedSites`.) The scheduler's run loop and the HTTP listeners are deliberately bare (documented in `safego.go`).

## 5. Context and cancellation (CTX)

- **CTX-1** A function that does I/O, blocks or may run long takes `ctx context.Context` as its first parameter. A missing context on a DB or HTTP call is a MAJOR. Subprocesses use `exec.CommandContext` (see §21).
- **CTX-2** Never store a `Context` in a struct; never pass `nil`. `context.TODO()` only as a marked temporary.
- **CTX-3** (house) `context.Background()` appears in `main`, tests, and top-level job entry points. Mid-chain, a fresh context that discards cancellation is a MAJOR, **except** release, unlock, stamp or cleanup work on a shutdown or cancel path, which must survive the cancellation (L6: does the unlock run on a dead context; L16: a child of a cancelled ctx is born expired). There, prefer `context.WithoutCancel(ctx)` bounded by `WithTimeout`, or `context.Background()` with a timeout, with a comment saying why.
- **CTX-4** Every `WithTimeout` / `WithCancel` is followed by `defer cancel()` or cancels on every path. A missing cancel is a BLOCKER. (enforced: govet lostcancel.)
- **CTX-5** Long loops over external work check `ctx.Err()` (or `ctx.Done()`) each iteration, and an interrupted walk reports itself as interrupted and exits non-zero (L14).
- **CTX-6** (house) Context values carry request-scoped metadata (auth identity, trace IDs) and **per-call policy flags** on typed, unexported keys set through `With*` constructors: `platform.WithoutETag`, `WithGraphQLFastFail`, `WithGraphQLBackgroundBudget`, which the forge contracts require (§20). Never pass dependencies (stores, loggers, clients) through a context in new code.

## 6. Concurrency (CONC)

- **CONC-1** Every goroutine has a clear owner and exit condition; a reviewer can answer "what stops this goroutine?" from the code. Fire-and-forget goroutines are a MAJOR unless they run through `safego` and end with their unit of work.
- **CONC-2** Prefer structured concurrency. `errgroup.Group` (with `SetLimit`) is welcome where it fits; `golang.org/x/sync` is already in the module graph as an indirect dependency, so promoting it to direct downloads nothing but still gets a DEP-1 line. A `sync.WaitGroup` with a documented error path is acceptable.
- **CONC-3** Bound concurrency against external systems: forge keys through the one `KeyPool` (SR-20), the DB pool sized from demand (`scheduler.PoolDemand`), worker slots, scancode and scorecard caps. An unbounded `go f()` in a loop over input data is a BLOCKER.
- **CONC-4** The sender closes a channel; receivers never close. Closing twice or sending on a possibly closed channel is a BLOCKER.
- **CONC-5** A mutex sits immediately above the fields it guards, with a comment naming them. Never copy a struct holding a `sync.Mutex`. (enforced: govet copylocks.) A process-wide setting saved and restored around work (the collector's `SetGCPercent`) is serialized.
- **CONC-6** Use typed atomics (`atomic.Int64`, `atomic.Bool`), not the function-style API on raw integers.
- **CONC-7** (house) `time.Sleep` never synchronizes with a goroutine. Production waits are ctx-aware and go through an injectable seam where tests need to control them (the `*RetrySleep` pattern). Tests of time-bound behaviour (TTL expiry, mid-flight cancellation, pacing) may use real durations with a stated tolerance or a clock seam (`docs/contributing/testing.md`). A test that depends on scheduling luck is flaky, and flaky is a finding.
- **CONC-8** Unbuffered by default. A buffered channel states its bound or reason when not obvious.

## 7. Types, interfaces and API design (API)

- **API-1** (house) Accept interfaces, return concrete types; define new interfaces in the consuming package, sized to what it uses (a narrow store interface for a testable subsystem is the model). **Exempt:** `platform.Client`, the documented contract every forge implements (`docs/contributing/adding-a-platform.md`), and the concrete `*db.PostgresStore` boundary.
- **API-2** Keep new interfaces small; one with more than about five methods needs a reason. `platform.Client` is composed of sub-interfaces by design.
- **API-3** Make the zero value useful where practical (the matview and supply-chain modes' zero values are "off"); otherwise provide a constructor and do not export fields that must be set together.
- **API-4** Functional options or a config struct for constructors with more than about three optional parameters. No long positional lists of the same type (`f(string, string, string, bool)`).
- **API-5** Avoid `any` in signatures when a concrete type or generics would work. Generics are for real type-parameterized logic.
- **API-6** Pointer vs value receivers are consistent per type; pointer receivers if any method mutates or the struct is large or holds a mutex.
- **API-7** Slices and maps retained by a struct are copied at the boundary when the caller could mutate them afterward, and vice versa for getters.
- **API-8** Enums are typed constants with an explicit unknown or zero value and a `String()` method when logged. A count the forge did not report is **unknown**, never a fabricated 0 (GitLab counts carry an unknown arm; the GUI never shows "0" where the honest state is "pending").

## 8. Resource management (RES)

- **RES-1** Every acquired resource is released on every path: `defer resp.Body.Close()`, `defer rows.Close()`, `defer tx.Rollback(ctx)`, `t.Cleanup(store.Close)` in tests (SR-9). A leak on an error path is a BLOCKER.
- **RES-2** Errors from `Close` on writable resources whose data matters (files written, gzip or buffered writers flushed) are checked, typically through a named return and a deferred closure. `.golangci.yml` excludes `Close` from errcheck broadly; this rule is the reviewer's job where a close really matters.
- **RES-3** Don't `defer` inside a loop over an unbounded number of resources; extract the body into a function.
- **RES-4** Tickers are stopped. `time.After` inside a `select` loop no longer leaks (since Go 1.23 unreferenced timers are collected), so it is not a finding by itself; flag it only when the loop is hot enough for the allocation to matter or when a timer must be reset rather than recreated.
- **RES-5** Reading untrusted input into memory is bounded (`io.LimitReader`, `http.MaxBytesReader`, the bounded stderr capture for subprocesses). A new unbounded `io.ReadAll` on a network response is a MAJOR. Existing unbounded sites are a recorded follow-up, not new-code findings.

## 9. Logging and observability (LOG)

- **LOG-1** `log/slog` with structured attributes. No `fmt.Print*` or `log.Print*` outside `cmd/` and `scripts/`. (Known exception to retire: `internal/collector/tools.go` prints install output for `install-tools`.)
- **LOG-2** The logger is injected, not a package global.
- **LOG-3** (house) The message is a constant, lowercase verb-noun phrase (`"collection complete"`, `"failed to upsert commit"`); never interpolate variables into it. Attribute keys follow the house vocabulary:
  - errors: `"error", err`, always the **last** attribute (not `err`);
  - the repository: `repo_id` (plus `owner`/`repo` where useful); URLs under keys ending in `url` or `_git`, which the redaction tripwire checks (`scripts/url_log_redaction_test.go`);
  - durations: a `time.Duration` value under `"duration"` (or `"elapsed"`, `"wait"`, `"bound"` where more specific). (The `duration_seconds` example in `code-conventions.md` predates this and should be aligned.)
- **LOG-4** Levels: `Debug` for noisy per-row detail; `Info` for per-cycle and per-repository lifecycle events; `Warn` for degraded but recoverable; `Error` for data integrity at risk or operator action needed. Per-item logging inside a loop over thousands of items is aggregated into one line per batch.
- **LOG-5** Never log secrets, tokens, auth headers, connection strings with passwords, or URLs carrying credentials (redacted through `platform.RedactURLUserinfo`; enforced by the URL redaction tripwire). User-derived strings pass through `logSafe`/`truncateForLog`. Contributor email addresses are collected data but are not logged at `Warn` or above without a reason.
- **LOG-7** (house) On request paths (api, web, monitor), a failure is logged through `httpserver.LogFailure(ctx, logger, level, err, …)` or behind `httpserver.RequestEnded(r.Context(), err)`: the request's own end — decided by the request's **context**, never by the error's type — is Debug, and the same error on a live request keeps its level (enforced: `TestRequestHandlersLogThroughLogFailure`, `TestNoEndpointBlamesTheRequestsEnd`). Classifying by `errors.Is(err, context.DeadlineExceeded)` read a database connect timeout as "the client left" (SR-5).
- **LOG-6** (house) Log the **effective** value a component uses, after any clamp or default, at the point of use (SR-10). Shutdown is not a failure: a cancellation is classified and returned without an error-level log (the shutdown classification ratchet, `scripts/shutdown_classification_baseline.txt`, is empty and shrink-only).

## 10. Configuration and secrets (CFG)

- **CFG-1** Configuration is loaded once into a typed struct (`aveloxis.json`) and validated; an invalid file is fatal, a missing one falls back to defaults only for `config.ErrNotFound`. Scattered `os.Getenv` calls deep in the code are a MAJOR (existing: `tools.go` reads `SHELL`/`GOBIN`; the test harness).
- **CFG-2** Secrets come from the gitignored config file, the `worker_oauth` table or the environment, never from source. A committed credential is a BLOCKER and must be rotated, not just removed.
- **CFG-3** Safe defaults: timeouts set, TLS verification on, cookies `Secure` unless `web.dev_mode`, a forge token only lent to an `https` base URL.
- **CFG-4** (house) Every config knob is tested **end to end** (the JSON value to the behaviour it controls), has exactly one default layer, appears in `docs/getting-started/configuration.md` and every example JSON (tripwired), and dead config is removed with a negative tripwire, not deprecated (SR-10).

## 11. Database / PostgreSQL (DB)

- **DB-1** All queries are parameterized (`$1, $2`). SQL built from any non-constant value is a BLOCKER. Dynamic identifiers go through `pgx.Identifier{...}.Sanitize()` or an allowlist (sort keys through `collectionRepoSorts`/`queueSorts`).
- **DB-2** `defer rows.Close()` and check `rows.Err()` after iteration; a missing `rows.Err()` is a MAJOR (a mid-stream failure returns a truncated result as success). No linter checks this for pgx; `TestEveryRowsLoopChecksErr` (scripts/) fails when a function iterates `x.Next()` without calling `x.Err()`.
- **DB-3** Transactions: `Begin`, `defer tx.Rollback(ctx)` immediately (a no-op after commit), `Commit` with its error checked. Deadlock victims (40P01) retry through the shared `withRetry` / `retryOnDeadlock`, never a hand-rolled loop.
- **DB-4** Bulk writes are batched `INSERT … ON CONFLICT` (or `COPY`), not per-row round trips. Bulk backfills walk **keyset windows** over the primary key (`runKeysetWindows`), never `LIMIT N` re-scans.
- **DB-5** (enforced: SR-14 `TestOnConflictClausesHaveRealArbiters`, `TestAllDataInsertTablesHaveOnConflict`.) Every INSERT into a data table has an `ON CONFLICT` with a real arbiter (a unique constraint or index that exists); a bare `DO NOTHING` with no unique silently duplicates. A `DO UPDATE` guards unchanged rows (`WHERE … IS DISTINCT FROM EXCLUDED …`) where a rewrite would churn a hot table. Every protected column has one registered write policy (SR-11) and every column a writer (SR-13).
- **DB-6** One `pgxpool` per process, sized from demand and capped by the server budget (or `database.pool_max_conns`). Never open connections per request or job.
- **DB-7** (house) Schema changes go through `schema.sql` plus idempotent steps in `internal/db/migrate.go`, re-run by every `aveloxis migrate`; one-shot backfills are ledgered (`runOnce`/`runOnceStep`). There are no versioned files and no down paths. The rules that make this safe: SR-1 (dedup before a unique index), SR-2 (new fleet-scale indexes are migration-owned and built `CONCURRENTLY`), SR-4 (dropped names are never reused), SR-8 (an `ALTER … ADD COLUMN IF NOT EXISTS` guard ahead of a same-release index); test every new migration on an **empty** database; and every operator-run step gets a row in `docs/getting-started/upgrading.md` and the release's deploy checklist. A migration that rewrites a large table or takes an `ACCESS EXCLUSIVE` lock on a hot table says so in the PR and the checklist.
- **DB-8** New queries on large tables come with the index they rely on, or a note that an existing index covers them; ask for `EXPLAIN (ANALYZE, BUFFERS)` when a query filters or joins on an unindexed column. Every foreign key is `DEFERRABLE INITIALLY DEFERRED` and its child column is indexed.
- **DB-9** Map `NULL` deliberately: pointers or `pgtype` for nullable columns (scanning NULL into a non-pointer is a BLOCKER); write a zero `time.Time` as NULL through `db.NullTime()`.
- **DB-10** Timestamps are `timestamptz` and `time.Time`. Exempt: the Augur/8Knot compatibility views, which cast to the types those consumers expect.
- **DB-11** (house) SQL lives in backtick raw-string literals in Go (a `const` or inline), formatted readably, so `internal/srctest/sqlscan` and the SR-11/13/14 tripwires can read it; never concatenated from fragments (a concatenated statement is invisible to those checks). `//go:embed` is for the schema, matview and view files only.
- **DB-12** (house) Reserved words are not column names (`vuln_references`, not `references`). `aveloxis web` and `aveloxis api` never migrate; only `serve` and `migrate` do.

## 12. HTTP clients, servers and external APIs (NET)

- **NET-1** Never `http.DefaultClient` or `http.Get`. Every client has an explicit `Timeout` and a tuned `Transport`, shared per upstream. Forge requests go through the platform HTTP client; registry fetches through the shared registry request path (per-host pacing, bounded `Retry-After`, answer cache).
- **NET-2** Always close response bodies; drain them (`io.Copy(io.Discard, resp.Body)`) before closing when connection reuse matters.
- **NET-3** Check status codes explicitly; a non-2xx is not an error from `Do`. Classify definitive answers through the shared rules (`platform.IsRepoGoneStatus` for 404/410/451; `platform.IsDefinitiveAnswer`); treating a non-2xx as success is a BLOCKER.
- **NET-4** (house) Retries use exponential backoff with jitter and a maximum attempt count. Retryable: 429, 5xx, connection resets and truncated bodies, GitHub's in-body GraphQL execution timeout, and GitHub rate-limit 403s, which the `KeyPool` handles (§20: a refusal benches the key and rotates; a secondary limit rests the key and paces). Definitive, never retried: 400, 404, 410, 422, 451. Read-only GraphQL POSTs count as idempotent. Any other 4xx retry is a MAJOR.
- **NET-5** Pagination has a termination guarantee beyond "no next link" (`ErrPaginationLimitExceeded`, cycle detection); incremental walks compare `Before(since)`, never `!After`.
- **NET-6** HTTP servers are built by `internal/httpserver` (every timeout from `http_timeout_seconds`, default 180 s; enforced: `TestHTTPServersComeFromHTTPServer`), and shut down through `Server.Shutdown(ctx)` with a deadline on SIGTERM. (house) Aveloxis's bound is a backstop, never the shortest in the chain: latency is tuned in nginx, below it.

## 13. Testing (TEST)

- **TEST-1** (house) New behaviour comes with tests, written **red first**: the test fails for the right reason before the fix. A behavioural fix is **mutation-proven**: in a scratch copy, break the fix and watch the test fail. The proof counts only if the baseline is green, the mutant compiles and the mutation verifiably landed. A test that would pass with the fix reverted is a MAJOR.
- **TEST-2** Table-driven tests with `t.Run(tc.name, ...)` for multiple cases. Test names describe the contract or incident, not the function.
- **TEST-3** Standard `testing`; failure messages show got and want, and say why the expectation holds when it is not obvious. `go-cmp` is not a dependency; adding it is DEP-1.
- **TEST-4** (house) `t.Helper()` in helpers, `t.TempDir()` for files, `t.Cleanup()` for teardown, and `t.Cleanup(store.Close)` rather than `defer` for pools (SR-9: defers run before cleanups). `t.Context()` is fine in a test body but **never inside a `t.Cleanup` callback**, where it is already cancelled; use `context.Background()` there.
- **TEST-5** (house) Unit tests are hermetic. The **DB tier** is gated by the `AVELOXIS_TEST_DB` environment variable (`t.Skip` when unset), and each package runs in its own fresh database through `testdb.Main` (registry-enforced); a test never relies on rows another test, package or run left. It runs locally against a scratch base database as well as in CI, and a closing suite run exports the variable (a skip-only run reads green). Live-API canaries are gated by `AVELOXIS_TEST_NETWORK=1`; external validators by `AVELOXIS_TEST_SBOM_TOOLS`. Real-time tests follow CONC-7.
- **TEST-6** Fake external systems with small hand-written fakes or `httptest.Server`; no mocking frameworks. **A fake honours the boundary it stands in for** (limits, pagination, error semantics), or per-batch assertions are vacuous. Every mocked forge or registry path is paired with a network-gated live canary.
- **TEST-7** Concurrency is tested under `-race` without depending on sleeps (CONC-7). Never signal a real PID in a test; go through the `sendSignal` seam.
- **TEST-8** Parsers of untrusted input (manifests, lockfiles, API payloads, git data, config) get a fuzz target when practical (ClusterFuzzLite, `cifuzz.yml`).
- **TEST-9** (house) Source-contract pins (a test that reads source) pin **behaviour, not shape**: strip comments before matching (`srctest.StripGoComments`), assert control-flow structure rather than token presence, scope by semantics, and guard the denominator (count the sites examined, not only the violations). Use the shared helpers (`internal/srctest`, `sqlscan`; SR-12). A pin escaped two or three times is at the wrong level: add a runtime test plus a wiring pin.
- **TEST-10** (house) Every config knob and every "rerun until done" contract has an end-to-end or convergence test (SR-10, SR-19).

## 14. Performance (PERF)

- **PERF-1** Correctness and clarity first. Performance-motivated complexity comes with a measurement: a benchmark, or a cost test (`internal/costtest`: input `n` against `4n`, at most `8×` the time) when the property is "grows linearly". Pin the optimization, or a later change silently undoes it (L17).
- **PERF-2** (house) Preallocate when the size is known **and trusted**. A capacity derived from request or other untrusted input is a constant or is clamped first (the CodeQL allocation-size rule).
- **PERF-3** `strings.Builder` for incremental string building in loops.
- **PERF-4** Don't hold a lock across I/O; a forge key's lease covers exactly the wire request and is released before any `Retry-After` sleep (SR-20).
- **PERF-5** Stream large result sets; never run live aggregates per request on listing surfaces (read the cached counts); page sizes clamp in the store.

## 15. Security (SEC)

- **SEC-1** Treat all external data (API payloads, repository contents, file paths, commit metadata, pasted URLs) as untrusted; validate at the boundary. A URL carrying credentials is refused at every writer and subprocess boundary.
- **SEC-2** File paths from input are cleaned and confined to an intended root (`os.Root` or equivalent `..` checks). Path traversal is a BLOCKER.
- **SEC-3** No `exec.Command("sh", "-c", ...)` with interpolated input; arguments are separate strings, and git invocations put `--` before a URL, ref or path (`validateGitURL` refuses flag-like and `file://` URLs).
- **SEC-4** `crypto/rand` for anything security-relevant; `math/rand/v2` only for jitter and sampling.
- **SEC-5** No `InsecureSkipVerify` outside tests.
- **SEC-6** (house) Web and API authorization: portal endpoints require a user; `/admin/*` requires an admin; `publicPaths` is an exact-match allowlist; cookies are `HttpOnly` always; `oauth_next` honours only relative paths or the configured SPA origin; an OAuth account is matched by the forge's numeric user ID (and, for GitLab, its instance), never by login name alone (SR-6); responses never echo store or upstream error text.

## 16. Dependencies (DEP)

- **DEP-1** Prefer the standard library. A new direct dependency needs a one-line justification in the PR: what it provides, maintenance health, licence compatibility.
- **DEP-2** `go.mod` and `go.sum` changes are reviewed, not rubber-stamped; unexplained major bumps or new transitive trees are a MAJOR.
- **DEP-3** (house) The root module is tidy (`go mod tidy -diff` clean). Never run `go mod tidy` in `testdata/` modules (it damaged a parser fixture once), and never let a tool run it unreviewed.

## 17. Comments and documentation (DOC)

- **DOC-1** Every exported identifier in **new** code has a doc comment starting with its name, attached to it (no blank line in between; `scripts/godoc_attribution_test.go`, shrink-only baseline).
- **DOC-2** Every package has a package comment stating its purpose and entry points (enforced: staticcheck ST1000). A `doc.go` is optional.
- **DOC-3** Comments explain why, invariants and non-obvious constraints (rate limits, API quirks, ordering, the incident a rule came from). Prose describing a mechanism is kept true when the mechanism changes (L12); a comment that contradicts the code is a MAJOR.
- **DOC-4** (house) No bare `TODO`: deferred work goes to the worklist with its reason, and a code comment may point at it.
- **DOC-5** Commented-out code is deleted.
- **DOC-6** (house) Public documentation lives in `docs/` (Read the Docs) and is built with Sphinx `-W`; private notes live outside it and are never linked from `docs/`. Edit sources, never `docs/_build`. Every user-visible change updates the docs in the same change, and every feature documents any GitLab gap.

## 18. General style (STY)

- **STY-1** Handle errors and edge cases early and return; keep the happy path at the left margin; no `else` after `return`.
- **STY-2** Functions do one thing. A new function over about 80 lines or nested more than three levels is examined for extraction; a MINOR unless it hides a bug.
- **STY-3** No naked returns in functions longer than a few lines.
- **STY-4** (house) No magic numbers: name constants with units, and **derive** every threshold from its constraint (write the derivation next to it) or ask. A recommended number carries its denominator (the `work_mem` formula, not "64 MB").
- **STY-5** (house) Durations are `time.Duration` inside Go. JSON config keys are unit-suffixed integers (`days_until_recollect`, `scancode_start_interval_s`, `supply_chain_refresh_hours`); the key set is frozen and tripwired. The accessor converts once (CFG-4) and the effective value is logged (LOG-6).
- **STY-6** Dead code, unused parameters and unused symbols are removed (enforced in part: `unused`). Remove, don't deprecate.
- **STY-7** (house) Every Go file starts with the two-line SPDX header (tripwired). ASCII quotes only in Go sources (SR-15). No emojis in code, comments, commits or docs.

---

## 19. Data integrity and identity (SR)

The standing rules, restated for reviewers. Each has an enforcing test in `scripts/standing_rules.go`. Breaking one is a BLOCKER.

- **SR-3** A progress, resume or completion marker is stamped only over rows proven written; `last_collected` advances only on success, from the job's start time.
- **SR-5 / SR-16** A lookup or probe **error** is not "no": only the typed not-found sentinel means absent, and every yes/no probe has an error arm distinguishing a definitive negative from a transport, rate-limit or 5xx failure (L1).
- **SR-6** Never fabricate identity: link contributors and accounts by platform numeric IDs or unambiguous matches only; an ambiguous match stays NULL.
- **SR-7** Watchdogs and detectors are observation-only: they log and dump, and never kill, requeue or mutate without operator approval.
- **SR-17** A key or normalizer shared across subsystems goes through one named function; a second inline spelling is a defect (L2, L3).
- **SR-18** The owning layer enforces an invariant; a wrong caller cannot succeed (L4).
- **SR-19** A "rerun until done" contract has a test that drives it to convergence (L5).

(SR-1, SR-2, SR-4, SR-8, SR-11, SR-13 and SR-14 are in §11; SR-9 in §13; SR-10 in §10; SR-12 in §13; SR-15 in §18; SR-20 in §20.)

## 20. Forge access (FORGE)

- **FORGE-1** `KeyPool.Acquire(ctx, resource)` is the only path to a forge key; the lease covers exactly the wire request and is released before any sleep (enforced: `TestKeyPoolIsTheOnlyPathToAForgeKey`).
- **FORGE-2** One shared rate-limit budget per platform; a refusal (403/429 with `Remaining: 0`) benches the key for every collector until its reset and the caller rotates to another key without spending a retry; a secondary limit (`Retry-After`) rests the key and paces (SR-20).
- **FORGE-3** Errors classify through `platform.ClassifyError`; `ETag` revalidation only in `paginate`, never for single-object or per-item reads (`WithoutETag`); `ForgetRepoETags` on every failed job.
- **FORGE-4** A new background GraphQL sweep sets both context flags (`WithGraphQLBackgroundBudget`, and `WithGraphQLFastFail` where subdivision is the retry); `RESOURCE_LIMITS_EXCEEDED` and persistent 5xx mean "subdivide", and only the explicit marker is lossy at the 48-hour floor.
- **FORGE-5** Every feature works for GitHub **and** GitLab where the API allows; where it cannot, the gap is documented in the README and `docs/`.

## 21. Processes, shutdown and background work (PROC)

- **PROC-1** Subprocesses start through the shared helpers (`groupKilled`, `startSweptCommand`, `runToolCommand`): their own process group, killed as a group on cancel or stop, a wall-clock timeout, bounded output capture, errors routed through `execErr`.
- **PROC-2** Shutdown is not a failure: every ctx-bound failure log classifies cancellation first (LOG-6). An interrupted operator command reports what it did and exits non-zero (L14).
- **PROC-3** Global per-job work (affiliations, enrichment, backfills) runs on a periodic single-flight ticker, never inside every job. A retry path over a claim pool has a failure counter, backoff and a strike sideline, or a failing cohort dominates the pool.
- **PROC-4** Two-process races are considered: two serves, a serve and a migrate, a serve and a heal (L17). One serve per database is the deployment model.

---

## Known pre-existing gaps (measured 2026-09-28)

A conformance scan of the tree against this document found these. They are recorded work, not findings against an unrelated change; a change that touches one of these sites fixes it (L11).

- Closed in 0.29.70 (old problems O1–O4): the DB-2 sites, the ERR-1 store writes and writer closes, and the ERR-3 error-text match are fixed, and each class now has a ratchet (see DB-2, ERR-1 and ERR-3 above).
- **ERR-8**: bare goroutines on external data in `internal/platform/github/contributor_history.go` (per-window parsing), `internal/collector/swept_command.go`, `internal/db/migrate.go` (`watchBlockers`).
- **RES-5**: about two dozen unbounded `io.ReadAll(resp.Body)` sites (the platform HTTP client and GraphQL paths, OAuth callbacks, registry and OSV fetches, Pony Mail, importers); bounding `handleResponse` and `GraphQLAt` covers about half.
- **LOG-1**: `internal/collector/tools.go` prints install output with `fmt`, which lands in `aveloxis.log` when serve runs the monthly tool check.
- **LOG-3**: a handful of concatenated or non-constant log messages (`internal/db/migrate.go`, `internal/platform/ratelimit.go`).
- **ERR-2 / STY-3 / NAME-1**: a few error strings starting "failed to", three naked returns, `SpdxId`-style initialisms.
- **TOOL-4 / DEP-3**: CI runs neither `govulncheck` nor `go mod tidy -diff`.

## Review output format

Reviewers report findings ordered by severity, each **verified** against the code before it is reported:

```
[BLOCKER] internal/collector/github.go:142 — CTX-4 (L14)
WithTimeout result is never cancelled; leaks a timer per call in a loop over ~3k repos.
Verified: go vet lostcancel reports it; the loop runs per repository.
Fix: add `defer cancel()` after line 142.

[MAJOR] internal/db/repos.go:88 — DB-2
rows.Err() not checked after iteration; a mid-stream network error returns a truncated slice as success.
Verified: read the function; no rows.Err() after the loop.
```

Each finding names the file and line, the rule ID and lens or standing rule where one applies (or "defect" if no rule covers it), the concrete failing scenario, how it was verified, and the fix. After the findings:

- a short list of candidates **checked and rejected**, one line each with the reason;
- the **tests run** and their results (with the DB tier confirmed to have run, not skipped);
- a one-line result: **"round N: X verified findings"**. There is no approve verdict; the maintainer commits and merges.

**Scope.** Review the diff, plus the **sibling sites** of any primitive it touches (L11 class sweep; a fix applied to one of several identical sites is incomplete), plus stale prose in touched functions (L12). A pre-existing defect elsewhere is reported in a separate "pre-existing" section and recorded (worklist or ledger), never silently dropped and never a blocker for the change under review. Before raising something in an area with history, check the release ledger: a documented decision is not a finding unless the code contradicts it.

Reviewers do not flag formatting (TOOL-1 handles it) or personal preference not grounded in a rule.
