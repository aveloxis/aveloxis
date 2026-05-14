# ScanCode Worker (v0.21.0+)

Per-file license + copyright + package detection is performed by ScanCode Toolkit ([aboutcode-org/scancode-toolkit](https://github.com/aboutcode-org/scancode-toolkit)) in a dedicated worker pool inside `aveloxis serve`, decoupled from the per-repo collection pipeline.

This document covers the design and operation of that worker. Operator-facing tuning lives in [`docs/getting-started/configuration.md`](../getting-started/configuration.md#scancode-worker-v0210); this file explains *why* the system looks the way it does.

## 1. What scancode does (and doesn't)

Scancode walks a working-tree checkout of a repository and emits per-file findings:

- License expression(s) detected — both raw and normalized SPDX.
- Copyright statements and the holder names extracted from them.
- Package manifests detected and their declared dependencies.
- Detection metadata (line numbers, match percentage, etc.).

Results land in `aveloxis_scan.scancode_scans` (one row per scan run) and `aveloxis_scan.scancode_file_results` (one row per file with at least one detection). Previous rows are rotated to `*_history` tables before each new scan.

**Scancode does NOT generate SBOMs.** SPDX and CycloneDX SBOMs are produced by Phase 6 of the per-repo collection pipeline (`internal/collector/sbom.go`) and refreshed on every collection cycle. SBOM generation uses scancode's license data as enrichment when available, but the SBOM artifact itself is regenerated independently.

## 2. Why the worker was decoupled (the 2026-05-14 incident)

Pre-v0.21.0 scancode ran inline in `AnalysisCollector.AnalyzeRepo` as Phase 4 of the per-repo collection pipeline, gated by a package-level 2-slot semaphore in `internal/collector/scancode.go`.

At fleet scale this shape doesn't work. On 2026-05-14, operator-side investigation of a 180-worker production fleet found 177 of 180 worker goroutines parked at `scancode.go:114` (the semaphore acquire) for 7+ hours. The two slot-holders were Linux-kernel-scale repos whose scans legitimately took hours; the other 177 worker goroutines were holding their `collection_queue` row locks the whole time, blocking other operations on those repos.

This is the same architectural anti-pattern v0.19.7 fixed for `PopulateAffiliations`: doing work-that-doesn't-fit-the-per-job-budget inside per-job code paths inevitably stalls the worker pool at scale. The fix follows the same pattern — move the work to a dedicated periodic ticker / worker pool.

The v0.21.0 worker:

- Runs in goroutines independent of the main collection pool. A slow scancode run can't park collection workers.
- Has its own concurrency cap (`collection.scancode_workers`, default 2) so operators can dial it independently of the collection worker count.
- Re-clones each repo (`git clone --depth 1`) instead of sharing the facade's bare clone, eliminating cross-worker filesystem-lock hazards.
- Defaults to a 6-month cadence (was 30 days inline) because per-file license headers in source code change rarely on that timescale.

## 3. Architecture

### 3.1 Components

```
                              aveloxis_data.repos
                              ├── scancode_last_run
                              ├── scancode_version
                              ├── scancode_locked_at
                              ├── scancode_locked_pid
                              ├── scancode_locked_boot_id
                              └── scancode_output_path
                                       ▲
                                       │
              ┌────────────────────────┼───────────────────────┐
              │                        │                       │
              ▼                        ▼                       ▼
       claim query              record lock state      mark complete /
   (FOR UPDATE SKIP LOCKED)     (after cmd.Start)        clear lock
              │                        │                       ▲
              │                        │                       │
              ▼                        ▼                       │
        ┌───────────┐             ┌─────────┐             ┌────────┐
        │dispatcher │──jobs chan─▶│ runner  │──exec──────▶│scancode│
        │ (90s tic) │             │  pool   │             │  proc  │
        └───────────┘             │ (N=2)   │             └────────┘
                                  └─────────┘
              ▲
              │
              │ on Run() startup
        ┌─────────────┐
        │ recover     │  reads ListLockedScancodeRows,
        │ Orphans     │  applies 4-state decision (§5)
        └─────────────┘
```

### 3.2 The lifecycle of a single scan

1. **Dispatcher tick** (every `scancode_start_interval_s`, default 90s): the dispatcher calls `ClaimNextScancodeRepo(ctx, cadence)`. The SQL uses `FOR UPDATE SKIP LOCKED` against `aveloxis_data.repos` filtered by:
   - `collection_queue.last_collected IS NOT NULL` — the repo has been collected at least once. Newly-added repos collect basic metrics first; scancode runs against them only after the first collection completes.
   - `repo_archived = FALSE` (or NULL).
   - `scancode_last_run IS NULL OR < NOW() - cadence` — cadence gate.
   - `scancode_locked_at IS NULL OR < NOW() - 12h` — stale-lock fallback (the silent-corpse safety net; the explicit `recoverOrphans` pass is the primary recovery path).
2. **Lock acquired**: the same SQL statement sets `scancode_locked_at = NOW()` on the candidate row and returns the (repo_id, owner, name, git_url) tuple. The row is now claimed atomically.
3. **Job sent to runner channel**: dispatcher writes the job; if no runner slot is free the dispatcher blocks until one is — this is the documented concurrency cap.
4. **Runner picks up job**:
   - Creates a fresh shallow clone: `git clone --depth 1 <repo_git> <scancode_clone_dir>/repo_<id>_<unix_ts>`. The shallow clone is enough for scancode (it walks current file state, not history).
   - Spawns scancode via `exec.CommandContext(ctx, "scancode", "-clpi", "--json", outputPath, ...)`.
   - Calls `cmd.Start()` (NOT `cmd.Run()`) so the OS PID is available *immediately*. Calls `cmd.Wait()` next to actually wait for completion.
   - Between `Start` and `Wait`, calls `store.RecordScancodeLockState(repoID, pid, bootID, outputPath)`. **This is the critical step** that makes crash recovery work — if aveloxis is killed before `cmd.Wait()` returns, the next aveloxis startup has the (pid, boot_id, output_path) tuple in the DB and can recover (see §5).
5. **Scan completes**: runner parses the JSON output, calls `ingestScancodeOutput()` to write `scancode_scans` + `scancode_file_results` (with history rotation), and calls `store.MarkScancodeComplete(repoID, version)`. That UPDATE atomically:
   - Sets `scancode_last_run = NOW()` and `scancode_version = $version`.
   - Clears all four lock columns (`scancode_locked_at`, `scancode_locked_pid`, `scancode_locked_boot_id`, `scancode_output_path`).
6. **Cleanup**: the runner's deferred `os.RemoveAll(tempDir)` removes the clone directory.

On any failure path (clone error, scancode crash, JSON parse error, ingest error), the runner calls `store.ClearScancodeLock(repoID)` to release the lock without setting `scancode_last_run`. The row becomes eligible for re-claim on the next dispatcher tick.

## 4. Cadence rationale (180 days default)

Per-file license + copyright headers in source files are near-immutable on the timescale that matters. A 6-month cadence catches:

- New files added to the repo since the last scan.
- Wholesale license changes (rare — but they happen, e.g. a project changes from GPL to MIT).
- Scancode version improvements (newer scancode versions detect licenses older versions missed).

It does NOT catch dependency-license changes, but those don't flow through scancode anyway — they're handled by Phase 4 dependency scanning + Phase 6 SBOM generation, both per-cycle.

Pre-v0.21.0 the inline cadence was 30 days, which produced one full re-scan of the whole fleet every month. At fleet scale (100K+ repos), that's a large continuous load for almost no fresh data. 180 days is a reasonable default; operators can dial via `collection.scancode_cadence_days`.

## 5. Crash recovery — the four-state table

On `aveloxis serve` startup, `ScancodeWorker.Run()` calls `recoverOrphans(ctx)` *before* the dispatcher starts claiming new jobs. The recovery pass examines every row with `scancode_locked_at IS NOT NULL` and applies one of four decisions:

| State | Detection | Action |
|---|---|---|
| **Reboot survivor** | stored `boot_id` ≠ current `/proc/sys/kernel/random/boot_id` | Scancode subprocess is definitively dead (the kernel that hosted it no longer exists). Clear all lock columns. |
| **Live orphan** | boot_id matches AND `kill(-0, pid)` succeeds | Subprocess survived a previous aveloxis crash and is now an orphan of init. Spawn a monitor goroutine that polls every 30s; when the PID dies, attempt to ingest the output file if present. |
| **Recoverable corpse** | boot_id matches, PID is dead, output file exists and parses | Scan finished but aveloxis crashed before ingest. Ingest the orphaned output, then clear the lock. |
| **Lost run** | boot_id matches, PID dead, no usable output | Scan died mid-flight. Clear the lock; the row will re-run on the next cadence tick. |

The boot_id check is what makes the PID check reliable. Linux PIDs are reused — a stored PID of 12345 from before a reboot could legitimately match an unrelated process after the reboot. The boot_id (kernel-generated UUID, changes on every boot) lets the recovery pass decide unambiguously.

On non-Linux dev machines (e.g. macOS) the `/proc` path is absent and `readBootID()` returns an empty string. The recovery pass treats empty boot_id as "unknown" and falls through to the PID check; correctness is preserved (PID reuse is rare on a single boot).

## 6. Graceful shutdown

When the scheduler's context is cancelled (`aveloxis stop serve`):

1. The dispatcher exits immediately on its `<-ctx.Done()` arm. No new claims happen.
2. The dispatcher closes the jobs channel.
3. Runners that were idle return immediately (their `range jobs` loop terminates).
4. Runners that were mid-scan keep going. The runner's `cmd.Wait()` is blocking on the scancode subprocess, which is NOT killed by the ctx cancel (Go's `exec.CommandContext` only kills the subprocess when the cmd object is garbage collected OR explicitly killed via `cmd.Process.Kill()`).
5. `Run()` waits up to `collection.scancode_shutdown_grace_minutes` (default 30 min) for all runners to finish.
6. If the grace expires with runners still active, `Run()` returns. The outstanding subprocesses become orphans — but they're tracked in the DB via the `(pid, boot_id, output_path)` triple recorded in step 4 of §3.2, so the next aveloxis startup's `recoverOrphans` pass will adopt them as live orphans (case 2 of §5).

The grace bound exists because Linux-kernel-sized scans can legitimately run for hours; without a bound, `aveloxis stop` would wait indefinitely on the slowest scancode. The trade-off is clear: lose the in-flight scan data on grace expiry vs. wait hours on stop. Operators who want a different balance can dial the grace.

## 7. Force-rerun cookbook

Cadence is enforced at claim time via the `scancode_last_run` column. To force a rescan, clear that column:

```sql
-- Single repo
UPDATE aveloxis_data.repos SET scancode_last_run = NULL
WHERE repo_owner = 'apache' AND repo_name = 'doris';

-- All repos that ran on a specific scancode version (e.g. after upgrade)
UPDATE aveloxis_data.repos SET scancode_last_run = NULL
WHERE scancode_version = '32.5.0';

-- Whole fleet
UPDATE aveloxis_data.repos SET scancode_last_run = NULL;
```

The claim query orders by `scancode_last_run NULLS FIRST`, so cleared repos move to the head of the queue and get claimed on subsequent dispatcher ticks. The order between cleared repos is `repo_id ASC` (stable).

## 8. Configuration reference

All five knobs live under the `collection` block in `aveloxis.json`:

| Key | Default | Purpose |
|---|---|---|
| `scancode_workers` | `2` | Max concurrent scancode subprocesses. Raise on machines with spare CPU cores. |
| `scancode_start_interval_s` | `90` | Minimum seconds between dispatcher claim attempts. Bounds clone-bandwidth bursts on restart. |
| `scancode_cadence_days` | `180` | Minimum days between successive scans on the same repo. Per-file licenses change rarely. |
| `scancode_clone_dir` | `/tmp/aveloxis-scancode` | Parent directory for per-run shallow clones. Size for ~50 MB × workers peak. |
| `scancode_shutdown_grace_minutes` | `30` | Wait budget for in-flight scans on `aveloxis stop`. Outstanding scans become live-orphans (see §5) if not finished. |

See [`docs/getting-started/configuration.md`](../getting-started/configuration.md#scancode-worker-v0210) for the tuning rationale per knob.

## 9. UX: the "last run" signal

The repo detail page in the web GUI shows:

> **Last run:** 2026-04-08 (scancode 32.5.0)

Or, for never-scanned repos:

> **Last run:** *not yet run — will run in the next scancode worker cycle*

The data flows from `aveloxis_data.repos.scancode_last_run` (written by `MarkScancodeComplete`) through the `/api/v1/repos/{id}/scancode-licenses` endpoint's `last_run` field to the rendered HTML.

## 10. Observability — what to grep in `aveloxis.log`

| Log line | When it fires | Meaning |
|---|---|---|
| `scancode worker started workers=N start_interval=...` | Once at startup | The pool is alive with N runners. If absent, scancode is disabled (binary not installed or `mkdir scancode_clone_dir` failed). |
| `scancode binary not installed; ScancodeWorker disabled` | Startup | Install with `pipx install scancode-toolkit` then restart serve. |
| `scancode recoverOrphans: examining locked rows count=...` | Startup | Recovery pass found stale locks. Followed by per-row decisions. |
| `scancode recover: reboot survivor — clearing lock` | Startup | Case 1 of §5. |
| `scancode recover: live orphan detected — spawning monitor` | Startup | Case 2 of §5. Monitor goroutine will log on completion. |
| `scancode recover: ingested orphaned scancode result` | Startup or monitor | Case 3 of §5 — orphaned data recovered. |
| `running ScanCode repo_id=... owner=... pid=...` | Per scan | Scan started; PID is the subprocess we're tracking. |
| `scancode worker complete repo_id=... version=...` | Per scan | Scan succeeded and was ingested. |
| `scancode runOne: scancode subprocess failed ... pid=...` | Per scan failure | Lock will be cleared. |
| `scancode worker shutdown grace expired ...` | On stop | Outstanding scans become live-orphans on next startup. |

## 11. Code map

| File | Purpose |
|---|---|
| `internal/collector/scancode_worker.go` | The worker itself: `ScancodeWorker` struct, `Run`, `dispatcher`, `runner`, `runOne`, `recoverOrphans`, `monitorOrphan`. |
| `internal/collector/scancode.go` | JSON parsing + ingest (`ingestScancodeOutput`). Shared between the worker and any future direct caller. |
| `internal/db/scancode_worker_store.go` | Store methods: `ClaimNextScancodeRepo`, `RecordScancodeLockState`, `MarkScancodeComplete`, `ClearScancodeLock`, `ListLockedScancodeRows`. |
| `internal/db/scancode_store.go` | Pre-existing scancode data access (file results, freshness reads). `ScancodeFreshness` is new in v0.21.0. |
| `internal/db/migrate.go` | Six column additions + partial index + backfill from `aveloxis_scan.scancode_scans`. |
| `internal/scheduler/scheduler.go` | `Config.Scancode*` fields, default values, goroutine spawn in `Run()`. |
| `internal/config/config.go` | `CollectionConfig.Scancode*` fields and accessor methods. |
| `internal/api/server.go` | `handleScancodeLicenses` returns `last_run` + `scancode_version` in addition to licenses + copyrights. |
| `internal/web/templates.go` | Repo detail page renders the freshness signal above the source-code-licenses table. |

## 12. Regression guards

The v0.21.0 work added these tests as architectural pins. A future refactor that breaks any of them fails the build before it ships:

| Test | Pins |
|---|---|
| `TestAnalyzeRepoNoLongerInvokesScancode` | `AnalyzeRepo` does NOT call `scanScanCode`. |
| `TestScancodeSemaphoreNoLongerExists` | `scancode.go` does NOT declare `scancodeSem`. |
| `TestScancodeNoLongerHas30DaySkipCheck` | `scancode.go` does NOT contain the inline cadence check. |
| `TestRunOneSplitsStartFromWait` | `runOne` calls `cmd.Start()` + `cmd.Wait()`, NOT `cmd.Run()`. |
| `TestRunOnePersistsPidAndBootId` | `runOne` calls `RecordScancodeLockState` between Start and Wait. |
| `TestRunOneClearsLockOnSuccess` / `TestRunOneClearsLockOnFailure` | Lock-clear paths exist on both branches. |
| `TestClaimUsesForUpdateSkipLocked` | Claim SQL uses `FOR UPDATE SKIP LOCKED`. |
| `TestClaimGatesOnLastCollected` | Claim filters on `last_collected IS NOT NULL`. |
| `TestClaimGatesOnCadenceAndStaleLock` | Claim respects both the cadence config and the 12h stale-lock fallback. |
| `TestClaimExcludesArchivedRepos` | Claim WHERE clause matches the partial-index predicate. |
| `TestScancodeWorkerCallsRecoverBeforeDispatcher` | Recovery runs before any new claim. |
| `TestSchedulerRunStartsScancodeWorker` | Scheduler spawns the worker. |
| `TestMainWiresScancodeConfig` | `cmd/aveloxis/main.go` reads the config knobs. |
| `TestScancodeLicensesEndpointReturnsFreshness` | API surfaces the freshness signal. |
