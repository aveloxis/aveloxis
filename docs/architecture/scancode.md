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

1. **Dispatcher claim** (paced by `scancode_start_interval_s`, default 90s, as MINIMUM GAP between successful starts — see §3.3 for the v0.21.3 design): the dispatcher calls `ClaimNextScancodeRepo(ctx, cadence)`. The SQL uses `FOR UPDATE SKIP LOCKED` against `aveloxis_data.repos` filtered by:
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

### 3.3 Dispatcher pacing (v0.21.3): minimum-gap, not throughput cap

The pacing semantic for `scancode_start_interval_s` changed between v0.21.0 and v0.21.3. The change is invisible to operators in steady state but materially affects first-pass throughput.

**Pre-v0.21.3 design (broken at scale)**: the dispatcher was driven by `time.NewTicker(startInterval)` — one claim attempt per tick, regardless of how many workers were idle. At 90 s/tick × 7 workers × ~3-min average scan time, the fleet-wide claim rate capped at 40 claims/hour while runners had capacity for ~140. On a 40K-repo fleet this produced ~42-day first-pass estimates when actual capacity was ~12 days. 6 of 7 workers sat idle on average.

**v0.21.3 design (correct)**: the dispatcher maintains a `nextStartAllowed time.Time` deadline that's stamped *after* each successful start. It then loops as fast as the runtime allows, gating each claim on `time.Now() >= nextStartAllowed`. The unbuffered jobs channel provides back-pressure — when all N workers are busy, the dispatcher's send blocks naturally and no over-claiming happens.

Operational effect:
- **Steady-state with idle workers**: claims happen at intervals of exactly `startInterval` between successful starts. Same behavior as before.
- **Steady-state with busy workers**: dispatcher pauses on the unbuffered send. When a runner frees up, the next claim happens after the `startInterval` window — same as before.
- **Burst on restart (the throughput-critical case)**: dispatcher claims one repo per `startInterval` seconds until all N worker slots are full. At 90 s × 7 workers = 630 seconds (~10 min) to saturate the pool. Same as before.
- **First-pass on a large fleet (the regression case)**: workers complete scans in single-digit-to-low-double-digit minutes; the dispatcher refills slots at `startInterval` cadence, so 7 workers stay nearly always busy. Throughput is now bounded by worker capacity, not dispatcher pacing.

For a 40K-repo fleet with `workers=7` and ~3-min average scan time:
- Worker capacity: 7 × (60 / 3) = ~140 repos/hour
- 40,000 ÷ 140 = ~286 hours ≈ **~12 days first-pass**

(Pre-v0.21.3 same configuration: ~42 days, dispatcher-bound.)

If you want to push further, raise `scancode_workers`. The `scancode_start_interval_s` rarely needs tuning unless you want denser starts on a very high-bandwidth network — the 90-second default works fine for most fleets.

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
| `scancode_start_interval_s` | `90` | Minimum seconds between *successful* claim starts. As of v0.21.3 this is a minimum-gap pacing primitive, NOT a throughput cap — the dispatcher claims as fast as workers free up, with this interval enforced only between consecutive starts. Bounds clone-bandwidth bursts on restart. See §3.3. |
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
| `scancode preflight: healthy` | Once at startup | The §13 health check passed — the toolchain works. |
| `scancode preflight: SYSTEM-LEVEL FAILURE — scancode will not work until fixed` | Once at startup (ERROR) | The §13 health check detected a systemic failure (corrupt libmagic, a repeated error, or no JSON). Read the `detail` field; `aveloxis_ops.aveloxis_status` is also set to `broken`. |
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

## 13. Startup health preflight + `aveloxis_ops.aveloxis_status`

A *system-level* scancode failure — one where every scan is doomed regardless of the repo — used to be invisible: the fleet just degraded. The 2026-06-09 `aveloxis_large` incident was the motivating case. On that Ubuntu 24.04 host the system `libmagic` database (`/usr/share/misc/magic.mgc`) was corrupt, so scancode (via `typecode` → `python-magic` → `libmagic`) emitted `Warning: offset ... invalid` at enormous volume — **14+ GB of stderr per large repo** — bogging scans down until the wall-clock timeout killed them. With the adaptive timeout stretching doomed scans to 16–24 h, a handful of repos wedged all worker slots and the scanned-repo count crawled.

### The preflight

On startup (`ScancodeWorker.Run`, before the dispatcher claims any work), the worker runs **one** scancode invocation against a tiny synthetic input and classifies the result:

- **Bounded and safe.** 90-second wall-clock timeout, process-group kill, and a capped 1 MB stderr capture — the health check itself can never hang the worker or buffer gigabytes (the very failure it detects).
- **`classifyScancodeHealth`** maps the outcome to a status:
  - **`broken`** if stderr carries the libmagic corruption fingerprint **in volume** — either the compiled-DB name `magic.mgc`, or the OS-independent `magic` … `Warning` … `offset` … `invalid` shape that libmagic's C parser emits on Linux **and** macOS — repeated ≥ 50× (the wedging bug emits one warning per bad magic-DB entry at load time, saturating stderr; a repaired libmagic emitting a handful of benign warnings while scans complete is **not** flagged), **or** any single line repeats ≥ 50× (generic "the toolchain is spamming" signal), **or** no valid JSON was produced. Volume, not mere presence, is the signal — see the 2026-06-10 false-positive note below.
  - **`not_installed`** if the `scancode` binary isn't on `PATH`.
  - **`ok`** otherwise.
- On anything other than `ok` it logs **`ERROR "scancode preflight: SYSTEM-LEVEL FAILURE — scancode will not work until fixed"`** with a `detail` string that names the remediation.

It is **awareness only** — the preflight does **not** disable scancode (a deliberate scope decision; auto-pause is a possible follow-up). It records, logs, and lets the worker proceed.

> **Volume, not presence (2026-06-10).** An early version of the libmagic check flagged `broken` on the mere *presence* of an `offset invalid` warning. That false-positives a working install: a repaired libmagic (e.g. after `aveloxis upgrade-tools` injects typecode-libmagic) can emit a *handful* of benign warnings while scans complete normally and produce valid data. The wedging bug is different in **kind** — the corrupt DB emits one warning per bad entry at load time, repeating the fingerprint thousands of times (it saturates the preflight's 1 MB stderr cap). The check now requires the fingerprint to repeat past the systemic-spam threshold (≥ 50), so a few incidental warnings no longer read as broken.

### The status table

The outcome is upserted into `aveloxis_ops.aveloxis_status` (one row per subsystem, keyed by `status_name`; see [schema.md](../schema.md)):

```sql
SELECT status_name, status, status_detail, tool_version, data_collection_date
FROM aveloxis_ops.aveloxis_status WHERE status_name = 'scancode';
```

A `broken` row's `status_detail` for the libmagic case reads, in part: *"system libmagic magic database appears corrupt … run `aveloxis upgrade-tools` to inject typecode-libmagic (works on any OS), …"* followed by an OS-aware reinstall hint — `brew reinstall libmagic` on macOS, `apt-get install --reinstall libmagic1 file` on Linux (chosen via `runtime.GOOS`). The table is generic by design — future subsystems record their own health under their own `status_name`, and the intent is to surface it to the operator (UI/API) over time.

**Code:** `internal/collector/scancode_preflight.go` (preflight + `classifyScancodeHealth`), `internal/db/aveloxis_status_store.go` (`SetAveloxisStatus` / `GetAveloxisStatus`), `internal/db/schema.sql` (table).
