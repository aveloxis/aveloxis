# Aveloxis Metrics — Improvements on CHAOSS metrics

Aveloxis implements the metric families below with deliberate,
practical deviations from their nominal CHAOSS definitions. This page
is generated in lockstep with the `/api/v1/metrics` catalog (a test
fails the build if they drift); the GUI's inline metric popovers and
its reference page render the same content.

Temporal metrics are served by `GET /api/v1/compare` (weekly or
monthly buckets, default window = trailing **3 years**); snapshot
metrics by `GET /api/v1/compare/snapshot`. Up to **7 entities**
(repositories or forge organizations) per comparison; an organization
is the union of its tracked repositories, with DISTINCT applied across
the union for people-counting metrics.

## Contributors {#contributors}

- **id**: `contributors` · **kind**: temporal · **unit**: people
- **Our definition**: Distinct people with ANY contribution event (issue, change request, commit, comment, or event action) in the bucket. Soft-deleted merge-loser identities excluded.
- **Improvement on CHAOSS**: Counts resolved platform identities (cntrb_id) across ALL activity types in one pass, rather than per-activity contributor lists.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-contributors/

## Change Requests {#change_requests}

- **id**: `change_requests` · **kind**: temporal · **unit**: change requests
- **Our definition**: Change requests (PRs/MRs) OPENED per bucket, state-agnostic. Companion series: change_requests_merged.
- **Improvement on CHAOSS**: State-agnostic open counts with merged as a separate series, so review throughput and demand are not conflated.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-change-requests/

## Change Requests Merged {#change_requests_merged}

- **id**: `change_requests_merged` · **kind**: temporal · **unit**: change requests
- **Our definition**: Change requests merged per bucket (by merge time).
- **Improvement on CHAOSS**: Bucketed by MERGE time, not open time — measures acceptance when it happened.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-change-requests-accepted/

## Issues {#issues}

- **id**: `issues` · **kind**: temporal · **unit**: issues
- **Our definition**: Issues opened per bucket, state-agnostic. Companion series: issues_closed.
- **Improvement on CHAOSS**: Single state-agnostic series with closure as a companion, matching how triage teams read demand.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-issues-new/

## Issues Closed {#issues_closed}

- **id**: `issues_closed` · **kind**: temporal · **unit**: issues
- **Our definition**: Issues closed per bucket (by close time).
- **Improvement on CHAOSS**: Closer attribution (closed_by) is collected/derived separately for 'who closes' analysis.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-issues-closed/

## Code Change Commits {#code_change_commits}

- **id**: `code_change_commits` · **kind**: temporal · **unit**: commits
- **Our definition**: Distinct default-branch commits per bucket (authored time). Matches the forge's own commit metadata counts.
- **Improvement on CHAOSS**: Default-branch-only via git log (not --all), deduplicated by commit hash across the per-file storage model.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-code-changes-commits/

## Committers {#committers}

- **id**: `committers` · **kind**: temporal · **unit**: people
- **Our definition**: Distinct resolved commit authors per bucket. Commits whose author could not be resolved to a platform identity (~8% fleet-wide) are excluded and documented.
- **Improvement on CHAOSS**: Uses deterministic platform-resolved identities (cmt_ght_author_id), not raw email strings, so renames and aliases collapse correctly.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-committers/

## Burstiness {#burstiness}

- **id**: `burstiness` · **kind**: temporal · **unit**: B coefficient
- **Our definition**: Goh–Barabási B = (σ−μ)/(σ+μ) over bucketed activity counts (commits + change requests + issues), computed per bucket over a trailing 26-bucket window. B ∈ [−1,1]: −1 metronome-regular, 0 Poisson-random, →1 bursty.
- **Improvement on CHAOSS**: Computed over bucketed activity counts rather than raw inter-event times — tractable at fleet scale and stable for cross-project comparison.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-burstiness/

## Project Velocity {#project_velocity}

- **id**: `project_velocity` · **kind**: temporal · **unit**: z-score
- **Our definition**: Composite of issues closed, change requests merged, and commits: each series is z-scored against the entity's own window mean, then averaged per bucket. Unitless; comparable across projects of different sizes.
- **Improvement on CHAOSS**: Self-normalized z-score composite instead of raw log-log axes, so a 7-entity overlay reads directly.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-project-velocity/

## Labor Investment {#labor_investment}

- **id**: `labor_investment` · **kind**: snapshot · **unit**: person-months
- **Our definition**: COCOMO-II basic estimate from the latest source scan: person-months = 2.94 × KLOC^1.0997. Reported in person-months; multiply by your loaded cost per person-month for currency.
- **Improvement on CHAOSS**: Derived from measured KLOC (SCC scan) with the cost multiplier left explicit and documented rather than baked in.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-labor-investment/

## Upstream Code Dependencies {#upstream_dependencies}

- **id**: `upstream_dependencies` · **kind**: snapshot · **unit**: dependencies
- **Our definition**: Count of direct manifest dependencies with resolvable releases; detail carries median libyear staleness.
- **Improvement on CHAOSS**: Pairs the raw count with median libyear so 'many but fresh' and 'few but rotten' are distinguishable.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-upstream-code-dependencies/

## License Coverage {#license_coverage}

- **id**: `license_coverage` · **kind**: snapshot · **unit**: percent of files
- **Our definition**: Percent of scanned source files carrying a detected SPDX license expression; detail carries files scanned and distinct SPDX ids.
- **Improvement on CHAOSS**: File-level scancode detection (not just declared license), so partial/mixed licensing is visible.
- **CHAOSS nominal**: https://chaoss.community/kb/metric-license-coverage/
