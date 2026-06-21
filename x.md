
Diagnosis: transient 401s permanently killing good keys

The timeline on aveloxis_large:
- June 15–16: 28 and 13 invalidation-related lines (normal noise).
- June 17 01:34:47 — keys start dying one at a time (valid_keys_remaining=17, then 16, 15…).
- The countdown ran slowly, from 01:34 to 16:59 — over 15 hours, one key at a time.
- June 17 16:59:05 — LAST API key invalidated — all collection for this platform will fail. Pool is now permanently dead.
- June 17–20: the pool-empty cascade — 1,043,221 breadth: failed to process contributor, 267,642 FetchRepoInfo failed, etc. All downstream of GetKey returning ErrAllKeysInvalidated.

Why these are false errors: the keys were dying over a 15-hour span, not all at once. A genuinely bad/revoked token 401s on every request and would die within minutes of startup. A key that works for hours and then occasionally 401s is a good token hitting GitHub's auth backend during a transient incident (GitHub intermittently returns 401 Bad credentials for valid tokens during auth-service hiccups — which is what started June 17).

The code defect (internal/platform/ratelimit.go + httpclient.go:429 + graphql.go:214):
1. A single 401 calls InvalidateKey → key.Invalid = true permanently. No body inspection, no strike threshold.
2. There is no resurrection path — GetKey's refill loop only restores Remaining, never clears Invalad it stays bad until process restart.
3. When all keys are invalid, GetKey returns ErrAllKeysInvalidated → ClassAuth → the scheduler aborts all in-flight jobs and stops. Restart rebuilds the pool from the DB (clearing Invalid), runs fine for a while, then bleeds out again on the next transient 401 wave. That is your "keeps crashing" loop.

The fix is to stop treating an isolated 401 as a death sentence. Before I touch credential-handlingperator-facing behavior:

⏺ User answered Claude's questions:
· How should a key that crosses the 401 failure threshold be handled? → Quarantine + auto-recover (Recommended)

Quarantine + auto-recover it is. Let me read the existing key-pool tests, the GraphQL success path, and other references to Invalid/ErrAllKeysInvalidated so I don't break contracts.

  Ran 1 shell command

Everything checks out — InvalidateKey stays (tests + legacy backstop); GraphQL calls UpdateFromResponse on every response so the strike reset covers both paths. Implementing now.

1. Add quarantine state to APIKey + constants:



[td] {'repo': 'scylladb__scylladb', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'azure__azure-sdk-for-js', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'python__cpython', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'azure__azure-sdk-for-net', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'azure__azure-sdk-for-java', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'xbmc__xbmc', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'pytorch__pytorch', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'paddlepaddle__paddle', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'elastic__kibana', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'dotnet__runtime', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] {'repo': 'netdata__netdata', 'months': 48, 'analyzed': 48, 'deduped': 0, 'empty': 0, 'errors': 0}
[td] done. {'months': 3312, 'analyzed': 3200, 'deduped': 20, 'empty': 92, 'errors': 0}. aggregate -> out/stage1/td_snapshots_all.csv
== stage 2: per-commit / per-author ==
+ python3 td_per_commit.py --out out/stage2 --cache cache --postgres-dsn host=chaoss.tv port=5434 dbname=augur user=augur sslmode=prefer options='-c search_path=augur_data,public' --schema augur_data --repo-ids 36244,36790,37736,45650,45651,45657,45693,45743,46119,46499,47931,48081,48398,50183,50249,51814,56361,147942,150372,207867,213461,213530,213894,215191,219857,224367,224388,224428,224533,224545,224606,225852,246423,250340,253299,256451,266929,298166,298210,298226,298291,298307,298378,298532,298821,298984,299148,299170,299176,299274,299442,299506,299631,299714,299745,299753,299895,299970,300042,300091,300365,300441,300480,300580,300607,300886,300898,301123,301126 --repo-map out/_run/repo_map.tsv --engine-bin /Users/sean/github/aveloxis/aveloxis-tech-debt/bin/opengrep --semgrep-config /Users/sean/.aveloxis/rules/_staged/gitlab --linters --cpp-dea-rules /Users/sean/.aveloxis/rules/0xdea/rules --cpp-dedup line --catalog remediation_catalog.yaml --workers 18 --clone-timeout 7200 --engine-jobs 1
[commit-td] 68 repos, 3204963 commits

