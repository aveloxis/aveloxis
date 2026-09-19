// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// contributor_activity.go — batched contributionsCollection fetch
// (v0.27.57). The GraphQL contributionsCollection is the only API
// surface that separates "publicly active" from "privately active but
// disclosed" (restrictedContributionsCount — the profile page's "N
// contributions in private repositories" line) from "quiet". The REST
// events feed the breadth worker uses returns an empty list for all of
// those states indistinguishably.
//
// GITHUB-ONLY by nature: GitLab has no restricted-contributions
// equivalent (private profiles are simply invisible), so this lives on
// *Client and is consumed through a narrow capability interface at the
// scheduler — NOT on platform.Client.

// contributorActivityBatchSize is how many aliased user() lookups ride
// one GraphQL query.
//
// NOT 100: contributionsCollection is resource-capped independently of
// the rate-limit point cost. The FetchIssueClosers precedent (100
// aliases) does NOT transfer — at 100 (and 50, and 40) GitHub answers
// EVERY alias with a per-path RESOURCE_LIMITS_EXCEEDED ("Resource
// limits for this query exceeded") and null nodes, which is how
// production stamped 216,000 contributors "checked, no data" on
// 2026-07-30/31 before v0.27.79. Live probe 2026-08-02 with real
// fleet logins: 40 → all fail, 35 → all succeed.
//
// v0.27.81: the edge MOVES — production RLE'd at 25 aliases on
// 2026-08-04 (query cost depends on how dense the batched accounts
// are, not just the alias count), which wedged classification: the
// deterministic NULL-first claim re-presented the same 2,500
// contributors every tick and the fixed-size fetch failed identically
// each time. No fixed batch size is safe, so the size is now just the
// STARTING point for fetchActivityWithSubdivide, which halves on
// RESOURCE_LIMITS_EXCEEDED down to size 1.
const contributorActivityBatchSize = 25

// FetchContributorActivity returns trailing-year contribution summaries
// for the given logins, keyed by login. Deleted/renamed users (per-path
// NOT_FOUND → null node) are ABSENT from the map — absence means
// "unknown", while presence with zeros means "confirmed quiet"; the
// activity classification depends on that distinction. Empty logins are
// skipped. On error the map holds every login the OTHER chunks fetched (a
// failing chunk is recorded and the rest still run, v0.29.56) and the second
// return names every login this call got no answer about — the failed
// chunks' and any never attempted. That set is not advisory: the scheduler
// writes what came back and marks absent ONLY logins whose own chunk
// completed, so a deleted account still retires while another chunk fails.
//
// v0.29.56: GitHub's in-body execution timeout ("Something went wrong
// while executing your query") is retried inside the GraphQL loop and,
// when the budget runs out, classifies ClassTransient, so it subdivides
// like RESOURCE_LIMITS_EXCEEDED. It used to classify fatal and fail the
// whole sweep every tick.
//
// v0.27.81 — accounts whose contributionsCollection is too expensive
// to resolve even ALONE (actions-user / ghost-class machine accounts)
// are also ABSENT: they are logged (one aggregated WARN per fetch since
// v0.29.56) and ride the scheduler's absent→mark-only path, which stamps
// them checked without a class ("" = unknown) and retires them from the
// claim head — provided their own chunk completed, which is what the
// unfetched set the caller gets is for. That reuse is
// deliberate — retrying an at-cap account next tick can never make it
// cheaper (the scancode at-cap lesson). The one shape that still
// FAILS the whole fetch is a full chunk where EVERY account skipped
// at size 1: 25 consecutive individually-unresolvable accounts is not
// a plausible account property, it's the systemic incident signature
// (2026-07-30/31: GitHub rejecting everything), and per the v0.27.79
// contract a resource-limit condition must never become an
// empty-but-successful result that mark-stamps the batch dataless.
func (c *Client) FetchContributorActivity(ctx context.Context, logins []string) (map[string]model.ContributionActivity, []string, error) {
	// Subdivision is this function's retry strategy, so cap the inner
	// GraphQL retry budget (the 2026-08-04 pilot measured a single
	// dense-account query burning ~7 minutes of the full 10-retry
	// backoff chain before subdivision could even start).
	ctx = platform.WithGraphQLFastFail(ctx)
	// Background sweep: reserve headroom for foreground collection (with
	// fast-fail also set, an under-reserve pool returns the typed
	// rate-limit error immediately and the ticker defers to its next tick).
	ctx = platform.WithGraphQLBackgroundBudget(ctx)
	out := make(map[string]model.ContributionActivity, len(logins))
	// v0.29.56: a chunk that fails as a whole does NOT stop the chunks after
	// it. The claim is deterministic (oldest-checked first, no lease), so
	// stopping at the first failing chunk means that once the chunks ahead of
	// it are stamped it becomes chunk one and the sweep never progresses
	// again — the v0.27.81 wedge. Its logins are simply not in the result, so
	// the scheduler leaves them unstamped and they are retried next tick,
	// while everyone else moves forward. A cancelled context still stops at
	// once: it is a shutdown, not a chunk failure.
	var (
		chunkErrs   []error
		unfetched   []string
		chunksRun   int
		failedRun   int // consecutive failures
		chunkFailed int
		gaveUp      bool
	)
	skippedTotal := 0
	// flush returns the logins this chunk got NO answer about, alongside the
	// error. A subdivided chunk can half-complete: the subchunks that
	// answered own their own absences (a deleted account among them must
	// still retire), so only the unanswered subtrees may be reported
	// (v0.29.57).
	flush := func(batch []string) ([]string, error) {
		skipped, lastSkipErr, unanswered, err := c.fetchActivityWithSubdivide(ctx, batch, out)
		if err != nil {
			if len(unanswered) == 0 {
				// Defensive: an error with nothing named is reported as the
				// whole batch. Over-reporting costs a retry next tick;
				// under-reporting would retire a live account nobody asked
				// about.
				unanswered = batch
			}
			return unanswered, err
		}
		if skipped == len(batch) && skipped > 0 {
			// Every alias unresolvable alone is the systemic shape: refuse
			// to mark the batch dataless, and treat none of it as answered.
			return batch, fmt.Errorf("contributor activity: all %d aliases in chunk unresolvable at size 1 — systemic resource-limit condition, refusing to mark the batch dataless: %w", skipped, lastSkipErr)
		}
		// Counted only for a chunk that SUCCEEDED: these are accounts GitHub
		// would not resolve individually. Counting a wholesale chunk failure
		// here would report an outage as a crowd of permanently expensive
		// accounts — the 2026-07-30/31 misdiagnosis in miniature.
		skippedTotal += skipped
		return nil, nil
	}
	// run reports a chunk's outcome. A failed chunk's logins are UNFETCHED —
	// the caller must not read their absence from `out` as "deleted" — and
	// the sweep carries on, because stopping at the first failing chunk is
	// what let one poison chunk wedge the claim head. It gives up after
	// activityChunkFailureLimit failures in a row: that run is the
	// GitHub-wide outage signature, and a fully-failing chunk costs ~147
	// requests, so attempting all 100 chunks of a tick would spend ~14,700.
	run := func(batch []string) error {
		chunksRun++
		noAnswerHere, err := flush(batch)
		if err == nil {
			failedRun = 0
			return nil
		}
		if ctx.Err() != nil {
			// Shutdown: stop here. Any chunk failures recorded before it are
			// deliberately dropped — the tick is being abandoned, nothing is
			// stamped, and the caller must see an unambiguous
			// context.Canceled so it classifies this as a shutdown rather
			// than a failure.
			return err
		}
		chunkFailed++
		failedRun++
		unfetched = append(unfetched, noAnswerHere...)
		// The failures in a run are the same condition; keep the first few
		// so the error stays readable.
		if len(chunkErrs) < activityChunkFailureLimit {
			chunkErrs = append(chunkErrs, err)
		}
		if failedRun >= activityChunkFailureLimit {
			gaveUp = true
		}
		return nil
	}
	// Chunk up front so every exit path can say exactly which logins were
	// never attempted. `unfetched` holds every login whose CHUNK produced no
	// answer — it failed, or was never attempted — and a caller that stamps
	// the rest depends on it being complete on the shutdown and give-up
	// paths too, not only the ones it happens to guard. An account skipped
	// at size 1 INSIDE a chunk that succeeded is deliberately NOT in it: the
	// caller must still retire it, or an at-cap account re-pins the
	// NULLS-FIRST claim head every tick (v0.27.81).
	chunks := chunkLogins(logins, contributorActivityBatchSize)
	for i, chunk := range chunks {
		if gaveUp {
			unfetched = append(unfetched, flattenChunks(chunks[i:])...)
			break
		}
		if err := run(chunk); err != nil {
			// Shutdown mid-sweep: this chunk and every chunk after it are
			// unanswered.
			unfetched = append(unfetched, flattenChunks(chunks[i:])...)
			return out, unfetched, err
		}
	}
	if skippedTotal > 0 {
		// Says what was OBSERVED this tick, not what the accounts are
		// (v0.29.57). The count includes accounts that failed alone with
		// ErrGraphQLExecutionTimeout, which the retry arm treats as
		// intermittent GitHub load — "would not resolve even alone" reads
		// as a property of the account and sends operators hunting bad
		// accounts during what is actually a GitHub incident.
		c.logger.Warn("contributor activity: accounts unresolved at batch size 1 this tick — skipped, will retry next tick",
			"accounts", skippedTotal, "of", len(logins))
	}
	if chunkFailed > 0 {
		// The two counts are different numbers and the scheduler logs this
		// text verbatim: chunkFailed is the tick's TOTAL, failedRun is the
		// run that triggered the give-up. Interpolating the total into "in a
		// row" reported a 5-chunk GitHub outage when only 3 were
		// consecutive.
		gave := ""
		if gaveUp {
			gave = fmt.Sprintf(" (%d in a row — giving up this tick)", failedRun)
		}
		// Count only logins with no answer: a chunk can fail after its left
		// half resolved (subdivision), and those logins ARE in `out`.
		noAnswer := 0
		for _, l := range unfetched {
			if _, ok := out[l]; !ok {
				noAnswer++
			}
		}
		return out, unfetched, fmt.Errorf("contributor activity: %d of %d chunks failed%s (%d logins unanswered): %w",
			chunkFailed, chunksRun, gave, noAnswer, errors.Join(chunkErrs...))
	}
	return out, unfetched, nil
}

// chunkLogins splits the claim into query-sized chunks, dropping empty
// logins (a contributor row with no login is not fetchable).
func chunkLogins(logins []string, size int) [][]string {
	var chunks [][]string
	var batch []string
	for _, l := range logins {
		if l == "" {
			continue
		}
		batch = append(batch, l)
		if len(batch) == size {
			chunks = append(chunks, batch)
			batch = nil
		}
	}
	if len(batch) > 0 {
		chunks = append(chunks, batch)
	}
	return chunks
}

// flattenChunks joins chunks back into one login list.
func flattenChunks(chunks [][]string) []string {
	var out []string
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}

// activityChunkFailureLimit is how many chunks may fail IN A ROW before the
// sweep stops for this tick. Three: one failing chunk is an account
// property, a run of them is the systemic shape (2026-07-30/31, GitHub
// answering everything with resource limits), and each one costs ~147
// requests plus backoff. Nothing is lost by stopping — the unfetched logins
// are not stamped, so the next tick re-claims them.
const activityChunkFailureLimit = 3

// fetchActivityWithSubdivide wraps fetchActivityBatch with the
// v0.27.81 transient-failure halving (the fetchPRBatchWithSubdivide
// pattern). ClassTransient covers the observed shapes of "GitHub did not
// answer this query": in-body
// RESOURCE_LIMITS_EXCEEDED (the 18:51/19:06 UTC production wedge on
// 2026-08-04) and deterministic 500s until retry exhaustion (the same
// day's pilot — one dense sub-batch drew 10/10 server errors over ~7
// minutes; the history worker's actions-user windows show the
// identical shape), and — since v0.29.56 — the in-body execution timeout
// (ErrGraphQLExecutionTimeout). RATE_LIMITED (temporal, not an account
// property) and auth failures bubble unchanged so the scheduler's
// marks-nothing-retry-next-tick contract applies. At size 1 a
// transient failure means the account itself exceeds what GitHub will
// resolve: log it (Debug — the aggregate is the WARN), count it skipped,
// move on. Recursion depth is
// bounded by log2(contributorActivityBatchSize).
//
// The execution timeout is the one shape where that size-1 reading is a
// TRADE rather than a fact: it is intermittent load, not a cost property
// of the account (the 2026-09-17 probe re-ran a batch that had failed
// seven production ticks and all 100 queries succeeded). An account that
// times out through the fast-fail budget at every subdivision level is
// skipped, and when the REST of its chunk succeeded the scheduler
// mark-stamps it: it leaves the claim head for a cooldown — a freshness
// cost accepted so the tick makes progress for everyone else. It cannot
// corrupt a class: MarkActivityCheckedBatch never writes gh_activity_class.
// When EVERY login in a chunk skips, the chunk is failed instead (the
// v0.27.79 rule: a resource-limit condition must never become an
// empty-but-successful result) — those logins stay unstamped and are
// retried, while the other chunks still run (v0.29.56).
func (c *Client) fetchActivityWithSubdivide(ctx context.Context, logins []string, out map[string]model.ContributionActivity) (skipped int, lastSkipErr error, unanswered []string, err error) {
	if len(logins) == 0 {
		return 0, nil, nil, nil
	}
	batchErr := c.fetchActivityBatch(ctx, logins, out)
	if batchErr == nil {
		return 0, nil, nil, nil
	}
	if platform.ClassifyError(batchErr) != platform.ClassTransient {
		// This subtree got no answer. Only THESE logins are unanswered —
		// a sibling that already completed answered for its own, including
		// the ones it legitimately omitted (v0.29.57).
		return 0, nil, logins, batchErr
	}
	if len(logins) == 1 {
		// Debug per login, not WARN: with keep-going a bad tick can reach
		// thousands of these, and one line per batch is the house rule (the
		// v0.27.91 flood class). The count is reported by the caller.
		c.logger.Debug("contributor activity: account unresolvable even alone (resource limits or persistent server errors) — skipping (scheduler will mark-only stamp it)",
			"login", logins[0], "error", batchErr)
		// Answered: GitHub resolved the query and said this account cannot
		// be served. The scheduler mark-only stamps it, so it is NOT
		// unanswered.
		return 1, batchErr, nil, nil
	}
	mid := len(logins) / 2
	leftSkipped, leftErr, leftUnanswered, err := c.fetchActivityWithSubdivide(ctx, logins[:mid], out)
	if err != nil {
		// The right half was never attempted, so it is unanswered too.
		return leftSkipped, leftErr, append(append([]string(nil), leftUnanswered...), logins[mid:]...), err
	}
	rightSkipped, rightErr, rightUnanswered, err := c.fetchActivityWithSubdivide(ctx, logins[mid:], out)
	lastSkipErr = rightErr
	if lastSkipErr == nil {
		lastSkipErr = leftErr
	}
	return leftSkipped + rightSkipped, lastSkipErr, append(leftUnanswered, rightUnanswered...), err
}

type activityNode struct {
	Login                   string `json:"login"`
	ContributionsCollection struct {
		RestrictedContributionsCount int   `json:"restrictedContributionsCount"`
		ContributionYears            []int `json:"contributionYears"`
		ContributionCalendar         struct {
			TotalContributions int `json:"totalContributions"`
		} `json:"contributionCalendar"`
	} `json:"contributionsCollection"`
}

func (c *Client) fetchActivityBatch(ctx context.Context, logins []string, out map[string]model.ContributionActivity) error {
	var b strings.Builder
	b.WriteString("query {")
	for i, login := range logins {
		fmt.Fprintf(&b, `
  u%d: user(login: %q) { login contributionsCollection { restrictedContributionsCount contributionYears contributionCalendar { totalContributions } } }`, i, login)
	}
	b.WriteString(" }")

	// The GraphQL helper logs per-path errors (NOT_FOUND for deleted
	// users) at WARN and still returns the data for the other aliases —
	// their nodes arrive as null and are skipped below.
	var resp map[string]json.RawMessage
	if err := c.http.GraphQL(ctx, b.String(), nil, &resp); err != nil {
		return fmt.Errorf("contributor activity batch: %w", err)
	}
	for i, login := range logins {
		raw, ok := resp[fmt.Sprintf("u%d", i)]
		if !ok || string(raw) == "null" {
			continue // deleted/renamed → absent from the result
		}
		var node activityNode
		if err := json.Unmarshal(raw, &node); err != nil {
			continue
		}
		out[login] = model.ContributionActivity{
			Login:             login,
			CalendarTotal:     node.ContributionsCollection.ContributionCalendar.TotalContributions,
			Restricted:        node.ContributionsCollection.RestrictedContributionsCount,
			ContributionYears: node.ContributionsCollection.ContributionYears,
		}
	}
	return nil
}
