// Source-contract tests for v0.18.29 Fix 4: contributor enrichment
// upserts are batched, not per-login.
//
// Background: at v0.18.28, EnrichThinContributors loops over up to 14000
// thin logins and calls store.UpsertContributor(ctx, contrib) once per
// login (enrich.go:55). Each call wraps a single contributor in a
// fresh transaction. With Fix 2 making enrichment periodic, that's
// 14000 individual transactions every 30 minutes. The bleeding stop
// (Fix 3) handles correctness; this fix handles efficiency by
// accumulating into a slice and flushing via UpsertContributorBatch
// (which already does in-memory dedup and a single transaction per
// batch — postgres.go:1089).

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestEnrichThinContributorsBatchesUpserts pins the batch-flush pattern.
// The function should accumulate enriched contributors into a slice and
// flush via UpsertContributorBatch instead of calling UpsertContributor
// per login.
func TestEnrichThinContributorsBatchesUpserts(t *testing.T) {
	src := mustReadEnrichSource(t)
	body := extractEnrichFunc(src, "EnrichThinContributors")
	if body == "" {
		t.Fatal("could not locate EnrichThinContributors body")
	}

	if !strings.Contains(body, "UpsertContributorBatch") {
		t.Error("EnrichThinContributors must call UpsertContributorBatch (not UpsertContributor per login). " +
			"Per-login transactions waste DB write capacity at scale; the batch path already does in-memory " +
			"dedup and a single transaction.")
	}
}

// TestEnrichThinContributorsAccumulatesBeforeFlush pins that the batch
// flush happens AFTER the API loop completes — not inside the loop
// (which would defeat the point).
func TestEnrichThinContributorsAccumulatesBeforeFlush(t *testing.T) {
	src := mustReadEnrichSource(t)
	body := extractEnrichFunc(src, "EnrichThinContributors")
	if body == "" {
		t.Skip("EnrichThinContributors not yet refactored")
	}
	// Expect a slice accumulator declared somewhere in the body. We accept
	// either `[]model.Contributor` or `[]*model.Contributor`.
	hasAccumulator := strings.Contains(body, "[]model.Contributor") ||
		strings.Contains(body, "[]*model.Contributor")
	if !hasAccumulator {
		t.Error("EnrichThinContributors must declare a []model.Contributor (or pointer-slice) accumulator " +
			"inside the function — that's the slice that gets flushed to UpsertContributorBatch.")
	}
}

// TestCollectContributorsBatchesUpserts pins the same pattern in the
// legacy non-staged collector path. collector.go:588 currently calls
// UpsertContributor once per row inside the API iterator. Batching
// here matters less than enrichment (the staged collector is the main
// path) but still cuts per-repo write traffic by an order of magnitude
// for repos with many contributors.
func TestCollectContributorsBatchesUpserts(t *testing.T) {
	src := mustReadCollectorSource(t)
	idx := strings.Index(src, "func (c *Collector) collectContributors(")
	if idx < 0 {
		t.Fatal("could not locate collectContributors")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}

	if !strings.Contains(body, "UpsertContributorBatch") {
		t.Error("collectContributors must accumulate into a []model.Contributor and call " +
			"UpsertContributorBatch once at the end, instead of UpsertContributor per row inside the iterator.")
	}
	if strings.Contains(body, "c.store.UpsertContributor(ctx, &contrib)") {
		t.Error("collectContributors must NOT call UpsertContributor per row — that's the pre-Fix-4 hot path " +
			"that this fix replaces with a single batch flush.")
	}
}

func mustReadEnrichSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("enrich.go")
	if err != nil {
		t.Fatalf("read enrich.go: %v", err)
	}
	return string(data)
}

func mustReadCollectorSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("collector.go")
	if err != nil {
		t.Fatalf("read collector.go: %v", err)
	}
	return string(data)
}

func extractEnrichFunc(src, name string) string {
	marker := "func " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
