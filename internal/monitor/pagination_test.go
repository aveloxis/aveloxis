package monitor

// Tests for parsePageParams — the pure request-parsing helper the monitor
// dashboard uses to translate ?page=&page_size=&q= query params into a
// bounded, validated struct. These are unit-testable without touching the
// database, so edge cases around bad/missing/oversized input live here.

import (
	"net/http/httptest"
	"testing"
)

func TestParsePageParamsDefaults(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	p := parsePageParams(req)

	if p.Page != 1 {
		t.Errorf("default Page = %d, want 1", p.Page)
	}
	if p.PageSize != defaultDashboardPageSize {
		t.Errorf("default PageSize = %d, want %d", p.PageSize, defaultDashboardPageSize)
	}
	if p.Search != "" {
		t.Errorf("default Search = %q, want empty", p.Search)
	}
	if p.Offset != 0 {
		t.Errorf("default Offset = %d, want 0", p.Offset)
	}
}

func TestParsePageParamsExplicit(t *testing.T) {
	req := httptest.NewRequest("GET", "/?page=3&page_size=50&q=foo", nil)
	p := parsePageParams(req)

	if p.Page != 3 {
		t.Errorf("Page = %d, want 3", p.Page)
	}
	if p.PageSize != 50 {
		t.Errorf("PageSize = %d, want 50", p.PageSize)
	}
	if p.Search != "foo" {
		t.Errorf("Search = %q, want foo", p.Search)
	}
	if p.Offset != 100 { // (3-1) * 50
		t.Errorf("Offset = %d, want 100", p.Offset)
	}
}

func TestParsePageParamsInvalidFallsBackToDefault(t *testing.T) {
	cases := []struct {
		name, query           string
		wantPage, wantPageSize int
	}{
		{"non-numeric page", "?page=abc", 1, defaultDashboardPageSize},
		{"zero page", "?page=0", 1, defaultDashboardPageSize},
		{"negative page", "?page=-5", 1, defaultDashboardPageSize},
		{"non-numeric size", "?page_size=xyz", 1, defaultDashboardPageSize},
		{"zero size", "?page_size=0", 1, defaultDashboardPageSize},
		{"negative size", "?page_size=-10", 1, defaultDashboardPageSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/"+tc.query, nil)
			p := parsePageParams(req)
			if p.Page != tc.wantPage {
				t.Errorf("Page = %d, want %d", p.Page, tc.wantPage)
			}
			if p.PageSize != tc.wantPageSize {
				t.Errorf("PageSize = %d, want %d", p.PageSize, tc.wantPageSize)
			}
		})
	}
}

// TestParsePageParamsOversizeClamped protects the DB: an operator asking
// for ?page_size=1000000 would otherwise stream a million rows through
// the monitor every 10 seconds.
func TestParsePageParamsOversizeClamped(t *testing.T) {
	req := httptest.NewRequest("GET", "/?page_size=10000", nil)
	p := parsePageParams(req)
	if p.PageSize != maxDashboardPageSize {
		t.Errorf("oversize PageSize = %d, want clamped to %d", p.PageSize, maxDashboardPageSize)
	}
}

func TestParsePageParamsSearchTrimmed(t *testing.T) {
	req := httptest.NewRequest("GET", "/?q=+++foo+bar+++", nil)
	p := parsePageParams(req)
	if p.Search != "foo bar" {
		t.Errorf("Search = %q, want %q (inner spaces preserved, outer trimmed)", p.Search, "foo bar")
	}
}

func TestParsePageParamsOffsetZeroBased(t *testing.T) {
	// Page 1 is the first page; offset must be 0.
	req := httptest.NewRequest("GET", "/?page=1&page_size=25", nil)
	p := parsePageParams(req)
	if p.Offset != 0 {
		t.Errorf("page=1 offset = %d, want 0 (DB OFFSET is zero-based)", p.Offset)
	}
}

// TestTotalPagesCeiling covers pagination-control rendering math.
func TestTotalPagesCeiling(t *testing.T) {
	cases := []struct {
		total, pageSize, want int
	}{
		{0, 100, 1},   // empty fleet still renders one page
		{1, 100, 1},
		{100, 100, 1},
		{101, 100, 2},
		{250, 100, 3},
		{999, 100, 10},
		{1000, 100, 10},
		{1001, 100, 11},
	}
	for _, tc := range cases {
		got := totalPages(tc.total, tc.pageSize)
		if got != tc.want {
			t.Errorf("totalPages(%d, %d) = %d, want %d",
				tc.total, tc.pageSize, got, tc.want)
		}
	}
}

// TestTotalPagesZeroPageSize guards against divide-by-zero if a caller
// somehow passes 0 (shouldn't happen given parsePageParams clamps, but
// defense in depth — a crash here would kill the whole monitor).
func TestTotalPagesZeroPageSize(t *testing.T) {
	if got := totalPages(500, 0); got != 1 {
		t.Errorf("totalPages(500, 0) = %d, want 1 (safe fallback)", got)
	}
}
